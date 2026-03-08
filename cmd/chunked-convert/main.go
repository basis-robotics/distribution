// Command chunked-convert reads an OCI image tar (docker save output) and pushes
// a chunked version of the image to a target registry. Each layer is converted
// from a traditional gzip/zstd+tar blob into a TOC blob + per-file chunk blobs.
//
// Usage:
//
//	chunked-convert --src <path-to-image.tar> --dst <registry>/<repo>:<tag> [--user user:pass]
package main

import (
	"archive/tar"
	"bytes"
	"context"
	_ "crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"path"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/distribution/distribution/v3/manifest/chunked"
	"github.com/distribution/distribution/v3/manifest/ocischema"
	"github.com/opencontainers/go-digest"
	"github.com/opencontainers/image-spec/specs-go"
	v1 "github.com/opencontainers/image-spec/specs-go/v1"
)

func main() {
	src := flag.String("src", "", "path to OCI image tar (docker save output)")
	dst := flag.String("dst", "", "destination [http://|https://]registry/repo:tag")
	user := flag.String("user", "", "registry credentials in user:pass format")
	jobs := flag.Int("jobs", 16, "parallel blob upload workers")
	flag.Parse()

	if *src == "" || *dst == "" {
		flag.Usage()
		os.Exit(1)
	}

	if err := run(context.Background(), *src, *dst, *user, *jobs); err != nil {
		log.Fatalf("chunked-convert: %v", err)
	}
}

// run converts and pushes the image.
func run(ctx context.Context, srcPath, dstRef, userPass string, jobs int) error {
	// Parse destination reference.
	scheme, registry, repo, tag, err := parseRef(dstRef)
	if err != nil {
		return fmt.Errorf("invalid destination ref %q: %w", dstRef, err)
	}

	client := &registryClient{
		base:     scheme + "://" + registry,
		repo:     repo,
		userPass: userPass,
	}

	// Read source tar.
	t0 := time.Now()
	f, err := os.Open(srcPath)
	if err != nil {
		return err
	}
	defer f.Close()

	files, err := readImageTar(f)
	if err != nil {
		return fmt.Errorf("reading image tar: %w", err)
	}
	log.Printf("read tar: %v", time.Since(t0).Round(time.Millisecond))

	// Parse manifest.json.
	mfstJSON, ok := files["manifest.json"]
	if !ok {
		return fmt.Errorf("manifest.json not found in image tar")
	}
	var dockerManifests []dockerManifestEntry
	if err := json.Unmarshal(mfstJSON, &dockerManifests); err != nil {
		return fmt.Errorf("parsing manifest.json: %w", err)
	}
	if len(dockerManifests) == 0 {
		return fmt.Errorf("no manifests in manifest.json")
	}
	dm := dockerManifests[0]

	// Read config.
	configData, ok := files[dm.Config]
	if !ok {
		return fmt.Errorf("config file %q not found in tar", dm.Config)
	}

	// Convert layers.
	var newLayers []v1.Descriptor
	var newDiffIDs []digest.Digest

	for _, layerPath := range dm.Layers {
		layerData, ok := files[layerPath]
		if !ok {
			return fmt.Errorf("layer %q not found in tar", layerPath)
		}

		// Determine media type: prefer LayerSources, fall back to path heuristic.
		mediaType := layerMediaType(layerPath)
		if src, ok := dm.LayerSources["sha256:"+path.Base(layerPath)]; ok && src.MediaType != "" {
			mediaType = src.MediaType
		}

		// Phase 1: convert — collect unique (digest→data) pairs without doing any I/O.
		pending := make(map[digest.Digest][]byte)
		pusher := func(_ context.Context, data []byte) (digest.Digest, error) {
			dgst := digest.FromBytes(data)
			if _, exists := pending[dgst]; !exists {
				pending[dgst] = data
			}
			return dgst, nil
		}

		log.Printf("converting layer %s ...", layerPath)
		tConvert := time.Now()
		toc, err := chunked.ConvertLayerToChunked(ctx, layerData, mediaType, pusher)
		if err != nil {
			return fmt.Errorf("converting layer %s: %w", layerPath, err)
		}
		log.Printf("convert done in %v — %d unique chunks queued for upload", time.Since(tConvert).Round(time.Millisecond), len(pending))

		// Phase 2: push all unique chunk blobs in parallel.
		type pendingBlob struct {
			dgst digest.Digest
			data []byte
		}
		tPush := time.Now()
		work := make(chan pendingBlob, len(pending))
		for dgst, data := range pending {
			work <- pendingBlob{dgst, data}
		}
		close(work)

		var pushed, skippedCount atomic.Int64
		var firstErr atomic.Pointer[error]
		var wg sync.WaitGroup
		for range jobs {
			wg.Add(1)
			go func() {
				defer wg.Done()
				for p := range work {
					if firstErr.Load() != nil {
						return
					}
					skip, err := client.pushBlob(ctx, p.dgst, p.data)
					if err != nil {
						firstErr.CompareAndSwap(nil, &err)
						return
					}
					if skip {
						skippedCount.Add(1)
					} else {
						pushed.Add(1)
					}
				}
			}()
		}
		wg.Wait()

		if ep := firstErr.Load(); ep != nil {
			return fmt.Errorf("uploading blobs for layer %s: %w", layerPath, *ep)
		}
		log.Printf("push done in %v — uploaded %d blobs, skipped %d (already present)",
			time.Since(tPush).Round(time.Millisecond), pushed.Load(), skippedCount.Load())

		// Push TOC blob.
		tocJSON, err := json.Marshal(toc)
		if err != nil {
			return err
		}
		tocDgst := digest.FromBytes(tocJSON)
		if _, err := client.pushBlob(ctx, tocDgst, tocJSON); err != nil {
			return fmt.Errorf("pushing TOC for %s: %w", layerPath, err)
		}

		origDgst := digest.FromBytes(layerData)
		newLayers = append(newLayers, v1.Descriptor{
			MediaType: chunked.MediaTypeLayerTOC,
			Digest:    tocDgst,
			Size:      int64(len(tocJSON)),
			Annotations: map[string]string{
				chunked.OriginalDigestAnnotation: origDgst.String(),
			},
		})
		newDiffIDs = append(newDiffIDs, tocDgst)
	}

	// Update config with new diff_ids.
	var cfg map[string]json.RawMessage
	if err := json.Unmarshal(configData, &cfg); err != nil {
		return fmt.Errorf("parsing config: %w", err)
	}
	var rootFS struct {
		Type    string          `json:"type"`
		DiffIDs []digest.Digest `json:"diff_ids"`
	}
	if raw, ok := cfg["rootfs"]; ok {
		_ = json.Unmarshal(raw, &rootFS)
	}
	rootFS.DiffIDs = newDiffIDs
	newRootFS, err := json.Marshal(rootFS)
	if err != nil {
		return err
	}
	cfg["rootfs"] = newRootFS
	newConfigData, err := json.Marshal(cfg)
	if err != nil {
		return err
	}

	configDgst := digest.FromBytes(newConfigData)
	if _, err := client.pushBlob(ctx, configDgst, newConfigData); err != nil {
		return fmt.Errorf("pushing config: %w", err)
	}

	// Build and push OCI manifest.
	ociManifest := ocischema.Manifest{
		Versioned: specs.Versioned{SchemaVersion: 2},
		MediaType: v1.MediaTypeImageManifest,
		Config: v1.Descriptor{
			MediaType: v1.MediaTypeImageConfig,
			Digest:    configDgst,
			Size:      int64(len(newConfigData)),
		},
		Layers: newLayers,
	}
	dm2, err := ocischema.FromStruct(ociManifest)
	if err != nil {
		return err
	}
	_, manifestPayload, err := dm2.Payload()
	if err != nil {
		return err
	}

	log.Printf("pushing manifest to %s/%s:%s ...", registry, repo, tag)
	return client.pushManifest(ctx, tag, v1.MediaTypeImageManifest, manifestPayload)
}

// ---- helpers ----

type dockerManifestEntry struct {
	Config       string   `json:"Config"`
	RepoTags     []string `json:"RepoTags"`
	Layers       []string `json:"Layers"`
	LayerSources map[string]struct {
		MediaType string `json:"mediaType"`
	} `json:"LayerSources"`
}

// readImageTar reads all files from a tar archive into a map[path]content.
func readImageTar(r io.Reader) (map[string][]byte, error) {
	files := make(map[string][]byte)
	tr := tar.NewReader(r)
	for {
		hdr, err := tr.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, err
		}
		data, err := io.ReadAll(tr)
		if err != nil {
			return nil, err
		}
		files[hdr.Name] = data
	}
	return files, nil
}

// layerMediaType infers the layer media type from the file name.
func layerMediaType(path string) string {
	if strings.HasSuffix(path, ".zst") || strings.HasSuffix(path, ".zstd") {
		return v1.MediaTypeImageLayerZstd
	}
	return v1.MediaTypeImageLayerGzip
}

// parseRef parses "[scheme://]registry/repo:tag" into components.
// If no scheme is specified, "https" is used by default.
func parseRef(ref string) (scheme, registry, repo, tag string, err error) {
	scheme = "https"
	if i := strings.Index(ref, "://"); i >= 0 {
		scheme = ref[:i]
		ref = ref[i+3:]
	}

	// Split tag.
	tag = "latest"
	if idx := strings.LastIndex(ref, ":"); idx > 0 {
		// Make sure the colon is not part of the host:port.
		if !strings.Contains(ref[idx:], "/") {
			tag = ref[idx+1:]
			ref = ref[:idx]
		}
	}

	// Split registry from repo.
	parts := strings.SplitN(ref, "/", 2)
	if len(parts) != 2 {
		return "", "", "", "", fmt.Errorf("expected registry/repo, got %q", ref)
	}
	return scheme, parts[0], parts[1], tag, nil
}

// registryClient is a minimal OCI registry HTTP client.
type registryClient struct {
	base     string
	repo     string
	userPass string
	token    string
}

func (c *registryClient) authHeader() string {
	if c.token != "" {
		return "Bearer " + c.token
	}
	if c.userPass != "" {
		return "Basic " + base64.StdEncoding.EncodeToString([]byte(c.userPass))
	}
	return ""
}

func (c *registryClient) do(req *http.Request) (*http.Response, error) {
	if h := c.authHeader(); h != "" {
		req.Header.Set("Authorization", h)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, err
	}
	// Handle bearer token challenge.
	if resp.StatusCode == http.StatusUnauthorized && c.token == "" {
		_ = resp.Body.Close()
		if err := c.fetchToken(resp); err == nil {
			req2, _ := http.NewRequestWithContext(req.Context(), req.Method, req.URL.String(), req.Body)
			for k, v := range req.Header {
				req2.Header[k] = v
			}
			if h := c.authHeader(); h != "" {
				req2.Header.Set("Authorization", h)
			}
			return http.DefaultClient.Do(req2)
		}
	}
	return resp, nil
}

func (c *registryClient) fetchToken(resp *http.Response) error {
	www := resp.Header.Get("Www-Authenticate")
	if !strings.HasPrefix(www, "Bearer ") {
		return fmt.Errorf("unsupported auth challenge: %s", www)
	}
	params := parseBearer(www[len("Bearer "):])
	realm := params["realm"]
	service := params["service"]
	scope := params["scope"]
	url := fmt.Sprintf("%s?service=%s&scope=%s", realm, service, scope)
	req, _ := http.NewRequest(http.MethodGet, url, nil)
	if c.userPass != "" {
		req.Header.Set("Authorization", "Basic "+base64.StdEncoding.EncodeToString([]byte(c.userPass)))
	}
	r, err := http.DefaultClient.Do(req)
	if err != nil {
		return err
	}
	defer r.Body.Close()
	var tok struct {
		Token string `json:"token"`
	}
	if err := json.NewDecoder(r.Body).Decode(&tok); err != nil {
		return err
	}
	c.token = tok.Token
	return nil
}

func parseBearer(s string) map[string]string {
	m := make(map[string]string)
	for _, kv := range strings.Split(s, ",") {
		kv = strings.TrimSpace(kv)
		if idx := strings.Index(kv, "="); idx >= 0 {
			k := kv[:idx]
			v := strings.Trim(kv[idx+1:], `"`)
			m[k] = v
		}
	}
	return m
}

// pushBlob pushes a blob using the monolithic upload method, skipping if already present.
// Returns (true, nil) if the blob was already present (skipped), (false, nil) if uploaded.
func (c *registryClient) pushBlob(ctx context.Context, dgst digest.Digest, data []byte) (skipped bool, err error) {
	// HEAD to check existence.
	headURL := fmt.Sprintf("%s/v2/%s/blobs/%s", c.base, c.repo, dgst)
	req, _ := http.NewRequestWithContext(ctx, http.MethodHead, headURL, nil)
	resp, err := c.do(req)
	if err != nil {
		return false, err
	}
	_ = resp.Body.Close()
	if resp.StatusCode == http.StatusOK {
		return true, nil // already present
	}

	// POST to start upload.
	postURL := fmt.Sprintf("%s/v2/%s/blobs/uploads/", c.base, c.repo)
	req, _ = http.NewRequestWithContext(ctx, http.MethodPost, postURL, nil)
	req.Header.Set("Content-Length", "0")
	resp, err = c.do(req)
	if err != nil {
		return false, err
	}
	_ = resp.Body.Close()
	if resp.StatusCode != http.StatusAccepted {
		return false, fmt.Errorf("POST blobs/uploads/ returned %d", resp.StatusCode)
	}
	location := resp.Header.Get("Location")
	if location == "" {
		return false, fmt.Errorf("no Location header from POST blobs/uploads/")
	}

	// Append query param for digest and PUT.
	sep := "?"
	if strings.Contains(location, "?") {
		sep = "&"
	}
	putURL := location + sep + "digest=" + dgst.String()
	req, _ = http.NewRequestWithContext(ctx, http.MethodPut, putURL, bytes.NewReader(data))
	req.Header.Set("Content-Type", "application/octet-stream")
	req.ContentLength = int64(len(data))
	resp, err = c.do(req)
	if err != nil {
		return false, err
	}
	_ = resp.Body.Close()
	if resp.StatusCode != http.StatusCreated {
		return false, fmt.Errorf("PUT blob returned %d", resp.StatusCode)
	}
	return false, nil
}

// pushManifest pushes a manifest by tag.
func (c *registryClient) pushManifest(ctx context.Context, ref, mediaType string, data []byte) error {
	url := fmt.Sprintf("%s/v2/%s/manifests/%s", c.base, c.repo, ref)
	req, _ := http.NewRequestWithContext(ctx, http.MethodPut, url, bytes.NewReader(data))
	req.Header.Set("Content-Type", mediaType)
	req.ContentLength = int64(len(data))
	resp, err := c.do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusCreated {
		body, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("PUT manifest returned %d: %s", resp.StatusCode, body)
	}
	return nil
}
