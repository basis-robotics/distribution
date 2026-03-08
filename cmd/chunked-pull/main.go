// Command chunked-pull fetches a chunked OCI image from a registry and
// reconstructs it as a standard OCI image tar (docker save format).
//
// Usage:
//
//	chunked-pull --src <[http://]registry/repo:tag> --out <path.tar> [--user user:pass]
package main

import (
	"archive/tar"
	"bytes"
	"compress/gzip"
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
	"strings"
	"sync"
	"time"

	"github.com/distribution/distribution/v3/manifest/chunked"
	"github.com/opencontainers/go-digest"
	v1 "github.com/opencontainers/image-spec/specs-go/v1"
)

func main() {
	src := flag.String("src", "", "source [http://|https://]registry/repo:tag")
	out := flag.String("out", "", "output path for OCI image tar")
	user := flag.String("user", "", "registry credentials in user:pass format")
	jobs := flag.Int("jobs", 16, "parallel chunk fetch workers")
	flag.Parse()

	if *src == "" || *out == "" {
		flag.Usage()
		os.Exit(1)
	}

	if err := run(context.Background(), *src, *out, *user, *jobs); err != nil {
		log.Fatalf("chunked-pull: %v", err)
	}
}

func run(ctx context.Context, srcRef, outPath, userPass string, jobs int) error {
	scheme, registry, repo, tag, err := parseRef(srcRef)
	if err != nil {
		return fmt.Errorf("invalid source ref %q: %w", srcRef, err)
	}

	client := &registryClient{
		base:     scheme + "://" + registry,
		repo:     repo,
		userPass: userPass,
	}

	// Fetch manifest.
	log.Printf("fetching manifest %s/%s:%s ...", registry, repo, tag)
	mediaType, manifestData, err := client.fetchManifest(ctx, tag)
	if err != nil {
		return fmt.Errorf("fetching manifest: %w", err)
	}
	if mediaType != v1.MediaTypeImageManifest {
		return fmt.Errorf("expected OCI image manifest, got %s", mediaType)
	}
	var manifest v1.Manifest
	if err := json.Unmarshal(manifestData, &manifest); err != nil {
		return fmt.Errorf("parsing manifest: %w", err)
	}

	// Fetch config.
	log.Printf("fetching config %s ...", manifest.Config.Digest.Hex()[:12])
	configData, err := client.fetchBlob(ctx, manifest.Config.Digest)
	if err != nil {
		return fmt.Errorf("fetching config: %w", err)
	}

	// Reconstruct each layer.
	var layers [][]byte
	var newDiffIDs []digest.Digest

	for i, layerDesc := range manifest.Layers {
		if layerDesc.MediaType != chunked.MediaTypeLayerTOC {
			return fmt.Errorf("layer %d has unsupported media type %s", i+1, layerDesc.MediaType)
		}

		log.Printf("layer %d: fetching TOC %s ...", i+1, layerDesc.Digest.Hex()[:12])
		tocData, err := client.fetchBlob(ctx, layerDesc.Digest)
		if err != nil {
			return fmt.Errorf("layer %d: fetching TOC: %w", i+1, err)
		}
		toc, err := chunked.ParseTOC(tocData)
		if err != nil {
			return fmt.Errorf("layer %d: parsing TOC: %w", i+1, err)
		}

		// Deduplicate chunk digests before fetching.
		allDigests := toc.ChunkDigests()
		unique := make(map[digest.Digest]struct{}, len(allDigests))
		for _, d := range allDigests {
			unique[d] = struct{}{}
		}
		log.Printf("layer %d: %d entries, %d unique chunks to fetch", i+1, len(toc.Entries), len(unique))

		// Fetch all chunks in parallel.
		tFetch := time.Now()
		chunkKeys := make([]digest.Digest, 0, len(unique))
		for d := range unique {
			chunkKeys = append(chunkKeys, d)
		}
		chunks, err := fetchChunks(ctx, client, chunkKeys, jobs)
		if err != nil {
			return fmt.Errorf("layer %d: fetching chunks: %w", i+1, err)
		}
		log.Printf("layer %d: fetched %d chunks in %v", i+1, len(chunks), time.Since(tFetch).Round(time.Millisecond))

		// Reconstruct the uncompressed tar layer.
		tReconstruct := time.Now()
		layerTar, err := reconstructLayer(toc, chunks)
		if err != nil {
			return fmt.Errorf("layer %d: reconstructing tar: %w", i+1, err)
		}
		diffID := digest.FromBytes(layerTar)
		log.Printf("layer %d: reconstructed %.1f MB, diffID=%s in %v",
			i+1, float64(len(layerTar))/(1<<20), diffID.Hex()[:12], time.Since(tReconstruct).Round(time.Millisecond))

		layers = append(layers, layerTar)
		newDiffIDs = append(newDiffIDs, diffID)
	}

	// Update config rootfs.diff_ids to reflect reconstructed layers.
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
	newRootFS, _ := json.Marshal(rootFS)
	cfg["rootfs"] = newRootFS
	newConfigData, err := json.Marshal(cfg)
	if err != nil {
		return fmt.Errorf("marshalling config: %w", err)
	}
	configDigest := digest.FromBytes(newConfigData)

	// Write docker-save-format output tar.
	log.Printf("writing %s ...", outPath)
	f, err := os.Create(outPath)
	if err != nil {
		return err
	}
	defer f.Close()

	repoTag := registry + "/" + repo + ":" + tag
	if err := writeDockerSaveTar(f, newConfigData, configDigest, layers, repoTag); err != nil {
		return fmt.Errorf("writing output tar: %w", err)
	}
	log.Printf("done")
	return nil
}

// fetchChunks fetches a list of blob digests from the registry in parallel.
func fetchChunks(ctx context.Context, client *registryClient, digests []digest.Digest, jobs int) (map[digest.Digest][]byte, error) {
	work := make(chan digest.Digest, len(digests))
	for _, d := range digests {
		work <- d
	}
	close(work)

	type result struct {
		dgst digest.Digest
		data []byte
		err  error
	}
	results := make(chan result, len(digests))

	var wg sync.WaitGroup
	for range jobs {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for dgst := range work {
				data, err := client.fetchBlob(ctx, dgst)
				results <- result{dgst, data, err}
			}
		}()
	}
	go func() { wg.Wait(); close(results) }()

	chunks := make(map[digest.Digest][]byte, len(digests))
	for r := range results {
		if r.err != nil {
			return nil, r.err
		}
		chunks[r.dgst] = r.data
	}
	return chunks, nil
}

// reconstructLayer rebuilds an uncompressed tar stream from a TOC and its chunk data.
func reconstructLayer(toc *chunked.TOC, chunks map[digest.Digest][]byte) ([]byte, error) {
	var buf bytes.Buffer
	tw := tar.NewWriter(&buf)

	for _, entry := range toc.Entries {
		hdr, err := tocEntryToHeader(entry)
		if err != nil {
			return nil, err
		}
		if err := tw.WriteHeader(hdr); err != nil {
			return nil, fmt.Errorf("write header %q: %w", entry.Name, err)
		}

		switch {
		case entry.Type == "reg" && entry.Digest != "":
			// Single-chunk file.
			data, ok := chunks[entry.Digest]
			if !ok {
				return nil, fmt.Errorf("chunk %s for %q not found", entry.Digest, entry.Name)
			}
			if _, err := tw.Write(data); err != nil {
				return nil, fmt.Errorf("write data %q: %w", entry.Name, err)
			}
		case entry.Type == "reg" && len(entry.Chunks) > 0:
			// Multi-chunk file: concatenate in order.
			for _, c := range entry.Chunks {
				data, ok := chunks[c.Digest]
				if !ok {
					return nil, fmt.Errorf("chunk %s for %q not found", c.Digest, entry.Name)
				}
				if _, err := tw.Write(data); err != nil {
					return nil, fmt.Errorf("write chunk data %q: %w", entry.Name, err)
				}
			}
		}
	}

	if err := tw.Close(); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

// tocEntryToHeader converts a TOCEntry back into a tar.Header.
func tocEntryToHeader(e chunked.TOCEntry) (*tar.Header, error) {
	hdr := &tar.Header{
		Name:     e.Name,
		Mode:     int64(e.Mode),
		Uid:      e.UID,
		Gid:      e.GID,
		Uname:    e.Uname,
		Gname:    e.Gname,
		ModTime:  e.ModTime,
		Linkname: e.LinkName,
		Devmajor: e.DevMajor,
		Devminor: e.DevMinor,
		Size:     e.Size,
	}

	for k, v := range e.Xattrs {
		if hdr.PAXRecords == nil {
			hdr.PAXRecords = make(map[string]string)
		}
		hdr.PAXRecords[k] = string(v)
	}

	switch e.Type {
	case "reg":
		hdr.Typeflag = tar.TypeReg
	case "dir":
		hdr.Typeflag = tar.TypeDir
		hdr.Size = 0
	case "symlink":
		hdr.Typeflag = tar.TypeSymlink
		hdr.Size = 0
	case "hardlink":
		hdr.Typeflag = tar.TypeLink
		hdr.Size = 0
	case "char":
		hdr.Typeflag = tar.TypeChar
		hdr.Size = 0
	case "block":
		hdr.Typeflag = tar.TypeBlock
		hdr.Size = 0
	case "fifo":
		hdr.Typeflag = tar.TypeFifo
		hdr.Size = 0
	case "whiteout":
		dir, name := "", e.Name
		if idx := strings.LastIndex(name, "/"); idx >= 0 {
			dir, name = name[:idx+1], name[idx+1:]
		}
		hdr.Name = dir + ".wh." + name
		hdr.Typeflag = tar.TypeReg
		hdr.Size = 0
	case "opaque":
		hdr.Name = strings.TrimSuffix(e.Name, "/") + "/.wh..wh..opq"
		hdr.Typeflag = tar.TypeReg
		hdr.Size = 0
	default:
		return nil, fmt.Errorf("unknown TOC entry type %q for %q", e.Type, e.Name)
	}
	return hdr, nil
}

// writeDockerSaveTar writes a docker-save-compatible tar archive.
func writeDockerSaveTar(w io.Writer, configData []byte, configDigest digest.Digest, layers [][]byte, repoTag string) error {
	tw := tar.NewWriter(w)

	configName := configDigest.Hex() + ".json"
	if err := addTarEntry(tw, configName, configData); err != nil {
		return err
	}

	var layerPaths []string
	for i, layerData := range layers {
		var gz bytes.Buffer
		gw := gzip.NewWriter(&gz)
		if _, err := gw.Write(layerData); err != nil {
			return err
		}
		gw.Close()

		dgst := digest.FromBytes(gz.Bytes())
		layerPath := dgst.Hex() + "/layer.tar"
		if err := addTarEntry(tw, layerPath, gz.Bytes()); err != nil {
			return fmt.Errorf("writing layer %d: %w", i+1, err)
		}
		layerPaths = append(layerPaths, layerPath)
	}

	type manifestEntry struct {
		Config   string   `json:"Config"`
		RepoTags []string `json:"RepoTags"`
		Layers   []string `json:"Layers"`
	}
	manifestJSON, err := json.Marshal([]manifestEntry{{
		Config:   configName,
		RepoTags: []string{repoTag},
		Layers:   layerPaths,
	}})
	if err != nil {
		return err
	}
	if err := addTarEntry(tw, "manifest.json", manifestJSON); err != nil {
		return err
	}

	return tw.Close()
}

func addTarEntry(tw *tar.Writer, name string, data []byte) error {
	if err := tw.WriteHeader(&tar.Header{
		Name: name,
		Size: int64(len(data)),
		Mode: 0o644,
	}); err != nil {
		return err
	}
	_, err := tw.Write(data)
	return err
}

// ---- registry client ----

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
	url := fmt.Sprintf("%s?service=%s&scope=%s", params["realm"], params["service"], params["scope"])
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
			m[kv[:idx]] = strings.Trim(kv[idx+1:], `"`)
		}
	}
	return m
}

func (c *registryClient) fetchManifest(ctx context.Context, ref string) (mediaType string, data []byte, err error) {
	url := fmt.Sprintf("%s/v2/%s/manifests/%s", c.base, c.repo, ref)
	req, _ := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	req.Header.Set("Accept", v1.MediaTypeImageManifest)
	resp, err := c.do(req)
	if err != nil {
		return "", nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return "", nil, fmt.Errorf("GET manifest returned %d", resp.StatusCode)
	}
	data, err = io.ReadAll(resp.Body)
	return resp.Header.Get("Content-Type"), data, err
}

func (c *registryClient) fetchBlob(ctx context.Context, dgst digest.Digest) ([]byte, error) {
	url := fmt.Sprintf("%s/v2/%s/blobs/%s", c.base, c.repo, dgst)
	req, _ := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	resp, err := c.do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("GET blob %s returned %d", dgst, resp.StatusCode)
	}
	return io.ReadAll(resp.Body)
}

// ---- helpers ----

func parseRef(ref string) (scheme, registry, repo, tag string, err error) {
	scheme = "https"
	if i := strings.Index(ref, "://"); i >= 0 {
		scheme = ref[:i]
		ref = ref[i+3:]
	}
	tag = "latest"
	if idx := strings.LastIndex(ref, ":"); idx > 0 {
		if !strings.Contains(ref[idx:], "/") {
			tag = ref[idx+1:]
			ref = ref[:idx]
		}
	}
	parts := strings.SplitN(ref, "/", 2)
	if len(parts) != 2 {
		return "", "", "", "", fmt.Errorf("expected registry/repo, got %q", ref)
	}
	return scheme, parts[0], parts[1], tag, nil
}
