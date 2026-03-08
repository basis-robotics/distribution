// localconvert reads an OCI image tar, converts each layer with ConvertLayerToChunked,
// writes chunk blobs to a local directory, and prints the TOC JSON for each layer.
package main

import (
	"archive/tar"
	"context"
	_ "crypto/sha256" // register sha256 for go-digest
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"strings"

	"github.com/distribution/distribution/v3/manifest/chunked"
	"github.com/opencontainers/go-digest"
	v1 "github.com/opencontainers/image-spec/specs-go/v1"
)

func main() {
	src := flag.String("src", "", "path to image tar (docker save output)")
	outDir := flag.String("out", "/tmp/chunks", "directory to write chunk blobs")
	flag.Parse()

	if *src == "" {
		flag.Usage()
		os.Exit(1)
	}

	if err := os.MkdirAll(*outDir, 0o755); err != nil {
		log.Fatal(err)
	}

	f, err := os.Open(*src)
	if err != nil {
		log.Fatal(err)
	}
	defer f.Close()

	files, err := readTar(f)
	if err != nil {
		log.Fatalf("reading tar: %v", err)
	}

	mfstJSON, ok := files["manifest.json"]
	if !ok {
		log.Fatal("manifest.json not found")
	}
	var manifests []struct {
		Config       string            `json:"Config"`
		Layers       []string          `json:"Layers"`
		LayerSources map[string]struct {
			MediaType string `json:"mediaType"`
		} `json:"LayerSources"`
	}
	if err := json.Unmarshal(mfstJSON, &manifests); err != nil {
		log.Fatalf("parsing manifest.json: %v", err)
	}
	if len(manifests) == 0 {
		log.Fatal("no manifests found")
	}
	dm := manifests[0]

	fmt.Printf("Image has %d layer(s)\n\n", len(dm.Layers))

	ctx := context.Background()
	totalChunks := 0
	totalBytes := 0

	for i, layerPath := range dm.Layers {
		layerData, ok := files[layerPath]
		if !ok {
			log.Fatalf("layer %q not found in tar", layerPath)
		}

		// Prefer media type from LayerSources if present.
		mediaType := layerMediaType(layerPath)
		dgstKey := filepath.Base(layerPath)
		if src, ok := dm.LayerSources["sha256:"+dgstKey]; ok && src.MediaType != "" {
			mediaType = src.MediaType
		}
		fmt.Printf("=== Layer %d: %s (%.1f MB, %s) ===\n",
			i+1, layerPath, float64(len(layerData))/1e6, mediaType)

		chunkCount := 0
		chunkBytes := 0

		pusher := func(ctx context.Context, data []byte) (digest.Digest, error) {
			dgst := digest.FromBytes(data)
			path := filepath.Join(*outDir, dgst.Hex()[:2], dgst.Hex())
			if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
				return "", err
			}
			if err := os.WriteFile(path, data, 0o644); err != nil {
				return "", err
			}
			chunkCount++
			chunkBytes += len(data)
			return dgst, nil
		}

		toc, err := chunked.ConvertLayerToChunked(ctx, layerData, mediaType, pusher)
		if err != nil {
			log.Fatalf("converting layer %d: %v", i+1, err)
		}

		tocJSON, err := json.MarshalIndent(toc, "", "  ")
		if err != nil {
			log.Fatal(err)
		}

		tocDigest := digest.FromBytes(tocJSON)
		tocPath := filepath.Join(*outDir, "toc-layer-"+fmt.Sprintf("%02d", i+1)+".json")
		if err := os.WriteFile(tocPath, tocJSON, 0o644); err != nil {
			log.Fatal(err)
		}

		fmt.Printf("TOC digest: %s\n", tocDigest)
		fmt.Printf("Entries:    %d\n", len(toc.Entries))
		fmt.Printf("Chunks:     %d blobs (%s)\n", chunkCount, humanBytes(chunkBytes))
		fmt.Printf("TOC file:   %s\n", tocPath)

		// Print entry type summary.
		counts := make(map[string]int)
		for _, e := range toc.Entries {
			counts[e.Type]++
		}
		fmt.Printf("Entry types: ")
		for _, t := range []string{"reg", "dir", "symlink", "hardlink", "char", "block", "fifo", "whiteout", "opaque"} {
			if n := counts[t]; n > 0 {
				fmt.Printf("%s=%d ", t, n)
			}
		}
		fmt.Println()

		// Print first 5 entries as a sample.
		fmt.Println("Sample entries:")
		for j, e := range toc.Entries {
			if j >= 5 {
				fmt.Printf("  ... (%d more)\n", len(toc.Entries)-5)
				break
			}
			line := fmt.Sprintf("  [%s] %s", e.Type, e.Name)
			if e.Size > 0 {
				line += fmt.Sprintf(" (%s)", humanBytes(int(e.Size)))
			}
			if e.Digest != "" {
				line += fmt.Sprintf(" chunk=%s", e.Digest.Hex()[:12]+"...")
			}
			fmt.Println(line)
		}
		fmt.Println()

		totalChunks += chunkCount
		totalBytes += chunkBytes
	}

	fmt.Printf("=== Summary ===\n")
	fmt.Printf("Total chunk blobs: %d (%s)\n", totalChunks, humanBytes(totalBytes))
	fmt.Printf("Chunk dir:         %s\n", *outDir)
}

func readTar(r io.Reader) (map[string][]byte, error) {
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

func layerMediaType(path string) string {
	if strings.HasSuffix(path, ".zst") || strings.HasSuffix(path, ".zstd") {
		return v1.MediaTypeImageLayerZstd
	}
	return v1.MediaTypeImageLayerGzip
}

func humanBytes(n int) string {
	switch {
	case n >= 1<<20:
		return fmt.Sprintf("%.1f MB", float64(n)/(1<<20))
	case n >= 1<<10:
		return fmt.Sprintf("%.1f KB", float64(n)/(1<<10))
	default:
		return fmt.Sprintf("%d B", n)
	}
}
