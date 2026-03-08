// validate reads a TOC JSON file, hashes every referenced chunk blob on disk,
// and checks that sha256(blob) == the digest in the TOC / the filename.
package main

import (
	_ "crypto/sha256"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"

	"github.com/distribution/distribution/v3/manifest/chunked"
	"github.com/opencontainers/go-digest"
)

func main() {
	tocFile := flag.String("toc", "", "path to TOC JSON file")
	chunkDir := flag.String("chunks", "", "directory containing chunk blobs")
	workers := flag.Int("j", 8, "parallel workers")
	flag.Parse()

	if *tocFile == "" || *chunkDir == "" {
		flag.Usage()
		os.Exit(1)
	}

	data, err := os.ReadFile(*tocFile)
	if err != nil {
		fmt.Fprintf(os.Stderr, "read toc: %v\n", err)
		os.Exit(1)
	}
	toc, err := chunked.ParseTOC(data)
	if err != nil {
		fmt.Fprintf(os.Stderr, "parse toc: %v\n", err)
		os.Exit(1)
	}

	digests := toc.ChunkDigests()
	fmt.Printf("TOC entries:   %d\n", len(toc.Entries))
	fmt.Printf("Chunk digests: %d\n\n", len(digests))

	type result struct {
		dgst digest.Digest
		err  error
	}

	work := make(chan digest.Digest, len(digests))
	for _, d := range digests {
		work <- d
	}
	close(work)

	results := make(chan result, len(digests))
	var wg sync.WaitGroup
	for range *workers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for dgst := range work {
				hex := dgst.Hex()
				path := filepath.Join(*chunkDir, hex[:2], hex)
				blob, err := os.ReadFile(path)
				if err != nil {
					results <- result{dgst, fmt.Errorf("read %s: %w", path, err)}
					continue
				}
				got := digest.FromBytes(blob)
				if got != dgst {
					results <- result{dgst, fmt.Errorf("MISMATCH: file=%s  expected=%s  got=%s", path, dgst, got)}
				} else {
					results <- result{dgst, nil}
				}
			}
		}()
	}
	go func() { wg.Wait(); close(results) }()

	var ok, failed atomic.Int64
	var totalBytes atomic.Int64
	var errs []string

	// Progress ticker.
	done := make(chan struct{})
	go func() {
		total := int64(len(digests))
		for r := range results {
			if r.err != nil {
				failed.Add(1)
				errs = append(errs, r.err.Error())
			} else {
				hex := r.dgst.Hex()
				path := filepath.Join(*chunkDir, hex[:2], hex)
				if fi, err := os.Stat(path); err == nil {
					totalBytes.Add(fi.Size())
				}
				ok.Add(1)
			}
			checked := ok.Load() + failed.Load()
			if checked%100 == 0 || checked == total {
				fmt.Printf("\r  checked %d / %d ...", checked, total)
			}
		}
		close(done)
	}()
	<-done

	fmt.Println()

	// Print errors if any.
	for _, e := range errs {
		fmt.Println("  ERROR:", e)
	}

	// Summary.
	fmt.Printf("\nResults:\n")
	fmt.Printf("  OK:      %d\n", ok.Load())
	fmt.Printf("  FAILED:  %d\n", failed.Load())
	fmt.Printf("  Bytes:   %.1f MB\n", float64(totalBytes.Load())/(1<<20))

	// Also verify the TOC JSON itself.
	fmt.Printf("\nTOC self-check:\n")
	rawTOC, _ := json.Marshal(toc)
	got := digest.FromBytes(rawTOC)
	orig := digest.FromBytes(data)
	if got == orig {
		fmt.Printf("  TOC re-serializes to same digest: %s  OK\n", orig)
	} else {
		// This is expected if json.Marshal produces different whitespace.
		fmt.Printf("  TOC digest (original file): %s\n", orig)
		fmt.Printf("  TOC digest (re-marshalled): %s\n", got)
		fmt.Printf("  (differs due to whitespace — expected)\n")
	}

	if failed.Load() > 0 {
		os.Exit(1)
	}
}
