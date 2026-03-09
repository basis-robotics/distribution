package storage

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"

	storagedriver "github.com/distribution/distribution/v3/registry/storage/driver"
	"github.com/opencontainers/go-digest"
)

// ManifestRef identifies a manifest that references a chunk.
type ManifestRef struct {
	Repository string        `json:"repository"`
	Digest     digest.Digest `json:"digest"`
}

// ChunkIndex is a storage-driver-backed index mapping chunk blob digests to
// all manifests known to reference them. It is safe for use across multiple
// registry workers.
//
// Marker path: /_clipper/chunkindex/<alg>/<hex>/<manifest-digest>
// Marker content: JSON-encoded ManifestRef
type ChunkIndex struct {
	driver storagedriver.StorageDriver
}

// DefaultChunkIndex is the process-wide chunk index. Initialised in NewRegistry.
var DefaultChunkIndex = &ChunkIndex{}

func chunkIndexPath(chunkDgst digest.Digest, manifestDgst digest.Digest) string {
	return fmt.Sprintf("/_clipper/chunkindex/%s/%s/%s/%s",
		chunkDgst.Algorithm(), chunkDgst.Hex(), manifestDgst.Algorithm(), manifestDgst.Hex())
}

func chunkIndexPrefix(chunkDgst digest.Digest) string {
	return fmt.Sprintf("/_clipper/chunkindex/%s/%s",
		chunkDgst.Algorithm(), chunkDgst.Hex())
}

// Add records that the given manifest references the supplied chunk digests.
// Each entry is written as a small JSON marker in the storage driver.
func (ci *ChunkIndex) Add(ctx context.Context, repo string, manifestDgst digest.Digest, chunkDigests []digest.Digest) {
	if ci.driver == nil || len(chunkDigests) == 0 {
		return
	}
	ref := ManifestRef{Repository: repo, Digest: manifestDgst}
	data, err := json.Marshal(ref)
	if err != nil {
		return
	}
	for _, chunkDgst := range chunkDigests {
		path := chunkIndexPath(chunkDgst, manifestDgst)
		_ = ci.driver.PutContent(ctx, path, data)
	}
}

// All returns every chunk digest and its manifest references known to the index.
func (ci *ChunkIndex) All(ctx context.Context) map[digest.Digest][]ManifestRef {
	if ci.driver == nil {
		return nil
	}
	result := map[digest.Digest][]ManifestRef{}
	_ = ci.driver.Walk(ctx, "/_clipper/chunkindex", func(fi storagedriver.FileInfo) error {
		if fi.IsDir() {
			return nil
		}
		data, err := ci.driver.GetContent(ctx, fi.Path())
		if err != nil {
			return nil
		}
		var ref ManifestRef
		if err := json.Unmarshal(data, &ref); err != nil {
			return nil
		}
		// Path: /_clipper/chunkindex/<chunk-alg>/<chunk-hex>/<manifest-alg>/<manifest-hex>
		path := fi.Path()
		parts := strings.Split(strings.TrimPrefix(path, "/"), "/")
		if len(parts) < 6 {
			return nil
		}
		chunkDgst := digest.NewDigestFromEncoded(digest.Algorithm(parts[2]), parts[3])
		if err := chunkDgst.Validate(); err != nil {
			return nil
		}
		result[chunkDgst] = append(result[chunkDgst], ref)
		return nil
	})
	return result
}


// Locate returns all manifests known to reference the given chunk digest.
func (ci *ChunkIndex) Locate(ctx context.Context, chunkDgst digest.Digest) []ManifestRef {
	if ci.driver == nil {
		return nil
	}
	prefix := chunkIndexPrefix(chunkDgst)
	entries, err := ci.driver.List(ctx, prefix)
	if err != nil {
		return nil
	}
	var refs []ManifestRef
	for _, entry := range entries {
		data, err := ci.driver.GetContent(ctx, entry)
		if err != nil {
			continue
		}
		var ref ManifestRef
		if err := json.Unmarshal(data, &ref); err != nil {
			continue
		}
		refs = append(refs, ref)
	}
	return refs
}
