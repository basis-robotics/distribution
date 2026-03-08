package storage

import (
	"sync"

	"github.com/opencontainers/go-digest"
)

// ChunkIndex is a thread-safe in-memory index mapping chunk blob digests to
// the repository that holds them. It is used to answer cross-repo chunk lookup
// requests without a full registry scan.
//
// Phase 1: in-memory only; the index is lost on restart and is rebuilt
// incrementally as manifests are pushed.
type ChunkIndex struct {
	mu    sync.RWMutex
	index map[digest.Digest]string // digest → repository name
}

// DefaultChunkIndex is the process-wide chunk index populated during manifest PUT.
var DefaultChunkIndex = &ChunkIndex{}

// Add records that the given repository contains the supplied chunk digests.
// Existing entries are not overwritten (first writer wins).
func (ci *ChunkIndex) Add(repoName string, digests []digest.Digest) {
	if len(digests) == 0 {
		return
	}
	ci.mu.Lock()
	defer ci.mu.Unlock()
	if ci.index == nil {
		ci.index = make(map[digest.Digest]string)
	}
	for _, d := range digests {
		if _, exists := ci.index[d]; !exists {
			ci.index[d] = repoName
		}
	}
}

// Locate returns the repository name that holds the given chunk digest.
func (ci *ChunkIndex) Locate(dgst digest.Digest) (repo string, found bool) {
	ci.mu.RLock()
	defer ci.mu.RUnlock()
	repo, found = ci.index[dgst]
	return
}

// Reset clears the index. Intended for test isolation.
func (ci *ChunkIndex) Reset() {
	ci.mu.Lock()
	defer ci.mu.Unlock()
	ci.index = nil
}
