package handlers

import (
	"encoding/json"
	"net/http"

	"github.com/distribution/distribution/v3/registry/storage"
	"github.com/gorilla/handlers"
	"github.com/opencontainers/go-digest"
)

// chunkLookupDispatcher creates the HTTP handler for the chunk locate endpoint.
func chunkLookupDispatcher(ctx *Context, r *http.Request) http.Handler {
	handler := &chunkLookupHandler{Context: ctx}
	return handlers.MethodHandler{
		http.MethodPost: http.HandlerFunc(handler.LocateChunks),
	}
}

type chunkLookupHandler struct {
	*Context
}

// chunkLocateRequest is the JSON body for POST /v2/<name>/_ext/chunks/locate.
type chunkLocateRequest struct {
	Digests []digest.Digest `json:"digests"`
}

// chunkLocateResult holds the location of a single chunk.
type chunkLocateResult struct {
	Repository string `json:"repository"`
}

// LocateChunks handles POST /v2/<name>/_ext/chunks/locate.
// It looks up each requested digest in the in-memory chunk index and returns
// a map of digest → location (or null when not found).
func (ch *chunkLookupHandler) LocateChunks(w http.ResponseWriter, r *http.Request) {
	var req chunkLocateRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		w.WriteHeader(http.StatusBadRequest)
		return
	}

	results := make(map[digest.Digest]*chunkLocateResult, len(req.Digests))
	for _, dgst := range req.Digests {
		if repo, found := storage.DefaultChunkIndex.Locate(dgst); found {
			results[dgst] = &chunkLocateResult{Repository: repo}
		} else {
			results[dgst] = nil
		}
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	_ = json.NewEncoder(w).Encode(results)
}
