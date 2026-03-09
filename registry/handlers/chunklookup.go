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

// chunkListDispatcher creates the HTTP handler for the global chunk list endpoint.
func chunkListDispatcher(ctx *Context, r *http.Request) http.Handler {
	handler := &chunkLookupHandler{Context: ctx}
	return handlers.MethodHandler{
		http.MethodGet: http.HandlerFunc(handler.ListChunks),
	}
}

type chunkLookupHandler struct {
	*Context
}

// chunkLocateRequest is the JSON body for POST /v2/<name>/_ext/chunks/locate.
type chunkLocateRequest struct {
	Digests []digest.Digest `json:"digests"`
}

// ListChunks handles GET /v2/_ext/chunks.
// Returns all chunk digests and their manifest references known to the index.
func (ch *chunkLookupHandler) ListChunks(w http.ResponseWriter, r *http.Request) {
	all := storage.DefaultChunkIndex.All(r.Context())
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	_ = json.NewEncoder(w).Encode(all)
}

// LocateChunks handles POST /v2/<name>/_ext/chunks/locate.
// Response: map of chunk digest → list of ManifestRef (or absent key when not found).
func (ch *chunkLookupHandler) LocateChunks(w http.ResponseWriter, r *http.Request) {
	var req chunkLocateRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		w.WriteHeader(http.StatusBadRequest)
		return
	}

	results := make(map[digest.Digest][]storage.ManifestRef, len(req.Digests))
	for _, dgst := range req.Digests {
		if refs := storage.DefaultChunkIndex.Locate(r.Context(), dgst); len(refs) > 0 {
			results[dgst] = refs
		}
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	_ = json.NewEncoder(w).Encode(results)
}
