package api

import "net/http"

// NewServer creates a configured *http.ServeMux with all Data API routes
// registered. Go 1.22 method+path patterns keep the routing explicit without
// a third-party router.
func NewServer(h *Handler) *http.ServeMux {
	mux := http.NewServeMux()

	mux.HandleFunc("POST /ExecuteSql", h.executeSQL)
	mux.Handle("POST /BeginTransaction", handleJSON(h.beginTransaction))
	mux.Handle("POST /CommitTransaction", handleJSON(h.commitTransaction))
	mux.Handle("POST /RollbackTransaction", handleJSON(h.rollbackTransaction))
	mux.Handle("POST /Execute", handleJSON(h.execute))
	mux.Handle("POST /BatchExecute", handleJSON(h.batchExecute))

	return mux
}
