package api

import (
	"context"
	"net/http"
)

type syncHandler struct {
	syncer interface {
		SyncNow(ctx context.Context) error
	}
}

func (h *syncHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if err := h.syncer.SyncNow(r.Context()); err != nil {
		writeJSON(w, http.StatusInternalServerError, errorResponse{Error: err.Error()})
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"status": "synced"})
}

type rebuildHandler struct {
	syncer interface {
		RebuildAll(ctx context.Context) error
	}
}

func (h *rebuildHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if err := h.syncer.RebuildAll(r.Context()); err != nil {
		writeJSON(w, http.StatusInternalServerError, errorResponse{Error: err.Error()})
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"status": "rebuilt"})
}
