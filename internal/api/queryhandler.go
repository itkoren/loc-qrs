package api

import (
	"context"
	"encoding/json"
	"net/http"

	"github.com/itkoren/loc-qrs/internal/query"
)

type queryHandler struct {
	engine *query.QueryEngine
}

type queryRequest struct {
	SQL string `json:"sql"`
}

func (h *queryHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	var req queryRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeJSON(w, http.StatusBadRequest, errorResponse{Error: "invalid JSON"})
		return
	}
	if req.SQL == "" {
		writeJSON(w, http.StatusBadRequest, errorResponse{Error: "missing 'sql' field"})
		return
	}

	result, err := h.engine.Execute(context.Background(), req.SQL)
	if err != nil {
		writeJSON(w, http.StatusBadRequest, errorResponse{Error: err.Error()})
		return
	}

	writeJSON(w, http.StatusOK, result)
}
