package api

import (
	"encoding/json"
	"net/http"
	"time"

	"github.com/itkoren/loc-qrs/internal/observability"
	"github.com/itkoren/loc-qrs/internal/schema"
	"github.com/itkoren/loc-qrs/internal/writer"
)

type ingestHandler struct {
	fw      *writer.FileWriter
	encoder writer.Encoder
	schema  *schema.Schema
	metrics *observability.Metrics
}

type ingestRequest struct {
	Record map[string]any `json:"record"`
}

type ingestResponse struct {
	Status string `json:"status"`
}

type errorResponse struct {
	Error  string `json:"error"`
	Detail any    `json:"detail,omitempty"`
}

func writeJSON(w http.ResponseWriter, code int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(v)
}

func (h *ingestHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	var req ingestRequest
	dec := json.NewDecoder(r.Body)
	if err := dec.Decode(&req); err != nil {
		if h.metrics != nil {
			h.metrics.RecordsRejected.WithLabelValues("schema").Add(1)
		}
		writeJSON(w, http.StatusBadRequest, errorResponse{Error: "invalid JSON", Detail: err.Error()})
		return
	}

	if req.Record == nil {
		if h.metrics != nil {
			h.metrics.RecordsRejected.WithLabelValues("schema").Add(1)
		}
		writeJSON(w, http.StatusBadRequest, errorResponse{Error: "missing 'record' field"})
		return
	}

	// Validate against schema.
	if errs := h.schema.ValidateRecord(req.Record); len(errs) > 0 {
		if h.metrics != nil {
			h.metrics.RecordsRejected.WithLabelValues("schema").Add(1)
		}
		details := make([]string, len(errs))
		for i, e := range errs {
			details[i] = e.Error()
		}
		writeJSON(w, http.StatusBadRequest, errorResponse{Error: "validation failed", Detail: details})
		return
	}

	// Encode the record.
	payload, err := h.encoder.Encode(req.Record, h.schema)
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, errorResponse{Error: "encode failed"})
		return
	}

	rec := writer.Record{
		Payload:    payload,
		IngestedAt: time.Now().UTC(),
	}

	if !h.fw.Submit(rec) {
		if h.metrics != nil {
			h.metrics.RecordsRejected.WithLabelValues("channel_full").Add(1)
		}
		writeJSON(w, http.StatusServiceUnavailable, errorResponse{Error: "server busy, channel full"})
		return
	}

	if h.metrics != nil {
		h.metrics.RecordsIngested.Add(1)
	}

	writeJSON(w, http.StatusAccepted, ingestResponse{Status: "accepted"})
}
