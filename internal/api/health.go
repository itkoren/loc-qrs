package api

import (
	"database/sql"
	"encoding/json"
	"net/http"
)

type healthHandler struct {
	db *sql.DB
	fw interface {
		ChannelLen() int
		ChannelCap() int
	}
}

type healthResponse struct {
	Status         string  `json:"status"`
	DuckDB         string  `json:"duckdb"`
	ChannelFillPct float64 `json:"channel_fill_pct"`
}

func (h *healthHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	resp := healthResponse{Status: "ok", DuckDB: "alive"}

	if err := h.db.PingContext(r.Context()); err != nil {
		resp.Status = "degraded"
		resp.DuckDB = "unreachable"
		w.WriteHeader(http.StatusServiceUnavailable)
	}

	if h.fw != nil && h.fw.ChannelCap() > 0 {
		resp.ChannelFillPct = float64(h.fw.ChannelLen()) / float64(h.fw.ChannelCap()) * 100
	}

	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(resp)
}
