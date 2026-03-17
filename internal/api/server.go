package api

import (
	"context"
	"database/sql"
	"log/slog"
	"net/http"

	"github.com/go-chi/chi/v5"
	"github.com/go-chi/chi/v5/middleware"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"

	"github.com/itkoren/loc-qrs/internal/observability"
	"github.com/itkoren/loc-qrs/internal/query"
	"github.com/itkoren/loc-qrs/internal/schema"
	"github.com/itkoren/loc-qrs/internal/writer"
)

// Syncer is the interface required by the sync and rebuild HTTP handlers.
// *locSync.SyncWorker satisfies this interface.
type Syncer interface {
	SyncNow(ctx context.Context) error
	RebuildAll(ctx context.Context) error
}

// ServerDeps groups dependencies injected into the HTTP server.
type ServerDeps struct {
	FileWriter      *writer.FileWriter
	Encoder         writer.Encoder
	Schema          *schema.Schema
	SyncWorker      Syncer
	QueryEngine     *query.QueryEngine
	QueryDB         *sql.DB
	Metrics         *observability.Metrics
	MetricsGatherer prometheus.Gatherer // if nil, uses prometheus.DefaultGatherer
	Logger          *slog.Logger
}

// NewRouter builds and returns the chi router with all routes registered.
func NewRouter(deps ServerDeps) http.Handler {
	r := chi.NewRouter()

	r.Use(middleware.RealIP)
	r.Use(RequestID)
	r.Use(Logger(deps.Logger))
	r.Use(Recover(deps.Logger))

	// Prometheus metrics.
	gatherer := deps.MetricsGatherer
	if gatherer == nil {
		gatherer = prometheus.DefaultGatherer
	}
	r.Handle("/metrics", promhttp.HandlerFor(gatherer, promhttp.HandlerOpts{}))

	// Health check.
	// Guard against typed-nil interface: only assign fw when non-nil.
	var healthFW interface {
		ChannelLen() int
		ChannelCap() int
	}
	if deps.FileWriter != nil {
		healthFW = deps.FileWriter
	}
	r.Get("/health", (&healthHandler{
		db: deps.QueryDB,
		fw: healthFW,
	}).ServeHTTP)

	// API v1.
	r.Route("/api/v1", func(r chi.Router) {
		r.Post("/records", (&ingestHandler{
			fw:      deps.FileWriter,
			encoder: deps.Encoder,
			schema:  deps.Schema,
			metrics: deps.Metrics,
		}).ServeHTTP)

		r.Post("/query", (&queryHandler{
			engine: deps.QueryEngine,
		}).ServeHTTP)

		r.Post("/sync", (&syncHandler{
			syncer: deps.SyncWorker,
		}).ServeHTTP)

		r.Post("/rebuild", (&rebuildHandler{
			syncer: deps.SyncWorker,
		}).ServeHTTP)
	})

	return r
}
