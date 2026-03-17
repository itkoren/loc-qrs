package observability

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

// Metrics holds all Prometheus metric instances.
type Metrics struct {
	RecordsIngested      prometheus.Counter
	RecordsRejected      *prometheus.CounterVec
	IngestionChannelDepth prometheus.Gauge
	ParquetSyncDuration  prometheus.Histogram
	SyncFailures         prometheus.Counter
	QueryLatency         prometheus.Histogram
	QueryErrors          *prometheus.CounterVec
	ActiveSyncs          prometheus.Gauge
}

// NewMetrics registers and returns all metrics.
func NewMetrics(reg prometheus.Registerer) *Metrics {
	if reg == nil {
		reg = prometheus.DefaultRegisterer
	}
	factory := promauto.With(reg)

	return &Metrics{
		RecordsIngested: factory.NewCounter(prometheus.CounterOpts{
			Name: "records_ingested_total",
			Help: "Total number of records successfully ingested.",
		}),
		RecordsRejected: factory.NewCounterVec(prometheus.CounterOpts{
			Name: "records_rejected_total",
			Help: "Total number of records rejected.",
		}, []string{"reason"}),
		IngestionChannelDepth: factory.NewGauge(prometheus.GaugeOpts{
			Name: "ingestion_channel_depth",
			Help: "Current number of records buffered in the ingest channel.",
		}),
		ParquetSyncDuration: factory.NewHistogram(prometheus.HistogramOpts{
			Name:    "parquet_sync_duration_seconds",
			Help:    "Duration of JSONL→Parquet sync operations.",
			Buckets: prometheus.DefBuckets,
		}),
		SyncFailures: factory.NewCounter(prometheus.CounterOpts{
			Name: "sync_failures_total",
			Help: "Total number of sync failures.",
		}),
		QueryLatency: factory.NewHistogram(prometheus.HistogramOpts{
			Name:    "query_latency_seconds",
			Help:    "Duration of query execution.",
			Buckets: prometheus.DefBuckets,
		}),
		QueryErrors: factory.NewCounterVec(prometheus.CounterOpts{
			Name: "query_errors_total",
			Help: "Total number of query errors.",
		}, []string{"reason"}),
		ActiveSyncs: factory.NewGauge(prometheus.GaugeOpts{
			Name: "active_syncs",
			Help: "Number of sync operations currently in progress.",
		}),
	}
}
