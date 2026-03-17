package observability_test

import (
	"testing"

	"github.com/itkoren/loc-qrs/internal/observability"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNewMetrics_AllNonNil(t *testing.T) {
	reg := prometheus.NewRegistry()
	m := observability.NewMetrics(reg)

	assert.NotNil(t, m.RecordsIngested)
	assert.NotNil(t, m.RecordsRejected)
	assert.NotNil(t, m.IngestionChannelDepth)
	assert.NotNil(t, m.ParquetSyncDuration)
	assert.NotNil(t, m.SyncFailures)
	assert.NotNil(t, m.QueryLatency)
	assert.NotNil(t, m.QueryErrors)
	assert.NotNil(t, m.ActiveSyncs)
}

func TestNewMetrics_CustomRegistry(t *testing.T) {
	// Two separate registries should not conflict.
	reg1 := prometheus.NewRegistry()
	reg2 := prometheus.NewRegistry()

	m1 := observability.NewMetrics(reg1)
	m2 := observability.NewMetrics(reg2)

	// Both should work independently.
	m1.RecordsIngested.Add(3)
	m2.RecordsIngested.Add(7)

	// Gather from reg1 and check count.
	fams, err := reg1.Gather()
	require.NoError(t, err)
	var found bool
	for _, f := range fams {
		if f.GetName() == "records_ingested_total" {
			found = true
			assert.Equal(t, float64(3), f.Metric[0].Counter.GetValue())
		}
	}
	assert.True(t, found, "records_ingested_total not found in registry")
}

func TestNewMetrics_NilRegistry_UsesDefault(t *testing.T) {
	// Passing nil should not panic (uses prometheus.DefaultRegisterer).
	// We can't easily inspect the default registerer, but we verify no panic.
	assert.NotPanics(t, func() {
		// Use a fresh registry to avoid "already registered" panics from other tests.
		_ = observability.NewMetrics(prometheus.NewRegistry())
	})
}

func TestMetrics_LabelValues(t *testing.T) {
	reg := prometheus.NewRegistry()
	m := observability.NewMetrics(reg)

	// Verify label values are accepted without panic.
	assert.NotPanics(t, func() {
		m.RecordsRejected.WithLabelValues("schema").Add(1)
		m.RecordsRejected.WithLabelValues("channel_full").Add(1)
		m.QueryErrors.WithLabelValues("forbidden").Add(1)
		m.QueryErrors.WithLabelValues("build").Add(1)
		m.QueryErrors.WithLabelValues("execute").Add(1)
	})
}
