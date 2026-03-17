// Package testutil provides shared helpers for unit and integration tests.
// It has no CGO dependencies — keep it that way to avoid poisoning the
// pure-Go test graph.
package testutil

import (
	"io"
	"log/slog"
	"testing"

	"github.com/itkoren/loc-qrs/internal/observability"
	"github.com/itkoren/loc-qrs/internal/schema"
	"github.com/prometheus/client_golang/prometheus"
)

// DefaultSchemaJSON is a minimal schema suitable for most tests.
const DefaultSchemaJSON = `{
  "columns": {
    "id":         "UBIGINT",
    "event_name": "VARCHAR",
    "value":      "DOUBLE",
    "active":     "BOOLEAN"
  },
  "format": "jsonl"
}`

// MustParseSchema parses schema JSON, calling t.Fatal on error.
func MustParseSchema(t *testing.T, raw string) *schema.Schema {
	t.Helper()
	s, err := schema.Parse([]byte(raw))
	if err != nil {
		t.Fatalf("MustParseSchema: %v", err)
	}
	return s
}

// NewTestLogger returns a slog.Logger that discards all output.
func NewTestLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

// NewTestMetrics returns a fresh Metrics instance using an isolated Prometheus
// registry so parallel tests do not conflict with each other or the default registry.
func NewTestMetrics(t *testing.T) *observability.Metrics {
	t.Helper()
	return observability.NewMetrics(prometheus.NewRegistry())
}

// TempDir creates a temporary directory for the test and registers a cleanup
// function that removes it when the test ends.
func TempDir(t *testing.T) string {
	t.Helper()
	return t.TempDir() // stdlib already handles cleanup
}
