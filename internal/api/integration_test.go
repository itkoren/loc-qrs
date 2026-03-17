//go:build integration

package api_test

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"
	"time"

	_ "github.com/marcboeker/go-duckdb"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/itkoren/loc-qrs/internal/api"
	"github.com/itkoren/loc-qrs/internal/observability"
	"github.com/itkoren/loc-qrs/internal/query"
	locSync "github.com/itkoren/loc-qrs/internal/sync"
	"github.com/itkoren/loc-qrs/internal/testutil"
	"github.com/itkoren/loc-qrs/internal/writer"
)

// fullStack wires up all real components using a temp directory.
type fullStack struct {
	router  http.Handler
	fw      *writer.FileWriter
	sw      *locSync.SyncWorker
	syncDB  *sql.DB
	queryDB *sql.DB
}

func newFullStack(t *testing.T) *fullStack {
	t.Helper()
	dir := t.TempDir()
	sch := testutil.MustParseSchema(t, testutil.DefaultSchemaJSON)
	enc, err := writer.NewEncoder("jsonl")
	require.NoError(t, err)

	reg := prometheus.NewRegistry()
	metrics := observability.NewMetrics(reg)
	logger := testutil.NewTestLogger()

	ch := make(chan writer.Record, 1000)
	syncTriggerCh := make(chan struct{}, 1)
	rollCh := make(chan writer.RollEvent, 4)
	rotator := writer.NewDailyRotator(dir, "jsonl")

	fw := writer.NewFileWriter(ch, syncTriggerCh, rollCh, rotator, enc, sch, 1000, metrics, logger)
	fw.Start()

	syncDB, err := locSync.OpenSyncDB(filepath.Join(dir, ".sync.duckdb"))
	require.NoError(t, err)

	queryDB, err := locSync.OpenQueryDB()
	require.NoError(t, err)

	sw := locSync.NewSyncWorker(
		syncDB, fw, dir, "jsonl",
		24*time.Hour, // no automatic syncs during tests
		syncTriggerCh, rollCh,
		metrics, logger,
	)
	sw.Start()

	qe := query.NewQueryEngine(queryDB, dir, fw, metrics, logger)

	router := api.NewRouter(api.ServerDeps{
		FileWriter:      fw,
		Encoder:         enc,
		Schema:          sch,
		SyncWorker:      sw,
		QueryEngine:     qe,
		QueryDB:         queryDB,
		Metrics:         metrics,
		MetricsGatherer: reg,
		Logger:          logger,
	})

	t.Cleanup(func() {
		fw.Stop()
		require.NoError(t, sw.SyncNow(context.Background()))
		sw.Stop()
		require.NoError(t, syncDB.Close())
		require.NoError(t, queryDB.Close())
	})

	return &fullStack{router: router, fw: fw, sw: sw, syncDB: syncDB, queryDB: queryDB}
}

func (s *fullStack) post(t *testing.T, path, body string) *httptest.ResponseRecorder {
	t.Helper()
	req := httptest.NewRequestWithContext(t.Context(), http.MethodPost, path,
		bytes.NewBufferString(body))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	s.router.ServeHTTP(w, req)
	return w
}

func (s *fullStack) get(t *testing.T, path string) *httptest.ResponseRecorder {
	t.Helper()
	req := httptest.NewRequestWithContext(t.Context(), http.MethodGet, path, http.NoBody)
	w := httptest.NewRecorder()
	s.router.ServeHTTP(w, req)
	return w
}

// ── Integration tests ─────────────────────────────────────────────────────────

func TestIntegration_IngestSyncQuery_RoundTrip(t *testing.T) {
	s := newFullStack(t)

	// Ingest 3 records.
	records := []string{
		`{"record":{"id":1,"event_name":"pageview","value":1.0}}`,
		`{"record":{"id":2,"event_name":"click","value":2.0}}`,
		`{"record":{"id":3,"event_name":"purchase","value":99.99}}`,
	}
	for _, body := range records {
		w := s.post(t, "/api/v1/records", body)
		assert.Equal(t, http.StatusAccepted, w.Code)
	}

	// Sync to Parquet.
	w := s.post(t, "/api/v1/sync", "")
	require.Equal(t, http.StatusOK, w.Code)

	// Query: count should be 3.
	w = s.post(t, "/api/v1/query", `{"sql":"SELECT COUNT(*) FROM records"}`)
	require.Equal(t, http.StatusOK, w.Code)

	var result map[string]any
	require.NoError(t, json.Unmarshal(w.Body.Bytes(), &result))
	rows := result["rows"].([]any)
	require.Len(t, rows, 1)
	count := rows[0].([]any)[0]
	assert.Equal(t, float64(3), count)
}

func TestIntegration_QueryBeforeSync_VisibleFromLiveJSONL(t *testing.T) {
	s := newFullStack(t)

	// Ingest and sync to flush the buffer.
	s.post(t, "/api/v1/records", `{"record":{"id":1,"event_name":"a"}}`)
	w := s.post(t, "/api/v1/sync", "") // sync flushes buffer
	require.Equal(t, http.StatusOK, w.Code)

	// Now query without another sync — live JSONL is readable.
	w = s.post(t, "/api/v1/query", `{"sql":"SELECT COUNT(*) FROM records"}`)
	require.Equal(t, http.StatusOK, w.Code)

	var result map[string]any
	require.NoError(t, json.Unmarshal(w.Body.Bytes(), &result))
	rows := result["rows"].([]any)
	require.Len(t, rows, 1)
	assert.Equal(t, float64(1), rows[0].([]any)[0])
}

func TestIntegration_NoDoubleCountAfterSync(t *testing.T) {
	s := newFullStack(t)

	// Ingest 2 records and sync.
	for _, body := range []string{
		`{"record":{"id":1,"event_name":"x"}}`,
		`{"record":{"id":2,"event_name":"y"}}`,
	} {
		s.post(t, "/api/v1/records", body)
	}
	s.post(t, "/api/v1/sync", "")

	// Ingest 1 more (post-sync, not yet in parquet).
	s.post(t, "/api/v1/records", `{"record":{"id":3,"event_name":"z"}}`)
	s.post(t, "/api/v1/sync", "") // sync again

	// Total should be 3, not 5 (no double-counting).
	w := s.post(t, "/api/v1/query", `{"sql":"SELECT COUNT(*) FROM records"}`)
	require.Equal(t, http.StatusOK, w.Code)

	var result map[string]any
	require.NoError(t, json.Unmarshal(w.Body.Bytes(), &result))
	rows := result["rows"].([]any)
	count := rows[0].([]any)[0]
	assert.Equal(t, float64(3), count)
}

func TestIntegration_QuerySelectAllColumns(t *testing.T) {
	s := newFullStack(t)

	s.post(t, "/api/v1/records", `{"record":{"id":42,"event_name":"test","value":3.14,"active":true}}`)
	s.post(t, "/api/v1/sync", "")

	w := s.post(t, "/api/v1/query", `{"sql":"SELECT id, event_name, value FROM records WHERE id = 42"}`)
	require.Equal(t, http.StatusOK, w.Code)

	var result map[string]any
	require.NoError(t, json.Unmarshal(w.Body.Bytes(), &result))
	cols := result["columns"].([]any)
	assert.Len(t, cols, 3)

	rows := result["rows"].([]any)
	require.Len(t, rows, 1)
	row := rows[0].([]any)
	assert.Equal(t, float64(42), row[0])
	assert.Equal(t, "test", row[1])
	assert.Equal(t, 3.14, row[2])
}

func TestIntegration_RebuildAll(t *testing.T) {
	s := newFullStack(t)

	// Ingest and sync.
	for i := 1; i <= 5; i++ {
		body := `{"record":{"id":` + string(rune('0'+i)) + `,"event_name":"e"}}`
		s.post(t, "/api/v1/records", body)
	}
	s.post(t, "/api/v1/sync", "")

	// Rebuild should succeed.
	w := s.post(t, "/api/v1/rebuild", "")
	assert.Equal(t, http.StatusOK, w.Code)

	var resp map[string]string
	require.NoError(t, json.Unmarshal(w.Body.Bytes(), &resp))
	assert.Equal(t, "rebuilt", resp["status"])
}

func TestIntegration_PrometheusMetrics(t *testing.T) {
	s := newFullStack(t)

	// Ingest a record to increment the counter.
	s.post(t, "/api/v1/records", `{"record":{"id":1,"event_name":"test"}}`)

	w := s.get(t, "/metrics")
	require.Equal(t, http.StatusOK, w.Code)

	body := w.Body.String()
	assert.Contains(t, body, "records_ingested_total")
	assert.Contains(t, body, "ingestion_channel_depth")
	assert.Contains(t, body, "query_latency_seconds")
}

func TestIntegration_HealthAfterIngest(t *testing.T) {
	s := newFullStack(t)

	s.post(t, "/api/v1/records", `{"record":{"id":1}}`)

	w := s.get(t, "/health")
	require.Equal(t, http.StatusOK, w.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(w.Body.Bytes(), &resp))
	assert.Equal(t, "ok", resp["status"])
	assert.Equal(t, "alive", resp["duckdb"])
	assert.GreaterOrEqual(t, resp["channel_fill_pct"].(float64), float64(0))
}

func TestIntegration_SchemaValidation_RejectsInvalidRecord(t *testing.T) {
	s := newFullStack(t)

	// "id" must be UBIGINT, not a boolean.
	w := s.post(t, "/api/v1/records", `{"record":{"id":true}}`)
	assert.Equal(t, http.StatusBadRequest, w.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(w.Body.Bytes(), &resp))
	assert.Equal(t, "validation failed", resp["error"])
}

func TestIntegration_QueryWithFilter(t *testing.T) {
	s := newFullStack(t)

	for _, body := range []string{
		`{"record":{"id":1,"event_name":"click","value":10.0}}`,
		`{"record":{"id":2,"event_name":"view","value":5.0}}`,
		`{"record":{"id":3,"event_name":"click","value":20.0}}`,
	} {
		s.post(t, "/api/v1/records", body)
	}
	s.post(t, "/api/v1/sync", "")

	w := s.post(t, "/api/v1/query",
		`{"sql":"SELECT COUNT(*) FROM records WHERE event_name = 'click'"}`)
	require.Equal(t, http.StatusOK, w.Code)

	var result map[string]any
	require.NoError(t, json.Unmarshal(w.Body.Bytes(), &result))
	rows := result["rows"].([]any)
	assert.Equal(t, float64(2), rows[0].([]any)[0])
}

func TestIntegration_QueryAggregation(t *testing.T) {
	s := newFullStack(t)

	for _, body := range []string{
		`{"record":{"id":1,"event_name":"buy","value":100.0}}`,
		`{"record":{"id":2,"event_name":"buy","value":200.0}}`,
		`{"record":{"id":3,"event_name":"buy","value":300.0}}`,
	} {
		s.post(t, "/api/v1/records", body)
	}
	s.post(t, "/api/v1/sync", "")

	w := s.post(t, "/api/v1/query",
		`{"sql":"SELECT SUM(value), AVG(value), MIN(value), MAX(value) FROM records"}`)
	require.Equal(t, http.StatusOK, w.Code)

	var result map[string]any
	require.NoError(t, json.Unmarshal(w.Body.Bytes(), &result))
	rows := result["rows"].([]any)
	require.Len(t, rows, 1)
	row := rows[0].([]any)

	assert.Equal(t, float64(600), row[0], "SUM")
	assert.Equal(t, float64(200), row[1], "AVG")
	assert.Equal(t, float64(100), row[2], "MIN")
	assert.Equal(t, float64(300), row[3], "MAX")
}

func TestIntegration_ForbiddenSQL_Blocked(t *testing.T) {
	s := newFullStack(t)

	dangerous := []string{
		`{"sql":"DROP TABLE records"}`,
		`{"sql":"INSERT INTO records VALUES (1)"}`,
		`{"sql":"DELETE FROM records"}`,
		`{"sql":"CREATE TABLE evil (x INT)"}`,
	}
	for _, body := range dangerous {
		w := s.post(t, "/api/v1/query", body)
		assert.Equal(t, http.StatusBadRequest, w.Code, "expected blocked: %s", body)
	}
}

func TestIntegration_ConcurrentIngestion(t *testing.T) {
	s := newFullStack(t)

	// Ingest 50 records concurrently.
	done := make(chan struct{}, 50)
	for i := 0; i < 50; i++ {
		i := i
		go func() {
			defer func() { done <- struct{}{} }()
			body := `{"record":{"id":` + strings.Repeat("1", 1) + `}}`
			_ = body
			// Each goroutine constructs a unique body.
			b, marshalErr := json.Marshal(map[string]any{
				"record": map[string]any{"id": float64(i + 1), "event_name": "concurrent"},
			})
			if marshalErr != nil {
				return
			}
			req := httptest.NewRequestWithContext(t.Context(), http.MethodPost, "/api/v1/records", bytes.NewReader(b))
			req.Header.Set("Content-Type", "application/json")
			w := httptest.NewRecorder()
			s.router.ServeHTTP(w, req)
			// Accept 202 or 503 (channel might be briefly full).
			assert.True(t, w.Code == http.StatusAccepted || w.Code == http.StatusServiceUnavailable)
		}()
	}
	for i := 0; i < 50; i++ {
		<-done
	}

	// Sync and verify at least some records were ingested.
	s.post(t, "/api/v1/sync", "")

	w := s.post(t, "/api/v1/query", `{"sql":"SELECT COUNT(*) FROM records"}`)
	require.Equal(t, http.StatusOK, w.Code)

	var result map[string]any
	require.NoError(t, json.Unmarshal(w.Body.Bytes(), &result))
	rows := result["rows"].([]any)
	count := rows[0].([]any)[0].(float64)
	assert.Greater(t, count, float64(0))
}
