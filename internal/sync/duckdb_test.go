//go:build integration

package sync_test

import (
	"context"
	"database/sql"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	locSync "github.com/itkoren/loc-qrs/internal/sync"

	_ "github.com/marcboeker/go-duckdb"
)

func TestOpenSyncDB(t *testing.T) {
	dir := t.TempDir()
	db, err := locSync.OpenSyncDB(filepath.Join(dir, ".sync.duckdb"))
	require.NoError(t, err)
	defer db.Close()
	assert.NoError(t, db.PingContext(context.Background()))
}

func TestOpenQueryDB(t *testing.T) {
	db, err := locSync.OpenQueryDB()
	require.NoError(t, err)
	defer db.Close()
	assert.NoError(t, db.PingContext(context.Background()))
}

func TestCopyToParquet_RoundTrip(t *testing.T) {
	dir := t.TempDir()

	// Write a JSONL file.
	jsonlPath := filepath.Join(dir, "data_2026-03-17.jsonl")
	lines := `{"id":1,"name":"alice","value":1.5}` + "\n" +
		`{"id":2,"name":"bob","value":2.5}` + "\n" +
		`{"id":3,"name":"carol","value":3.5}` + "\n"
	require.NoError(t, os.WriteFile(jsonlPath, []byte(lines), 0o644))

	// Open sync DB.
	db, err := locSync.OpenSyncDB(filepath.Join(dir, ".sync.duckdb"))
	require.NoError(t, err)
	defer db.Close()

	// Convert to Parquet.
	parquetPath := filepath.Join(dir, "data_2026-03-17.parquet")
	err = locSync.CopyToParquet(context.Background(), db, jsonlPath, parquetPath)
	require.NoError(t, err)

	// Verify Parquet file exists.
	info, err := os.Stat(parquetPath)
	require.NoError(t, err)
	assert.Greater(t, info.Size(), int64(0))

	// No temp file should remain.
	_, err = os.Stat(parquetPath + ".tmp")
	assert.True(t, os.IsNotExist(err), "temp file should be removed after rename")

	// Read back via a fresh DuckDB instance.
	queryDB, err := sql.Open("duckdb", "")
	require.NoError(t, err)
	defer queryDB.Close()

	rows, err := queryDB.QueryContext(context.Background(),
		`SELECT id, name, value FROM read_parquet('`+parquetPath+`') ORDER BY id`)
	require.NoError(t, err)
	defer rows.Close()

	type row struct {
		id    int64
		name  string
		value float64
	}
	var results []row
	for rows.Next() {
		var r row
		require.NoError(t, rows.Scan(&r.id, &r.name, &r.value))
		results = append(results, r)
	}
	require.NoError(t, rows.Err())

	require.Len(t, results, 3)
	assert.Equal(t, int64(1), results[0].id)
	assert.Equal(t, "alice", results[0].name)
	assert.Equal(t, 1.5, results[0].value)
	assert.Equal(t, int64(3), results[2].id)
}

func TestCopyToParquet_EmptyFile(t *testing.T) {
	dir := t.TempDir()
	jsonlPath := filepath.Join(dir, "empty.jsonl")
	require.NoError(t, os.WriteFile(jsonlPath, []byte(""), 0o644))

	db, err := locSync.OpenSyncDB(filepath.Join(dir, ".sync.duckdb"))
	require.NoError(t, err)
	defer db.Close()

	// CopyToParquet on an empty JSONL may succeed or fail; the SyncWorker skips empty files
	// before calling this in production. We only verify it doesn't panic.
	if copyErr := locSync.CopyToParquet(context.Background(), db, jsonlPath, filepath.Join(dir, "out.parquet")); copyErr != nil {
		t.Logf("empty-file CopyToParquet returned (acceptable): %v", copyErr)
	}
}

func TestCopyCSVToParquet_RoundTrip(t *testing.T) {
	dir := t.TempDir()

	csvPath := filepath.Join(dir, "data_2026-03-17.csv")
	csvData := "1,alice,10.0\n2,bob,20.0\n"
	require.NoError(t, os.WriteFile(csvPath, []byte(csvData), 0o644))

	db, err := locSync.OpenSyncDB(filepath.Join(dir, ".sync.duckdb"))
	require.NoError(t, err)
	defer db.Close()

	parquetPath := filepath.Join(dir, "data_2026-03-17.parquet")
	err = locSync.CopyCSVToParquet(context.Background(), db, csvPath, parquetPath, false)
	require.NoError(t, err)

	info, err := os.Stat(parquetPath)
	require.NoError(t, err)
	assert.Greater(t, info.Size(), int64(0))
}

func TestCopyToParquet_AtomicRename(t *testing.T) {
	dir := t.TempDir()
	jsonlPath := filepath.Join(dir, "data.jsonl")
	require.NoError(t, os.WriteFile(jsonlPath, []byte(`{"id":1}`+"\n"), 0o644))

	db, err := locSync.OpenSyncDB(filepath.Join(dir, ".sync.duckdb"))
	require.NoError(t, err)
	defer db.Close()

	parquetPath := filepath.Join(dir, "data.parquet")
	require.NoError(t, locSync.CopyToParquet(context.Background(), db, jsonlPath, parquetPath))

	// Final file exists.
	_, err = os.Stat(parquetPath)
	assert.NoError(t, err)

	// Temp file does not exist.
	_, err = os.Stat(parquetPath + ".tmp")
	assert.True(t, os.IsNotExist(err))
}
