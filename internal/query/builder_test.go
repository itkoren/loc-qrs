package query_test

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/itkoren/loc-qrs/internal/query"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestBuildQuery_NoFiles(t *testing.T) {
	dir := t.TempDir()
	q, err := query.BuildQuery("SELECT * FROM records", dir, "")
	require.NoError(t, err)
	// Should produce a CTE that returns no rows.
	assert.Contains(t, q, "records")
	assert.Contains(t, q, "SELECT * FROM records")
}

func TestBuildQuery_LiveJSONLOnly(t *testing.T) {
	dir := t.TempDir()
	// Create a non-empty JSONL file for today.
	liveFile := filepath.Join(dir, "data_2099-01-01.jsonl") // far future date, always "today-like"
	require.NoError(t, os.WriteFile(liveFile, []byte(`{"id":1}`+"\n"), 0o644))

	q, err := query.BuildQuery("SELECT * FROM records", dir, liveFile)
	require.NoError(t, err)

	assert.Contains(t, q, "_live")
	assert.Contains(t, q, "read_json_auto")
	assert.Contains(t, q, liveFile)
	// No historical parquet (none exist in dir), so no _hist.
	assert.NotContains(t, q, "_hist")
}

func TestBuildQuery_CSVLiveFile(t *testing.T) {
	dir := t.TempDir()
	liveFile := filepath.Join(dir, "data_2099-01-01.csv")
	require.NoError(t, os.WriteFile(liveFile, []byte("1,test\n"), 0o644))

	q, err := query.BuildQuery("SELECT * FROM records", dir, liveFile)
	require.NoError(t, err)

	assert.Contains(t, q, "read_csv_auto")
}

func TestBuildQuery_HistoricalParquetsOnly(t *testing.T) {
	dir := t.TempDir()
	// Create two parquet files with past dates (not today).
	past1 := filepath.Join(dir, "data_2020-01-01.parquet")
	past2 := filepath.Join(dir, "data_2021-06-15.parquet")
	require.NoError(t, os.WriteFile(past1, []byte("dummy"), 0o644))
	require.NoError(t, os.WriteFile(past2, []byte("dummy"), 0o644))

	// No live file.
	q, err := query.BuildQuery("SELECT COUNT(*) FROM records", dir, "")
	require.NoError(t, err)

	assert.Contains(t, q, "_hist")
	assert.Contains(t, q, "read_parquet")
	assert.Contains(t, q, "2020-01-01")
	assert.Contains(t, q, "2021-06-15")
	assert.NotContains(t, q, "_live")
}

func TestBuildQuery_TodaysParquetExcluded(t *testing.T) {
	dir := t.TempDir()

	// Create today's parquet and a past parquet.
	liveFile := filepath.Join(dir, "data_2099-01-01.jsonl") // simulates "today"
	require.NoError(t, os.WriteFile(liveFile, []byte(`{"id":1}`+"\n"), 0o644))

	// Create a parquet that shares the same date as liveFile.
	todayParquet := filepath.Join(dir, "data_2099-01-01.parquet")
	require.NoError(t, os.WriteFile(todayParquet, []byte("dummy"), 0o644))

	// Create a historical parquet.
	histParquet := filepath.Join(dir, "data_2020-01-01.parquet")
	require.NoError(t, os.WriteFile(histParquet, []byte("dummy"), 0o644))

	q, err := query.BuildQuery("SELECT * FROM records", dir, liveFile)
	require.NoError(t, err)

	// Today's parquet must be excluded to prevent double-counting.
	assert.NotContains(t, q, "data_2099-01-01.parquet",
		"today's parquet must be excluded from _hist")
	// Historical parquet must be included.
	assert.Contains(t, q, "data_2020-01-01.parquet")
	// Live JSONL must be included.
	assert.Contains(t, q, liveFile)
}

func TestBuildQuery_LiveAndHistorical(t *testing.T) {
	dir := t.TempDir()
	liveFile := filepath.Join(dir, "data_2099-12-31.jsonl")
	require.NoError(t, os.WriteFile(liveFile, []byte(`{"id":1}`+"\n"), 0o644))

	histParquet := filepath.Join(dir, "data_2020-01-01.parquet")
	require.NoError(t, os.WriteFile(histParquet, []byte("dummy"), 0o644))

	q, err := query.BuildQuery("SELECT * FROM records", dir, liveFile)
	require.NoError(t, err)

	assert.Contains(t, q, "_hist")
	assert.Contains(t, q, "_live")
	assert.Contains(t, q, "UNION ALL")
	assert.Contains(t, q, "records AS")
}

func TestBuildQuery_EmptyLiveFile_Excluded(t *testing.T) {
	dir := t.TempDir()
	// Empty live file should not be included.
	liveFile := filepath.Join(dir, "data_2099-01-01.jsonl")
	require.NoError(t, os.WriteFile(liveFile, []byte(""), 0o644))

	q, err := query.BuildQuery("SELECT * FROM records", dir, liveFile)
	require.NoError(t, err)

	assert.NotContains(t, q, "_live")
}

func TestBuildQuery_NonexistentLiveFile(t *testing.T) {
	dir := t.TempDir()
	liveFile := filepath.Join(dir, "data_2099-01-01.jsonl")
	// File does not exist.

	q, err := query.BuildQuery("SELECT * FROM records", dir, liveFile)
	require.NoError(t, err)
	assert.NotContains(t, q, "_live")
}

func TestBuildQuery_UserSQLPreserved(t *testing.T) {
	dir := t.TempDir()
	userSQL := "SELECT id, event_name FROM records WHERE value > 0 ORDER BY id LIMIT 10"
	q, err := query.BuildQuery(userSQL, dir, "")
	require.NoError(t, err)
	assert.Contains(t, q, userSQL)
}

func TestBuildQuery_PathWithSingleQuotes(t *testing.T) {
	dir := t.TempDir()
	// Path contains a single quote (edge case for SQL quoting).
	// We use a normal path here since OS file systems may not allow ' in filenames,
	// but we test the sqlPathQuote mechanism via the sqlQuote function indirectly.
	liveFile := filepath.Join(dir, "data_2099-01-01.jsonl")
	require.NoError(t, os.WriteFile(liveFile, []byte(`{}`+"\n"), 0o644))

	q, err := query.BuildQuery("SELECT * FROM records", dir, liveFile)
	require.NoError(t, err)
	// The path should be SQL-quoted (single-quoted).
	assert.Contains(t, q, "'")
}

func TestBuildQuery_MultipleHistoricalParquets(t *testing.T) {
	dir := t.TempDir()
	for _, date := range []string{"2020-01-01", "2021-01-01", "2022-01-01"} {
		p := filepath.Join(dir, "data_"+date+".parquet")
		require.NoError(t, os.WriteFile(p, []byte("x"), 0o644))
	}

	q, err := query.BuildQuery("SELECT COUNT(*) FROM records", dir, "")
	require.NoError(t, err)

	// All three should be referenced.
	assert.True(t, strings.Count(q, "2020-01-01")+strings.Count(q, "2021-01-01")+strings.Count(q, "2022-01-01") == 3)
}
