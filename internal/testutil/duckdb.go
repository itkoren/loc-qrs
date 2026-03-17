//go:build integration

package testutil

import (
	"database/sql"
	"path/filepath"
	"testing"

	_ "github.com/marcboeker/go-duckdb"
)

// NewInMemoryDuckDB opens an in-memory DuckDB connection for integration tests.
// The connection is closed automatically when the test ends.
func NewInMemoryDuckDB(t *testing.T) *sql.DB {
	t.Helper()
	db, err := sql.Open("duckdb", "")
	if err != nil {
		t.Fatalf("NewInMemoryDuckDB open: %v", err)
	}
	if err := db.Ping(); err != nil {
		db.Close()
		t.Fatalf("NewInMemoryDuckDB ping: %v", err)
	}
	t.Cleanup(func() { db.Close() })
	return db
}

// NewTempSyncDB opens a file-backed DuckDB in the test's temp directory.
// Suitable for sync operations that need a persistent (read-write) instance.
func NewTempSyncDB(t *testing.T) *sql.DB {
	t.Helper()
	path := filepath.Join(t.TempDir(), ".sync.duckdb")
	db, err := sql.Open("duckdb", path)
	if err != nil {
		t.Fatalf("NewTempSyncDB open: %v", err)
	}
	db.SetMaxOpenConns(1)
	if err := db.Ping(); err != nil {
		db.Close()
		t.Fatalf("NewTempSyncDB ping: %v", err)
	}
	t.Cleanup(func() { db.Close() })
	return db
}
