package sync

import (
	"database/sql"
	"fmt"
	"os"
	"path/filepath"

	_ "github.com/marcboeker/go-duckdb"
)

// OpenSyncDB opens a read-write DuckDB instance for sync operations.
// Use ":memory:" for an in-memory database.
func OpenSyncDB(path string) (*sql.DB, error) {
	db, err := sql.Open("duckdb", path)
	if err != nil {
		return nil, fmt.Errorf("open sync duckdb %s: %w", path, err)
	}
	// DuckDB is embedded; ping to verify.
	if err := db.Ping(); err != nil {
		db.Close()
		return nil, fmt.Errorf("ping sync duckdb: %w", err)
	}
	// Single writer connection to avoid WAL contention.
	db.SetMaxOpenConns(1)
	return db, nil
}

// OpenQueryDB opens a read-only DuckDB instance for query operations.
func OpenQueryDB() (*sql.DB, error) {
	// Read-only in-memory DuckDB: we open a fresh connection without a file.
	// DuckDB supports read-only mode via connection string.
	db, err := sql.Open("duckdb", "")
	if err != nil {
		return nil, fmt.Errorf("open query duckdb: %w", err)
	}
	if err := db.Ping(); err != nil {
		db.Close()
		return nil, fmt.Errorf("ping query duckdb: %w", err)
	}
	return db, nil
}

// CopyToParquet converts a JSONL file to Parquet using DuckDB.
// It writes to a .tmp file then atomically renames it.
func CopyToParquet(db *sql.DB, srcJSONL, dstParquet string) error {
	tmpPath := dstParquet + ".tmp"

	// Remove stale tmp file if any.
	_ = os.Remove(tmpPath)

	query := fmt.Sprintf(
		`COPY (SELECT * FROM read_json_auto(%s, format='newline_delimited')) TO %s (FORMAT PARQUET)`,
		sqlQuote(srcJSONL),
		sqlQuote(tmpPath),
	)
	if _, err := db.Exec(query); err != nil {
		return fmt.Errorf("copy to parquet: %w", err)
	}

	// POSIX atomic rename.
	if err := os.Rename(tmpPath, dstParquet); err != nil {
		_ = os.Remove(tmpPath)
		return fmt.Errorf("rename parquet: %w", err)
	}
	return nil
}

// CopyCSVToParquet converts a CSV file to Parquet.
func CopyCSVToParquet(db *sql.DB, srcCSV, dstParquet string, header bool) error {
	tmpPath := dstParquet + ".tmp"
	_ = os.Remove(tmpPath)

	headerStr := "FALSE"
	if header {
		headerStr = "TRUE"
	}

	query := fmt.Sprintf(
		`COPY (SELECT * FROM read_csv_auto(%s, header=%s)) TO %s (FORMAT PARQUET)`,
		sqlQuote(srcCSV),
		headerStr,
		sqlQuote(tmpPath),
	)
	if _, err := db.Exec(query); err != nil {
		return fmt.Errorf("copy csv to parquet: %w", err)
	}
	if err := os.Rename(tmpPath, dstParquet); err != nil {
		_ = os.Remove(tmpPath)
		return fmt.Errorf("rename parquet: %w", err)
	}
	return nil
}

// ParquetPathForSource returns the .parquet path for a given source data file.
func ParquetPathForSource(srcPath string) string {
	ext := filepath.Ext(srcPath)
	return srcPath[:len(srcPath)-len(ext)] + ".parquet"
}

// sqlQuote wraps a file path in single quotes for SQL, escaping internal quotes.
func sqlQuote(s string) string {
	escaped := ""
	for _, c := range s {
		if c == '\'' {
			escaped += "''"
		} else {
			escaped += string(c)
		}
	}
	return "'" + escaped + "'"
}
