package query

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
)

// BuildQuery wraps the user SQL in a CTE that unions:
//   - Historical Parquet files (all dates except the live file's date)
//   - The live JSONL/CSV file (always read directly to avoid double-counting)
//
// The combined data is exposed as the "records" alias for the user SQL.
// currentFile should be the path of the active (today's) write file.
func BuildQuery(userSQL, dataDir, currentFile string) (string, error) {
	// Determine the date of the live file to exclude its parquet counterpart.
	liveDate := liveDateFromPath(currentFile)

	// Find all parquet files, excluding the one matching the live file's date.
	// filepath.Glob only errors on malformed patterns; the pattern here is a static constant.
	allParquets, _ := filepath.Glob(filepath.Join(dataDir, "data_*.parquet"))
	var historicalParquets []string
	for _, p := range allParquets {
		base := filepath.Base(p) // data_YYYY-MM-DD.parquet
		if liveDate == "" || !strings.Contains(base, liveDate) {
			historicalParquets = append(historicalParquets, p)
		}
	}

	// Check if current live file exists and is non-empty.
	hasLive := false
	if currentFile != "" {
		info, err := os.Stat(currentFile)
		hasLive = err == nil && info.Size() > 0
	}

	liveFormat := "jsonl"
	if strings.HasSuffix(currentFile, ".csv") {
		liveFormat = "csv"
	}

	var parts []string

	if len(historicalParquets) > 0 {
		quotedPaths := make([]string, len(historicalParquets))
		for i, p := range historicalParquets {
			quotedPaths[i] = sqlPathQuote(p)
		}
		if len(quotedPaths) == 1 {
			parts = append(parts, fmt.Sprintf(
				"_hist AS (SELECT * FROM read_parquet(%s))",
				quotedPaths[0],
			))
		} else {
			parts = append(parts, fmt.Sprintf(
				"_hist AS (SELECT * FROM read_parquet([%s], union_by_name=true))",
				strings.Join(quotedPaths, ", "),
			))
		}
	}

	if hasLive {
		if liveFormat == "csv" {
			parts = append(parts, fmt.Sprintf(
				"_live AS (SELECT * FROM read_csv_auto(%s, header=false))",
				sqlPathQuote(currentFile),
			))
		} else {
			parts = append(parts, fmt.Sprintf(
				"_live AS (SELECT * FROM read_json_auto(%s, format='newline_delimited'))",
				sqlPathQuote(currentFile),
			))
		}
	}

	if len(parts) == 0 {
		return fmt.Sprintf("WITH records AS (SELECT NULL LIMIT 0) %s", userSQL), nil
	}

	var unions []string
	if len(historicalParquets) > 0 {
		unions = append(unions, "SELECT * FROM _hist")
	}
	if hasLive {
		unions = append(unions, "SELECT * FROM _live")
	}
	parts = append(parts, fmt.Sprintf("records AS (%s)", strings.Join(unions, " UNION ALL ")))

	return "WITH " + strings.Join(parts, ", ") + " " + userSQL, nil
}

// liveDateFromPath extracts the YYYY-MM-DD date segment from a data file path.
// E.g. "data/data_2026-03-17.jsonl" → "2026-03-17".
// Returns empty string if the pattern is not found.
func liveDateFromPath(path string) string {
	if path == "" {
		return ""
	}
	base := filepath.Base(path)
	// Expected: "data_YYYY-MM-DD.ext"
	base = strings.TrimPrefix(base, "data_")
	ext := filepath.Ext(base)
	return strings.TrimSuffix(base, ext)
}

// sqlPathQuote quotes a file path for DuckDB.
func sqlPathQuote(s string) string {
	return "'" + strings.ReplaceAll(s, "'", "''") + "'"
}
