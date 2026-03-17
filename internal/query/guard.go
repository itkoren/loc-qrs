package query

import (
	"fmt"
	"path/filepath"
	"strings"
)

// blockedKeywords are SQL statements that must not appear in user queries.
var blockedKeywords = []string{
	"INSERT", "UPDATE", "DELETE", "DROP", "CREATE", "ALTER",
	"TRUNCATE", "REPLACE", "MERGE", "GRANT", "REVOKE",
	"ATTACH", "DETACH", "LOAD", "IMPORT", "EXPORT",
	"COPY", "SET", "PRAGMA", "CALL", "EXECUTE",
}

// GuardSQL validates that the SQL is safe to execute against the query engine.
func GuardSQL(sql string) error {
	upper := strings.ToUpper(sql)
	for _, kw := range blockedKeywords {
		// Check for keyword followed by whitespace or end of string to avoid
		// false positives on substrings (e.g. "TRUNCATED" should not match "TRUNCATE").
		if containsKeyword(upper, kw) {
			return fmt.Errorf("forbidden SQL keyword: %s", kw)
		}
	}
	return nil
}

// containsKeyword checks if the uppercase SQL contains the keyword as a token.
func containsKeyword(upperSQL, keyword string) bool {
	idx := 0
	for {
		pos := strings.Index(upperSQL[idx:], keyword)
		if pos < 0 {
			return false
		}
		abs := idx + pos
		// Check character before keyword (must be non-alpha or start of string).
		before := abs == 0 || !isAlpha(upperSQL[abs-1])
		// Check character after keyword (must be non-alpha or end of string).
		end := abs + len(keyword)
		after := end >= len(upperSQL) || !isAlpha(upperSQL[end])
		if before && after {
			return true
		}
		idx = abs + 1
		if idx >= len(upperSQL) {
			return false
		}
	}
}

func isAlpha(c byte) bool {
	return (c >= 'A' && c <= 'Z') || (c >= 'a' && c <= 'z') || (c >= '0' && c <= '9') || c == '_'
}

// GuardPath validates that a file path is within the expected data directory.
func GuardPath(path, dataDir string) error {
	abs, err := filepath.Abs(path)
	if err != nil {
		return fmt.Errorf("invalid path: %w", err)
	}
	absData, err := filepath.Abs(dataDir)
	if err != nil {
		return fmt.Errorf("invalid data dir: %w", err)
	}
	if !strings.HasPrefix(abs, absData+string(filepath.Separator)) && abs != absData {
		return fmt.Errorf("path traversal detected: %s is outside data directory", path)
	}
	return nil
}
