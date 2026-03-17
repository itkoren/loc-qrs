package query_test

import (
	"testing"

	"github.com/itkoren/loc-qrs/internal/query"
	"github.com/stretchr/testify/assert"
)

// ── GuardSQL ──────────────────────────────────────────────────────────────────

func TestGuardSQL_AllowedStatements(t *testing.T) {
	allowed := []string{
		"SELECT * FROM records",
		"SELECT COUNT(*) FROM records WHERE id > 0",
		"SELECT id, name FROM records LIMIT 10",
		"SELECT * FROM records WHERE event_name = 'test'",
		"SELECT id FROM records ORDER BY id DESC",
		"SELECT MIN(value), MAX(value), AVG(value) FROM records",
		"SELECT * FROM records WHERE active = true",
		"WITH q AS (SELECT * FROM records) SELECT * FROM q",
		"SELECT * FROM records WHERE id IN (1,2,3)",
	}
	for _, sql := range allowed {
		t.Run(sql, func(t *testing.T) {
			assert.NoError(t, query.GuardSQL(sql))
		})
	}
}

func TestGuardSQL_BlockedStatements(t *testing.T) {
	blocked := []string{
		"INSERT INTO records VALUES (1)",
		"INSERT INTO t SELECT * FROM records",
		"DROP TABLE records",
		"DROP DATABASE mydb",
		"DELETE FROM records",
		"DELETE FROM records WHERE id = 1",
		"UPDATE records SET id = 1",
		"UPDATE records SET value = value + 1 WHERE id > 0",
		"CREATE TABLE foo (id INT)",
		"CREATE VIEW v AS SELECT * FROM records",
		"ALTER TABLE records ADD COLUMN x INT",
		"TRUNCATE records",
		"REPLACE INTO records VALUES (1)",
		"MERGE records USING src ON records.id = src.id",
		"GRANT SELECT ON records TO user",
		"REVOKE SELECT ON records FROM user",
		"ATTACH 'evil.db'",
		"DETACH mydb",
		"LOAD 'extension.so'",
		"IMPORT DATABASE 'backup'",
		"EXPORT DATABASE 'backup'",
		"COPY records TO 'out.csv'",
		"SET max_memory = '1GB'",
		"PRAGMA threads = 4",
		"CALL some_function()",
		"EXECUTE stmt",
	}
	for _, sql := range blocked {
		t.Run(sql, func(t *testing.T) {
			assert.Error(t, query.GuardSQL(sql), "expected %q to be blocked", sql)
		})
	}
}

func TestGuardSQL_SubstringFalsePositives(t *testing.T) {
	// Keywords embedded inside longer words must NOT be blocked.
	safeSQL := []string{
		// "INSERT" in a value string
		"SELECT * FROM records WHERE event_name = 'inserted_event'",
		// "DROP" in a column alias
		"SELECT value AS drop_rate FROM records",
		// "DELETE" as a column name value
		"SELECT * FROM records WHERE action = 'delete_handler'",
		// "TRUNCATED" contains "TRUNCATE"
		"SELECT * FROM records WHERE status = 'truncated'",
		// "CREATEDAT" contains "CREATE"
		"SELECT createdat FROM records",
		// "SETTINGS" contains "SET"
		"SELECT settings FROM records",
		// "EXECUTE" in a string
		"SELECT * FROM records WHERE name = 'execute_plan'",
	}
	for _, sql := range safeSQL {
		t.Run(sql, func(t *testing.T) {
			assert.NoError(t, query.GuardSQL(sql), "expected %q to be allowed", sql)
		})
	}
}

func TestGuardSQL_CaseInsensitive(t *testing.T) {
	// Blocked keywords should be detected regardless of case.
	cases := []string{
		"insert into records values (1)",
		"DROP TABLE records",
		"Delete FROM records",
		"uPdAtE records SET x = 1",
		"create table t (x int)",
	}
	for _, sql := range cases {
		t.Run(sql, func(t *testing.T) {
			assert.Error(t, query.GuardSQL(sql))
		})
	}
}

func TestGuardSQL_EmptyString(t *testing.T) {
	assert.NoError(t, query.GuardSQL(""))
}

func TestGuardSQL_WhitespaceOnly(t *testing.T) {
	assert.NoError(t, query.GuardSQL("   \t\n  "))
}

// ── GuardPath ─────────────────────────────────────────────────────────────────

func TestGuardPath_ValidPaths(t *testing.T) {
	dataDir := "/tmp/data"
	valid := []string{
		"/tmp/data/data_2026-03-17.parquet",
		"/tmp/data/data_2026-03-17.jsonl",
		"/tmp/data/subdir/file.parquet",
	}
	for _, p := range valid {
		assert.NoError(t, query.GuardPath(p, dataDir), "expected %q to be valid", p)
	}
}

func TestGuardPath_TraversalAttempts(t *testing.T) {
	dataDir := "/tmp/data"
	invalid := []string{
		"/tmp/data/../../../etc/passwd",
		"/etc/passwd",
		"/tmp/data/../data2/file",
		"/root/.ssh/id_rsa",
	}
	for _, p := range invalid {
		assert.Error(t, query.GuardPath(p, dataDir), "expected %q to be rejected", p)
	}
}

func TestGuardPath_ExactDataDir(t *testing.T) {
	// Path equal to dataDir itself should be allowed (edge case).
	assert.NoError(t, query.GuardPath("/tmp/data", "/tmp/data"))
}
