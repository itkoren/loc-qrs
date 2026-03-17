package schema_test

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/itkoren/loc-qrs/internal/schema"
)

// ── Parse ─────────────────────────────────────────────────────────────────────

func TestParse_ValidJSONL(t *testing.T) {
	raw := `{"columns":{"id":"UBIGINT","name":"VARCHAR"},"format":"jsonl"}`
	s, err := schema.Parse([]byte(raw))
	require.NoError(t, err)
	assert.Equal(t, 2, len(s.Columns))
	assert.Equal(t, "jsonl", s.Format)
	assert.Equal(t, []string{"id", "name"}, s.ColumnNames())
	assert.NotEmpty(t, s.Version)
}

func TestParse_ValidCSV(t *testing.T) {
	raw := `{"columns":{"x":"INTEGER"},"format":"csv"}`
	s, err := schema.Parse([]byte(raw))
	require.NoError(t, err)
	assert.Equal(t, "csv", s.Format)
}

func TestParse_PreservesColumnOrder(t *testing.T) {
	raw := `{"columns":{"z":"VARCHAR","a":"INTEGER","m":"DOUBLE"},"format":"jsonl"}`
	s, err := schema.Parse([]byte(raw))
	require.NoError(t, err)
	assert.Equal(t, []string{"z", "a", "m"}, s.ColumnNames())
}

func TestParse_DefaultFormat(t *testing.T) {
	raw := `{"columns":{"id":"UBIGINT"}}`
	s, err := schema.Parse([]byte(raw))
	require.NoError(t, err)
	assert.Equal(t, "jsonl", s.Format)
}

func TestParse_UnsupportedFormat(t *testing.T) {
	raw := `{"columns":{"id":"UBIGINT"},"format":"xml"}`
	_, err := schema.Parse([]byte(raw))
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "xml")
}

func TestParse_UnsupportedColumnType(t *testing.T) {
	raw := `{"columns":{"id":"FOOBAR"}}`
	_, err := schema.Parse([]byte(raw))
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "FOOBAR")
}

func TestParse_EmptyColumns(t *testing.T) {
	raw := `{"columns":{}}`
	_, err := schema.Parse([]byte(raw))
	assert.Error(t, err)
}

func TestParse_VersionIsSHA256(t *testing.T) {
	raw := `{"columns":{"id":"UBIGINT"}}`
	s, err := schema.Parse([]byte(raw))
	require.NoError(t, err)
	assert.Len(t, s.Version, 64)
	// Version must be hex characters only.
	for _, c := range s.Version {
		assert.True(t, (c >= '0' && c <= '9') || (c >= 'a' && c <= 'f'),
			"non-hex char in version: %c", c)
	}
}

func TestParse_VersionChangesWithContent(t *testing.T) {
	s1, err := schema.Parse([]byte(`{"columns":{"id":"UBIGINT"}}`))
	require.NoError(t, err)
	s2, err := schema.Parse([]byte(`{"columns":{"id":"BIGINT"}}`))
	require.NoError(t, err)
	assert.NotEqual(t, s1.Version, s2.Version)
}

func TestParse_VersionStableForSameContent(t *testing.T) {
	raw := `{"columns":{"id":"UBIGINT"}}`
	s1, err := schema.Parse([]byte(raw))
	require.NoError(t, err)
	s2, err := schema.Parse([]byte(raw))
	require.NoError(t, err)
	assert.Equal(t, s1.Version, s2.Version)
}

func TestParse_DuplicateColumn(t *testing.T) {
	// Go's json.Decoder does not raise errors on duplicate keys by default,
	// but our ordered parser should catch them.
	raw := `{"columns":{"id":"UBIGINT","id":"BIGINT"}}`
	_, err := schema.Parse([]byte(raw))
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "duplicate")
}

func TestParse_AllValidTypes(t *testing.T) {
	types := []string{
		"BOOLEAN", "TINYINT", "SMALLINT", "INTEGER", "BIGINT",
		"UBIGINT", "FLOAT", "DOUBLE", "VARCHAR", "TEXT",
		"BLOB", "DATE", "TIME", "TIMESTAMP", "INTERVAL",
		"UUID", "JSON", "HUGEINT", "UINTEGER",
	}
	for _, typ := range types {
		t.Run(typ, func(t *testing.T) {
			raw := `{"columns":{"col":"` + typ + `"}}`
			_, err := schema.Parse([]byte(raw))
			assert.NoError(t, err, "type %s should be valid", typ)
		})
	}
}

func TestParse_InvalidJSON(t *testing.T) {
	_, err := schema.Parse([]byte(`not json`))
	assert.Error(t, err)
}

func TestParse_ManyColumns(t *testing.T) {
	// Generate a schema with 20 columns and verify order is preserved.
	var parts []string
	var expectedOrder []string
	for i := 0; i < 20; i++ {
		name := string(rune('a'+i%26)) + string(rune('0'+i/26))
		parts = append(parts, `"`+name+`":"VARCHAR"`)
		expectedOrder = append(expectedOrder, name)
	}
	raw := `{"columns":{` + strings.Join(parts, ",") + `}}`
	s, err := schema.Parse([]byte(raw))
	require.NoError(t, err)
	assert.Equal(t, expectedOrder, s.ColumnNames())
}

// ── ValidateRecord ────────────────────────────────────────────────────────────

func TestValidateRecord_ValidRecord(t *testing.T) {
	sch := mustSchema(t, `{"columns":{"id":"UBIGINT","name":"VARCHAR","score":"DOUBLE","active":"BOOLEAN"}}`)
	rec := map[string]any{"id": float64(1), "name": "test", "score": 9.5, "active": true}
	assert.Empty(t, sch.ValidateRecord(rec))
}

func TestValidateRecord_UnknownField(t *testing.T) {
	sch := mustSchema(t, `{"columns":{"id":"UBIGINT"}}`)
	errs := sch.ValidateRecord(map[string]any{"id": float64(1), "extra": "oops"})
	require.Len(t, errs, 1)
	assert.Contains(t, errs[0].Error(), "extra")
	assert.Contains(t, errs[0].Error(), "unknown")
}

func TestValidateRecord_MultipleUnknownFields(t *testing.T) {
	sch := mustSchema(t, `{"columns":{"id":"UBIGINT"}}`)
	errs := sch.ValidateRecord(map[string]any{"x": 1, "y": 2})
	assert.Len(t, errs, 2)
}

func TestValidateRecord_WrongTypeDouble(t *testing.T) {
	sch := mustSchema(t, `{"columns":{"v":"DOUBLE"}}`)
	errs := sch.ValidateRecord(map[string]any{"v": "notanumber"})
	require.Len(t, errs, 1)
	assert.Contains(t, errs[0].Error(), "v")
}

func TestValidateRecord_WrongTypeBoolean(t *testing.T) {
	sch := mustSchema(t, `{"columns":{"flag":"BOOLEAN"}}`)
	errs := sch.ValidateRecord(map[string]any{"flag": "yes"})
	require.Len(t, errs, 1)
}

func TestValidateRecord_WrongTypeInteger(t *testing.T) {
	sch := mustSchema(t, `{"columns":{"n":"INTEGER"}}`)
	errs := sch.ValidateRecord(map[string]any{"n": true})
	require.Len(t, errs, 1)
}

func TestValidateRecord_UnsignedNegative(t *testing.T) {
	sch := mustSchema(t, `{"columns":{"id":"UBIGINT"}}`)
	errs := sch.ValidateRecord(map[string]any{"id": float64(-1)})
	require.Len(t, errs, 1)
	assert.Contains(t, errs[0].Error(), "unsigned")
}

func TestValidateRecord_NilValueAllowed(t *testing.T) {
	sch := mustSchema(t, `{"columns":{"id":"UBIGINT"}}`)
	assert.Empty(t, sch.ValidateRecord(map[string]any{"id": nil}))
}

func TestValidateRecord_MissingFieldsAllowed(t *testing.T) {
	sch := mustSchema(t, `{"columns":{"id":"UBIGINT","name":"VARCHAR"}}`)
	assert.Empty(t, sch.ValidateRecord(map[string]any{}))
}

func TestValidateRecord_EmptyRecord(t *testing.T) {
	sch := mustSchema(t, `{"columns":{"id":"UBIGINT"}}`)
	assert.Empty(t, sch.ValidateRecord(map[string]any{}))
}

func TestValidateRecord_IntegerTypes(t *testing.T) {
	for _, typ := range []string{"TINYINT", "SMALLINT", "INTEGER", "BIGINT", "UINTEGER"} {
		t.Run(typ, func(t *testing.T) {
			sch := mustSchema(t, `{"columns":{"n":"`+typ+`"}}`)
			assert.Empty(t, sch.ValidateRecord(map[string]any{"n": float64(42)}))
		})
	}
}

func TestValidateRecord_NonIntegerFloat(t *testing.T) {
	sch := mustSchema(t, `{"columns":{"n":"INTEGER"}}`)
	errs := sch.ValidateRecord(map[string]any{"n": float64(1.5)})
	require.Len(t, errs, 1)
	assert.Contains(t, errs[0].Error(), "integer")
}

func TestValidateRecord_DateAsString(t *testing.T) {
	for _, typ := range []string{"DATE", "TIME", "TIMESTAMP", "INTERVAL"} {
		t.Run(typ, func(t *testing.T) {
			sch := mustSchema(t, `{"columns":{"t":"`+typ+`"}}`)
			assert.Empty(t, sch.ValidateRecord(map[string]any{"t": "2026-03-17"}))
		})
	}
}

func TestValidateRecord_DateNotString(t *testing.T) {
	sch := mustSchema(t, `{"columns":{"t":"TIMESTAMP"}}`)
	errs := sch.ValidateRecord(map[string]any{"t": float64(12345)})
	require.Len(t, errs, 1)
}

func TestValidateRecord_JSONColumn(t *testing.T) {
	sch := mustSchema(t, `{"columns":{"meta":"JSON"}}`)
	assert.Empty(t, sch.ValidateRecord(map[string]any{"meta": `{"key":"value"}`}))
}

func TestValidateRecord_UUIDColumn(t *testing.T) {
	sch := mustSchema(t, `{"columns":{"uid":"UUID"}}`)
	assert.Empty(t, sch.ValidateRecord(map[string]any{"uid": "550e8400-e29b-41d4-a716-446655440000"}))
}

func TestValidationError_Error(t *testing.T) {
	e := schema.ValidationError{Field: "myfield", Message: "is broken"}
	assert.Equal(t, `field "myfield": is broken`, e.Error())
}

// ── Helpers ───────────────────────────────────────────────────────────────────

func mustSchema(t *testing.T, raw string) *schema.Schema {
	t.Helper()
	s, err := schema.Parse([]byte(raw))
	require.NoError(t, err)
	return s
}
