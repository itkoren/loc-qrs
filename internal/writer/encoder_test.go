package writer_test

import (
	"strings"
	"testing"

	"github.com/itkoren/loc-qrs/internal/schema"
	"github.com/itkoren/loc-qrs/internal/writer"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestJSONLEncoder(t *testing.T) {
	sch, err := schema.Parse([]byte(`{"columns":{"id":"UBIGINT","name":"VARCHAR"}}`))
	require.NoError(t, err)

	enc := &writer.JSONLEncoder{}
	rec := map[string]any{"id": float64(1), "name": "test"}

	b, err := enc.Encode(rec, sch)
	require.NoError(t, err)
	assert.True(t, strings.HasSuffix(string(b), "\n"))
	assert.Contains(t, string(b), `"id"`)
	assert.Contains(t, string(b), `"name"`)
}

func TestCSVEncoder(t *testing.T) {
	sch, err := schema.Parse([]byte(`{"columns":{"id":"UBIGINT","name":"VARCHAR"},"format":"csv"}`))
	require.NoError(t, err)

	enc := &writer.CSVEncoder{}
	rec := map[string]any{"id": float64(1), "name": "hello, world"}

	b, err := enc.Encode(rec, sch)
	require.NoError(t, err)
	result := string(b)
	assert.True(t, strings.HasSuffix(result, "\n"))
	// CSV encoder should quote fields with commas
	assert.Contains(t, result, `"hello, world"`)
}

func TestNewEncoder(t *testing.T) {
	_, err := writer.NewEncoder("jsonl")
	assert.NoError(t, err)

	_, err = writer.NewEncoder("csv")
	assert.NoError(t, err)

	_, err = writer.NewEncoder("")
	assert.NoError(t, err)

	_, err = writer.NewEncoder("xml")
	assert.Error(t, err)
}
