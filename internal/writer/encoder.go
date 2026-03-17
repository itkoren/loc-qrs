package writer

import (
	"bytes"
	"encoding/csv"
	"encoding/json"
	"fmt"

	"github.com/itkoren/loc-qrs/internal/schema"
)

// Encoder encodes a validated record map into bytes (including trailing newline).
type Encoder interface {
	Encode(record map[string]any, s *schema.Schema) ([]byte, error)
}

// JSONLEncoder encodes records as JSON Lines.
type JSONLEncoder struct{}

// Encode marshals the record to JSON and appends a newline.
func (e *JSONLEncoder) Encode(record map[string]any, _ *schema.Schema) ([]byte, error) {
	b, err := json.Marshal(record)
	if err != nil {
		return nil, fmt.Errorf("jsonl encode: %w", err)
	}
	return append(b, '\n'), nil
}

// CSVEncoder encodes records as CSV lines (columns in schema declaration order).
type CSVEncoder struct{}

// Encode marshals the record to a CSV line.
func (e *CSVEncoder) Encode(record map[string]any, s *schema.Schema) ([]byte, error) {
	var buf bytes.Buffer
	w := csv.NewWriter(&buf)

	row := make([]string, len(s.Ordered))
	for i, col := range s.Ordered {
		v, ok := record[col.Name]
		if !ok || v == nil {
			row[i] = ""
			continue
		}
		row[i] = fmt.Sprintf("%v", v)
	}
	if err := w.Write(row); err != nil {
		return nil, fmt.Errorf("csv encode: %w", err)
	}
	w.Flush()
	if err := w.Error(); err != nil {
		return nil, fmt.Errorf("csv flush: %w", err)
	}
	return buf.Bytes(), nil
}

// NewEncoder returns the appropriate Encoder for the given format string.
func NewEncoder(format string) (Encoder, error) {
	switch format {
	case "jsonl", "":
		return &JSONLEncoder{}, nil
	case "csv":
		return &CSVEncoder{}, nil
	default:
		return nil, fmt.Errorf("unsupported format: %q", format)
	}
}
