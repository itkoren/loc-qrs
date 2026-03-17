package schema

import (
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"os"
)

// ColumnType is a DuckDB column type string (e.g. "VARCHAR", "UBIGINT").
type ColumnType string

// Column represents an ordered column definition.
type Column struct {
	Name string
	Type ColumnType
}

// Schema is the parsed and validated record schema.
type Schema struct {
	// Columns maps column name → DuckDB type.
	Columns map[string]ColumnType
	// Ordered preserves declaration order for CSV and DDL generation.
	Ordered []Column
	// Version is the SHA256 hex of the raw schema.json bytes.
	Version string
	// Format is "jsonl" or "csv".
	Format string
}

// raw is used only for JSON unmarshalling.
type raw struct {
	Columns map[string]string `json:"columns"`
	Format  string            `json:"format"`
	// columnOrder preserves insertion order via a separate key (Go maps are unordered).
}

// validTypes is the set of allowed DuckDB column types.
var validTypes = map[string]bool{
	"BOOLEAN": true, "TINYINT": true, "SMALLINT": true, "INTEGER": true,
	"BIGINT": true, "UBIGINT": true, "FLOAT": true, "DOUBLE": true,
	"VARCHAR": true, "TEXT": true, "BLOB": true, "DATE": true,
	"TIME": true, "TIMESTAMP": true, "INTERVAL": true, "UUID": true,
	"JSON": true, "HUGEINT": true, "UINTEGER": true,
}

// Load reads schema.json from path and returns a parsed Schema.
func Load(path string) (*Schema, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read schema: %w", err)
	}
	return Parse(data)
}

// Parse parses schema JSON bytes.
func Parse(data []byte) (*Schema, error) {
	// We use a json.Decoder to detect duplicate keys via a custom approach.
	// For simplicity we use json.RawMessage to get ordered keys.
	var rawMsg struct {
		Columns json.RawMessage `json:"columns"`
		Format  string          `json:"format"`
	}
	if err := json.Unmarshal(data, &rawMsg); err != nil {
		return nil, fmt.Errorf("parse schema JSON: %w", err)
	}

	if rawMsg.Format == "" {
		rawMsg.Format = "jsonl"
	}
	if rawMsg.Format != "jsonl" && rawMsg.Format != "csv" {
		return nil, fmt.Errorf("unsupported format %q: must be jsonl or csv", rawMsg.Format)
	}

	// Parse columns preserving order via json.Decoder token streaming.
	ordered, colMap, err := parseOrderedColumns(rawMsg.Columns)
	if err != nil {
		return nil, err
	}
	if len(ordered) == 0 {
		return nil, fmt.Errorf("schema must define at least one column")
	}

	hash := sha256.Sum256(data)
	return &Schema{
		Columns: colMap,
		Ordered: ordered,
		Version: fmt.Sprintf("%x", hash),
		Format:  rawMsg.Format,
	}, nil
}

// ColumnNames returns column names in declaration order.
func (s *Schema) ColumnNames() []string {
	names := make([]string, len(s.Ordered))
	for i, c := range s.Ordered {
		names[i] = c.Name
	}
	return names
}

// parseOrderedColumns decodes a JSON object preserving key insertion order.
func parseOrderedColumns(raw json.RawMessage) ([]Column, map[string]ColumnType, error) {
	dec := json.NewDecoder(bytesReader(raw))
	// consume '{'
	tok, err := dec.Token()
	if err != nil {
		return nil, nil, fmt.Errorf("columns must be a JSON object: %w", err)
	}
	if d, ok := tok.(json.Delim); !ok || d != '{' {
		return nil, nil, fmt.Errorf("columns must be a JSON object")
	}

	var ordered []Column
	colMap := make(map[string]ColumnType)

	for dec.More() {
		// key
		keyTok, err := dec.Token()
		if err != nil {
			return nil, nil, fmt.Errorf("reading column key: %w", err)
		}
		name, ok := keyTok.(string)
		if !ok {
			return nil, nil, fmt.Errorf("column key must be a string")
		}
		// value
		var typStr string
		if err := dec.Decode(&typStr); err != nil {
			return nil, nil, fmt.Errorf("reading type for column %q: %w", name, err)
		}
		if !validTypes[typStr] {
			return nil, nil, fmt.Errorf("column %q: unsupported type %q", name, typStr)
		}
		if _, dup := colMap[name]; dup {
			return nil, nil, fmt.Errorf("duplicate column %q", name)
		}
		col := Column{Name: name, Type: ColumnType(typStr)}
		ordered = append(ordered, col)
		colMap[name] = ColumnType(typStr)
	}
	return ordered, colMap, nil
}

// bytesReader wraps []byte for json.NewDecoder.
type bytesReaderT struct {
	data []byte
	pos  int
}

func (r *bytesReaderT) Read(p []byte) (int, error) {
	if r.pos >= len(r.data) {
		return 0, fmt.Errorf("EOF")
	}
	n := copy(p, r.data[r.pos:])
	r.pos += n
	return n, nil
}

func bytesReader(b []byte) *bytesReaderT {
	return &bytesReaderT{data: b}
}
