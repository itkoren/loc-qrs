package schema

import (
	"encoding/json"
	"fmt"
	"math"
	"strconv"
)

// ValidationError describes a single field validation failure.
type ValidationError struct {
	Field   string
	Message string
}

func (e ValidationError) Error() string {
	return fmt.Sprintf("field %q: %s", e.Field, e.Message)
}

// ValidateRecord validates a decoded JSON record map against the schema.
// It returns a slice of ValidationErrors (nil means valid).
func (s *Schema) ValidateRecord(record map[string]any) []ValidationError {
	var errs []ValidationError

	for _, col := range s.Ordered {
		val, ok := record[col.Name]
		if !ok {
			// Missing fields are allowed (nullable).
			continue
		}
		if err := validateValue(col.Name, col.Type, val); err != nil {
			errs = append(errs, *err)
		}
	}

	// Check for unknown fields.
	for k := range record {
		if _, known := s.Columns[k]; !known {
			errs = append(errs, ValidationError{
				Field:   k,
				Message: "unknown field not in schema",
			})
		}
	}

	return errs
}

func validateValue(field string, typ ColumnType, val any) *ValidationError {
	if val == nil {
		return nil
	}
	fail := func(msg string) *ValidationError {
		return &ValidationError{Field: field, Message: msg}
	}

	switch typ {
	case "BOOLEAN":
		if _, ok := val.(bool); !ok {
			return fail("expected boolean")
		}
	case "TINYINT", "SMALLINT", "INTEGER", "BIGINT", "UBIGINT", "HUGEINT", "UINTEGER":
		switch v := val.(type) {
		case float64:
			if v != math.Trunc(v) {
				return fail(fmt.Sprintf("expected integer, got %v", v))
			}
			if typ == "UBIGINT" || typ == "UINTEGER" {
				if v < 0 {
					return fail("expected unsigned integer, got negative value")
				}
			}
		case json.Number:
			if _, err := strconv.ParseInt(v.String(), 10, 64); err != nil {
				if _, err2 := strconv.ParseUint(v.String(), 10, 64); err2 != nil {
					return fail("expected integer number")
				}
			}
		case string:
			// Allow numeric strings for flexibility.
			if _, err := strconv.ParseInt(v, 10, 64); err != nil {
				if _, err2 := strconv.ParseUint(v, 10, 64); err2 != nil {
					return fail("expected integer, got non-numeric string")
				}
			}
		default:
			return fail(fmt.Sprintf("expected integer type, got %T", val))
		}
	case "FLOAT", "DOUBLE":
		switch val.(type) {
		case float64, json.Number:
			// ok
		default:
			return fail(fmt.Sprintf("expected float, got %T", val))
		}
	case "VARCHAR", "TEXT", "BLOB", "UUID", "JSON":
		if _, ok := val.(string); !ok {
			return fail(fmt.Sprintf("expected string, got %T", val))
		}
	case "DATE", "TIME", "TIMESTAMP", "INTERVAL":
		if _, ok := val.(string); !ok {
			return fail(fmt.Sprintf("expected string for %s, got %T", typ, val))
		}
	}
	return nil
}

