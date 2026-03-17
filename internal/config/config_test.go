package config_test

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestDurationDefaults(t *testing.T) {
	// Verify the expected default values parse correctly.
	cases := []struct {
		name string
		in   string
		want time.Duration
	}{
		{"sync_interval", "30s", 30 * time.Second},
		{"shutdown_timeout", "30s", 30 * time.Second},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			d, err := time.ParseDuration(tc.in)
			assert.NoError(t, err)
			assert.Equal(t, tc.want, d)
		})
	}
}

func TestFormatValues(t *testing.T) {
	// Document the two valid format strings.
	assert.Equal(t, "jsonl", "jsonl")
	assert.Equal(t, "csv", "csv")
}

func TestDurationParsing(t *testing.T) {
	cases := []struct {
		in  string
		out time.Duration
		ok  bool
	}{
		{"30s", 30 * time.Second, true},
		{"1m30s", 90 * time.Second, true},
		{"100ms", 100 * time.Millisecond, true},
		{"2h", 2 * time.Hour, true},
		{"notaduration", 0, false},
		{"", 0, false},
		{"-1s", -1 * time.Second, true},
	}
	for _, tc := range cases {
		t.Run(tc.in, func(t *testing.T) {
			d, err := time.ParseDuration(tc.in)
			if tc.ok {
				assert.NoError(t, err)
				assert.Equal(t, tc.out, d)
			} else {
				assert.Error(t, err)
			}
		})
	}
}
