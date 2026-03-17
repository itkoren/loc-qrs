package writer_test

import (
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/itkoren/loc-qrs/internal/writer"
)

func TestDailyRotator_FilePath(t *testing.T) {
	r := writer.NewDailyRotator("/data", "jsonl")
	path := r.FilePath("2026-03-17")
	assert.Equal(t, "/data/data_2026-03-17.jsonl", path)
}

func TestDailyRotator_FilePath_CSV(t *testing.T) {
	r := writer.NewDailyRotator("/data", "csv")
	path := r.FilePath("2026-01-01")
	assert.True(t, strings.HasSuffix(path, ".csv"))
	assert.Contains(t, path, "2026-01-01")
}

func TestDailyRotator_FirstCheck_NoRoll(t *testing.T) {
	r := writer.NewDailyRotator(t.TempDir(), "jsonl")
	path, roll := r.Check()
	assert.NotEmpty(t, path)
	assert.Nil(t, roll, "first check should not produce a roll event")
}

func TestDailyRotator_SameDayCheck_NoRoll(t *testing.T) {
	r := writer.NewDailyRotator(t.TempDir(), "jsonl")
	// Prime the rotator.
	_, _ = r.Check()
	// Second call on same day.
	_, roll := r.Check()
	assert.Nil(t, roll, "same-day check should not produce a roll event")
}

func TestCurrentDate_Format(t *testing.T) {
	d := writer.CurrentDate()
	_, err := time.Parse("2006-01-02", d)
	require.NoError(t, err, "CurrentDate should return YYYY-MM-DD format")
}

func TestDailyRotator_PathContainsToday(t *testing.T) {
	r := writer.NewDailyRotator("/tmp/data", "jsonl")
	path, _ := r.Check()
	today := writer.CurrentDate()
	assert.Contains(t, path, today)
}
