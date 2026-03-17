package writer

import (
	"fmt"
	"path/filepath"
	"time"
)

// RollEvent is sent on rollCh when the active file has been rotated.
// OldPath is the path of the file that was just closed.
type RollEvent struct {
	OldPath string
	Date    string // YYYY-MM-DD
}

// DailyRotator tracks the current date and generates file paths.
type DailyRotator struct {
	dataDir string
	ext     string
	current string // current YYYY-MM-DD
}

// NewDailyRotator creates a rotator for the given data directory and file extension.
func NewDailyRotator(dataDir, ext string) *DailyRotator {
	return &DailyRotator{dataDir: dataDir, ext: ext}
}

// FilePath returns the data file path for the given date string.
func (r *DailyRotator) FilePath(date string) string {
	return filepath.Join(r.dataDir, fmt.Sprintf("data_%s.%s", date, r.ext))
}

// CurrentDate returns the date string for now (UTC).
func CurrentDate() string {
	return time.Now().UTC().Format("2006-01-02")
}

// Check returns the current file path. If the date has changed since last call,
// it also returns a RollEvent with the old path.
func (r *DailyRotator) Check() (currentPath string, roll *RollEvent) {
	today := CurrentDate()
	if r.current == "" {
		r.current = today
		return r.FilePath(today), nil
	}
	if r.current == today {
		return r.FilePath(today), nil
	}
	old := r.current
	r.current = today
	return r.FilePath(today), &RollEvent{
		OldPath: r.FilePath(old),
		Date:    old,
	}
}
