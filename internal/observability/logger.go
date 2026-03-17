package observability

import (
	"log/slog"
	"os"
	"strings"
)

// SetupLogger configures the global slog logger.
// level should be "debug", "info", "warn", or "error".
func SetupLogger(level string) *slog.Logger {
	var l slog.Level
	switch strings.ToLower(level) {
	case "debug":
		l = slog.LevelDebug
	case "warn", "warning":
		l = slog.LevelWarn
	case "error":
		l = slog.LevelError
	default:
		l = slog.LevelInfo
	}

	handler := slog.NewJSONHandler(os.Stderr, &slog.HandlerOptions{
		Level: l,
	})
	logger := slog.New(handler)
	slog.SetDefault(logger)
	return logger
}
