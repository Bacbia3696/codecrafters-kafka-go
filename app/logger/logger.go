package logger

import (
	"log/slog"
	"os"
	"strings"
)

// DefaultLevel is the log level used if LOG_LEVEL is not set or invalid.
const DefaultLevel = slog.LevelInfo

// New initializes a new slog.Logger with configuration from environment variables.
func New() *slog.Logger {
	levelStr := os.Getenv("LOG_LEVEL")
	level := parseLevel(levelStr)

	opts := &slog.HandlerOptions{
		Level:     level,
		AddSource: true, // Uncomment to include source file and line number
	}

	// handler := slog.NewTextHandler(os.Stderr, opts)
	// Alternatively, use JSONHandler:
	handler := slog.NewJSONHandler(os.Stderr, opts)

	logger := slog.New(handler)
	return logger
}

// parseLevel converts a string level (e.g., "debug", "info") to slog.Level.
func parseLevel(levelStr string) slog.Level {
	switch strings.ToLower(levelStr) {
	case "debug":
		return slog.LevelDebug
	case "info":
		return slog.LevelInfo
	case "warn", "warning":
		return slog.LevelWarn
	case "error", "err":
		return slog.LevelError
	default:
		if levelStr != "" {
			// Log a warning using the default logger if the level is invalid?
			// Or just silently use the default. Let's be silent for now.
		}
		return DefaultLevel
	}
}
