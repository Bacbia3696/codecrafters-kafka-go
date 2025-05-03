package logger

import (
	"log/slog"
	"os"
	"strings"
	"time"

	"github.com/lmittmann/tint"
)

// DefaultLevel is the log level used if LOG_LEVEL is not set or invalid.
const DefaultLevel = slog.LevelDebug

// New initializes a new slog.Logger with configuration from environment variables.
func New() *slog.Logger {
	levelStr := os.Getenv("LOG_LEVEL")
	level := parseLevel(levelStr)

	handler := tint.NewHandler(os.Stderr, &tint.Options{
		Level:      level,        // Log all levels
		TimeFormat: time.Kitchen, // Prettier time format
		AddSource:  true,         // Add source file and line number
	})

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
			panic("invalid log level: " + levelStr)
		}
		return DefaultLevel
	}
}
