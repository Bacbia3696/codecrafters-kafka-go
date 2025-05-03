package logger

import (
	"os"
	"time"

	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
)

// Setup initializes the global logger with the given configuration
func Setup() {
	// Configure zerolog
	zerolog.TimeFieldFormat = time.RFC3339
	log.Logger = log.Output(zerolog.ConsoleWriter{
		Out:        os.Stdout,
		TimeFormat: time.RFC3339,
	})

	// Set global level to debug in development
	zerolog.SetGlobalLevel(zerolog.DebugLevel)
}

// Logger returns a new logger instance with the given component name
func Logger(component string) zerolog.Logger {
	return log.With().Str("component", component).Logger()
}
