package main

import (
	"context"
	"os"
	"os/signal"
	"syscall"

	"github.com/codecrafters-io/kafka-starter-go/app/config"
	"github.com/codecrafters-io/kafka-starter-go/app/logger"
	"github.com/codecrafters-io/kafka-starter-go/app/server"
)

func main() {
	// Initialize logger using the new package
	log := logger.New()
	// slog.SetDefault(log) // SetDefault is still useful if other packages might use slog.Default()

	// Load configuration
	cfg, err := config.New()
	if err != nil {
		log.Error("Failed to load configuration", "error", err)
		os.Exit(1)
	}

	// Create and start server
	srv := server.New(cfg, log) // Pass the configured logger
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	if err := srv.Start(ctx); err != nil {
		log.Error("Failed to start server", "error", err)
		os.Exit(1)
	}

	// Handle shutdown gracefully
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	<-sigChan
	log.Info("Received shutdown signal")

	cancel() // Signal server to stop accepting/handling
	if err := srv.Stop(); err != nil {
		log.Error("Error during server shutdown", "error", err)
	}

	log.Info("Server shut down completed.")
}
