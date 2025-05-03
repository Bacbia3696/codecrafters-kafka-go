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
	// Initialize logger
	logger.Setup()
	log := logger.Logger("main")

	// Load configuration
	cfg, err := config.New()
	if err != nil {
		log.Fatal().Err(err).Msg("Failed to load configuration")
	}

	// Create and start server
	srv := server.New(cfg)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	if err := srv.Start(ctx); err != nil {
		log.Fatal().Err(err).Msg("Failed to start server")
	}

	// Handle shutdown gracefully
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	<-sigChan
	log.Info().Msg("Shutting down server...")

	cancel()
	if err := srv.Stop(); err != nil {
		log.Error().Err(err).Msg("Error during shutdown")
	}
}
