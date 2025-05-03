package server

import (
	"context"
	"fmt"
	"net"
	"sync"
	"time"

	"github.com/codecrafters-io/kafka-starter-go/app/config"
	"github.com/codecrafters-io/kafka-starter-go/app/logger"
	"github.com/codecrafters-io/kafka-starter-go/app/protocol"
	"github.com/rs/zerolog"
)

// Server represents the Kafka server
type Server struct {
	config   *config.Config
	log      zerolog.Logger
	listener net.Listener
	wg       sync.WaitGroup
}

// New creates a new Kafka server instance
func New(cfg *config.Config) *Server {
	return &Server{
		config: cfg,
		log:    logger.Logger("server"),
	}
}

// Start starts the Kafka server
func (s *Server) Start(ctx context.Context) error {
	var err error
	s.listener, err = net.Listen("tcp", s.config.Address())
	if err != nil {
		return fmt.Errorf("failed to bind to %s: %w", s.config.Address(), err)
	}

	s.log.Info().Msgf("Kafka server listening on %s", s.config.Address())

	s.wg.Add(1)
	go s.acceptConnections(ctx)

	return nil
}

// Stop gracefully stops the server
func (s *Server) Stop() error {
	if s.listener != nil {
		if err := s.listener.Close(); err != nil {
			return fmt.Errorf("failed to close listener: %w", err)
		}
	}

	// Wait for goroutines to finish, with a timeout
	waitChan := make(chan struct{})
	go func() {
		defer close(waitChan)
		s.wg.Wait()
	}()

	select {
	case <-waitChan:
		s.log.Info().Msg("All goroutines finished cleanly.")
		return nil // Clean shutdown
	case <-time.After(10 * time.Second): // Example 10-second timeout
		s.log.Warn().Msg("Shutdown timed out waiting for connections.")
		return fmt.Errorf("shutdown timed out")
	}
}

func (s *Server) acceptConnections(ctx context.Context) {
	defer s.wg.Done()

	for {
		conn, err := s.listener.Accept()
		if err != nil {
			select {
			case <-ctx.Done():
				return
			default:
				s.log.Error().Err(err).Msg("Error accepting connection")
				continue
			}
		}

		s.wg.Add(1)
		go func() {
			defer s.wg.Done()
			s.handleConnection(conn)
		}()
	}
}

func (s *Server) handleConnection(conn net.Conn) {
	defer conn.Close()

	clientLog := s.log.With().
		Str("client_addr", conn.RemoteAddr().String()).
		Logger()

	clientLog.Debug().Msg("New connection accepted")

	protocol.HandleConnection(conn)
}
