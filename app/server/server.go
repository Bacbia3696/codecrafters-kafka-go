package server

import (
	"context"
	"fmt"
	"net"
	"sync"
	"time"

	"log/slog"

	"github.com/codecrafters-io/kafka-starter-go/app/config"
	"github.com/codecrafters-io/kafka-starter-go/app/protocol"
	"github.com/codecrafters-io/kafka-starter-go/app/protocol/apiversions"
	"github.com/codecrafters-io/kafka-starter-go/app/protocol/describetopic"
)

// ApiHandlers maps API keys to their respective handlers
var ApiHandlers = map[int16]protocol.RequestHandlerFunc{
	protocol.ApiKeyApiVersions:             apiversions.HandleApiVersions,
	protocol.ApiKeyDescribeTopicPartitions: describetopic.HandleDescribeTopic,
	// Add more handlers as they are implemented
}

// Server represents the Kafka server
type Server struct {
	config   *config.Config
	log      *slog.Logger
	listener net.Listener
	wg       sync.WaitGroup
}

// New creates a new Kafka server instance
func New(cfg *config.Config, log *slog.Logger) *Server {
	return &Server{
		config: cfg,
		log:    log,
	}
}

// Start starts the Kafka server
func (s *Server) Start(ctx context.Context) error {
	var err error
	addr := s.config.Address()
	s.listener, err = net.Listen("tcp", addr)
	if err != nil {
		return fmt.Errorf("failed to bind to %s: %w", addr, err)
	}

	s.log.Info("Kafka server listening", "address", addr)

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
		s.log.Info("All goroutines finished cleanly.")
		return nil // Clean shutdown
	case <-time.After(10 * time.Second): // Example 10-second timeout
		s.log.Warn("Shutdown timed out waiting for connections.")
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
				s.log.Debug("Context cancelled, stopping accept loop.")
				return
			default:
				s.log.Error("Error accepting connection", "error", err)
				continue
			}
		}

		s.wg.Add(1)
		go func() {
			defer s.wg.Done()
			s.handleConnection(s.log, conn)
		}()
	}
}

func (s *Server) handleConnection(log *slog.Logger, conn net.Conn) {
	defer conn.Close()

	// Create a logger specific to this client connection
	clientLog := log.With("client_addr", conn.RemoteAddr().String())

	clientLog.Debug("New connection accepted")

	// Create a map with the exact type expected by HandleConnection
	handlersForCall := make(map[int16]protocol.RequestHandlerFunc)
	for apiKey, handlerFunc := range ApiHandlers {
		handlersForCall[apiKey] = handlerFunc
	}

	// Pass the client-specific logger and correctly typed handlers to protocol handler
	protocol.HandleConnection(clientLog, conn, handlersForCall)

	clientLog.Info("Client disconnected")
}
