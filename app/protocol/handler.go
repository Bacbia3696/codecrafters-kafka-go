package protocol

import (
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
)

// RequestHandlerFunc defines the function signature for API handlers
type RequestHandlerFunc func(log *slog.Logger, conn net.Conn, header RequestHeader)

// API handlers map
var ApiHandlers = map[int16]RequestHandlerFunc{
	ApiKeyApiVersions: HandleApiVersions,
	// Add more handlers as they are implemented
}

// HandleConnection processes a Kafka protocol connection, using the provided logger
func HandleConnection(log *slog.Logger, conn net.Conn) {
	log.Debug("Processing connection") // Already logged acceptance in server.go

	for { // Loop to handle multiple requests
		// Parse the request header, passing the logger
		header, totalSize, err := ParseRequestHeader(log, conn)
		if err != nil {
			// Check for specific errors
			if errors.Is(err, errConnectionClosedBeforeSize) {
				// Common case (e.g., health check), log as debug and close connection cleanly
				log.Debug("Client disconnected before sending data (likely health check)", "error", err)
			} else if err == io.EOF || errors.Is(err, net.ErrClosed) {
				// Client closed connection cleanly after at least one successful request
				log.Debug("Client closed connection")
			} else {
				// Unexpected error during header parsing
				log.Error("Error parsing request header", "error", err)
			}
			return // Exit loop on any header parsing error or clean disconnect
		}
		log.Debug("Received request",
			"apiKey", header.ApiKey,
			"apiVersion", header.ApiVersion,
			"correlationID", header.CorrelationID,
			"size", totalSize,
		)

		// Calculate the body size
		bodySize := int32(totalSize) - HeaderSize(header.ApiKey, header.ApiVersion)
		if bodySize < 0 {
			log.Error("Invalid message size calculated", "totalSize", totalSize, "calculatedHeaderSize", HeaderSize(header.ApiKey, header.ApiVersion))
			// Close connection or send error response?
			return // Exit loop for this connection on critical error
		}

		// Handle body and dispatch sequentially
		// TODO: Instead of discarding, read the actual request body based on size.
		if err := DiscardNBytes(log, conn, int64(bodySize)); err != nil {
			// Reverted error handling: Log all errors from DiscardNBytes as Error
			log.Error("Error handling request body", "correlationID", header.CorrelationID, "apiKey", header.ApiKey, "error", err)
			// Decide if we should return here or continue the loop?
			// Returning seems safer for now if we can't handle the body.
			return // Exit loop on body handling error
		}

		// Dispatch based on API Key
		if handler, ok := ApiHandlers[header.ApiKey]; ok {
			// Pass logger, conn, and the header
			handler(log, conn, header)
		} else {
			log.Warn("Unsupported API key", "correlationID", header.CorrelationID, "apiKey", header.ApiKey)
			// SendErrorResponse(log, conn, header.CorrelationID, ErrorCodeUnknownServerError)
		}

	} // End of for loop
}

// Helper function to discard a specific number of bytes (reverting to previous state)
func DiscardNBytes(log *slog.Logger, r io.Reader, n int64) error {
	if n < 0 {
		return fmt.Errorf("cannot discard negative number of bytes: %d", n)
	}
	if n == 0 {
		return nil
	}
	written, err := io.CopyN(io.Discard, r, n)
	if err != nil {
		log.Error("Error discarding request body bytes", "error", err, "expectedBytes", n, "discardedBytes", written)
		return fmt.Errorf("error discarding request body bytes: %w", err)
	}
	return nil
}

func HeaderSize(apiKey int16, apiVersion int16) int32 {
	switch apiKey {
	case ApiKeyApiVersions:
		return 10
	default:
		return 10
	}
}
