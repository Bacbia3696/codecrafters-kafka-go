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

		// Launch a goroutine to handle the request body and dispatch
		go func(currentHeader RequestHeader, bodySizeToHandle int32) {
			// TODO: Instead of discarding, read the actual request body based on size.
			//       The actual handler should read the body from 'conn'.
			//       This discard is temporary until body parsing is implemented per request type.
			if err := DiscardNBytes(log, conn, int64(bodySizeToHandle)); err != nil {
				log.Error("Error handling request body in goroutine", "correlationID", currentHeader.CorrelationID, "apiKey", currentHeader.ApiKey, "error", err)
				// We might not want to close the whole connection here, depends on error type.
				// For now, just log and let the goroutine exit.
				return
			}

			// Dispatch based on API Key
			if handler, ok := ApiHandlers[currentHeader.ApiKey]; ok {
				// Pass logger, conn, and the captured header
				handler(log, conn, currentHeader)
			} else {
				log.Warn("Unsupported API key in goroutine", "correlationID", currentHeader.CorrelationID, "apiKey", currentHeader.ApiKey)
				// SendErrorResponse(log, conn, currentHeader.CorrelationID, ErrorCodeUnknownServerError)
			}
		}(header, bodySize) // Pass copies of header and bodySize to the goroutine

	} // End of for loop
}

// Helper function to discard a specific number of bytes (replace DiscardRemainingRequest)
func DiscardNBytes(log *slog.Logger, r io.Reader, n int64) error {
	if n < 0 {
		return fmt.Errorf("cannot discard negative number of bytes: %d", n)
	}
	if n == 0 {
		return nil
	}
	written, err := io.CopyN(io.Discard, r, n)
	if err != nil {
		if err == io.EOF {
			log.Error("Connection closed unexpectedly while discarding request body", "expectedBytes", n, "discardedBytes", written)
			return fmt.Errorf("connection closed unexpectedly while discarding request body: %w", err)
		}
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
