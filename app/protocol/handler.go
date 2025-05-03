package protocol

import (
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
	// No need to close here, it's handled by the caller (server.handleConnection)
	// defer conn.Close()
	log.Debug("Processing connection") // Already logged acceptance in server.go

	// Parse the request header, passing the logger
	header, totalSize, err := ParseRequestHeader(log, conn)
	if err != nil {
		// ParseRequestHeader already logged the error
		return
	}
	log.Debug("Received request",
		"apiKey", header.ApiKey,
		"apiVersion", header.ApiVersion,
		"correlationID", header.CorrelationID,
		"size", totalSize,
	)

	// Discard the rest of the request (if any), passing the logger
	// TODO: Instead of discarding, read the actual request body based on size
	if err := DiscardRemainingRequest(log, conn, totalSize); err != nil {
		// DiscardRemainingRequest already logged the error
		return
	}

	// Dispatch based on API Key
	if handler, ok := ApiHandlers[header.ApiKey]; ok {
		handler(log, conn, header) // Pass logger to handler
	} else {
		// Unknown API Key - Send Error Response?
		// For now, just log. Sending response should probably happen here.
		log.Warn("Unsupported API key", "apiKey", header.ApiKey)
		// Sending an error response might be better than panicking
		// SendErrorResponse(log, conn, header.CorrelationID, ErrorCodeUnknownServerError)
	}
}
