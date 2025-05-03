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
			// ParseRequestHeader already logged the error.
			// If it's EOF, the client closed the connection cleanly.
			if err == io.EOF || errors.Is(err, net.ErrClosed) {
				log.Debug("Client closed connection")
			} else {
				log.Error("Error parsing request header", "error", err)
			}
			return // Exit loop on any header parsing error
		}
		log.Debug("Received request",
			"apiKey", header.ApiKey,
			"apiVersion", header.ApiVersion,
			"correlationID", header.CorrelationID,
			"size", totalSize,
		)

		// TODO: Instead of discarding, read the actual request body based on size
		// Calculate the body size
		// Cast totalSize to int32 to match HeaderSize return type
		bodySize := int32(totalSize) - HeaderSize(header.ApiKey, header.ApiVersion) // You'll need a way to get header size
		if bodySize < 0 {
			log.Error("Invalid message size calculated", "totalSize", totalSize, "calculatedHeaderSize", HeaderSize(header.ApiKey, header.ApiVersion))
			// Decide how to handle this error, maybe close connection?
			return
		}

		// Read or discard the body (This part still needs the actual request body parsing logic)
		if err := DiscardNBytes(log, conn, int64(bodySize)); err != nil {
			log.Error("Error handling request body", "error", err)
			return // Exit loop on body handling error
		}

		// Dispatch based on API Key
		if handler, ok := ApiHandlers[header.ApiKey]; ok {
			handler(log, conn, header) // Pass logger to handler
		} else {
			log.Warn("Unsupported API key", "apiKey", header.ApiKey)
			// SendErrorResponse(log, conn, header.CorrelationID, ErrorCodeUnknownServerError)
		}
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

// TODO: Implement HeaderSize function
// This function needs to return the size of the header based on the API key and version
// For ApiVersions v4 request, the header size is fixed.
func HeaderSize(apiKey int16, apiVersion int16) int32 {
	// Example: Fixed header size for initial requests (adjust as needed)
	// Request Header = ApiKey (int16) + ApiVersion (int16) + CorrelationID (int32) + ClientID Length (CompactString/Varint) + ClientID Bytes + Tagged Fields Length (Varint) + Tagged Fields Bytes
	// This needs proper implementation based on Kafka protocol docs for *requests*
	// For ApiVersions v4, Request Header = ApiKey (2) + ApiVersion (2) + CorrelationID (4) + ClientID (nullable string, min 1 byte for null) + TaggedFields (1 byte for 0) = 10 bytes minimum
	// Let's assume a fixed size for now for simplicity, needs refinement.
	switch apiKey {
	case ApiKeyApiVersions:
		if apiVersion >= 3 { // V3+ requests use tagged fields
			// Need to actually *read* client ID length to know the full header size
			// For now, returning a placeholder, THIS IS INCORRECT
			return 10 // Placeholder: ApiKey(2)+ApiVersion(2)+CorrID(4)+ClientID(1 for null)+Tags(1 for 0)
		} else {
			// Older versions might have different fixed sizes or structures
			return 8 // Placeholder: ApiKey(2)+ApiVersion(2)+CorrID(4)+ClientID(0 for empty string?)
		}
	default:
		// Default placeholder, needs correct logic per API key/version
		return 10
	}
}

// Remove or comment out the old DiscardRemainingRequest function if it exists
// func DiscardRemainingRequest(...) { ... }
