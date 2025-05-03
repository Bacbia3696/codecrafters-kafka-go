package protocol

import (
	"log"
	"net"
)

// RequestHandlerFunc defines the function signature for API handlers
type RequestHandlerFunc func(conn net.Conn, header RequestHeader)

// API handlers map
var ApiHandlers = map[int16]RequestHandlerFunc{
	ApiKeyApiVersions: HandleApiVersions,
	// Add more handlers as they are implemented
}

// HandleConnection processes a Kafka protocol connection
func HandleConnection(conn net.Conn) {
	defer conn.Close()
	log.Printf("Connection accepted from %s", conn.RemoteAddr())

	// Parse the request header
	header, totalSize, err := ParseRequestHeader(conn)
	if err != nil {
		log.Printf("Error parsing request header: %v", err)
		return
	}
	log.Printf("Received request: ApiKey=%d, ApiVersion=%d, CorrelationID=%d",
		header.ApiKey, header.ApiVersion, header.CorrelationID)

	// Discard the rest of the request (if any)
	if err := DiscardRemainingRequest(conn, totalSize); err != nil {
		log.Printf("Error processing request: %v", err)
		return
	}

	// Dispatch based on API Key
	if handler, ok := ApiHandlers[header.ApiKey]; ok {
		handler(conn, header)
	} else {
		// Unknown API Key - Send Error Response
		log.Panicf("Unsupported API key %d. Sending error response.", header.ApiKey)
	}
}
