package protocol

import (
	"encoding/binary"
	"log/slog"
	"net"
)

// RequestHandlerFunc defines the function signature for API handlers
type RequestHandlerFunc func(log *slog.Logger, conn net.Conn, header *RequestHeader)

// API handlers map
var ApiHandlers = map[int16]RequestHandlerFunc{
	ApiKeyApiVersions: HandleApiVersions,
	// Add more handlers as they are implemented
}

// HandleConnection processes a Kafka protocol connection, using the provided logger
func HandleConnection(log *slog.Logger, conn net.Conn) {
	log.Debug("Processing connection") // Already logged acceptance in server.go

	for {
		var length uint32
		if err := binary.Read(conn, binary.BigEndian, &length); err != nil {
			log.Error("invalid message length", "error", err)
			return
		}
		header, err := DecodeRequestHeader(conn)
		if err != nil {
			log.Error("Error decoding request header", "error", err)
			return
		}
		log.Info("Received request",
			"apiKey", header.ApiKey,
			"apiVersion", header.ApiVersion,
			"correlationID", header.CorrelationID,
			"clientID", header.ClientID,
		)

		if handler, ok := ApiHandlers[header.ApiKey]; ok {
			go handler(log, conn, header)
		} else {
			log.Warn("Unsupported API key", "correlationID", header.CorrelationID, "apiKey", header.ApiKey)
		}

	} // End of for loop
}

func HeaderSize(apiKey int16, apiVersion int16) int32 {
	switch apiKey {
	case ApiKeyApiVersions:
		return 10
	default:
		return 10
	}
}
