package protocol

import (
	"bufio"
	"io"
	"log/slog"
	"net"
)

// RequestHandlerFunc defines the function signature for API handlers
type RequestHandlerFunc func(log *slog.Logger, conn net.Conn, header *RequestHeader)

// API handlers map
var ApiHandlers = map[int16]RequestHandlerFunc{
	ApiKeyApiVersions:             HandleApiVersions,
	ApiKeyDescribeTopicPartitions: HandleDescribeTopic,
	// Add more handlers as they are implemented
}

// HandleConnection processes a Kafka protocol connection, using the provided logger
func HandleConnection(log *slog.Logger, conn net.Conn) {
	for {
		length, err := DecodeInt32(conn)
		if err != nil {
			if err == io.EOF {
				log.Debug("Connection closed")
				return
			}
			log.Error("invalid message length", "error", err)
			return
		}
		rd := bufio.NewReader(conn)
		header, err := DecodeRequestHeader(rd)
		if err != nil {
			log.Error("Error decoding request header", "error", err)
			return
		}
		log.Info("Received request",
			"length", length,
			"apiKey", header.ApiKey,
			"apiVersion", header.ApiVersion,
			"correlationID", header.CorrelationID,
			"clientID", header.ClientID,
		)

		if handler, ok := ApiHandlers[header.ApiKey]; ok {
			handler(log, conn, header)
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
