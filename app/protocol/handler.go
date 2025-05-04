package protocol

import (
	"bufio"
	"errors"
	"io"
	"log/slog"
	"net"
)

// RequestHandlerFunc defines the function signature for API handlers
type RequestHandlerFunc func(log *slog.Logger, rd *bufio.Reader, w io.Writer, header *RequestHeader)

// HandleConnection processes a Kafka protocol connection, using the provided logger and handlers map
func HandleConnection(log *slog.Logger, conn net.Conn, handlers map[int16]RequestHandlerFunc) {
	for {
		length, err := DecodeInt32(conn)
		if err != nil {
			if errors.Is(err, io.EOF) {
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

		if handler, ok := handlers[header.ApiKey]; ok {
			handler(log, rd, conn, header)
		} else {
			log.Warn("Unsupported API key", "correlationID", header.CorrelationID, "apiKey", header.ApiKey)
		}

	} // End of for loop
}
