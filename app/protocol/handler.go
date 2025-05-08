package protocol

import (
	"bufio"
	"bytes"
	"errors"
	"io"
	"log/slog"
	"net"

	"github.com/codecrafters-io/kafka-starter-go/app/decoder"
	"github.com/codecrafters-io/kafka-starter-go/app/encoder"
)

// RequestHeader is assumed to be defined elsewhere in the protocol package (e.g., header.go)
// Make sure RequestHeader is accessible here or move its definition if necessary.

// RequestHandler defines the interface for an API request handler.
type RequestHandler interface {
	ApiKey() int16
	Handle(log *slog.Logger, rd *bufio.Reader, w io.Writer, header *RequestHeader)
}

// RequestHandlerFunc defines the function signature for API handlers
// This type might become obsolete or be used internally by concrete handlers if preferred.
// type RequestHandlerFunc func(log *slog.Logger, rd *bufio.Reader, w io.Writer, header *RequestHeader)

// HandleConnection processes a Kafka protocol connection, using the provided logger and a map of registered handlers.
func HandleConnection(log *slog.Logger, conn net.Conn, handlers map[int16]RequestHandler) {
	for {
		var length int32
		err := decoder.DecodeValue(conn, &length)
		if err != nil {
			if errors.Is(err, io.EOF) {
				log.Debug("Connection closed")
				return
			}
			log.Error("invalid message length", "error", err)
			return
		}

		// Limit the reader to the message length to prevent reading into the next message.
		lr := io.LimitReader(conn, int64(length))
		rd := bufio.NewReader(lr) // Use the limited reader

		header, err := DecodeRequestHeader(rd) // DecodeRequestHeader needs to read from rd
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

		var bufWriter = bytes.Buffer{}
		if handler, ok := handlers[header.ApiKey]; ok {
			// The handler.Handle method now directly takes the bufio.Reader and io.Writer
			handler.Handle(log, rd, &bufWriter, header)

			// Prepare response
			responseBytes := bufWriter.Bytes()
			responseLength := int32(len(responseBytes))

			// Send response length
			if err := encoder.EncodeValue(conn, responseLength); err != nil {
				log.Error("Failed to encode response length", "error", err)
				return // Important to return on error to prevent further writes
			}
			// Send response body
			if _, err := conn.Write(responseBytes); err != nil {
				log.Error("Failed to write response body", "error", err)
				return // Important to return on error
			}

		} else {
			log.Warn("Unsupported API key", "correlationID", header.CorrelationID, "apiKey", header.ApiKey)
			// Consider sending an error response back to the client for unsupported API keys,
			// e.g., using a generic error response function if available.
		}

	} // End of for loop
}
