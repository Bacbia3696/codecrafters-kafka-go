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

// RequestHandlerFunc defines the function signature for API handlers
type RequestHandlerFunc func(log *slog.Logger, rd *bufio.Reader, w io.Writer, header *RequestHeader)

// HandleConnection processes a Kafka protocol connection, using the provided logger and handlers map
func HandleConnection(log *slog.Logger, conn net.Conn, handlers map[int16]RequestHandlerFunc) {
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

		var bufWriter = bytes.Buffer{}
		if handler, ok := handlers[header.ApiKey]; ok {
			handler(log, rd, &bufWriter, header)
			encoder.EncodeValue(conn, int32(bufWriter.Len()))
			bufWriter.WriteTo(conn)
		} else {
			log.Warn("Unsupported API key", "correlationID", header.CorrelationID, "apiKey", header.ApiKey)
		}

	} // End of for loop
}
