package fetch

import (
	"bufio"
	"io"
	"log/slog"

	"github.com/codecrafters-io/kafka-starter-go/app/protocol"
)

func HandleFetch(log *slog.Logger, rd *bufio.Reader, w io.Writer, header *protocol.RequestHeader) {
	log.Info("Received fetch request", "correlationID", header.CorrelationID)
	request, err := DecodeFetchRequest(rd)
	if err != nil {
		log.Error("failed to decode fetch request", "error", err)
		return
	}
	log.Info("Decoded fetch request", "request", request)
}
