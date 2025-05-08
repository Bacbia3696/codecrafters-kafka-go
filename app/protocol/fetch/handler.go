package fetch

import (
	"bufio"
	"io"
	"log/slog"

	"github.com/codecrafters-io/kafka-starter-go/app/protocol"
)

// FetchHandler implements the protocol.RequestHandler interface for Fetch requests.
type FetchHandler struct{}

// NewFetchHandler creates a new handler for Fetch requests.
func NewFetchHandler() *FetchHandler {
	return &FetchHandler{}
}

// ApiKey returns the API key for Fetch requests.
func (h *FetchHandler) ApiKey() int16 {
	return protocol.ApiKeyFetch
}

// Handle handles the Fetch request.
func (h *FetchHandler) Handle(log *slog.Logger, rd *bufio.Reader, w io.Writer, header *protocol.RequestHeader) {
	log.Info("Handling Fetch request", "correlationID", header.CorrelationID)
	request, err := DecodeFetchRequest(rd)
	if err != nil {
		log.Error("failed to decode fetch request", "error", err)
		return
	}
	log.Info("Decoded fetch request", "request", request)

	// TODO: Implement actual fetch logic: reading from topics/partitions
	// and sending a FetchResponse.

	// For now, let's send a minimal response or nothing, as per current implementation.
	// Example of sending an empty response (if your protocol expects a response header even for no data):
	// responseHeader := protocol.ResponseHeaderV0{CorrelationID: header.CorrelationID}
	// if err := responseHeader.Encode(w); err != nil {
	// 	log.Error("failed to encode fetch response header", "error", err)
	// 	return
	// }
	// An actual FetchResponse would follow.
}
