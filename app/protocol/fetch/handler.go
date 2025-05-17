package fetch

import (
	"bufio"
	"fmt"
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

	responseHeader := &protocol.ResponseHeaderV1{
		CorrelationID: header.CorrelationID,
	}
	err = responseHeader.Encode(w)
	if err != nil {
		log.Error("failed to encode fetch response header", "error", err)
		return
	}

	clusterMeta, err := protocol.ReadClusterMetadata()
	if err != nil {
		log.Error("failed to read cluster metadata", "error", err)
		// Consider how to handle this error; maybe return an error response to client
	}

	response := &FetchResponse{
		ThrottleTimeMs: 0,
		ErrorCode:      protocol.ErrorCodeNone,
		SessionID:      0,
		Responses:      make([]TopicResponse, len(request.Topics)),
	}
	for i, t := range request.Topics {
		response.Responses[i] = TopicResponse{
			TopicID: t.TopicID,
			Partitions: []PartitionResponse{
				{
					PartitionIndex: 0,
					ErrorCode:      protocol.ErrorCodeNone,
					HighWatermark:  0,
				},
			},
		}
		topicRecord := protocol.GetTopicRecordById(clusterMeta, t.TopicID)
		if topicRecord == nil {
			response.Responses[i].Partitions[0].ErrorCode = protocol.ErrorCodeUnknownTopicID
		} else {
			recordBatchsRaw, err := protocol.ReadTopicLogFileRaw(topicRecord.Name, 0)
			fmt.Printf("raw: %b\n", recordBatchsRaw)
			if err != nil {
				log.Error("failed to read topic log", "error", err)
				// empty records
				response.Responses[i].Partitions[0].RecordBatchsRaw = []byte{0x01}
				continue
			}
			response.Responses[i].Partitions[0].RecordBatchsRaw = recordBatchsRaw
		}
	}
	err = response.Encode(w)
	if err != nil {
		log.Error("failed to encode fetch response", "error", err)
		return
	}
	log.Info("Sent Fetch response")
}
