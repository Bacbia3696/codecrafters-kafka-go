package fetch

import (
	"bufio"
	"bytes"
	"io"
	"log/slog"

	"github.com/codecrafters-io/kafka-starter-go/app/encoder"
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
		return
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
					PartitionIndex: request.Topics[0].Partitions[0].PartitionID,
					ErrorCode:      protocol.ErrorCodeNone,
					HighWatermark:  0,
				},
			},
		}
		topicRecord := protocol.GetTopicRecordById(clusterMeta, t.TopicID)
		if topicRecord == nil {
			log.Info("unknown topic", "topicID", t.TopicID)
			response.Responses[i].Partitions[0].RecordBatchsRaw = []byte{0x01}
			response.Responses[i].Partitions[0].ErrorCode = protocol.ErrorCodeUnknownTopicID
		} else {
			recordBatchs, err := protocol.ReadTopicLogFile(topicRecord.Name, request.Topics[i].Partitions[0].PartitionID)
			if err != nil {
				log.Error("failed to read topic log", "error", err)
				// empty topic
				response.Responses[i].Partitions[0].RecordBatchsRaw = []byte{0x01}
				continue
			}
			raw, err := protocol.ReadTopicLogFileRaw(topicRecord.Name, request.Topics[i].Partitions[0].PartitionID)
			log.Info("read topic log file", "topicName", topicRecord.Name, "partitionID", request.Topics[i].Partitions[0].PartitionID, "recordBatchs", recordBatchs)
			if err != nil {
				log.Error("failed to read topic log", "error", err)
				continue
			}
			log.Info("raw raw", "raw", raw)
			bufWriter := bytes.NewBuffer(nil)
			err = encoder.EncodeCompactArrayLength(bufWriter, len(recordBatchs.RecordBatchs))
			if err != nil {
				log.Error("failed to encode record batchs length", "error", err)
				return
			}
			// bufWriter.Write(raw)
			for _, recordBatch := range recordBatchs.RecordBatchs {
				err = recordBatch.Encode(bufWriter)
				if err != nil {
					log.Error("failed to encode record batch", "error", err)
					return
				}
			}
			response.Responses[i].Partitions[0].RecordBatchsRaw = bufWriter.Bytes()
		}
		log.Info("response", "response", response.Responses[i], "t", t, "topicRecord", topicRecord)
	}
	err = response.Encode(w)
	if err != nil {
		log.Error("failed to encode fetch response", "error", err)
		return
	}
	log.Info("Sent Fetch response")
}
