package describetopic

import (
	"bufio"
	"io"
	"log/slog"

	"github.com/codecrafters-io/kafka-starter-go/app/protocol"
	"github.com/google/uuid"
)

func HandleDescribeTopic(log *slog.Logger, reader *bufio.Reader, writer io.Writer, header *protocol.RequestHeader) {
	log.Info("HandleDescribeTopic")
	request, err := DecodeDescribeTopicRequest(reader)
	if err != nil {
		log.Error("failed to decode describe topic request", "error", err)
		return
	}
	log.Info("Received DescribeTopic request", "topics", request.Topics, "responsePartitionLimit", request.ResponsePartitionLimit, "cursor", request.Cursor)
	responseHeader := &protocol.ResponseHeaderV1{
		CorrelationID: header.CorrelationID,
	}
	response := &DescribeTopicResponse{
		ThrottleTime: 0,
		Topics:       make([]TopicResponse, len(request.Topics)),
		NextCursor:   nil,
	}
	metadata, err := protocol.ReadClusterMetadata()
	if err != nil {
		log.Error("failed to read cluster metadata", "error", err)
	}
	log.Info("Cluster metadata", "metadata", metadata)
	for i, t := range request.Topics {
		response.Topics[i] = TopicResponse{
			ErrorCode:  protocol.ErrorCodeUnknownTopic,
			Name:       t.Name,
			TopicID:    uuid.Nil,
			IsInternal: false,
			Partitions: []PartitionResponse{},
		}
	}

	err = responseHeader.Encode(writer)
	if err != nil {
		log.Error("failed to encode describe topic response header", "error", err)
		return
	}
	err = response.Encode(writer)
	if err != nil {
		log.Error("failed to encode describe topic response", "error", err)
		return
	}
	log.Info("Sent DescribeTopic response", "response", response)
}
