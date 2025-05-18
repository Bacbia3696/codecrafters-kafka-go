package describetopic

import (
	"bufio"
	"io"
	"log/slog"

	"github.com/codecrafters-io/kafka-starter-go/app/protocol"
	"github.com/google/uuid"
)

// DescribeTopicHandler implements the protocol.RequestHandler interface for DescribeTopic requests.
type DescribeTopicHandler struct{}

// NewDescribeTopicHandler creates a new handler for DescribeTopic requests.
func NewDescribeTopicHandler() *DescribeTopicHandler {
	return &DescribeTopicHandler{}
}

// ApiKey returns the API key for DescribeTopic requests.
func (h *DescribeTopicHandler) ApiKey() int16 {
	return protocol.ApiKeyDescribeTopicPartitions
}

// Handle handles the DescribeTopic request.
func (h *DescribeTopicHandler) Handle(log *slog.Logger, reader *bufio.Reader, writer io.Writer, header *protocol.RequestHeader) {
	log.Info("Handling DescribeTopic request")
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
	clusterMeta, err := protocol.ReadClusterMetadata()
	if err != nil {
		log.Error("failed to read cluster metadata", "error", err)
		return
	}
	topicMap := protocol.GetMapTopicByName(clusterMeta)
	for i, t := range request.Topics {
		response.Topics[i] = TopicResponse{
			ErrorCode:  protocol.ErrorCodeUnknownTopicOrPartition,
			Name:       t.Name,
			TopicID:    uuid.Nil,
			IsInternal: false,
			Partitions: []PartitionResponse{},
		}
		if topic, ok := topicMap[t.Name]; ok {
			response.Topics[i].TopicID = topic.TopicId
			response.Topics[i].ErrorCode = protocol.ErrorCodeNone

			partitions := protocol.GetPartitionsByTopicId(clusterMeta, topic.TopicId)
			response.Topics[i].Partitions = make([]PartitionResponse, len(partitions))
			for j, p := range partitions {
				response.Topics[i].Partitions[j] = PartitionResponse{
					ErrorCode:              protocol.ErrorCodeNone,
					PartitionIndex:         p.PartitionId,
					LeaderID:               p.Leader,
					LeaderEpoch:            p.LeaderEpoch,
					ReplicaNodes:           p.Replicas,
					IsrNodes:               p.Isr,
					EligibleLeaderReplicas: []int32{},
					LastKnownELR:           []int32{},
					OfflineReplicas:        []int32{},
				}
			}
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
	log.Info("Sent DescribeTopic response")
}
