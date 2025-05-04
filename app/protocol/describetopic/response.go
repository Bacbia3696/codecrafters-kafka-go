package describetopic

import (
	"fmt"
	"io"

	"github.com/codecrafters-io/kafka-starter-go/app/protocol"
	"github.com/google/uuid"
)

// This file is reserved for DescribeTopic response structs and encoding logic.
type DescribeTopicResponse struct {
	ThrottleTime int32
	Topics       []TopicResponse
	NextCursor   *Cursor
	// tagged fields
}

type TopicResponse struct {
	ErrorCode                 int16
	Name                      string    // compact string
	TopicID                   uuid.UUID // uuid
	IsInternal                bool
	Partitions                []PartitionResponse // x01 for empty compact array
	TopicAuthorizedOperations int32
	// TagBuffer
}

type PartitionResponse struct {
	ErrorCode              int16
	PartitionIndex         int32
	LeaderID               int32
	LeaderEpoch            int32
	ReplicaNodes           []int32
	IsrNodes               []int32
	EligibleLeaderReplicas []int32
	LastKnownELR           []int32
	OfflineReplicas        []int32
	// TagBuffer
}

func (r *Cursor) Encode(w io.Writer) error {
	if r == nil {
		return protocol.EncodeValue(w, int8(-1))
	}
	err := protocol.EncodeCompactString(w, r.TopicName)
	if err != nil {
		return fmt.Errorf("failed to encode topic name: %w", err)
	}
	err = protocol.EncodeValue(w, r.PartitionIndex)
	if err != nil {
		return fmt.Errorf("failed to encode partition index: %w", err)
	}
	return protocol.EncodeTaggedField(w)
}

func (r *PartitionResponse) Encode(w io.Writer) error {
	// ErrorCode              int16
	// PartitionIndex         int32
	// LeaderID               int32
	// LeaderEpoch            int32
	// ReplicaNodes           []int32
	// IsrNodes               []int32
	// EligibleLeaderReplicas []int32
	// LastKnownELR           []int32
	// OfflineReplicas        []int32
	// TagBuffer
	err := protocol.EncodeValue(w, r.ErrorCode)
	if err != nil {
		return fmt.Errorf("failed to encode error code: %w", err)
	}
	err = protocol.EncodeValue(w, r.PartitionIndex)
	if err != nil {
		return fmt.Errorf("failed to encode partition index: %w", err)
	}
	err = protocol.EncodeValue(w, r.LeaderID)
	if err != nil {
		return fmt.Errorf("failed to encode leader id: %w", err)
	}
	err = protocol.EncodeValue(w, r.LeaderEpoch)
	if err != nil {
		return fmt.Errorf("failed to encode leader epoch: %w", err)
	}
	err = protocol.EncodeArray(w, r.ReplicaNodes)
	if err != nil {
		return fmt.Errorf("failed to encode replica nodes: %w", err)
	}
	err = protocol.EncodeArray(w, r.IsrNodes)
	if err != nil {
		return fmt.Errorf("failed to encode isr nodes: %w", err)
	}
	err = protocol.EncodeArray(w, r.EligibleLeaderReplicas)
	if err != nil {
		return fmt.Errorf("failed to encode eligible leader replicas: %w", err)
	}
	err = protocol.EncodeArray(w, r.LastKnownELR)
	if err != nil {
		return fmt.Errorf("failed to encode last known elr: %w", err)
	}
	err = protocol.EncodeArray(w, r.OfflineReplicas)
	if err != nil {
		return fmt.Errorf("failed to encode offline replicas: %w", err)
	}
	return protocol.EncodeTaggedField(w)
}

func (r *TopicResponse) Encode(w io.Writer) error {
	err := protocol.EncodeValue(w, r.ErrorCode)
	if err != nil {
		return fmt.Errorf("failed to encode error code: %w", err)
	}
	err = protocol.EncodeCompactString(w, r.Name)
	if err != nil {
		return fmt.Errorf("failed to encode topic name: %w", err)
	}
	err = protocol.EncodeValue(w, r.TopicID)
	if err != nil {
		return fmt.Errorf("failed to encode topic id: %w", err)
	}
	err = protocol.EncodeValue(w, r.IsInternal)
	if err != nil {
		return fmt.Errorf("failed to encode is internal: %w", err)
	}
	numPartitions := len(r.Partitions)
	err = protocol.EncodeUvarint(w, uint64(numPartitions+1))
	for _, p := range r.Partitions {
		err = p.Encode(w)
		if err != nil {
			return fmt.Errorf("failed to encode partition: %w", err)
		}
	}
	if err != nil {
		return fmt.Errorf("failed to encode partitions: %w", err)
	}
	err = protocol.EncodeValue(w, r.TopicAuthorizedOperations)
	if err != nil {
		return fmt.Errorf("failed to encode topic authorized operations: %w", err)
	}
	return protocol.EncodeTaggedField(w)
}

func (r *DescribeTopicResponse) Encode(w io.Writer) error {
	err := protocol.EncodeValue(w, r.ThrottleTime)
	if err != nil {
		return fmt.Errorf("failed to encode throttle time: %w", err)
	}
	numTopics := len(r.Topics)
	err = protocol.EncodeUvarint(w, uint64(numTopics+1))
	if err != nil {
		return fmt.Errorf("failed to encode number of topics: %w", err)
	}
	for _, topic := range r.Topics {
		err = topic.Encode(w)
		if err != nil {
			return fmt.Errorf("failed to encode topic: %w", err)
		}
	}
	err = r.NextCursor.Encode(w)
	if err != nil {
		return fmt.Errorf("failed to encode next cursor: %w", err)
	}
	return protocol.EncodeTaggedField(w)
}
