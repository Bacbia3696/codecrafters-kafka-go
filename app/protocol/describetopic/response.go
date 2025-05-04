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
}

func (r *Cursor) Encode(w io.Writer) error {
	if r == nil {
		return protocol.EncodeI8(w, -1)
	}
	err := protocol.EncodeCompactString(w, r.TopicName)
	if err != nil {
		return fmt.Errorf("failed to encode topic name: %w", err)
	}
	err = protocol.EncodeI32(w, r.PartitionIndex)
	if err != nil {
		return fmt.Errorf("failed to encode partition index: %w", err)
	}
	return protocol.EncodeTaggedField(w)
}

func (r *PartitionResponse) Encode(w io.Writer) error {
	return nil
}

func (r *TopicResponse) Encode(w io.Writer) error {
	err := protocol.EncodeI16(w, r.ErrorCode)
	if err != nil {
		return fmt.Errorf("failed to encode error code: %w", err)
	}
	err = protocol.EncodeCompactString(w, r.Name)
	if err != nil {
		return fmt.Errorf("failed to encode topic name: %w", err)
	}
	err = protocol.EncodeUuid(w, r.TopicID)
	if err != nil {
		return fmt.Errorf("failed to encode topic id: %w", err)
	}
	err = protocol.EncodeBool(w, r.IsInternal)
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
	err = protocol.EncodeI32(w, r.TopicAuthorizedOperations)
	if err != nil {
		return fmt.Errorf("failed to encode topic authorized operations: %w", err)
	}
	return protocol.EncodeTaggedField(w)
}

func (r *DescribeTopicResponse) Encode(w io.Writer) error {
	err := protocol.EncodeI32(w, r.ThrottleTime)
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
