package fetch

import (
	"bufio"
	"fmt"

	"github.com/codecrafters-io/kafka-starter-go/app/decoder"
	"github.com/google/uuid"
)

// Fetch Request (Version: 16) => max_wait_ms min_bytes max_bytes isolation_level session_id session_epoch [topics] [forgotten_topics_data] rack_id _tagged_fields
//   max_wait_ms => INT32
//   min_bytes => INT32
//   max_bytes => INT32
//   isolation_level => INT8
//   session_id => INT32
//   session_epoch => INT32
//   topics => topic_id [partitions] _tagged_fields
//     topic_id => UUID
//     partitions => partition current_leader_epoch fetch_offset last_fetched_epoch log_start_offset partition_max_bytes _tagged_fields
//       partition => INT32
//       current_leader_epoch => INT32
//       fetch_offset => INT64
//       last_fetched_epoch => INT32
//       log_start_offset => INT64
//       partition_max_bytes => INT32
//   forgotten_topics_data => topic_id [partitions] _tagged_fields
//     topic_id => UUID
//     partitions => INT32
//   rack_id => COMPACT_STRING

type FetchRequest struct {
	MaxWaitMs           int32
	MinBytes            int32
	MaxBytes            int32
	IsolationLevel      int8
	SessionID           int32
	SessionEpoch        int32
	Topics              []Topic
	ForgottenTopicsData []Topic
	RackID              string
	// TaggedFields
}

type Topic struct {
	TopicID    uuid.UUID
	Partitions []Partition
	// TaggedFields
}

type Partition struct {
	PartitionID        int32
	CurrentLeaderEpoch int32
	FetchOffset        int64
	LastFetchedEpoch   int32
	LogStartOffset     int64
	PartitionMaxBytes  int32
	// TaggedFields
}

func DecodeFetchRequest(r *bufio.Reader) (*FetchRequest, error) {
	request := &FetchRequest{}
	err := decoder.DecodeValue(r, &request.MaxWaitMs)
	if err != nil {
		return nil, fmt.Errorf("failed to decode max wait ms: %w", err)
	}
	err = decoder.DecodeValue(r, &request.MinBytes)
	if err != nil {
		return nil, fmt.Errorf("failed to decode min bytes: %w", err)
	}
	err = decoder.DecodeValue(r, &request.MaxBytes)
	if err != nil {
		return nil, fmt.Errorf("failed to decode max bytes: %w", err)
	}
	err = decoder.DecodeValue(r, &request.IsolationLevel)
	if err != nil {
		return nil, fmt.Errorf("failed to decode isolation level: %w", err)
	}
	err = decoder.DecodeValue(r, &request.SessionID)
	if err != nil {
		return nil, fmt.Errorf("failed to decode session id: %w", err)
	}
	err = decoder.DecodeValue(r, &request.SessionEpoch)
	if err != nil {
		return nil, fmt.Errorf("failed to decode session epoch: %w", err)
	}
	topicLen, err := decoder.DecodeCompactArrayLength(r)
	if err != nil {
		return nil, fmt.Errorf("failed to decode topic length: %w", err)
	}
	topics := make([]Topic, 0, topicLen)
	for range topics {
		topic, err := DecodeTopic(r)
		if err != nil {
			return nil, fmt.Errorf("failed to decode topic: %w", err)
		}
		topics = append(topics, *topic)
	}
	request.Topics = topics
	topicForgottenLen, err := decoder.DecodeCompactArrayLength(r)
	if err != nil {
		return nil, fmt.Errorf("failed to decode topic forgotten length: %w", err)
	}
	topicForgotten := make([]Topic, 0, topicForgottenLen)
	for range topicForgotten {
		topic, err := DecodeTopic(r)
		if err != nil {
			return nil, fmt.Errorf("failed to decode topic: %w", err)
		}
		topicForgotten = append(topicForgotten, *topic)
	}
	request.ForgottenTopicsData = topicForgotten
	request.RackID, err = decoder.DecodeCompactString(r)
	if err != nil {
		return nil, fmt.Errorf("failed to decode rack id: %w", err)
	}
	decoder.DecodeTaggedField(r)
	return request, nil
}

func DecodeTopic(r *bufio.Reader) (*Topic, error) {
	topic := &Topic{}
	err := decoder.DecodeValue(r, &topic.TopicID)
	if err != nil {
		return nil, fmt.Errorf("failed to decode topic id: %w", err)
	}
	partitionLen, err := decoder.DecodeCompactArrayLength(r)
	if err != nil {
		return nil, fmt.Errorf("failed to decode partition length: %w", err)
	}
	partitions := make([]Partition, partitionLen)
	for i := 0; i < partitionLen; i++ {
		partition, err := DecodePartition(r)
		if err != nil {
			return nil, fmt.Errorf("failed to decode partition: %w", err)
		}
		partitions = append(partitions, *partition)
	}
	topic.Partitions = partitions
	decoder.DecodeTaggedField(r)
	return topic, nil
}

func DecodePartition(r *bufio.Reader) (*Partition, error) {
	partition := &Partition{}
	err := decoder.DecodeValue(r, &partition.PartitionID)
	if err != nil {
		return nil, fmt.Errorf("failed to decode partition id: %w", err)
	}
	err = decoder.DecodeValue(r, &partition.CurrentLeaderEpoch)
	if err != nil {
		return nil, fmt.Errorf("failed to decode current leader epoch: %w", err)
	}
	err = decoder.DecodeValue(r, &partition.FetchOffset)
	if err != nil {
		return nil, fmt.Errorf("failed to decode fetch offset: %w", err)
	}
	err = decoder.DecodeValue(r, &partition.LastFetchedEpoch)
	if err != nil {
		return nil, fmt.Errorf("failed to decode last fetched epoch: %w", err)
	}
	err = decoder.DecodeValue(r, &partition.LogStartOffset)
	if err != nil {
		return nil, fmt.Errorf("failed to decode log start offset: %w", err)
	}
	err = decoder.DecodeValue(r, &partition.PartitionMaxBytes)
	if err != nil {
		return nil, fmt.Errorf("failed to decode partition max bytes: %w", err)
	}
	decoder.DecodeTaggedField(r)
	return partition, nil
}
