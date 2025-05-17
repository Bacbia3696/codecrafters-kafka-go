package describetopic

import (
	"bufio"
	"fmt"
	"log/slog"

	"github.com/codecrafters-io/kafka-starter-go/app/decoder"
)

// DescribeTopicPartitions Request (Version: 0) => [topics] response_partition_limit cursor _tagged_fields
//
//	topics => name _tagged_fields
//	  name => COMPACT_STRING
//	response_partition_limit => INT32
//	cursor => topic_name partition_index _tagged_fields
//	  topic_name => COMPACT_STRING
//	  partition_index => INT32
type DescribeTopicRequest struct {
	// compact array
	Topics                 []Topic
	ResponsePartitionLimit int32
	Cursor                 *Cursor
	// TaggedFields []TaggedField // Assuming TaggedField is defined elsewhere or not needed for decoding
}

func DecodeCursor(r *bufio.Reader) (*Cursor, error) {
	next, err := decoder.PeekNextByte(r)
	if err != nil {
		return nil, fmt.Errorf("failed to peek next byte: %w", err)
	}
	if next == 0xff {
		_, err = r.ReadByte()
		if err != nil {
			return nil, fmt.Errorf("failed to read byte: %w", err)
		}
		return nil, nil
	}
	cursor := &Cursor{}
	cursor.TopicName, err = decoder.DecodeCompactString(r)
	if err != nil {
		return nil, fmt.Errorf("failed to decode cursor topic name: %w", err)
	}
	var cursorPartitionIndexInt32 int32
	err = decoder.DecodeValue(r, &cursorPartitionIndexInt32)
	if err != nil {
		return nil, fmt.Errorf("failed to decode cursor partition index: %w", err)
	}
	cursor.PartitionIndex = cursorPartitionIndexInt32
	decoder.DecodeEmptyTaggedField(r) // Cursor tagged fields
	return cursor, nil
}

type Topic struct {
	Name string
	// TaggedFields []TaggedField
}

type Cursor struct {
	TopicName      string
	PartitionIndex int32
	// TaggedFields []TaggedField
}

func DecodeDescribeTopicRequest(r *bufio.Reader) (*DescribeTopicRequest, error) {
	slog.Info("DecodeDescribeTopicRequest")
	request := &DescribeTopicRequest{}

	// 1.parse topics
	numTopic, err := decoder.DecodeUvarint(r)
	slog.Info("numTopic", "numTopic", numTopic)
	if err != nil {
		return nil, fmt.Errorf("failed to decode array length: %w", err)
	}
	if numTopic == 0 {
		return nil, fmt.Errorf("invalid number of topics: %d", numTopic)
	}
	topics := make([]Topic, numTopic-1)
	for i := range topics {
		name, err := decoder.DecodeCompactString(r)
		if err != nil {
			return nil, fmt.Errorf("failed to decode topic name: %w", err)
		}
		topics[i].Name = name
		decoder.DecodeEmptyTaggedField(r)
	}
	request.Topics = topics

	// 2.parse response partition limit
	var responsePartitionLimitInt32 int32
	err = decoder.DecodeValue(r, &responsePartitionLimitInt32)
	if err != nil {
		return nil, fmt.Errorf("failed to decode response partition limit: %w", err)
	}
	request.ResponsePartitionLimit = responsePartitionLimitInt32
	// 3.parse cursor
	request.Cursor, err = DecodeCursor(r)
	if err != nil {
		return nil, fmt.Errorf("failed to decode cursor: %w", err)
	}
	decoder.DecodeEmptyTaggedField(r) // Overall request tagged fields

	return request, nil
}
