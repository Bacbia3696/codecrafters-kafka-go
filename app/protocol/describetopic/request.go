package describetopic

import (
	"bufio"
	"fmt"

	"github.com/codecrafters-io/kafka-starter-go/app/protocol"
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
	request := &DescribeTopicRequest{}

	// 1.parse topics
	numTopic, err := protocol.DecodeUvarint(r)
	if err != nil {
		return nil, fmt.Errorf("failed to decode array length: %w", err)
	}
	if numTopic == 0 {
		return nil, fmt.Errorf("invalid number of topics: %d", numTopic)
	}
	topics := make([]Topic, numTopic-1)
	for i := range topics {
		name, err := protocol.DecodeCompactString(r)
		if err != nil {
			return nil, fmt.Errorf("failed to decode topic name: %w", err)
		}
		topics[i].Name = name
		protocol.DecodeTaggedField(r) // Assuming DecodeTaggedField handles potential tagged fields
	}
	request.Topics = topics

	// 2.parse response partition limit
	responsePartitionLimit, err := protocol.DecodeInt32(r)
	if err != nil {
		return nil, fmt.Errorf("failed to decode response partition limit: %w", err)
	}
	request.ResponsePartitionLimit = responsePartitionLimit

	// 3.parse cursor
	nextByte, err := protocol.PeekNextByte(r)
	if err != nil {
		return nil, fmt.Errorf("failed to peek next byte: %w", err)
	}
	// cursor is null
	if nextByte == 0xff {
		// forward the reader to the next byte
		r.ReadByte()
	} else {
		request.Cursor = &Cursor{}
		request.Cursor.TopicName, err = protocol.DecodeCompactString(r)
		if err != nil {
			return nil, fmt.Errorf("failed to decode cursor topic name: %w", err)
		}
		request.Cursor.PartitionIndex, err = protocol.DecodeInt32(r)
		if err != nil {
			return nil, fmt.Errorf("failed to decode cursor partition index: %w", err)
		}

	}

	protocol.DecodeTaggedField(r) // Assuming DecodeTaggedField handles potential tagged fields

	return request, nil
}
