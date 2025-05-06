package describetopic

import (
	"bufio"
	"fmt"

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
	numTopic, err := decoder.DecodeUvarint(r)
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
		decoder.DecodeTaggedField(r)
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
	nextByte, err := decoder.PeekNextByte(r)
	if err != nil {
		return nil, fmt.Errorf("failed to peek next byte: %w", err)
	}
	if nextByte == 0xff { // check for compact nullable string null (-1 length is 0 in uvarint)
		_, err = decoder.DecodeUvarint(r) // Consume the null marker (length 0)
		if err != nil {
			return nil, fmt.Errorf("failed to consume null cursor marker: %w", err)
		}
		request.Cursor = nil
	} else {
		request.Cursor = &Cursor{}
		request.Cursor.TopicName, err = decoder.DecodeCompactString(r)
		if err != nil {
			return nil, fmt.Errorf("failed to decode cursor topic name: %w", err)
		}
		var cursorPartitionIndexInt32 int32
		err = decoder.DecodeValue(r, &cursorPartitionIndexInt32)
		if err != nil {
			return nil, fmt.Errorf("failed to decode cursor partition index: %w", err)
		}
		request.Cursor.PartitionIndex = cursorPartitionIndexInt32
		decoder.DecodeTaggedField(r) // Cursor tagged fields
	}

	decoder.DecodeTaggedField(r) // Overall request tagged fields

	return request, nil
}
