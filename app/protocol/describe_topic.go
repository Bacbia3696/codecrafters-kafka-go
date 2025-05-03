package protocol

import (
	"bufio"
	"fmt"
	"log/slog"
	"net"
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
	// TaggedFields []TaggedField
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
	numTopic, err := DecodeUvarint(r)
	if err != nil {
		return nil, fmt.Errorf("failed to decode array length: %w", err)
	}
	if numTopic == 0 {
		return nil, fmt.Errorf("invalid number of topics: %d", numTopic)
	}
	topics := make([]Topic, numTopic-1)
	for i := range topics {
		name, err := DecodeCompactString(r)
		if err != nil {
			return nil, fmt.Errorf("failed to decode topic name: %w", err)
		}
		topics[i].Name = name
		DecodeTaggedField(r)
	}
	request.Topics = topics

	// 2.parse response partition limit
	responsePartitionLimit, err := DecodeInt32(r)
	if err != nil {
		return nil, fmt.Errorf("failed to decode response partition limit: %w", err)
	}
	request.ResponsePartitionLimit = responsePartitionLimit

	// 3.parse cursor
	request.Cursor = &Cursor{}
	request.Cursor.TopicName, err = DecodeCompactString(r)
	if err != nil {
		return nil, fmt.Errorf("failed to decode cursor topic name: %w", err)
	}
	request.Cursor.PartitionIndex, err = DecodeInt32(r)
	if err != nil {
		return nil, fmt.Errorf("failed to decode cursor partition index: %w", err)
	}

	DecodeTaggedField(r)

	return request, nil
}

func HandleDescribeTopic(log *slog.Logger, conn net.Conn, header *RequestHeader) {
	rd := bufio.NewReader(conn)
	request, err := DecodeDescribeTopicRequest(rd)
	if err != nil {
		log.Error("failed to decode describe topic request", "error", err)
		return
	}

	log.Info("Received DescribeTopic request", "topics", request.Topics, "responsePartitionLimit", request.ResponsePartitionLimit, "cursor", request.Cursor)
}
