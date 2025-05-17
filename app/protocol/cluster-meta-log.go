package protocol

import (
	"bufio"
	"errors"
	"io"
	"os"

	"github.com/codecrafters-io/kafka-starter-go/app/protocol/metadata"
	"github.com/google/uuid"
)

type ClusterMetadata struct {
	RecordBatchs []metadata.RecordBatch
}

func ReadClusterMetadata() (*ClusterMetadata, error) {
	file, err := os.Open("/tmp/kraft-combined-logs/__cluster_metadata-0/00000000000000000000.log")
	if err != nil {
		return nil, err
	}
	defer file.Close()

	reader := bufio.NewReader(file)
	clusterMetadata := &ClusterMetadata{}
	for {
		recordBatch, err := metadata.DecodeRecordBatch(reader)
		if err != nil {
			if errors.Is(err, io.EOF) {
				break
			}
			return nil, err
		}
		clusterMetadata.RecordBatchs = append(clusterMetadata.RecordBatchs, *recordBatch)
	}
	return clusterMetadata, nil
}

func GetMapTopicByName(data *ClusterMetadata) map[string]metadata.TopicRecord {
	topicMap := make(map[string]metadata.TopicRecord)
	for _, recordBatch := range data.RecordBatchs {
		for _, record := range recordBatch.Records {
			if record.ValueEncodedRecordType == metadata.RecordTypeTopic {
				topic := record.ValueEncodedRecord.(*metadata.TopicRecord)
				topicMap[topic.Name] = *topic
			}
		}
	}
	return topicMap
}

func CheckTopicExistById(data *ClusterMetadata, topicId uuid.UUID) bool {
	for _, recordBatch := range data.RecordBatchs {
		for _, record := range recordBatch.Records {
			if record.ValueEncodedRecordType == metadata.RecordTypeTopic {
				topic := record.ValueEncodedRecord.(*metadata.TopicRecord)
				if topic.TopicId == topicId {
					return true
				}
			}
		}
	}
	return false
}

func GetPartitionsByTopicId(data *ClusterMetadata, topicId uuid.UUID) []metadata.PartitionRecord {
	partitions := []metadata.PartitionRecord{}
	for _, recordBatch := range data.RecordBatchs {
		for _, record := range recordBatch.Records {
			if record.ValueEncodedRecordType == metadata.RecordTypePartition {
				partition := record.ValueEncodedRecord.(*metadata.PartitionRecord)
				if partition.TopicId == topicId {
					partitions = append(partitions, *partition)
				}
			}
		}
	}
	return partitions
}
