package protocol

import (
	"bufio"
	"errors"
	"io"
	"os"

	"github.com/codecrafters-io/kafka-starter-go/app/protocol/metadata"
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
