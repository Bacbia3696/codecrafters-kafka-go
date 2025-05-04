package describetopic

import (
	"bufio"
	"bytes"
	"io"
	"log/slog"

	"github.com/codecrafters-io/kafka-starter-go/app/protocol"
	"github.com/google/uuid"
)

func HandleDescribeTopic(log *slog.Logger, reader *bufio.Reader, writer io.Writer, header *protocol.RequestHeader) {
	log.Info("HandleDescribeTopic")
	request, err := DecodeDescribeTopicRequest(reader)
	if err != nil {
		log.Error("failed to decode describe topic request", "error", err)
		return
	}
	log.Info("Received DescribeTopic request", "topics", request.Topics, "responsePartitionLimit", request.ResponsePartitionLimit, "cursor", request.Cursor)
	responseHeader := &protocol.ResponseHeader{
		CorrelationID: header.CorrelationID,
	}
	response := &DescribeTopicResponse{
		ThrottleTime: 0,
		Topics:       make([]TopicResponse, len(request.Topics)),
		NextCursor:   nil,
	}
	for i, t := range request.Topics {
		response.Topics[i] = TopicResponse{
			ErrorCode:  protocol.ErrorCodeUnknownTopic,
			Name:       t.Name,
			TopicID:    uuid.Nil,
			IsInternal: false,
			Partitions: []PartitionResponse{},
		}
	}

	var buf bytes.Buffer
	err = responseHeader.Encode(&buf)
	if err != nil {
		log.Error("failed to encode describe topic response header", "error", err)
		return
	}
	err = response.Encode(&buf)
	if err != nil {
		log.Error("failed to encode describe topic response to buffer", "error", err)
		return
	}

	bytesWritten := buf.Len()

	err = protocol.EncodeI32(writer, int32(bytesWritten))
	if err != nil {
		log.Error("failed to encode describe topic response length", "error", err)
		return
	}
	_, err = buf.WriteTo(writer)
	if err != nil {
		log.Error("failed to write describe topic response buffer to writer", "error", err)
		return
	}

	log.Info("Sent DescribeTopic response", "response", response, "bytesWritten", bytesWritten)
}
