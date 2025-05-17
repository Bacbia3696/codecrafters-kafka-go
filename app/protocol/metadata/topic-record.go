package metadata

import (
	"bufio"

	"github.com/codecrafters-io/kafka-starter-go/app/decoder"
	"github.com/google/uuid"
)

// {
// 	"apiKey": 2,
// 	"type": "metadata",
// 	"name": "TopicRecord",
// 	"validVersions": "0",
// 	"flexibleVersions": "0+",
// 	"fields": [
// 	  { "name": "Name", "type": "string", "versions": "0+", "entityType": "topicName",
// 		"about": "The topic name." },
// 	  { "name": "TopicId", "type": "uuid", "versions": "0+",
// 		"about": "The unique ID of this topic." }
// 	]
//   }

type TopicRecord struct {
	Name    string
	TopicId uuid.UUID
	// tagged field
}

func DecodeTopicRecord(r *bufio.Reader) (*TopicRecord, error) {
	record := &TopicRecord{}
	var err error
	record.Name, err = decoder.DecodeCompactString(r)
	if err != nil {
		return nil, err
	}
	err = decoder.DecodeValue(r, &record.TopicId)
	if err != nil {
		return nil, err
	}
	decoder.DecodeEmptyTaggedField(r)
	return record, nil
}
