package metadata

import (
	"bufio"

	"github.com/codecrafters-io/kafka-starter-go/app/decoder"
	"github.com/google/uuid"
)

// {
// 	"apiKey": 3,
// 	"type": "metadata",
// 	"name": "PartitionRecord",
// 	// Version 1 adds Directories for KIP-858
// 	// Version 2 implements Eligible Leader Replicas and LastKnownElr as described in KIP-966.
// 	"validVersions": "0-2",
// 	"flexibleVersions": "0+",
// 	"fields": [
// 	  { "name": "PartitionId", "type": "int32", "versions": "0+", "default": "-1",
// 		"about": "The partition id." },
// 	  { "name": "TopicId", "type": "uuid", "versions": "0+",
// 		"about": "The unique ID of this topic." },
// 	  { "name": "Replicas", "type":  "[]int32", "versions":  "0+", "entityType": "brokerId",
// 		"about": "The replicas of this partition, sorted by preferred order." },
// 	  { "name": "Isr", "type":  "[]int32", "versions":  "0+",
// 		"about": "The in-sync replicas of this partition" },
// 	  { "name": "RemovingReplicas", "type":  "[]int32", "versions":  "0+", "entityType": "brokerId",
// 		"about": "The replicas that we are in the process of removing." },
// 	  { "name": "AddingReplicas", "type":  "[]int32", "versions":  "0+", "entityType": "brokerId",
// 		"about": "The replicas that we are in the process of adding." },
// 	  { "name": "Leader", "type": "int32", "versions": "0+", "default": "-1", "entityType": "brokerId",
// 		"about": "The lead replica, or -1 if there is no leader." },
// 	  { "name": "LeaderRecoveryState", "type": "int8", "default": "0", "versions": "0+", "taggedVersions": "0+", "tag": 0,
// 		"about": "1 if the partition is recovering from an unclean leader election; 0 otherwise." },
// 	  { "name": "LeaderEpoch", "type": "int32", "versions": "0+", "default": "-1",
// 		"about": "The epoch of the partition leader." },
// 	  { "name": "PartitionEpoch", "type": "int32", "versions": "0+", "default": "-1",
// 		"about": "An epoch that gets incremented each time we change anything in the partition." },
// 	  { "name": "Directories", "type": "[]uuid", "versions": "1+",
// 		"about": "The log directory hosting each replica, sorted in the same exact order as the Replicas field."},
// ======SKIP===============
// 	  { "name": "EligibleLeaderReplicas", "type": "[]int32", "default": "null", "entityType": "brokerId",
// 		"versions": "2+", "nullableVersions": "2+", "taggedVersions": "2+", "tag": 1,
// 		"about": "The eligible leader replicas of this partition." },
// 	  { "name": "LastKnownElr", "type": "[]int32", "default": "null", "entityType": "brokerId",
// 		"versions": "2+", "nullableVersions": "2+", "taggedVersions": "2+", "tag": 2,
// 		"about": "The last known eligible leader replicas of this partition." }
// ======SKIP===============
// 	]
//   }

type PartitionRecord struct {
	PartitionId      int32
	TopicId          uuid.UUID
	Replicas         []int32
	Isr              []int32
	RemovingReplicas []int32
	AddingReplicas   []int32
	Leader           int32
	LeaderEpoch      int32
	PartitionEpoch   int32
	Directories      []uuid.UUID
	// EligibleLeaderReplicas []int32
	// tagged field
	// LastKnownElr           []int32
}

func DecodePartitionRecord(r *bufio.Reader) (*PartitionRecord, error) {
	record := &PartitionRecord{}
	var err error
	err = decoder.DecodeValue(r, &record.PartitionId)
	if err != nil {
		return nil, err
	}
	err = decoder.DecodeValue(r, &record.TopicId)
	if err != nil {
		return nil, err
	}
	record.Replicas, err = DecodeCompactArrayInt32(r)
	if err != nil {
		return nil, err
	}
	record.Isr, err = DecodeCompactArrayInt32(r)
	if err != nil {
		return nil, err
	}
	record.RemovingReplicas, err = DecodeCompactArrayInt32(r)
	if err != nil {
		return nil, err
	}
	record.AddingReplicas, err = DecodeCompactArrayInt32(r)
	if err != nil {
		return nil, err
	}
	err = decoder.DecodeValue(r, &record.Leader)
	if err != nil {
		return nil, err
	}
	err = decoder.DecodeValue(r, &record.LeaderEpoch)
	if err != nil {
		return nil, err
	}
	err = decoder.DecodeValue(r, &record.PartitionEpoch)
	if err != nil {
		return nil, err
	}
	directoriesLength, err := decoder.DecodeCompactArrayLength(r)
	if err != nil {
		return nil, err
	}
	record.Directories = make([]uuid.UUID, directoriesLength)
	for i := range record.Directories {
		record.Directories[i], err = decoder.DecodeUUID(r)
		if err != nil {
			return nil, err
		}
	}
	decoder.DecodeTaggedField(r)
	return record, nil
}
func DecodeCompactArrayInt32(r *bufio.Reader) ([]int32, error) {
	length, err := decoder.DecodeCompactArrayLength(r)
	if err != nil {
		return nil, err
	}
	arr := make([]int32, length)
	for i := range arr {
		err = decoder.DecodeValue(r, &arr[i])
		if err != nil {
			return nil, err
		}
	}
	return arr, nil
}
