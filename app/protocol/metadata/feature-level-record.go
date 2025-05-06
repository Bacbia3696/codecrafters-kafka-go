package metadata

import (
	"bufio"

	"github.com/codecrafters-io/kafka-starter-go/app/decoder"
)

// {
// 	// Note: New metadata logs and snapshots begin with a FeatureLevelRecord which specifies the
// 	// metadata level that is required to read them. The version of that record cannot advance
// 	// beyond 0, for backwards compatibility reasons.
// 	"apiKey": 12,
// 	"type": "metadata",
// 	"name": "FeatureLevelRecord",
// 	"validVersions": "0",
// 	"flexibleVersions": "0+",
// 	"fields": [
// 	  { "name": "Name", "type": "string", "versions": "0+",
// 		"about": "The feature name." },
// 	  { "name": "FeatureLevel", "type": "int16", "versions": "0+",
// 		"about": "The current finalized feature level of this feature for the cluster, a value of 0 means feature not supported." }
// 	]
//   }

type FeatureLevelRecord struct {
	Name         string
	FeatureLevel int16
	// tagged field
}

func DecodeFeatureLevelRecord(r *bufio.Reader) (*FeatureLevelRecord, error) {
	record := &FeatureLevelRecord{}
	var err error
	record.Name, err = decoder.DecodeCompactString(r)
	if err != nil {
		return nil, err
	}
	err = decoder.DecodeValue(r, &record.FeatureLevel)
	if err != nil {
		return nil, err
	}
	decoder.DecodeTaggedField(r)
	return record, nil
}
