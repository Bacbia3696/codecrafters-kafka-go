package metadata

type RecordType int8

const (
	RecordTypeTopic        RecordType = 2
	RecordTypePartition    RecordType = 3
	RecordTypeFeatureLevel RecordType = 12
)
