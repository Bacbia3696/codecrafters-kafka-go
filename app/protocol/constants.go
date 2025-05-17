package protocol

// API Keys
const (
	ApiKeyFetch                   int16 = 1
	ApiKeyApiVersions             int16 = 18
	ApiKeyDescribeTopicPartitions int16 = 75
	// Add more API keys as needed
)

// Error Codes
const (
	ErrorCodeNone                    int16 = 0
	ErrorCodeUnknownTopicOrPartition int16 = 3
	ErrorCodeUnsupportedVersion      int16 = 35
	ErrorCodeUnknownTopicID          int16 = 100
)
