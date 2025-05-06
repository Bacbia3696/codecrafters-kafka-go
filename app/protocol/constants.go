package protocol

// API Keys
const (
	ApiKeyApiVersions             int16 = 18
	ApiKeyDescribeTopicPartitions int16 = 75
	// Add more API keys as needed
)

// Error Codes
const (
	ErrorCodeNone               int16 = 0
	ErrorCodeUnknownTopic       int16 = 3
	ErrorCodeUnsupportedVersion int16 = 35
)
