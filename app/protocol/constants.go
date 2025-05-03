package protocol

// Protocol message field sizes in bytes
const (
	MessageSizeLen   = 4 // Size prefix for messages
	ApiKeyLen        = 2 // API key field
	ApiVersionLen    = 2 // API version field
	CorrelationIDLen = 4 // Correlation ID field
	ErrorCodeLen     = 2 // Error codes are INT16
	ThrottleTimeLen  = 4 // Throttle time field (INT32) - Present in V1+
	TaggedFieldsLen  = 1 // UNSIGNED_VARINT for Tagged Fields count (always 0 for now)
)

// API Keys
const (
	ApiKeyApiVersions             int16 = 18
	ApiKeyDescribeTopicPartitions int16 = 75
	// Add more API keys as needed
)

// Error Codes
const (
	ErrorCodeNone               int16 = 0
	ErrorCodeUnsupportedVersion int16 = 35
)
