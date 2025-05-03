package protocol

// Protocol message field sizes in bytes
const (
	MessageSizeLen   = 4 // Size prefix for messages
	ApiKeyLen        = 2 // API key field
	ApiVersionLen    = 2 // API version field
	CorrelationIDLen = 4 // Correlation ID field
	ClientIDLen      = 2 // Client ID length prefix in header v1+
	ErrorCodeLen     = 2 // Error codes are INT16
	ArrayLengthLen   = 4 // Array length field (INT32)
	ThrottleTimeLen  = 4 // Throttle time field (INT32) - Present in V1+
)

// Header field offsets
const (
	ApiKeyOffset        = 0
	ApiVersionOffset    = ApiKeyOffset + ApiKeyLen
	CorrelationIDOffset = ApiVersionOffset + ApiVersionLen
)

// Minimum header size (without client ID, which is variable length)
const RequestHeaderMinLen = ApiKeyLen + ApiVersionLen + CorrelationIDLen

// API Keys
const (
	ApiKeyApiVersions int16 = 18
	// Add more API keys as needed
)

// Error Codes
const (
	ErrorCodeNone               int16 = 0
	ErrorCodeUnsupportedVersion int16 = 35
	ErrorCodeUnknownServerError int16 = -1
)
