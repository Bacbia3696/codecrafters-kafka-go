package apiversions

import (
	"encoding/binary"
	"fmt"
	"io"

	"github.com/codecrafters-io/kafka-starter-go/app/protocol"
)

// ApiVersion holds information about a supported API key and its version range
// Used to construct the response array.
type ApiVersion struct {
	ApiKey     int16
	MinVersion int16 // Added in V2 array item
	MaxVersion int16 // In v0/v1 response, this is the only version sent in the array item
}

// ApiVersionsResponse represents the API Versions response (V3 compatible)
type ApiVersionsResponseV3 struct {
	ErrorCode      int16
	ThrottleTimeMs int32 // Added in V1
	ApiVersions    []ApiVersion
	// TaggedFields omitted, will encode as 0 bytes
}

// Encode writes the ApiVersionsResponse V3 to the writer
func (r *ApiVersionsResponseV3) Encode(w io.Writer, correlationID int32) error {
	// Calculate body size (V3 format: ErrorCode + ThrottleTimeMs + ApiVersions array + OverallTaggedFields)
	arrayLenBytes := protocol.EncodeVarint(int32(len(r.ApiVersions)) + 1)
	bodySize := int32(protocol.ErrorCodeLen + protocol.ThrottleTimeLen + len(arrayLenBytes) + protocol.TaggedFieldsLen)
	for range r.ApiVersions {
		// Array item V3: ApiKey + MinVersion + MaxVersion + TaggedFields
		bodySize += int32(protocol.ApiKeyLen + protocol.ApiVersionLen + protocol.ApiVersionLen + protocol.TaggedFieldsLen)
	}

	headerSize := int32(protocol.CorrelationIDLen)
	messageBodySize := headerSize + bodySize
	totalBufferSize := protocol.MessageSizeLen + messageBodySize
	buf := make([]byte, totalBufferSize)
	offset := 0

	// Encode Message Size (size of header + body)
	binary.BigEndian.PutUint32(buf[offset:offset+protocol.MessageSizeLen], uint32(messageBodySize))
	offset += protocol.MessageSizeLen

	// Encode Header: Correlation ID
	binary.BigEndian.PutUint32(buf[offset:offset+protocol.CorrelationIDLen], uint32(correlationID))
	offset += protocol.CorrelationIDLen

	// --- Start Body Encoding --- (Order based on V3 schema)

	// Encode Body: ErrorCode
	binary.BigEndian.PutUint16(buf[offset:offset+protocol.ErrorCodeLen], uint16(r.ErrorCode))
	offset += protocol.ErrorCodeLen

	// Encode Body: ApiVersions Array Length (UNSIGNED_VARINT)
	copy(buf[offset:], arrayLenBytes)
	offset += len(arrayLenBytes)

	// Encode Body: ApiVersions Array Elements (V3 format)
	for _, version := range r.ApiVersions {
		binary.BigEndian.PutUint16(buf[offset:offset+protocol.ApiKeyLen], uint16(version.ApiKey))
		offset += protocol.ApiKeyLen

		binary.BigEndian.PutUint16(buf[offset:offset+protocol.ApiVersionLen], uint16(version.MinVersion))
		offset += protocol.ApiVersionLen

		binary.BigEndian.PutUint16(buf[offset:offset+protocol.ApiVersionLen], uint16(version.MaxVersion))
		offset += protocol.ApiVersionLen

		// Tagged fields for array item (V3+) - sending 0
		buf[offset] = 0 // UNSIGNED_VARINT 0 takes 1 byte
		offset += protocol.TaggedFieldsLen
	}

	// Encode Body: ThrottleTimeMs (Comes *after* array in V3)
	binary.BigEndian.PutUint32(buf[offset:offset+protocol.ThrottleTimeLen], uint32(r.ThrottleTimeMs))
	offset += protocol.ThrottleTimeLen

	// Encode Body: Overall Tagged fields (V3+) - sending 0
	buf[offset] = 0 // UNSIGNED_VARINT 0 takes 1 byte
	offset += protocol.TaggedFieldsLen

	// --- End Body Encoding ---

	if offset != int(totalBufferSize) {
		return fmt.Errorf("api versions v3 encode error: offset %d != calculated size %d", offset, totalBufferSize)
	}

	// Write to network
	_, err := w.Write(buf)
	if err != nil {
		return fmt.Errorf("failed to write ApiVersions response: %w", err)
	}
	return nil
}
