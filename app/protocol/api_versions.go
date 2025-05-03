package protocol

import (
	"encoding/binary"
	"fmt"
	"io"
	"log"
	"net"
	"sort"
)

// SupportedApiVersions maps API keys to their version range
// Key: ApiKey, Value: MaxVersion (minimum is assumed to be 0 for the *logic*)
var SupportedApiVersions = map[int16]int16{
	ApiKeyApiVersions: 4, // We will respond with V3 structure
	// Add more API keys as they are implemented
}

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
	// Calculate body size (V3 format: ErrorCode + ApiVersions array + ThrottleTimeMs + TaggedFields)
	bodySize := int32(ErrorCodeLen + ArrayLengthLen + ThrottleTimeLen + TaggedFieldsLen)
	for range r.ApiVersions {
		// Array item V3: ApiKey + MinVersion + MaxVersion + TaggedFields
		bodySize += int32(ApiKeyLen + ApiVersionLen + ApiVersionLen + TaggedFieldsLen)
	}

	headerSize := int32(CorrelationIDLen)
	messageBodySize := headerSize + bodySize
	totalBufferSize := MessageSizeLen + messageBodySize
	buf := make([]byte, totalBufferSize)
	offset := 0

	// Encode Message Size (size of header + body)
	binary.BigEndian.PutUint32(buf[offset:offset+MessageSizeLen], uint32(messageBodySize))
	offset += MessageSizeLen

	// Encode Header: Correlation ID
	binary.BigEndian.PutUint32(buf[offset:offset+CorrelationIDLen], uint32(correlationID))
	offset += CorrelationIDLen

	// --- Start Body Encoding ---

	// Encode Body: ErrorCode
	binary.BigEndian.PutUint16(buf[offset:offset+ErrorCodeLen], uint16(r.ErrorCode))
	offset += ErrorCodeLen

	// Encode Body: ApiVersions Array Length
	binary.BigEndian.PutUint32(buf[offset:offset+ArrayLengthLen], uint32(len(r.ApiVersions)))
	offset += ArrayLengthLen

	// Encode Body: ApiVersions Array Elements (V3 format)
	for _, version := range r.ApiVersions {
		binary.BigEndian.PutUint16(buf[offset:offset+ApiKeyLen], uint16(version.ApiKey))
		offset += ApiKeyLen
		binary.BigEndian.PutUint16(buf[offset:offset+ApiVersionLen], uint16(version.MinVersion))
		offset += ApiVersionLen
		binary.BigEndian.PutUint16(buf[offset:offset+ApiVersionLen], uint16(version.MaxVersion))
		offset += ApiVersionLen
		// Tagged fields for array item (V3+) - sending 0
		buf[offset] = 0 // UNSIGNED_VARINT 0 takes 1 byte
		offset += TaggedFieldsLen
	}

	// Encode Body: ThrottleTimeMs (V1+)
	binary.BigEndian.PutUint32(buf[offset:offset+ThrottleTimeLen], uint32(r.ThrottleTimeMs))
	offset += ThrottleTimeLen

	// Encode Body: Overall Tagged fields (V3+) - sending 0
	buf[offset] = 0 // UNSIGNED_VARINT 0 takes 1 byte
	offset += TaggedFieldsLen

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

// HandleApiVersions handles the ApiVersions request
func HandleApiVersions(conn net.Conn, header RequestHeader) {
	log.Printf("Handling ApiVersions request (Key %d, Version %d)", header.ApiKey, header.ApiVersion)

	// Note: A real broker *might* check header.ApiVersion to determine response format.
	// We are implementing V3 response format here.

	// Build supported versions from our map
	keys := make([]int16, 0, len(SupportedApiVersions))
	for k := range SupportedApiVersions {
		keys = append(keys, k)
	}
	sort.Slice(keys, func(i, j int) bool { return keys[i] < keys[j] })

	versions := make([]ApiVersion, 0, len(SupportedApiVersions))
	for _, apiKey := range keys {
		maxVersion := SupportedApiVersions[apiKey]
		versions = append(versions, ApiVersion{
			ApiKey:     apiKey,
			MinVersion: 0,          // Assuming min supported is 0 for now
			MaxVersion: maxVersion, // This is the max version *we* support for this key
		})
	}

	response := ApiVersionsResponseV3{
		ErrorCode:      ErrorCodeNone,
		ThrottleTimeMs: 0,
		ApiVersions:    versions,
	}

	log.Printf("Sending ApiVersions response V3: %+v", response)
	if err := response.Encode(conn, header.CorrelationID); err != nil {
		log.Printf("Error sending ApiVersions response: %v", err)
	} else {
		log.Printf("Successfully sent ApiVersions response")
	}
}
