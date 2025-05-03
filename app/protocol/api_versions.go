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
// Key: ApiKey, Value: MaxVersion (minimum is assumed to be 0)
var SupportedApiVersions = map[int16]int16{
	ApiKeyApiVersions: 4,
	// Add more API keys as they are implemented
}

// ApiVersion holds information about a supported API key and its version range
type ApiVersion struct {
	ApiKey     int16
	MinVersion int16 // For v0 response, this isn't sent
	MaxVersion int16 // In v0 response, this is the only version sent
}

// ApiVersionsResponse represents the API Versions response (V1 compatible)
type ApiVersionsResponse struct {
	ErrorCode      int16
	ThrottleTimeMs int32 // Added in V1
	ApiVersions    []ApiVersion
}

// Encode writes the ApiVersionsResponse (V1 compatible) to the writer
func (r *ApiVersionsResponse) Encode(w io.Writer, correlationID int32) error {
	// Calculate body size (V1 format: ErrorCode + ThrottleTimeMs + ApiVersions array)
	bodySize := int32(ErrorCodeLen + ThrottleTimeLen + ArrayLengthLen)
	for range r.ApiVersions {
		bodySize += int32(ApiKeyLen + ApiVersionLen*2) // Key + MaxVersion + MinVersion (V0/V1 array item format)
	}
	fmt.Println("r.ApiVersions", r.ApiVersions)

	headerSize := int32(CorrelationIDLen)
	messageBodySize := headerSize + bodySize
	totalBufferSize := MessageSizeLen + messageBodySize
	buf := make([]byte, totalBufferSize)
	offset := 0

	// Encode Message Size (size of header + body)
	binary.BigEndian.PutUint32(buf[offset:offset+MessageSizeLen], uint32(totalBufferSize))
	offset += MessageSizeLen

	// Encode Header: Correlation ID
	binary.BigEndian.PutUint32(buf[offset:offset+CorrelationIDLen], uint32(correlationID))
	offset += CorrelationIDLen

	// Encode Body: ErrorCode
	binary.BigEndian.PutUint16(buf[offset:offset+ErrorCodeLen], uint16(r.ErrorCode))
	offset += ErrorCodeLen

	// Encode Body: ApiVersions Array Length
	binary.BigEndian.PutUint32(buf[offset:offset+ArrayLengthLen], uint32(len(r.ApiVersions)))
	offset += ArrayLengthLen

	// Encode Body: ApiVersions Array Elements (v0 format)
	for _, version := range r.ApiVersions {
		binary.BigEndian.PutUint16(buf[offset:offset+ApiKeyLen], uint16(version.ApiKey))
		offset += ApiKeyLen
		// v0 only includes MaxVersion as "Version"
		binary.BigEndian.PutUint16(buf[offset:offset+ApiVersionLen], uint16(version.MinVersion))
		offset += ApiVersionLen
		binary.BigEndian.PutUint16(buf[offset:offset+ApiVersionLen], uint16(version.MaxVersion))
		offset += ApiVersionLen
	}

	// Encode Body: ThrottleTimeMs (V1+)
	binary.BigEndian.PutUint32(buf[offset:offset+ThrottleTimeLen], uint32(r.ThrottleTimeMs))
	offset += ThrottleTimeLen

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

	// Note: A real broker would check header.ApiVersion and potentially use
	// a different response schema if the client requested an older version.
	// For now, we always respond with V1 structure.

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
			MinVersion: 0, // For consistency, not in v0 response
			MaxVersion: maxVersion,
		})
	}
	fmt.Println("versions", versions)

	response := ApiVersionsResponse{
		ErrorCode:      ErrorCodeNone,
		ThrottleTimeMs: 0, // Set to 0 as required for V1+
		ApiVersions:    versions,
	}

	if err := response.Encode(conn, header.CorrelationID); err != nil {
		log.Printf("Error sending ApiVersions response: %v", err)
	} else {
		log.Printf("Successfully sent ApiVersions response")
	}
}
