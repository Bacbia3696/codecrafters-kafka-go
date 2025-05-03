package protocol

import (
	"encoding/binary"
	"fmt"
	"io"
	"log/slog"
	"net"
	"slices"
)

// SupportedApiVersions maps API keys to their version range
// Key: ApiKey, Value: MaxVersion (minimum is assumed to be 0 for the *logic*)
var SupportedApiVersions = map[int16]int16{
	ApiKeyApiVersions:             4,
	ApiKeyDescribeTopicPartitions: 0,
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

type ApiVersionsRequest struct {
	ClientSoftwareName    string // compact string
	ClientSoftwareVersion string // compact string
}

func DecodeApiVersionsRequest(r io.Reader) (*ApiVersionsRequest, error) {
	request := &ApiVersionsRequest{}
	var err error
	request.ClientSoftwareName, err = DecodeCompactString(r)
	if err != nil {
		return nil, fmt.Errorf("failed to decode client_software_name: %w", err)
	}
	request.ClientSoftwareVersion, err = DecodeCompactString(r)
	if err != nil {
		return nil, fmt.Errorf("failed to decode client_software_version: %w", err)
	}
	// SkipTaggedField(r)
	DecodeTaggedField(r)
	return request, nil
}

// Encode writes the ApiVersionsResponse V3 to the writer
func (r *ApiVersionsResponseV3) Encode(w io.Writer, correlationID int32) error {
	// Calculate body size (V3 format: ErrorCode + ThrottleTimeMs + ApiVersions array + OverallTaggedFields)
	arrayLenBytes := encodeVarint(int32(len(r.ApiVersions)) + 1)
	bodySize := int32(ErrorCodeLen + ThrottleTimeLen + len(arrayLenBytes) + TaggedFieldsLen)
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

	// --- Start Body Encoding --- (Order based on V3 schema)

	// Encode Body: ErrorCode
	binary.BigEndian.PutUint16(buf[offset:offset+ErrorCodeLen], uint16(r.ErrorCode))
	offset += ErrorCodeLen

	// Encode Body: ApiVersions Array Length (UNSIGNED_VARINT)
	copy(buf[offset:], arrayLenBytes)
	offset += len(arrayLenBytes)

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

	// Encode Body: ThrottleTimeMs (Comes *after* array in V3)
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

// HandleApiVersions handles the ApiVersions request, using the provided logger
func HandleApiVersions(log *slog.Logger, conn net.Conn, header *RequestHeader) {
	log.Info("Handling ApiVersions request", "apiKey", header.ApiKey, "apiVersion", header.ApiVersion)

	if header.ApiVersion != 4 { // Assuming we only support V4 requests for V3 response format
		log.Warn("Unsupported ApiVersion requested", "requestedVersion", header.ApiVersion)
		response := ApiVersionsResponseV3{
			ErrorCode: ErrorCodeUnsupportedVersion,
		}
		// Log error from encode?
		if err := response.Encode(conn, header.CorrelationID); err != nil {
			log.Error("Error sending unsupported version response", "error", err)
		}
		return
	}

	request, err := DecodeApiVersionsRequest(conn)
	if err != nil {
		log.Error("Error decoding ApiVersions request", "error", err)
		return
	}
	log.Debug("Received ApiVersions request", "clientSoftwareName", request.ClientSoftwareName, "clientSoftwareVersion", request.ClientSoftwareVersion)

	// Build supported versions from our map
	keys := make([]int16, 0, len(SupportedApiVersions))
	for k := range SupportedApiVersions {
		keys = append(keys, k)
	}
	slices.Sort(keys)

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

	log.Debug("Sending ApiVersions response", "response", response) // Use Debug level for full response
	if err := response.Encode(conn, header.CorrelationID); err != nil {
		log.Error("Error sending ApiVersions response", "error", err)
	} else {
		log.Info("Successfully sent ApiVersions response")
	}
}
