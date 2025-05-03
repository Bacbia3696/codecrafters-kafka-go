package main

import (
	"encoding/binary"
	"fmt"
	"io"
	"log"
	"net"
	"sort"
)

const (
	messageSizeLen      = 4 // Bytes for message size
	apiKeyLen           = 2 // Bytes for API key
	apiVersionLen       = 2 // Bytes for API version
	correlationIDLen    = 4 // Bytes for correlation ID
	clientIDLen         = 2 // Bytes for client ID length prefix in header v1+
	requestHeaderMinLen = apiKeyLen + apiVersionLen + correlationIDLen

	// Offsets within the minimal V0/V1 header part we read
	apiKeyOffset        = 0
	apiVersionOffset    = apiKeyOffset + apiKeyLen
	correlationIDOffset = apiVersionOffset + apiVersionLen

	// Specific values for current stage requirements
	expectedApiVersion          = 4
	unsupportedVersionErrorCode = 35 // Kafka Error Code: UNSUPPORTED_VERSION
	errorCodeLen                = 2  // Error codes are INT16

	// API Keys
	ApiVersionsApiKey = 18

	// Error Codes
	NoError                     = 0
	UnsupportedVersionErrorCode = 35 // Kafka Error Code: UNSUPPORTED_VERSION
	UnknownServerErrorCode      = -1 // Example for unknown API Key

	// Kafka protocol uses INT32 for array lengths
	arrayLengthLen = 4
)

// requestHeader represents the part of the header we decoded
type requestHeader struct {
	apiKey        int16
	apiVersion    int16 // Version of the *request* schema
	correlationID int32
	// clientID omitted for now
}

// --- ApiVersions Structures ---

// ApiVersion holds information about a supported API key and its version range.
// For V0 response, MinVersion = MaxVersion = Supported Version.
type ApiVersion struct {
	ApiKey     int16
	MinVersion int16 // V0 uses 'Version', maps to MinVersion
	MaxVersion int16 // V0 uses 'Version', maps to MaxVersion
}

// ApiVersionsResponseV0 represents the body of the ApiVersions response V0.
type ApiVersionsResponseV0 struct {
	ErrorCode   int16
	ApiVersions []ApiVersion
}

// Encode writes the ApiVersionsResponseV0 (including header) to the writer.
func (r *ApiVersionsResponseV0) Encode(w io.Writer, correlationID int32) error {
	// Calculate body size
	// Recalculate V0 body size: ErrorCode (2) + ArrayLen (4) + NumVersions * (ApiKey (2) + Version (2))
	v0BodySize := int32(errorCodeLen + arrayLengthLen)
	for range r.ApiVersions {
		v0BodySize += int32(apiKeyLen + apiVersionLen) // V0: Key + Version
	}

	// Calculate overall message size
	headerSize := int32(correlationIDLen)
	messageBodySize := headerSize + v0BodySize // Size field refers to header + body
	totalBufferSize := messageSizeLen + messageBodySize
	buf := make([]byte, totalBufferSize)
	offset := 0

	// Encode Message Size (size of header + body)
	binary.BigEndian.PutUint32(buf[offset:offset+messageSizeLen], uint32(messageBodySize))
	offset += messageSizeLen

	// Encode Header: Correlation ID
	binary.BigEndian.PutUint32(buf[offset:offset+correlationIDLen], uint32(correlationID))
	offset += correlationIDLen

	// Encode Body: ErrorCode
	binary.BigEndian.PutUint16(buf[offset:offset+errorCodeLen], uint16(r.ErrorCode))
	offset += errorCodeLen

	// Encode Body: ApiVersions Array Length
	binary.BigEndian.PutUint32(buf[offset:offset+arrayLengthLen], uint32(len(r.ApiVersions)))
	offset += arrayLengthLen

	// Encode Body: ApiVersions Array Elements (V0 format: ApiKey, Version)
	for _, version := range r.ApiVersions {
		binary.BigEndian.PutUint16(buf[offset:offset+apiKeyLen], uint16(version.ApiKey))
		offset += apiKeyLen
		// For V0 response, Version field represents the MaxVersion broker supports
		binary.BigEndian.PutUint16(buf[offset:offset+apiVersionLen], uint16(version.MaxVersion))
		offset += apiVersionLen
	}

	// Write to network
	_, err := w.Write(buf)
	if err != nil {
		return fmt.Errorf("failed to write ApiVersionsResponseV0: %w", err)
	}
	return nil
}

// ErrorResponse represents a response containing only a header and an error code body.
type ErrorResponse struct {
	correlationID int32
	errorCode     int16
}

// Encode writes the error response (message size, header correlationID, body errorCode) to the writer.
func (r *ErrorResponse) Encode(w io.Writer) error {
	headerSize := int32(correlationIDLen)
	bodySize := int32(errorCodeLen)
	messageBodySize := headerSize + bodySize // Size field refers to header + body
	totalBufferSize := messageSizeLen + messageBodySize
	buf := make([]byte, totalBufferSize)

	// Encode Message Size (size of header + body)
	binary.BigEndian.PutUint32(buf[0:messageSizeLen], uint32(messageBodySize))

	// Encode Header Correlation ID
	binary.BigEndian.PutUint32(buf[messageSizeLen:messageSizeLen+correlationIDLen], uint32(r.correlationID))

	// Encode Body Error Code
	binary.BigEndian.PutUint16(buf[messageSizeLen+correlationIDLen:totalBufferSize], uint16(r.errorCode))

	// Write to network
	_, err := w.Write(buf)
	if err != nil {
		return fmt.Errorf("failed to write error response: %w", err)
	}
	return nil
}

// Define the API versions supported by *this* broker implementation.
// Key: ApiKey, Value: Max Supported Version (used in V0 response)
var supportedApiVersions = map[int16]int16{
	ApiVersionsApiKey: 0, // We support ApiVersions V0 request/response initially
	// Add other supported APIs here as they are implemented, e.g.,
	// FetchApiKey: 0,
}

// requestHandler defines the function signature for handling a specific API key.
type requestHandler func(conn net.Conn, header requestHeader)

// apiHandlers maps API keys to their handler functions.
var apiHandlers = map[int16]requestHandler{
	ApiVersionsApiKey: handleApiVersions,
	// Add other handlers here:
	// FetchApiKey: handleFetch,
}

// handleApiVersions handles the ApiVersions request (Key 18).
func handleApiVersions(conn net.Conn, header requestHeader) {
	log.Printf("Handling ApiVersions request (Key %d, Version %d) from %s", header.apiKey, header.apiVersion, conn.RemoteAddr())

	// Construct the list of supported versions from our map
	// Sort keys for deterministic order in response (good practice)
	keys := make([]int16, 0, len(supportedApiVersions))
	for k := range supportedApiVersions {
		keys = append(keys, k)
	}
	sort.Slice(keys, func(i, j int) bool { return keys[i] < keys[j] })

	versions := make([]ApiVersion, 0, len(supportedApiVersions))
	for _, apiKey := range keys {
		maxVersion := supportedApiVersions[apiKey]
		versions = append(versions, ApiVersion{
			ApiKey:     apiKey,
			MinVersion: 0,          // For V0 response, MinVersion isn't sent, but set for consistency
			MaxVersion: maxVersion, // V0 response sends this in the 'Version' field
		})
	}

	response := ApiVersionsResponseV0{
		ErrorCode:   NoError,
		ApiVersions: versions,
	}

	log.Printf("Sending ApiVersions response: %+v", response)
	if err := response.Encode(conn, header.correlationID); err != nil {
		log.Printf("Error sending ApiVersions response to %s: %v", conn.RemoteAddr(), err)
	} else {
		log.Printf("Successfully sent ApiVersions response to %s", conn.RemoteAddr())
	}
}

// handleConnection reads the request, dispatches to the correct handler based on API key.
func handleConnection(conn net.Conn) {
	defer conn.Close()
	log.Printf("Connection accepted from %s", conn.RemoteAddr())

	// 1. Read Message Size
	var sizeBuf [messageSizeLen]byte
	if _, err := io.ReadFull(conn, sizeBuf[:]); err != nil {
		// Distinguish EOF from other errors
		if err == io.EOF || err == io.ErrUnexpectedEOF {
			log.Printf("Connection closed by %s before message size received.", conn.RemoteAddr())
		} else {
			log.Printf("Error reading request size from %s: %v", conn.RemoteAddr(), err)
		}
		return // Close connection on error
	}
	totalSize := binary.BigEndian.Uint32(sizeBuf[:])
	log.Printf("Received request of size %d from %s", totalSize, conn.RemoteAddr())

	// Check if size is reasonable (at least minimum header size)
	if totalSize < requestHeaderMinLen {
		log.Printf("Error: Request size %d is too small for header from %s", totalSize, conn.RemoteAddr())
		return
	}

	// 2. Read the minimum header containing ApiKey, ApiVersion, CorrelationID
	headerBuf := make([]byte, requestHeaderMinLen)
	if _, err := io.ReadFull(conn, headerBuf); err != nil {
		log.Printf("Error reading request header from %s: %v", conn.RemoteAddr(), err)
		return
	}

	// 3. Decode header fields
	header := requestHeader{
		apiKey:        int16(binary.BigEndian.Uint16(headerBuf[apiKeyOffset : apiKeyOffset+apiKeyLen])),
		apiVersion:    int16(binary.BigEndian.Uint16(headerBuf[apiVersionOffset : apiVersionOffset+apiVersionLen])),
		correlationID: int32(binary.BigEndian.Uint32(headerBuf[correlationIDOffset : correlationIDOffset+correlationIDLen])),
	}
	log.Printf("Extracted header %+v from %s", header, conn.RemoteAddr())

	// 4. Read and discard the rest of the request payload
	bytesRemaining := int64(totalSize) - int64(requestHeaderMinLen)
	if bytesRemaining < 0 {
		// Should not happen if initial size check passes, but good practice
		log.Printf("Error: Negative remaining bytes calculated (%d) for request from %s", bytesRemaining, conn.RemoteAddr())
		return
	}
	if bytesRemaining > 0 {
		log.Printf("Discarding %d remaining request bytes from %s", bytesRemaining, conn.RemoteAddr())
		if _, err := io.CopyN(io.Discard, conn, bytesRemaining); err != nil {
			err = fmt.Errorf("error reading rest of request payload from %s: %w", conn.RemoteAddr(), err)
			log.Println(err)
			return // Close connection on error
		}
	}

	// 5. Dispatch based on API Key
	if handler, ok := apiHandlers[header.apiKey]; ok {
		handler(conn, header)
	} else {
		// Unknown API Key - Send Error Response
		log.Printf("Unsupported API key %d received from %s. Sending error response.", header.apiKey, conn.RemoteAddr())
		errorResp := ErrorResponse{
			correlationID: header.correlationID,
			errorCode:     UnknownServerErrorCode, // Or specific Kafka code if available like UNSUPPORTED_VERSION (though that's for version mismatch)
		}
		if err := errorResp.Encode(conn); err != nil {
			log.Printf("Error sending 'Unknown API Key' error response to %s: %v", conn.RemoteAddr(), err)
		}
		// Note: Original implementation just closed connection on unsupported version. Sending error is better.
	}
}
