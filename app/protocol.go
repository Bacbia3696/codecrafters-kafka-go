package main

import (
	"encoding/binary"
	"fmt"
	"io"
	"log"
	"net"
)

const (
	messageSizeLen        = 4 // Bytes for message size
	apiKeyLen             = 2 // Bytes for API key
	apiVersionLen         = 2 // Bytes for API version
	correlationIDLen      = 4 // Bytes for correlation ID
	clientIDLen           = 2 // Bytes for client ID length prefix in header v1+
	requestHeaderV0MinLen = apiKeyLen + apiVersionLen + correlationIDLen

	// Offsets within the minimal V0/V1 header part we read
	apiKeyOffset        = 0
	apiVersionOffset    = apiKeyOffset + apiKeyLen
	correlationIDOffset = apiVersionOffset + apiVersionLen

	// Specific values for current stage requirements
	expectedApiVersion          = 4
	unsupportedVersionErrorCode = 35 // Kafka Error Code: UNSUPPORTED_VERSION
	errorCodeLen                = 2  // Error codes are INT16
)

// requestHeader represents the part of the header we need for correlation ID
// (Note: Kafka protocol versions can vary header structures)
type requestHeader struct {
	apiKey        int16
	apiVersion    int16
	correlationID int32
	// clientID omitted for now
}

// responseHeaderV0 represents the response header we send back
// (Used for simple correlation ID only responses initially)
type responseHeaderV0 struct {
	correlationID int32
}

// Encode writes the response header (including message size) to the writer.
func (h *responseHeaderV0) Encode(w io.Writer) error {
	bodySize := int32(correlationIDLen) // v0 header only has correlation ID
	totalSize := messageSizeLen + bodySize
	buf := make([]byte, totalSize)

	// Encode Message Size
	binary.BigEndian.PutUint32(buf[0:messageSizeLen], uint32(bodySize))

	// Encode Correlation ID
	binary.BigEndian.PutUint32(buf[messageSizeLen:totalSize], uint32(h.correlationID))

	// Write to network
	_, err := w.Write(buf)
	if err != nil {
		return fmt.Errorf("failed to write response header: %w", err)
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

// handleConnection reads the request, extracts correlation ID, and sends a response.
func handleConnection(conn net.Conn) {
	defer conn.Close()
	log.Printf("Connection accepted from %s", conn.RemoteAddr())

	// 1. Read Message Size
	var sizeBuf [messageSizeLen]byte
	if _, err := io.ReadFull(conn, sizeBuf[:]); err != nil {
		log.Printf("Error reading request size from %s: %v", conn.RemoteAddr(), err)
		return // Close connection on error
	}
	totalSize := binary.BigEndian.Uint32(sizeBuf[:])
	log.Printf("Received request of size %d from %s", totalSize, conn.RemoteAddr())

	// 2. Read the minimum header containing correlation ID
	if totalSize < requestHeaderV0MinLen {
		log.Printf("Error: Request size %d is too small to contain correlation ID from %s", totalSize, conn.RemoteAddr())
		return
	}
	headerBuf := make([]byte, requestHeaderV0MinLen)
	if _, err := io.ReadFull(conn, headerBuf); err != nil {
		log.Printf("Error reading request header from %s: %v", conn.RemoteAddr(), err)
		return
	}

	// 3. Decode header fields (only need correlation ID for now)
	header := requestHeader{
		apiKey:        int16(binary.BigEndian.Uint16(headerBuf[apiKeyOffset : apiKeyOffset+apiKeyLen])),
		apiVersion:    int16(binary.BigEndian.Uint16(headerBuf[apiVersionOffset : apiVersionOffset+apiVersionLen])),
		correlationID: int32(binary.BigEndian.Uint32(headerBuf[correlationIDOffset : correlationIDOffset+correlationIDLen])),
	}
	log.Printf("Extracted header %+v from %s", header, conn.RemoteAddr())

	// Check API Version
	if header.apiVersion != expectedApiVersion {
		log.Printf("Unsupported API version %d received from %s, expected %d. Sending error response.",
			header.apiVersion, conn.RemoteAddr(), expectedApiVersion)
		errorResp := ErrorResponse{
			correlationID: header.correlationID,
			errorCode:     unsupportedVersionErrorCode,
		}
		if err := errorResp.Encode(conn); err != nil {
			log.Printf("Error sending error response to %s: %v", conn.RemoteAddr(), err)
		}
		return // Sent error response, close connection
	}

	// 4. Read and discard the rest of the request payload
	bytesRemaining := int64(totalSize) - int64(requestHeaderV0MinLen)
	if bytesRemaining > 0 {
		if _, err := io.CopyN(io.Discard, conn, bytesRemaining); err != nil {
			err = fmt.Errorf("error reading rest of request payload from %s: %w", conn.RemoteAddr(), err)
			log.Println(err)
			return // Close connection on error
		}
	}

	// 5. Prepare and send the response (header v0)
	responseHdr := responseHeaderV0{
		correlationID: header.correlationID,
	}

	if err := responseHdr.Encode(conn); err != nil {
		log.Printf("Error sending response to %s: %v", conn.RemoteAddr(), err)
		return // Close connection on error
	}
	log.Printf("Sent response to %s", conn.RemoteAddr())
}
