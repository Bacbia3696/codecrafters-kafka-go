package main

import (
	"encoding/binary"
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
type responseHeaderV0 struct {
	correlationID int32
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
		apiKey:        int16(binary.BigEndian.Uint16(headerBuf[0:apiKeyLen])),
		apiVersion:    int16(binary.BigEndian.Uint16(headerBuf[apiKeyLen : apiKeyLen+apiVersionLen])),
		correlationID: int32(binary.BigEndian.Uint32(headerBuf[apiKeyLen+apiVersionLen : apiKeyLen+apiVersionLen+correlationIDLen])),
	}
	log.Printf("Extracted header %+v from %s", header, conn.RemoteAddr())

	// 4. Read and discard the rest of the request payload
	bytesRemaining := int64(totalSize) - int64(requestHeaderV0MinLen)
	if bytesRemaining > 0 {
		if _, err := io.CopyN(io.Discard, conn, bytesRemaining); err != nil {
			log.Printf("Error reading rest of request payload from %s: %v", conn.RemoteAddr(), err)
			return // Close connection on error
		}
	}

	// 5. Prepare and send the response (header v0)
	responseHdr := responseHeaderV0{
		correlationID: header.correlationID,
	}
	responseBodySize := int32(correlationIDLen) // Only correlation ID in v0 response header
	resp := make([]byte, messageSizeLen+responseBodySize)
	binary.BigEndian.PutUint32(resp[0:messageSizeLen], uint32(responseBodySize))
	binary.BigEndian.PutUint32(resp[messageSizeLen:messageSizeLen+correlationIDLen], uint32(responseHdr.correlationID))

	_, err := conn.Write(resp)
	if err != nil {
		log.Printf("Error writing response to %s: %v", conn.RemoteAddr(), err)
		return // Close connection on error
	}
	log.Printf("Sent response to %s", conn.RemoteAddr())
}
