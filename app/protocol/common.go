package protocol

import (
	"encoding/binary"
	"fmt"
	"io"
	"log"
	"net"
)

// RequestHeader represents the common header for all Kafka requests
type RequestHeader struct {
	ApiKey        int16
	ApiVersion    int16
	CorrelationID int32
	// ClientID omitted for now
}

// ParseRequestHeader reads and parses the request header from a connection
func ParseRequestHeader(conn net.Conn) (RequestHeader, uint32, error) {
	// 1. Read Message Size
	var sizeBuf [MessageSizeLen]byte
	if _, err := io.ReadFull(conn, sizeBuf[:]); err != nil {
		if err == io.EOF || err == io.ErrUnexpectedEOF {
			return RequestHeader{}, 0, fmt.Errorf("connection closed before message size received: %w", err)
		}
		return RequestHeader{}, 0, fmt.Errorf("error reading message size: %w", err)
	}

	totalSize := binary.BigEndian.Uint32(sizeBuf[:])
	log.Printf("Received request of size %d from %s", totalSize, conn.RemoteAddr())

	// Check if size is reasonable
	if totalSize < RequestHeaderMinLen {
		return RequestHeader{}, 0, fmt.Errorf("request size %d is too small for minimum header", totalSize)
	}

	// 2. Read the minimum header
	headerBuf := make([]byte, RequestHeaderMinLen)
	if _, err := io.ReadFull(conn, headerBuf); err != nil {
		return RequestHeader{}, 0, fmt.Errorf("error reading request header: %w", err)
	}

	// 3. Decode header fields
	header := RequestHeader{
		ApiKey:        int16(binary.BigEndian.Uint16(headerBuf[ApiKeyOffset : ApiKeyOffset+ApiKeyLen])),
		ApiVersion:    int16(binary.BigEndian.Uint16(headerBuf[ApiVersionOffset : ApiVersionOffset+ApiVersionLen])),
		CorrelationID: int32(binary.BigEndian.Uint32(headerBuf[CorrelationIDOffset : CorrelationIDOffset+CorrelationIDLen])),
	}

	return header, totalSize, nil
}

// DiscardRemainingRequest reads and discards the remaining bytes in a request after the header
func DiscardRemainingRequest(conn net.Conn, totalSize uint32) error {
	bytesRemaining := int64(totalSize) - int64(RequestHeaderMinLen)
	if bytesRemaining <= 0 {
		return nil
	}

	log.Printf("Discarding %d remaining request bytes", bytesRemaining)
	if _, err := io.CopyN(io.Discard, conn, bytesRemaining); err != nil {
		return fmt.Errorf("error reading rest of request payload: %w", err)
	}
	return nil
}
