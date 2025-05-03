package protocol

import (
	"encoding/binary"
	"fmt"
	"io"
	"log/slog"
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
// Accepts a logger for debugging.
func ParseRequestHeader(log *slog.Logger, conn net.Conn) (RequestHeader, uint32, error) {
	// 1. Read Message Size
	var sizeBuf [MessageSizeLen]byte
	if _, err := io.ReadFull(conn, sizeBuf[:]); err != nil {
		if err == io.EOF || err == io.ErrUnexpectedEOF {
			log.Warn("Connection closed before message size received", "error", err)
			return RequestHeader{}, 0, fmt.Errorf("connection closed before message size received: %w", err)
		}
		log.Error("Error reading message size", "error", err)
		return RequestHeader{}, 0, fmt.Errorf("error reading message size: %w", err)
	}

	totalSize := binary.BigEndian.Uint32(sizeBuf[:])
	log.Debug("Received request size", "size", totalSize)

	// Check if size is reasonable
	if totalSize < RequestHeaderMinLen {
		log.Error("Request size too small for header", "size", totalSize, "minHeaderSize", RequestHeaderMinLen)
		return RequestHeader{}, 0, fmt.Errorf("request size %d is too small for minimum header", totalSize)
	}

	// 2. Read the minimum header
	headerBuf := make([]byte, RequestHeaderMinLen)
	if _, err := io.ReadFull(conn, headerBuf); err != nil {
		log.Error("Error reading request header", "error", err)
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
// Accepts a logger for debugging.
func DiscardRemainingRequest(log *slog.Logger, conn net.Conn, totalSize uint32) error {
	bytesRemaining := int64(totalSize) - int64(RequestHeaderMinLen)
	if bytesRemaining <= 0 {
		return nil
	}

	log.Debug("Discarding remaining request bytes", "count", bytesRemaining)
	if _, err := io.CopyN(io.Discard, conn, bytesRemaining); err != nil {
		log.Error("Error discarding remaining request bytes", "error", err)
		return fmt.Errorf("error reading rest of request payload: %w", err)
	}
	return nil
}

// encodeVarint encodes an int32 into Kafka's unsigned varint format.
// Returns the byte slice and the number of bytes written.
func encodeVarint(value int32) []byte {
	var buf []byte
	// Use unsigned directly for length
	uv := uint32(value)
	for uv >= 0x80 {
		buf = append(buf, byte(uv)|0x80)
		uv >>= 7
	}
	buf = append(buf, byte(uv))
	return buf
}
