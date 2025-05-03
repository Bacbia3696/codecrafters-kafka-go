package protocol

import (
	"encoding/binary"
	"fmt"
	"io"
	"log/slog"
	"net"
)

// ErrorResponse represents a response containing only a header and an error code body.
type ErrorResponse struct {
	CorrelationID int32
	ErrorCode     int16
}

// SendErrorResponse sends an error response to the client, using the provided logger.
func SendErrorResponse(log *slog.Logger, conn net.Conn, correlationID int32, errorCode int16) error {
	resp := ErrorResponse{
		CorrelationID: correlationID,
		ErrorCode:     errorCode,
	}
	return resp.Encode(log, conn)
}

// Encode writes the error response (message size, header correlationID, body errorCode) to the writer.
// Accepts a logger for debugging.
func (r *ErrorResponse) Encode(log *slog.Logger, w io.Writer) error {
	headerSize := int32(CorrelationIDLen)
	bodySize := int32(ErrorCodeLen)
	messageBodySize := headerSize + bodySize // Size field refers to header + body
	totalBufferSize := MessageSizeLen + messageBodySize
	buf := make([]byte, totalBufferSize)
	offset := 0

	// Encode Message Size (size of header + body)
	binary.BigEndian.PutUint32(buf[offset:offset+MessageSizeLen], uint32(messageBodySize))
	offset += MessageSizeLen

	// Encode Header Correlation ID
	binary.BigEndian.PutUint32(buf[offset:offset+CorrelationIDLen], uint32(r.CorrelationID))
	offset += CorrelationIDLen

	// Encode Body Error Code
	binary.BigEndian.PutUint16(buf[offset:offset+ErrorCodeLen], uint16(r.ErrorCode))
	offset += ErrorCodeLen // Corrected offset increment

	// Write to network
	_, err := w.Write(buf)
	if err != nil {
		log.Error("Failed to write error response", "error", err)
		return fmt.Errorf("failed to write error response: %w", err)
	}

	log.Info("Sent error response", "correlationID", r.CorrelationID, "errorCode", r.ErrorCode)
	return nil
}
