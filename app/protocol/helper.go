package protocol

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"io"
)

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

func DecodeCompactString(r io.Reader) (string, error) {
	// Wrap the reader to satisfy io.ByteReader requirement
	br := bufio.NewReader(r)

	length, err := binary.ReadUvarint(br)
	if err != nil {
		return "", fmt.Errorf("failed to decode compact string length: %w", err)
	}

	if length <= 1 {
		return "", nil // Empty string
	}
	// Read the string bytes
	buf := make([]byte, length-1)
	if _, err := io.ReadFull(br, buf); err != nil {
		return "", fmt.Errorf("failed to read compact string bytes: %w", err)
	}

	return string(buf), nil
}

func DecodeString(r io.Reader) (string, error) {
	var length int16
	err := binary.Read(r, binary.BigEndian, &length)
	if err != nil {
		return "", fmt.Errorf("failed to decode string length: %w", err)
	}
	if length < 0 {
		return "", fmt.Errorf("invalid string length: %d", length)
	}
	buf := make([]byte, length)
	_, err = io.ReadFull(r, buf)
	if err != nil {
		return "", fmt.Errorf("failed to read string bytes: %w", err)
	}
	return string(buf), nil
}
