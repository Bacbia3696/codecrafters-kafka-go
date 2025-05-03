package protocol

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"io"
)

// EncodeVarint encodes an int32 into Kafka's unsigned varint format.
// Returns the byte slice and the number of bytes written.
func EncodeVarint(value int32) []byte {
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

func DecodeCompactString(r *bufio.Reader) (string, error) {
	length, err := DecodeUvarint(r)
	if err != nil {
		return "", fmt.Errorf("failed to decode compact string length: %w", err)
	}

	if length <= 1 {
		return "", nil // Empty string
	}
	// Read the string bytes
	buf := make([]byte, length-1)
	if _, err := io.ReadFull(r, buf); err != nil {
		return "", fmt.Errorf("failed to read compact string bytes: %w", err)
	}

	return string(buf), nil
}

func DecodeString(r io.Reader) (string, error) {
	length, err := DecodeInt16(r)
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

func DecodeUvarint(r *bufio.Reader) (uint64, error) {
	return binary.ReadUvarint(r)
}

func DecodeInt32(r io.Reader) (int32, error) {
	var value int32
	err := binary.Read(r, binary.BigEndian, &value)
	if err != nil {
		return 0, fmt.Errorf("failed to decode int32: %w", err)
	}
	return value, nil
}

func DecodeInt16(r io.Reader) (int16, error) {
	var value int16
	err := binary.Read(r, binary.BigEndian, &value)
	if err != nil {
		return 0, fmt.Errorf("failed to decode int16: %w", err)
	}
	return value, nil
}

func DecodeTaggedField(r *bufio.Reader) {
	tag, err := DecodeUvarint(r)
	if err != nil {
		panic("failed to decode tag: " + err.Error())
	}
	if tag != 0 {
		panic("tag is not 0")
	}
}

func PeekNextByte(r *bufio.Reader) (byte, error) {
	buf := make([]byte, 1)
	_, err := r.Read(buf)
	if err != nil {
		return 0, fmt.Errorf("failed to read next byte: %w", err)
	}
	r.UnreadByte()
	return buf[0], nil
}
