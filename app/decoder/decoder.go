package decoder

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"io"
	"strconv"

	"github.com/google/uuid"
)

func DecodeCompactString(r *bufio.Reader) (string, error) {
	length, err := DecodeUvarint(r) // Assumes DecodeUvarint is in this package or imported
	if err != nil {
		return "", fmt.Errorf("failed to decode compact string length: %w", err)
	}
	// For non-nullable compact strings, length is actual_length + 1.
	// For nullable, Uvarint 0 means null (actual_length -1).
	if length == 0 { // Null string
		return "", nil
	}
	if length < 1 { // Should not happen if 0 is null and others are length+1
		return "", fmt.Errorf("invalid compact string length: %d", length)
	}
	actualLength := length - 1
	buf := make([]byte, actualLength)
	if _, err := io.ReadFull(r, buf); err != nil {
		return "", fmt.Errorf("failed to read compact string bytes: %w", err)
	}
	return string(buf), nil
}

func DecodeSpecialBytes(r *bufio.Reader) ([]byte, error) {
	length, err := DecodeVarint(r) // Assumes DecodeVarint is in this package or imported
	if err != nil {
		return nil, fmt.Errorf("failed to decode compact bytes length: %w", err)
	}
	if length == -1 {
		return nil, nil // Empty bytes
	}
	if length < 0 {
		return nil, fmt.Errorf("invalid length: %d", length)
	}
	buf := make([]byte, length)
	if _, err := io.ReadFull(r, buf); err != nil {
		return nil, fmt.Errorf("failed to read bytes: %w", err)
	}
	return buf, nil
}

func DecodeUvarint(r *bufio.Reader) (uint64, error) {
	return binary.ReadUvarint(r)
}

func DecodeVarint(r *bufio.Reader) (int64, error) {
	return binary.ReadVarint(r)
}

func DecodeUUID(r *bufio.Reader) (uuid.UUID, error) {
	var value uuid.UUID
	err := binary.Read(r, binary.BigEndian, &value)
	if err != nil {
		return uuid.Nil, fmt.Errorf("failed to decode uuid: %w", err)
	}
	return value, nil
}

func DecodeValue(r io.Reader, value any) error {
	err := binary.Read(r, binary.BigEndian, value)
	if err != nil {
		return fmt.Errorf("failed to decode value: %w", err)
	}
	return nil
}

func DecodeTaggedField(r *bufio.Reader) {
	tag, err := DecodeUvarint(r) // Assumes DecodeUvarint is in this package
	if err != nil {
		panic("failed to decode tag: " + err.Error())
	}
	if tag != 0 {
		panic("tag is not 0: " + strconv.FormatUint(tag, 10))
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
