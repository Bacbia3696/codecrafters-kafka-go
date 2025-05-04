package protocol

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"io"
	"strconv"
)

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

func DecodeCompactBytes(r *bufio.Reader) ([]byte, error) {
	length, err := DecodeUvarint(r)
	if err != nil {
		return nil, fmt.Errorf("failed to decode compact bytes length: %w", err)
	}
	buf := make([]byte, length-1)
	if _, err := io.ReadFull(r, buf); err != nil {
		return nil, fmt.Errorf("failed to read bytes: %w", err)
	}
	return buf, nil
}

func DecodeUvarint(r *bufio.Reader) (uint64, error) {
	return binary.ReadUvarint(r)
}

func DecodeInt64(r io.Reader) (int64, error) {
	var value int64
	err := binary.Read(r, binary.BigEndian, &value)
	if err != nil {
		return 0, fmt.Errorf("failed to decode int64: %w", err)
	}
	return value, nil
}

func DecodeInt32(r io.Reader) (int32, error) {
	var value int32
	err := binary.Read(r, binary.BigEndian, &value)
	if err != nil {
		return 0, fmt.Errorf("failed to decode int32: %w", err)
	}
	return value, nil
}

func DecodeInt8(r io.Reader) (int8, error) {
	var value int8
	err := binary.Read(r, binary.BigEndian, &value)
	if err != nil {
		return 0, fmt.Errorf("failed to decode int8: %w", err)
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
		panic("tag is not 0: " + strconv.FormatUint(tag, 10))
	}
}
