package protocol

import (
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
	length, err := DecodeUvarint(r)
	fmt.Println("length", length)
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

func DecodeUvarint(r io.Reader) (uint64, error) {
	var x uint64
	var s uint
	buf := make([]byte, 1)

	for i := 0; i < binary.MaxVarintLen64; i++ {
		n, err := r.Read(buf)
		if err != nil {
			if i > 0 && err == io.EOF {
				err = io.ErrUnexpectedEOF
			}
			return x, err
		}
		if n != 1 {
			if i > 0 {
				return x, io.ErrUnexpectedEOF
			}
			return x, io.EOF
		}

		b := buf[0]
		if b < 0x80 {
			if i == binary.MaxVarintLen64-1 && b > 1 {
				return x, fmt.Errorf("varint overflow")
			}
			return x | uint64(b)<<s, nil
		}
		x |= uint64(b&0x7f) << s
		s += 7
	}

	return x, fmt.Errorf("varint overflow")
}

func DecodeInt32(r io.Reader) (int32, error) {
	var value int32
	err := binary.Read(r, binary.BigEndian, &value)
	if err != nil {
		return 0, fmt.Errorf("failed to decode int32: %w", err)
	}
	return value, nil
}
