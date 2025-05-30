package encoder

import (
	"encoding/binary"
	"fmt"
	"io"
)

func EncodeTaggedField(w io.Writer) error {
	return binary.Write(w, binary.BigEndian, int8(0))
}

func EncodeCompactString(w io.Writer, s string) error {
	// Note: This implementation assumes length+1 for non-nullable compact strings.
	// For nullable, a different approach or a separate function is needed.
	length := uint64(len(s))
	err := EncodeUvarint(w, length+1) // Assumes EncodeUvarint is also in this package or imported
	if err != nil {
		return fmt.Errorf("failed to encode compact string length: %w", err)
	}
	_, err = w.Write([]byte(s))
	if err != nil {
		return fmt.Errorf("failed to encode compact string: %w", err)
	}
	return nil
}

func EncodeSpecialBytes(w io.Writer, b []byte) error {
	if b == nil {
		return EncodeVarint(w, -1)
	}
	err := EncodeVarint(w, int64(len(b)))
	if err != nil {
		return fmt.Errorf("failed to encode special bytes length: %w", err)
	}
	_, err = w.Write(b)
	if err != nil {
		return fmt.Errorf("failed to encode special bytes: %w", err)
	}
	return nil
}

// EncodeCompactNullableString encodes a nullable string using the compact format.
// A nil string is encoded as Uvarint 0 length.
func EncodeCompactNullableString(w io.Writer, s *string) error {
	if s == nil {
		return EncodeUvarint(w, 0)
	}
	length := uint64(len(*s))
	err := EncodeUvarint(w, length+1)
	if err != nil {
		return fmt.Errorf("failed to encode compact nullable string length: %w", err)
	}
	_, err = w.Write([]byte(*s))
	if err != nil {
		return fmt.Errorf("failed to encode compact nullable string bytes: %w", err)
	}
	return nil
}

func EncodeValue(w io.Writer, value any) error {
	return binary.Write(w, binary.BigEndian, value)
}

func EncodeUvarint(w io.Writer, value uint64) error {
	buf := make([]byte, binary.MaxVarintLen64)
	n := binary.PutUvarint(buf, value)
	_, err := w.Write(buf[:n])
	return err
}

func EncodeVarint(w io.Writer, value int64) error {
	buf := make([]byte, binary.MaxVarintLen32)
	n := binary.PutVarint(buf, value)
	_, err := w.Write(buf[:n])
	return err
}

// EncodeCompactArrayLength encodes the length for a compact array.
func EncodeCompactArrayLength(w io.Writer, length int) error {
	return EncodeUvarint(w, uint64(length+1))
}

// EncodeInt32Array encodes a compact array of int32 values.
func EncodeInt32Array(w io.Writer, arr []int32) error {
	err := EncodeCompactArrayLength(w, len(arr))
	if err != nil {
		return fmt.Errorf("failed to encode int32 array length: %w", err)
	}
	for _, item := range arr {
		err = EncodeValue(w, item) // EncodeValue handles int32
		if err != nil {
			return fmt.Errorf("failed to encode int32 array item: %w", err)
		}
	}
	return nil
}
