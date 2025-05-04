package protocol

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"io"
)

func EncodeTaggedField(w io.Writer) error {
	return binary.Write(w, binary.BigEndian, int8(0))
}

func EncodeCompactString(w io.Writer, s string) error {
	length := uint64(len(s))
	err := EncodeUvarint(w, length+1)
	if err != nil {
		return fmt.Errorf("failed to encode compact string length: %w", err)
	}
	_, err = w.Write([]byte(s))
	if err != nil {
		return fmt.Errorf("failed to encode compact string: %w", err)
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

func PeekNextByte(r *bufio.Reader) (byte, error) {
	buf := make([]byte, 1)
	_, err := r.Read(buf)
	if err != nil {
		return 0, fmt.Errorf("failed to read next byte: %w", err)
	}
	r.UnreadByte()
	return buf[0], nil
}
