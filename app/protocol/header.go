package protocol

import (
	"bufio"
	"fmt"
	"io"
	"reflect"
)

type RequestHeader struct {
	ApiKey        int16
	ApiVersion    int16
	CorrelationID int32
	ClientID      *string
}

type ResponseHeaderV0 struct {
	CorrelationID int32
}

type ResponseHeaderV1 struct {
	CorrelationID int32
	// tagged fields
}

func (r *ResponseHeaderV0) Encode(w io.Writer) error {
	err := EncodeValue(w, r.CorrelationID)
	if err != nil {
		return fmt.Errorf("failed to encode correlation id: %w", err)
	}
	return nil
}

func (r *ResponseHeaderV1) Encode(w io.Writer) error {
	err := EncodeValue(w, r.CorrelationID)
	if err != nil {
		return fmt.Errorf("failed to encode correlation id: %w", err)
	}
	return EncodeTaggedField(w)
}

func DecodeRequestHeader(r *bufio.Reader) (*RequestHeader, error) {
	h := &RequestHeader{}
	var err error
	h.ApiKey, err = DecodeInt16(r)
	if err != nil {
		return nil, fmt.Errorf("failed to decode api key: %w", err)
	}
	h.ApiVersion, err = DecodeInt16(r)
	if err != nil {
		return nil, fmt.Errorf("failed to decode api version: %w", err)
	}
	h.CorrelationID, err = DecodeInt32(r)
	if err != nil {
		return nil, fmt.Errorf("failed to decode correlation id: %w", err)
	}
	h.ClientID, err = DecodeNullString(r)
	if err != nil {
		return nil, fmt.Errorf("failed to decode client id: %w", err)
	}
	DecodeTaggedField(r)
	return h, nil
}

func DecodeNullString(r io.Reader) (*string, error) {
	clientIDLength, err := DecodeInt16(r)
	if err != nil {
		return nil, fmt.Errorf("failed to decode client id length: %w", err)
	}
	if clientIDLength >= 0 {
		clientIDBytes := make([]byte, clientIDLength)
		if _, err := io.ReadFull(r, clientIDBytes); err != nil {
			return nil, fmt.Errorf("failed to decode client id: %w", err)
		}
		clientIDStr := string(clientIDBytes)
		return &clientIDStr, nil
	}
	return nil, nil
}

func EncodeArray(w io.Writer, array any) error {
	// check if array is a slice
	if reflect.ValueOf(array).Kind() != reflect.Slice {
		return fmt.Errorf("array is not a slice")
	}
	slice := reflect.ValueOf(array)
	err := EncodeUvarint(w, uint64(slice.Len()+1))
	if err != nil {
		return fmt.Errorf("failed to encode array length: %w", err)
	}
	for i := 0; i < slice.Len(); i++ {
		err := EncodeValue(w, slice.Index(i).Interface())
		if err != nil {
			return fmt.Errorf("failed to encode array value: %w", err)
		}
	}
	return nil
}
