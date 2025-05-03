package protocol

import (
	"bufio"
	"fmt"
	"io"
)

type RequestHeader struct {
	ApiKey        int16
	ApiVersion    int16
	CorrelationID int32
	ClientID      *string
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
