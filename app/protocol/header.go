package protocol

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"io"
)

type RequestHeader struct {
	ApiKey        int16
	ApiVersion    int16
	CorrelationID int32
	ClientID      *string
}

func DecodeRequestHeader(r io.Reader) (*RequestHeader, error) {
	h := &RequestHeader{}
	if err := binary.Read(r, binary.BigEndian, &h.ApiKey); err != nil {
		return nil, fmt.Errorf("failed to decode api key: %w", err)
	}
	if err := binary.Read(r, binary.BigEndian, &h.ApiVersion); err != nil {
		return nil, fmt.Errorf("failed to decode api version: %w", err)
	}
	if err := binary.Read(r, binary.BigEndian, &h.CorrelationID); err != nil {
		return nil, fmt.Errorf("failed to decode correlation id: %w", err)
	}
	clientIDLength := int16(0)
	if err := binary.Read(r, binary.BigEndian, &clientIDLength); err != nil {
		return nil, fmt.Errorf("failed to decode client id length: %w", err)
	}
	if clientIDLength >= 0 {
		clientIDBytes := make([]byte, clientIDLength)
		if _, err := io.ReadFull(r, clientIDBytes); err != nil {
			return nil, fmt.Errorf("failed to decode client id: %w", err)
		}
		clientIDStr := string(clientIDBytes)
		h.ClientID = &clientIDStr
	} else {
		h.ClientID = nil
	}
	ReadTaggedField(r)
	return h, nil
}

func ReadTaggedField(r io.Reader) {
	tag, err := binary.ReadUvarint(bufio.NewReader(r))
	if err != nil {
		panic("failed to decode tag: " + err.Error())
	}
	if tag != 0 {
		panic("tag is not 0")
	}
}
