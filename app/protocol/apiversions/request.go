package apiversions

import (
	"bufio"
	"fmt"

	"github.com/codecrafters-io/kafka-starter-go/app/decoder"
)

type ApiVersionsRequest struct {
	ClientSoftwareName    string // compact string
	ClientSoftwareVersion string // compact string
}

func DecodeApiVersionsRequest(r *bufio.Reader) (*ApiVersionsRequest, error) {
	request := &ApiVersionsRequest{}
	var err error
	request.ClientSoftwareName, err = decoder.DecodeCompactString(r)
	if err != nil {
		return nil, fmt.Errorf("failed to decode client_software_name: %w", err)
	}
	request.ClientSoftwareVersion, err = decoder.DecodeCompactString(r)
	if err != nil {
		return nil, fmt.Errorf("failed to decode client_software_version: %w", err)
	}
	decoder.DecodeTaggedField(r)
	return request, nil
}
