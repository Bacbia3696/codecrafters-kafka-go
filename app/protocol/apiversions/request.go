package apiversions

import (
	"bufio"
	"fmt"

	"github.com/codecrafters-io/kafka-starter-go/app/protocol"
)

type ApiVersionsRequest struct {
	ClientSoftwareName    string // compact string
	ClientSoftwareVersion string // compact string
}

func DecodeApiVersionsRequest(r *bufio.Reader) (*ApiVersionsRequest, error) {
	request := &ApiVersionsRequest{}
	var err error
	request.ClientSoftwareName, err = protocol.DecodeCompactString(r)
	if err != nil {
		return nil, fmt.Errorf("failed to decode client_software_name: %w", err)
	}
	request.ClientSoftwareVersion, err = protocol.DecodeCompactString(r)
	if err != nil {
		return nil, fmt.Errorf("failed to decode client_software_version: %w", err)
	}
	protocol.DecodeTaggedField(r)
	return request, nil
}
