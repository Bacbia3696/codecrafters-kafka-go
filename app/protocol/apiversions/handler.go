package apiversions

import (
	"bufio"
	"io"
	"log/slog"
	"maps"
	"slices"

	"github.com/codecrafters-io/kafka-starter-go/app/protocol"
)

// SupportedApiVersions maps API keys to their version range
// Key: ApiKey, Value: MaxVersion (minimum is assumed to be 0 for the *logic*)
var SupportedApiVersions = map[int16]int16{
	protocol.ApiKeyApiVersions:             4,
	protocol.ApiKeyDescribeTopicPartitions: 0,
	protocol.ApiKeyFetch:                   16,
	// Add more API keys as they are implemented
}

// HandleApiVersions handles the ApiVersions request, using the provided logger
func HandleApiVersions(log *slog.Logger, rd *bufio.Reader, w io.Writer, header *protocol.RequestHeader) {
	log.Info("Handling ApiVersions request")
	request, err := DecodeApiVersionsRequest(rd)
	if err != nil {
		log.Error("Error decoding ApiVersions request", "error", err)
		return
	}
	log.Info("Received ApiVersions request", "clientSoftwareName", request.ClientSoftwareName, "clientSoftwareVersion", request.ClientSoftwareVersion)

	responseHeader := protocol.ResponseHeaderV0{
		CorrelationID: header.CorrelationID,
	}

	var response ApiVersionsResponseV3

	if header.ApiVersion == 4 {
		// Build supported versions from our map
		keys := slices.Collect(maps.Keys(SupportedApiVersions))
		slices.Sort(keys)
		versions := make([]ApiVersion, 0, len(SupportedApiVersions))
		for _, apiKey := range keys {
			maxVersion := SupportedApiVersions[apiKey]
			versions = append(versions, ApiVersion{
				ApiKey:     apiKey,
				MinVersion: 0,
				MaxVersion: maxVersion,
			})
		}

		response = ApiVersionsResponseV3{
			ErrorCode:      protocol.ErrorCodeNone,
			ThrottleTimeMs: 0,
			ApiVersions:    versions,
		}
	} else {
		response = ApiVersionsResponseV3{
			ErrorCode: protocol.ErrorCodeUnsupportedVersion,
		}
	}

	err = responseHeader.Encode(w)
	if err != nil {
		log.Error("failed to encode api versions response header", "error", err)
		return
	}
	err = response.Encode(w)
	if err != nil {
		log.Error("failed to encode api versions response", "error", err)
		return
	}

	log.Info("Sent ApiVersions response", "response", response)
}
