package apiversions

import (
	"bufio"
	"io"
	"log/slog"
	"slices"

	"github.com/codecrafters-io/kafka-starter-go/app/protocol"
)

// SupportedApiVersions maps API keys to their version range
// Key: ApiKey, Value: MaxVersion (minimum is assumed to be 0 for the *logic*)
var SupportedApiVersions = map[int16]int16{
	protocol.ApiKeyApiVersions:             4,
	protocol.ApiKeyDescribeTopicPartitions: 0,
	// Add more API keys as they are implemented
}

// HandleApiVersions handles the ApiVersions request, using the provided logger
func HandleApiVersions(log *slog.Logger, rd *bufio.Reader, w io.Writer, header *protocol.RequestHeader) {
	log.Info("Handling ApiVersions request")

	if header.ApiVersion != 4 { // Assuming we only support V4 requests for V3 response format
		log.Warn("Unsupported ApiVersion requested", "requestedVersion", header.ApiVersion)
		response := ApiVersionsResponseV3{
			ErrorCode: protocol.ErrorCodeUnsupportedVersion,
		}
		// Log error from encode?
		if err := response.Encode(w, header.CorrelationID); err != nil {
			log.Error("Error sending unsupported version response", "error", err)
		}
		return
	}

	request, err := DecodeApiVersionsRequest(rd)
	if err != nil {
		log.Error("Error decoding ApiVersions request", "error", err)
		return
	}
	log.Info("Received ApiVersions request", "clientSoftwareName", request.ClientSoftwareName, "clientSoftwareVersion", request.ClientSoftwareVersion)

	// Build supported versions from our map
	keys := make([]int16, 0, len(SupportedApiVersions))
	for k := range SupportedApiVersions {
		keys = append(keys, k)
	}
	slices.Sort(keys)

	versions := make([]ApiVersion, 0, len(SupportedApiVersions))
	for _, apiKey := range keys {
		maxVersion := SupportedApiVersions[apiKey]
		versions = append(versions, ApiVersion{
			ApiKey:     apiKey,
			MinVersion: 0,          // Assuming min supported is 0 for now
			MaxVersion: maxVersion, // This is the max version *we* support for this key
		})
	}

	response := ApiVersionsResponseV3{
		ErrorCode:      protocol.ErrorCodeNone,
		ThrottleTimeMs: 0,
		ApiVersions:    versions,
	}

	log.Debug("Sending ApiVersions response", "response", response) // Use Debug level for full response
	if err := response.Encode(w, header.CorrelationID); err != nil {
		log.Error("Error sending ApiVersions response", "error", err)
	} else {
		log.Info("Successfully sent ApiVersions response")
	}
}
