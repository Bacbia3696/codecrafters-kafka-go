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
	protocol.ApiKeyApiVersions:             4,  // This handler itself supports up to v4
	protocol.ApiKeyDescribeTopicPartitions: 0,  // Example: DescribeTopicPartitions support
	protocol.ApiKeyFetch:                   16, // Example: Fetch support
	// Add more API keys as they are implemented
}

// ApiVersionsHandler implements the protocol.RequestHandler interface for ApiVersions requests.
type ApiVersionsHandler struct{}

// NewApiVersionsHandler creates a new handler for ApiVersions requests.
func NewApiVersionsHandler() *ApiVersionsHandler {
	return &ApiVersionsHandler{}
}

// ApiKey returns the API key for ApiVersions requests.
func (h *ApiVersionsHandler) ApiKey() int16 {
	return protocol.ApiKeyApiVersions
}

// Handle handles the ApiVersions request, using the provided logger.
func (h *ApiVersionsHandler) Handle(log *slog.Logger, rd *bufio.Reader, w io.Writer, header *protocol.RequestHeader) {
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

	// According to Kafka protocol, ApiVersions V0-V2 request/response is one way (no client software name/version)
	// V3 adds client software name/version to request and full list of ApiVersions in response.
	// V4 is the same as V3 for ApiVersions API itself. We use V3 response format for V3+ requests.
	if header.ApiVersion == 4 { // Check if client is requesting V3 or newer
		// Build supported versions from our map
		keys := slices.Collect(maps.Keys(SupportedApiVersions))
		slices.Sort(keys)
		versions := make([]ApiVersion, 0, len(SupportedApiVersions))
		for _, apiKey := range keys {
			maxVersion := SupportedApiVersions[apiKey]
			versions = append(versions, ApiVersion{
				ApiKey:     apiKey,
				MinVersion: 0, // Assuming min version is 0 for all reported APIs
				MaxVersion: maxVersion,
			})
		}

		response = ApiVersionsResponseV3{
			ErrorCode:      protocol.ErrorCodeNone,
			ThrottleTimeMs: 0,
			ApiVersions:    versions,
		}
	} else {
		// For older request versions (V0-V2), Kafka might return an empty list or a V0 response.
		// For simplicity here, we'll return an error if an older version is explicitly requested
		// that doesn't match our primary supported version (e.g. v4 for the ApiVersions API itself).
		// A more compliant server might try to respond with an older response schema.
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

	log.Info("Sent ApiVersions response") // Avoid logging full response in production if it's large
}
