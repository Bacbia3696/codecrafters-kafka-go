package apiversions

import (
	"fmt"
	"io"

	"github.com/codecrafters-io/kafka-starter-go/app/encoder"
)

type ApiVersion struct {
	ApiKey     int16
	MinVersion int16
	MaxVersion int16
	// TaggedFields
}

type ApiVersionsResponseV3 struct {
	ErrorCode      int16
	ApiVersions    []ApiVersion
	ThrottleTimeMs int32
	// TaggedFields
}

func (r *ApiVersion) Encode(w io.Writer) error {
	var err error
	err = encoder.EncodeValue(w, r.ApiKey)
	if err != nil {
		return fmt.Errorf("failed to encode api key: %w", err)
	}
	err = encoder.EncodeValue(w, r.MinVersion)
	if err != nil {
		return fmt.Errorf("failed to encode min version: %w", err)
	}
	err = encoder.EncodeValue(w, r.MaxVersion)
	if err != nil {
		return fmt.Errorf("failed to encode max version: %w", err)
	}
	err = encoder.EncodeTaggedField(w)
	if err != nil {
		return fmt.Errorf("failed to encode tagged field: %w", err)
	}
	return nil
}

func (r *ApiVersionsResponseV3) Encode(w io.Writer) error {
	err := encoder.EncodeValue(w, r.ErrorCode)
	if err != nil {
		return fmt.Errorf("failed to encode error code: %w", err)
	}
	length := len(r.ApiVersions)
	encoder.EncodeUvarint(w, uint64(length+1))
	for _, apiVersion := range r.ApiVersions {
		err = apiVersion.Encode(w)
		if err != nil {
			return fmt.Errorf("failed to encode api version: %w", err)
		}
	}
	err = encoder.EncodeValue(w, r.ThrottleTimeMs)
	if err != nil {
		return fmt.Errorf("failed to encode throttle time ms: %w", err)
	}
	return encoder.EncodeTaggedField(w)
}
