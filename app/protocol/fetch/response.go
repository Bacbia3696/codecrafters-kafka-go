package fetch

import (
	"fmt"
	"io"

	"github.com/codecrafters-io/kafka-starter-go/app/encoder"
	"github.com/codecrafters-io/kafka-starter-go/app/protocol/metadata"
	"github.com/google/uuid"
)

// Fetch Response (Version: 16) => throttle_time_ms error_code session_id [responses] _tagged_fields
//   throttle_time_ms => INT32
//   error_code => INT16
//   session_id => INT32
//   responses => topic_id [partitions] _tagged_fields
//     topic_id => UUID
//     partitions => partition_index error_code high_watermark last_stable_offset log_start_offset [aborted_transactions] preferred_read_replica records _tagged_fields
//       partition_index => INT32
//       error_code => INT16
//       high_watermark => INT64
//       last_stable_offset => INT64
//       log_start_offset => INT64
//       aborted_transactions => producer_id first_offset _tagged_fields
//         producer_id => INT64
//         first_offset => INT64
//       preferred_read_replica => INT32
//       records => COMPACT_RECORDS

type FetchResponse struct {
	ThrottleTimeMs int32
	ErrorCode      int16
	SessionID      int32
	Responses      []TopicResponse
	// TaggedFields
}

type TopicResponse struct {
	TopicID    uuid.UUID
	Partitions []PartitionResponse
	// TaggedFields
}

type PartitionResponse struct {
	PartitionIndex       int32
	ErrorCode            int16
	HighWatermark        int64
	LastStableOffset     int64
	LogStartOffset       int64
	AbortedTransactions  []AbortedTransaction
	PreferredReadReplica int32
	RecordBatchs         []metadata.RecordBatch
	RecordBatchsRaw      []byte
	// TaggedFields
}

type AbortedTransaction struct {
	ProducerID  int64
	FirstOffset int64
	// TaggedFields
}

// type RecordResponse struct {
// 	Key       []byte
// 	Value     []byte
// 	Offset    int64
// 	Partition int32
// 	Timestamp time.Time
// 	// TaggedFields
// }

func (r *FetchResponse) Encode(w io.Writer) error {
	var err error
	err = encoder.EncodeValue(w, r.ThrottleTimeMs)
	if err != nil {
		return fmt.Errorf("failed to encode throttle time ms: %w", err)
	}
	err = encoder.EncodeValue(w, r.ErrorCode)
	if err != nil {
		return fmt.Errorf("failed to encode error code: %w", err)
	}
	err = encoder.EncodeValue(w, r.SessionID)
	if err != nil {
		return fmt.Errorf("failed to encode session id: %w", err)
	}
	err = encoder.EncodeCompactArrayLength(w, len(r.Responses))
	if err != nil {
		return fmt.Errorf("failed to encode responses length: %w", err)
	}
	for _, response := range r.Responses {
		err = response.Encode(w)
		if err != nil {
			return fmt.Errorf("failed to encode response: %w", err)
		}
	}
	err = encoder.EncodeTaggedField(w)
	if err != nil {
		return fmt.Errorf("failed to encode tagged fields: %w", err)
	}
	return nil
}

func (r *TopicResponse) Encode(w io.Writer) error {
	var err error
	err = encoder.EncodeValue(w, r.TopicID)
	if err != nil {
		return fmt.Errorf("failed to encode topic id: %w", err)
	}
	err = encoder.EncodeCompactArrayLength(w, len(r.Partitions))
	if err != nil {
		return fmt.Errorf("failed to encode partitions length: %w", err)
	}
	for _, partition := range r.Partitions {
		err = partition.Encode(w)
		if err != nil {
			return fmt.Errorf("failed to encode partition: %w", err)
		}
	}
	err = encoder.EncodeTaggedField(w)
	if err != nil {
		return fmt.Errorf("failed to encode tagged fields: %w", err)
	}
	return nil
}

func (r *PartitionResponse) Encode(w io.Writer) error {
	var err error
	err = encoder.EncodeValue(w, r.PartitionIndex)
	if err != nil {
		return fmt.Errorf("failed to encode partition index: %w", err)
	}
	err = encoder.EncodeValue(w, r.ErrorCode)
	if err != nil {
		return fmt.Errorf("failed to encode error code: %w", err)
	}
	err = encoder.EncodeValue(w, r.HighWatermark)
	if err != nil {
		return fmt.Errorf("failed to encode high watermark: %w", err)
	}
	err = encoder.EncodeValue(w, r.LastStableOffset)
	if err != nil {
		return fmt.Errorf("failed to encode last stable offset: %w", err)
	}
	err = encoder.EncodeValue(w, r.LogStartOffset)
	if err != nil {
		return fmt.Errorf("failed to encode log start offset: %w", err)
	}
	err = encoder.EncodeCompactArrayLength(w, len(r.AbortedTransactions))
	if err != nil {
		return fmt.Errorf("failed to encode aborted transactions length: %w", err)
	}
	for _, abortedTransaction := range r.AbortedTransactions {
		err = abortedTransaction.Encode(w)
		if err != nil {
			return fmt.Errorf("failed to encode aborted transaction: %w", err)
		}
	}
	err = encoder.EncodeValue(w, r.PreferredReadReplica)
	if err != nil {
		return fmt.Errorf("failed to encode preferred read replica: %w", err)
	}
	err = encoder.EncodeValue(w, r.RecordBatchsRaw)
	if err != nil {
		return fmt.Errorf("failed to encode record batchs raw: %w", err)
	}
	// err = encoder.EncodeCompactArrayLength(w, len(r.RecordBatchs))
	// if err != nil {
	// 	return fmt.Errorf("failed to encode records length: %w", err)
	// }
	// for _, recordBatch := range r.RecordBatchs {
	// 	err = recordBatch.Encode(w)
	// 	if err != nil {
	// 		return fmt.Errorf("failed to encode record: %w", err)
	// 	}
	// }
	err = encoder.EncodeTaggedField(w)
	if err != nil {
		return fmt.Errorf("failed to encode tagged fields: %w", err)
	}
	return nil
}

func (r *AbortedTransaction) Encode(w io.Writer) error {
	var err error
	err = encoder.EncodeValue(w, r.ProducerID)
	if err != nil {
		return fmt.Errorf("failed to encode producer id: %w", err)
	}
	err = encoder.EncodeValue(w, r.FirstOffset)
	if err != nil {
		return fmt.Errorf("failed to encode first offset: %w", err)
	}
	err = encoder.EncodeTaggedField(w)
	if err != nil {
		return fmt.Errorf("failed to encode tagged fields: %w", err)
	}
	return nil
}

// func (r *RecordResponse) Encode(w io.Writer) error {
// 	var err error
// 	err = encoder.EncodeValue(w, r.Key)
// 	if err != nil {
// 		return fmt.Errorf("failed to encode key: %w", err)
// 	}
// 	err = encoder.EncodeValue(w, r.Value)
// 	if err != nil {
// 		return fmt.Errorf("failed to encode value: %w", err)
// 	}
// 	err = encoder.EncodeValue(w, r.Offset)
// 	if err != nil {
// 		return fmt.Errorf("failed to encode offset: %w", err)
// 	}
// 	err = encoder.EncodeValue(w, r.Partition)
// 	if err != nil {
// 		return fmt.Errorf("failed to encode partition: %w", err)
// 	}
// 	err = encoder.EncodeValue(w, r.Timestamp)
// 	if err != nil {
// 		return fmt.Errorf("failed to encode timestamp: %w", err)
// 	}
// 	err = encoder.EncodeTaggedField(w)
// 	if err != nil {
// 		return fmt.Errorf("failed to encode tagged fields: %w", err)
// 	}
// 	return nil
// }
