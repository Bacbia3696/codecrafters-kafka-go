package fetch

import (
	"time"

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
	Records              []RecordResponse
	// TaggedFields
}

type AbortedTransaction struct {
	ProducerID  int64
	FirstOffset int64
	// TaggedFields
}

type RecordResponse struct {
	Key       []byte
	Value     []byte
	Offset    int64
	Partition int32
	Timestamp time.Time
	// TaggedFields
}
