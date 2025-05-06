package metadata

import (
	"bufio"
	"bytes"
	"fmt"

	"github.com/codecrafters-io/kafka-starter-go/app/decoder"
)

type RecordBatch struct {
	BaseOffset           int64
	BatchLength          int32
	PartitionLeaderEpoch int32
	Magic                int8
	CRC                  int32
	Attributes           int16
	LastOffsetDelta      int32
	FirstTimestamp       int64
	MaxTimestamp         int64
	ProducerId           int64
	ProducerEpoch        int16
	BaseSequence         int32
	Records              []Record
}

type Record struct {
	Length                 int64
	Attributes             int8
	TimestampDelta         int64
	OffsetDelta            int64
	Key                    []byte
	Value                  []byte
	ValueEncodedBaseRecode BaseRecord
	ValueEncodedRecord     any
	ValueEncodedRecordType RecordType
	Headers                []RecordHeader
}

type RecordHeader struct {
	Key   string
	Value []byte
}

func DecodeRecordHeader(r *bufio.Reader) (*RecordHeader, error) {
	header := &RecordHeader{}
	var err error
	header.Key, err = decoder.DecodeCompactString(r)
	if err != nil {
		return nil, err
	}
	header.Value, err = decoder.DecodeSpecialBytes(r)
	if err != nil {
		return nil, err
	}
	return header, nil
}

// decodeSpecificRecordValue is a helper to decode the specific record type from the value bytes.
func decodeSpecificRecordValue(rd *bufio.Reader, recordType RecordType) (valueEncodedRecord any, valueEncodedRecordType RecordType, err error) {
	switch recordType {
	case RecordTypePartition:
		valueEncodedRecord, err = DecodePartitionRecord(rd)
		valueEncodedRecordType = RecordTypePartition
		if err != nil {
			return nil, 0, err // Return zero value for RecordType on error
		}
	case RecordTypeTopic:
		valueEncodedRecord, err = DecodeTopicRecord(rd)
		valueEncodedRecordType = RecordTypeTopic
		if err != nil {
			return nil, 0, err
		}
	case RecordTypeFeatureLevel:
		valueEncodedRecord, err = DecodeFeatureLevelRecord(rd)
		valueEncodedRecordType = RecordTypeFeatureLevel
		if err != nil {
			return nil, 0, err
		}
	default:
		return nil, 0, fmt.Errorf("invalid record type: %d", recordType)
	}
	return valueEncodedRecord, valueEncodedRecordType, nil
}

func DecodeRecord(r *bufio.Reader) (*Record, error) {
	record := &Record{}
	var err error
	record.Length, err = decoder.DecodeVarint(r)
	if err != nil {
		return nil, err
	}
	err = decoder.DecodeValue(r, &record.Attributes)
	if err != nil {
		return nil, err
	}
	record.TimestampDelta, err = decoder.DecodeVarint(r)
	if err != nil {
		return nil, err
	}
	record.OffsetDelta, err = decoder.DecodeVarint(r)
	if err != nil {
		return nil, err
	}
	record.Key, err = decoder.DecodeSpecialBytes(r)
	if err != nil {
		return nil, err
	}
	record.Value, err = decoder.DecodeSpecialBytes(r)
	if err != nil {
		return nil, err
	}
	// encode record.Value
	rd := bufio.NewReader(bytes.NewReader(record.Value))
	baseRecord, err := DecodeBaseRecord(rd)
	if err != nil {
		return nil, err
	}
	record.ValueEncodedBaseRecode = *baseRecord

	// Call the new helper function
	record.ValueEncodedRecord, record.ValueEncodedRecordType, err = decodeSpecificRecordValue(rd, baseRecord.Type)
	if err != nil {
		return nil, fmt.Errorf("failed to decode specific record value: %w", err)
	}

	// finished decode record.Value
	headerLength, err := decoder.DecodeUvarint(r)
	if err != nil {
		return nil, err
	}
	if headerLength > 0 {
		// The Kafka protocol specifies header count as Uvarint, not Uvarint-1.
		// If headerLength from DecodeUvarint is 0, it means no headers.
		// If it's > 0, it's the actual count. Let's assume it's count, not count+1 for now.
		// If it was count+1, then 0 would mean null, 1 would mean 0 headers.
		// For safety, let's assume headerLength is the actual count and if it's >0, process.
		actualHeaderCount := int(headerLength)
		if headerLength == 0 { // Check if this is how zero headers is represented
			// No headers, do nothing or ensure record.Headers is nil/empty
			record.Headers = nil
		} else {
			// If Uvarint 0 means null and Uvarint 1 means 0 elements for compact arrays, then:
			// compact_array_len, err := decoder.DecodeCompactArrayLength(rd) // If using a helper for this
			// if err == nil && compact_array_len > 0 {
			//    record.Headers = make([]RecordHeader, compact_array_len)
			// } else if err != nil { return nil, err }
			// For now, assuming simple Uvarint count

			record.Headers = make([]RecordHeader, actualHeaderCount)
			for i := range record.Headers {
				header, err := DecodeRecordHeader(r) // IMPORTANT: Use original reader 'r', not 'rd' from record.Value
				if err != nil {
					return nil, err
				}
				record.Headers[i] = *header
			}
		}
	}
	return record, nil
}

func DecodeRecordBatch(r *bufio.Reader) (*RecordBatch, error) {
	recordBatch := &RecordBatch{}
	var err error
	err = decoder.DecodeValue(r, &recordBatch.BaseOffset)
	if err != nil {
		return nil, err
	}
	err = decoder.DecodeValue(r, &recordBatch.BatchLength)
	if err != nil {
		return nil, err
	}
	err = decoder.DecodeValue(r, &recordBatch.PartitionLeaderEpoch)
	if err != nil {
		return nil, err
	}
	err = decoder.DecodeValue(r, &recordBatch.Magic)
	if err != nil {
		return nil, err
	}
	err = decoder.DecodeValue(r, &recordBatch.CRC)
	if err != nil {
		return nil, err
	}
	err = decoder.DecodeValue(r, &recordBatch.Attributes)
	if err != nil {
		return nil, err
	}
	err = decoder.DecodeValue(r, &recordBatch.LastOffsetDelta)
	if err != nil {
		return nil, err
	}
	err = decoder.DecodeValue(r, &recordBatch.FirstTimestamp)
	if err != nil {
		return nil, err
	}
	err = decoder.DecodeValue(r, &recordBatch.MaxTimestamp)
	if err != nil {
		return nil, err
	}
	err = decoder.DecodeValue(r, &recordBatch.ProducerId)
	if err != nil {
		return nil, err
	}
	err = decoder.DecodeValue(r, &recordBatch.ProducerEpoch)
	if err != nil {
		return nil, err
	}
	err = decoder.DecodeValue(r, &recordBatch.BaseSequence)
	if err != nil {
		return nil, err
	}
	var lengthRecords int32
	err = decoder.DecodeValue(r, &lengthRecords)
	if err != nil {
		return nil, err
	}
	recordBatch.Records = make([]Record, lengthRecords)
	for i := range recordBatch.Records {
		recordInternal, err := DecodeRecord(r)
		if err != nil {
			return nil, err
		}
		recordBatch.Records[i] = *recordInternal
	}
	return recordBatch, nil
}

type BaseRecord struct {
	FrameVersion int8
	Type         RecordType
	Version      int8
}

func DecodeBaseRecord(r *bufio.Reader) (*BaseRecord, error) {
	record := &BaseRecord{}
	err := decoder.DecodeValue(r, &record.FrameVersion)
	if err != nil {
		return nil, err
	}
	record.Type, err = DecodeRecordType(r)
	if err != nil {
		return nil, err
	}
	err = decoder.DecodeValue(r, &record.Version)
	if err != nil {
		return nil, err
	}
	return record, nil
}

func DecodeRecordType(r *bufio.Reader) (RecordType, error) {
	var recordType int8
	err := decoder.DecodeValue(r, &recordType)
	if err != nil {
		return 0, err
	}
	switch recordType {
	case int8(RecordTypeTopic):
		return RecordTypeTopic, nil
	case int8(RecordTypePartition):
		return RecordTypePartition, nil
	case int8(RecordTypeFeatureLevel):
		return RecordTypeFeatureLevel, nil
	default:
		return 0, fmt.Errorf("invalid record type: %d", recordType)
	}
}
