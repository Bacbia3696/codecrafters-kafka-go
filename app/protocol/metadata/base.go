package metadata

import (
	"bufio"
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
	headerLength, err := decoder.DecodeUvarint(r)
	if err != nil {
		return nil, err
	}
	if headerLength > 0 {
		record.Headers = make([]RecordHeader, headerLength-1)
		for i := range record.Headers {
			header, err := DecodeRecordHeader(r)
			if err != nil {
				return nil, err
			}
			record.Headers[i] = *header
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
	lengthRecords, err := decoder.DecodeUvarint(r)
	if err != nil {
		return nil, err
	}
	recordBatch.Records = make([]Record, lengthRecords)
	for i := range recordBatch.Records {
		record, err := DecodeRecord(r)
		if err != nil {
			return nil, err
		}
		recordBatch.Records[i] = *record
	}
	return recordBatch, nil
}

type RecordType int8

const (
	RecordTypeTopic     RecordType = 2
	RecordTypePartition RecordType = 3
)

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
	default:
		return 0, fmt.Errorf("invalid record type: %d", recordType)
	}
}
