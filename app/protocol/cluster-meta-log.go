package protocol

import (
	"bufio"
	"errors"
	"io"
	"os"
)

type ClusterMetadata struct {
	RecordBatchs []RecordBatch
}

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
	Length         int32
	Attributes     int8
	TimestampDelta int64
	OffsetDelta    int32
	Key            []byte
	Value          []byte
	Headers        []RecordHeader
}

type RecordHeader struct {
	Key   string
	Value []byte
}

func DecodeRecordHeader(r *bufio.Reader) (*RecordHeader, error) {
	header := &RecordHeader{}
	var err error
	header.Key, err = DecodeCompactString(r)
	if err != nil {
		return nil, err
	}
	header.Value, err = DecodeCompactBytes(r)
	if err != nil {
		return nil, err
	}
	return header, nil
}

func DecodeRecord(r *bufio.Reader) (*Record, error) {
	record := &Record{}
	var err error
	record.Length, err = DecodeInt32(r)
	if err != nil {
		return nil, err
	}
	record.Attributes, err = DecodeInt8(r)
	if err != nil {
		return nil, err
	}
	record.TimestampDelta, err = DecodeInt64(r)
	if err != nil {
		return nil, err
	}
	record.OffsetDelta, err = DecodeInt32(r)
	if err != nil {
		return nil, err
	}
	record.Key, err = DecodeCompactBytes(r)
	if err != nil {
		return nil, err
	}
	record.Value, err = DecodeCompactBytes(r)
	if err != nil {
		return nil, err
	}
	headerLength, err := DecodeUvarint(r)
	if err != nil {
		return nil, err
	}
	record.Headers = make([]RecordHeader, headerLength-1)
	for i := range record.Headers {
		header, err := DecodeRecordHeader(r)
		if err != nil {
			return nil, err
		}
		record.Headers[i] = *header
	}
	return record, nil
}

func DecodeRecordBatch(r *bufio.Reader) (*RecordBatch, error) {
	recordBatch := &RecordBatch{}
	var err error
	recordBatch.BaseOffset, err = DecodeInt64(r)
	if err != nil {
		return nil, err
	}
	recordBatch.BatchLength, err = DecodeInt32(r)
	if err != nil {
		return nil, err
	}
	recordBatch.PartitionLeaderEpoch, err = DecodeInt32(r)
	if err != nil {
		return nil, err
	}
	recordBatch.Magic, err = DecodeInt8(r)
	if err != nil {
		return nil, err
	}
	recordBatch.CRC, err = DecodeInt32(r)
	if err != nil {
		return nil, err
	}
	recordBatch.Attributes, err = DecodeInt16(r)
	if err != nil {
		return nil, err
	}
	recordBatch.LastOffsetDelta, err = DecodeInt32(r)
	if err != nil {
		return nil, err
	}
	recordBatch.FirstTimestamp, err = DecodeInt64(r)
	if err != nil {
		return nil, err
	}
	recordBatch.MaxTimestamp, err = DecodeInt64(r)
	if err != nil {
		return nil, err
	}
	recordBatch.ProducerId, err = DecodeInt64(r)
	if err != nil {
		return nil, err
	}
	recordBatch.ProducerEpoch, err = DecodeInt16(r)
	if err != nil {
		return nil, err
	}
	recordBatch.BaseSequence, err = DecodeInt32(r)
	if err != nil {
		return nil, err
	}
	lengthRecords, err := DecodeUvarint(r)
	if err != nil {
		return nil, err
	}
	recordBatch.Records = make([]Record, lengthRecords-1)
	for i := range recordBatch.Records {
		record, err := DecodeRecord(r)
		if err != nil {
			return nil, err
		}
		recordBatch.Records[i] = *record
	}
	return recordBatch, nil
}

func (r *Record) Encode(w io.Writer) error {
	return nil
}

func ReadClusterMetadata() (*ClusterMetadata, error) {
	file, err := os.Open("/tmp/kraft-combined-logs/__cluster_metadata-0/00000000000000000000.log")
	if err != nil {
		return nil, err
	}
	defer file.Close()

	reader := bufio.NewReader(file)
	clusterMetadata := &ClusterMetadata{}
	for {
		recordBatch, err := DecodeRecordBatch(reader)
		if err != nil {
			if errors.Is(err, io.EOF) {
				break
			}
			return nil, err
		}
		clusterMetadata.RecordBatchs = append(clusterMetadata.RecordBatchs, *recordBatch)
	}
	return clusterMetadata, nil
}
