package main

import (
	"bytes"
	"log"
	"testing"

	"github.com/codecrafters-io/kafka-starter-go/app/protocol"
)

func TestDecodeCompactString(t *testing.T) {
	a := []int{1, 2, 3}
	b := "asd"
	c := &b
	check(a)
	check(b)
	check(c)
}

func check(v any) {
	switch v.(type) {
	case []int:
		log.Println("a is an int slice")
	case string:
		log.Println("a is a string")
	case *string:
		log.Println("a is a pointer to a string")
	default:
		log.Println("a is not an int slice")
	}
}

func TestReadClusterMetadata(t *testing.T) {
	data := []byte{
		0, 0, 0, 0, 0, 0, 0, 0,
		0, 0, 0, 79,
		0, 0, 0, 1, 2, 176, 105, 69, 124, 0, 0, 0, 0, 0, 0, 0, 0, 1, 145, 224, 90, 248, 24, 0, 0, 1, 145, 224, 90, 248, 24, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 0, 0, 0, 1, 58, 0, 0, 0, 1, 46, 1, 12, 0, 17, 109, 101, 116, 97, 100, 97, 116, 97, 46, 118, 101, 114, 115, 105, 111, 110, 0, 20, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 228, 0, 0, 0, 1, 2, 36, 219, 18, 221, 0, 0, 0, 0, 0, 2, 0, 0, 1, 145, 224, 91, 45, 21, 0, 0, 1, 145, 224, 91, 45, 21, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 0, 0, 0, 3, 60, 0, 0, 0, 1, 48, 1, 2, 0, 4, 115, 97, 122, 0, 0, 0, 0, 0, 0, 64, 0, 128, 0, 0, 0, 0, 0, 0, 145, 0, 0, 144, 1, 0, 0, 2, 1, 130, 1, 1, 3, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 64, 0, 128, 0, 0, 0, 0, 0, 0, 145, 2, 0, 0, 0, 1, 2, 0, 0, 0, 1, 1, 1, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0, 2, 16, 0, 0, 0, 0, 0, 64, 0, 128, 0, 0, 0, 0, 0, 0, 1, 0, 0, 144, 1, 0, 0, 4, 1, 130, 1, 1, 3, 1, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 64, 0, 128, 0, 0, 0, 0, 0, 0, 145, 2, 0, 0, 0, 1, 2, 0, 0, 0, 1, 1, 1, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0, 2, 16, 0, 0, 0, 0, 0, 64, 0, 128, 0, 0, 0, 0, 0, 0, 1, 0, 0,
	}
	clusterMetadata, err := protocol.DecodeClusterMetadata(data, true)
	if err != nil {
		t.Fatal(err)
	}
	t.Log(clusterMetadata)
}

func TestDecodeTopicLog(t *testing.T) {
	data := []byte{
		0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 68, 0, 0, 0, 0, 2, 100, 97, 124, 74, 0, 0, 0, 0, 0, 0, 0, 0, 1, 145, 224, 91, 109, 139, 0, 0, 1, 145, 224, 91, 109, 139, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 36, 0, 0, 0, 1, 24, 72, 101, 108, 108, 111, 32, 69, 97, 114, 116, 104, 33, 0,
	}
	clusterMetadata, err := protocol.DecodeClusterMetadata(data, false)
	if err != nil {
		t.Fatal(err)
	}
	t.Log(clusterMetadata)
	buf := bytes.NewBuffer(nil)
	for _, recordBatch := range clusterMetadata.RecordBatchs {
		recordBatch.Encode(buf)
	}
	t.Log(data)
	t.Log(buf.Bytes())
	clusterMetadata2, err := protocol.DecodeClusterMetadata(buf.Bytes(), false)
	if err != nil {
		t.Fatal(err)
	}
	t.Log(clusterMetadata2)
}
