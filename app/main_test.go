package main

import (
	"bufio"
	"fmt"
	"strings"
	"testing"
)

func TestMain(t *testing.T) {
	r := strings.NewReader("aaa")
	br := bufio.NewReader(r)
	b, err := br.ReadByte()
	if err != nil {
		t.Fatal(err)
	}
	fmt.Println("b", b)
	b, err = r.ReadByte()
	if err != nil {
		t.Fatal(err)
	}
	fmt.Println("b", b)
}
