package main

import (
	"testing"

	"log"
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
