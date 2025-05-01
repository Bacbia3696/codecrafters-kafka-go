package main

import (
	"encoding/binary"
	"fmt"
	"net"
	"os"
)

func main() {
	l, err := net.Listen("tcp", "0.0.0.0:9092")
	if err != nil {
		fmt.Println("Failed to bind to port 9092")
		os.Exit(1)
	}
	conn, err := l.Accept()
	if err != nil {
		fmt.Println("Error accepting connection: ", err.Error())
		os.Exit(1)
	}
	fmt.Println("Connection accepted", conn.RemoteAddr())
	defer conn.Close()
	resp := make([]byte, 8)
	binary.BigEndian.PutUint32(resp[0:4], uint32(0))
	binary.BigEndian.PutUint32(resp[4:8], uint32(7))
	_, err = conn.Read(resp)
	if err != nil {
		fmt.Println("Error reading response: ", err.Error())
		os.Exit(1)
	}
	_, err = conn.Write(resp)
	if err != nil {
		fmt.Println("Error writing response: ", err.Error())
		os.Exit(1)
	}
}
