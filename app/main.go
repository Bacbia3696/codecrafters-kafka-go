package main

import (
	"encoding/binary"
	"fmt"
	"io"
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
	var sizeBuf [4]byte
	if _, err := io.ReadFull(conn, sizeBuf[:]); err != nil {
		fmt.Println("Error reading request size:", err)
		os.Exit(1)
	}
	size := binary.BigEndian.Uint32(sizeBuf[:])
	if size > 0 {
		if _, err := io.CopyN(io.Discard, conn, int64(size)); err != nil {
			fmt.Println("Error reading request payload:", err)
			os.Exit(1)
		}
	}
	resp := make([]byte, 8)
	binary.BigEndian.PutUint32(resp[0:4], uint32(4))
	binary.BigEndian.PutUint32(resp[4:8], uint32(7))
	_, err = conn.Write(resp)
	if err != nil {
		fmt.Println("Error writing response: ", err.Error())
		os.Exit(1)
	}
}
