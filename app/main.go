package main

import (
	"log"
	"net"

	"github.com/codecrafters-io/kafka-starter-go/app/protocol"
)

func main() {
	// Start TCP server on port 9092
	l, err := net.Listen("tcp", "0.0.0.0:9092")
	if err != nil {
		log.Fatalf("Failed to bind to port 9092: %v", err)
	}
	defer l.Close()
	log.Println("Kafka server listening on port 9092")

	// Accept and handle connections
	for {
		conn, err := l.Accept()
		if err != nil {
			log.Printf("Error accepting connection: %v", err)
			continue
		}
		go protocol.HandleConnection(conn)
	}
}
