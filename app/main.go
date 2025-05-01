package main

import (
	"log"
	"net"
)

func main() {
	l, err := net.Listen("tcp", "0.0.0.0:9092")
	if err != nil {
		log.Fatalf("Failed to bind to port 9092: %v", err)
	}
	defer l.Close()
	log.Println("Kafka server listening on port 9092")

	for {
		conn, err := l.Accept()
		if err != nil {
			log.Printf("Error accepting connection: %v", err)
			continue // Continue accepting other connections
		}
		go handleConnection(conn) // Handle connection in a new goroutine
	}
}
