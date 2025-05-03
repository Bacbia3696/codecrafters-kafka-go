package config

import (
	"fmt"
	"os"
	"strconv"
)

// Config holds all configuration for the Kafka server
type Config struct {
	Host string
	Port int
}

// New creates a new Config with values from environment variables or defaults
func New() (*Config, error) {
	port := 9092 // default port
	if portStr := os.Getenv("KAFKA_PORT"); portStr != "" {
		p, err := strconv.Atoi(portStr)
		if err != nil {
			return nil, fmt.Errorf("invalid port number: %w", err)
		}
		port = p
	}

	host := "0.0.0.0" // default host
	if h := os.Getenv("KAFKA_HOST"); h != "" {
		host = h
	}

	return &Config{
		Host: host,
		Port: port,
	}, nil
}

// Address returns the full address string for the server
func (c *Config) Address() string {
	return fmt.Sprintf("%s:%d", c.Host, c.Port)
}
