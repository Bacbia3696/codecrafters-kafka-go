package config

import (
	"fmt"
	"log/slog" // Import slog for logging
	"strings"

	"github.com/spf13/viper" // Import viper
)

// Config holds all configuration for the Kafka server
type Config struct {
	Host string
	Port int
}

// Constants for configuration keys
const (
	KeyHost = "kafka.host"
	KeyPort = "kafka.port"
)

// New creates a new Config using viper for loading values
func New(log *slog.Logger) (*Config, error) {
	v := viper.New()

	// 1. Set Defaults
	v.SetDefault(KeyHost, "0.0.0.0")
	v.SetDefault(KeyPort, 9092)

	// 2. Configure Environment Variables
	// Allow viper to read KAFKA_HOST and KAFKA_PORT
	v.SetEnvPrefix("KAFKA")
	v.SetEnvKeyReplacer(strings.NewReplacer(".", "_")) // Replace '.' with '_' for env vars (e.g., kafka.host -> KAFKA_HOST)
	v.AutomaticEnv()                                   // Read matching environment variables

	// 3. Get values
	host := v.GetString(KeyHost)
	port := v.GetInt(KeyPort)

	log.Info("Configuration loaded", "host", host, "port", port)

	return &Config{
		Host: host,
		Port: port,
	}, nil
}

// Address returns the full address string for the server
func (c *Config) Address() string {
	return fmt.Sprintf("%s:%d", c.Host, c.Port)
}
