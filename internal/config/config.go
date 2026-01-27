package config

import (
	"encoding/json"
	"fmt"
	"os"
)

type Config struct {
	Server  ServerConfig  `json:"server"`
	Storage StorageConfig `json:"storage"`
}

// ServerConfig is the TCP broker bind address.
type ServerConfig struct {
	Host string `json:"host"`
	Port int    `json:"port"`
}

type StorageConfig struct {
	DataDir        string `json:"data_dir"`
	SyncIntervalMs int    `json:"sync_interval_ms"`
}

// Load reads the config from a file
func Load(filename string) (*Config, error) {
	file, err := os.Open(filename)
	if err != nil {
		return nil, fmt.Errorf("could not open config file: %w", err)
	}
	defer file.Close()

	decoder := json.NewDecoder(file)
	cfg := &Config{}
	err = decoder.Decode(cfg)
	if err != nil {
		return nil, fmt.Errorf("Could not parse config json: %w", err)
	}
	return cfg, nil
}
