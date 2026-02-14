package config

import (
	"os"

	"gopkg.in/yaml.v3"
)

type Config struct {
	Server  ServerConfig  `yaml:"server"`
	Storage StorageConfig `yaml:"storage"`
	System  SystemConfig  `yaml:"system"`
}

type ServerConfig struct {
	Addr string `yaml:"addr"` // e.g. ":8080"
}

type StorageConfig struct {
	Path          string `yaml:"path"`
	WalBufferSize int    `yaml:"wal_buffer_size"`
}

type SystemConfig struct {
	ShardCount     int     `yaml:"shard_count"`
	BloomSize      uint    `yaml:"bloom_size"`
	BloomFalseProb float64 `yaml:"bloom_false_prob"`
}

func Load(configPath string) (*Config, error) {
	cfg := &Config{
		Server: ServerConfig{Addr: ":8080"},
		Storage: StorageConfig{
			Path:          "neuro.db",
			WalBufferSize: 5000,
		},
		System: SystemConfig{
			ShardCount:     16,
			BloomSize:      100000,
			BloomFalseProb: 0.01,
		},
	}

	data, err := os.ReadFile(configPath)
	if err != nil {
		return cfg, err
	}

	if err := yaml.Unmarshal(data, cfg); err != nil {
		return cfg, err
	}

	return cfg, nil
}
