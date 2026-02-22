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
	Addr    string `yaml:"addr"`     // HTTP Listen Address (e.g. :8080)
	TCPAddr string `yaml:"tcp_addr"` // TCP Listen Address (e.g. :9090)
}

type StorageConfig struct {
	Path                   string `yaml:"path"`
	WalBufferSize          int    `yaml:"wal_buffer_size"`
	MemTableFlushThreshold int `yaml:"memtable_flush_threshold"`
	CompactionThreshold    int `yaml:"compaction_threshold"`
	WalBatchSize           int `yaml:"wal_batch_size"`
}

type SystemConfig struct {
	ShardCount     int     `yaml:"shard_count"`
	BloomSize      uint    `yaml:"bloom_size"`
	BloomFalseProb float64 `yaml:"bloom_false_prob"`
}

func Load(configPath string) (*Config, error) {
	cfg := &Config{
		Server: ServerConfig{
			Addr:    ":8080",
			TCPAddr: ":9090",
		},
		Storage: StorageConfig{
			Path:                   "neuro_data",
			WalBufferSize:          5000,
			MemTableFlushThreshold: 2000,
			CompactionThreshold:    4,
			WalBatchSize:           500,
		},
		System: SystemConfig{
			ShardCount:     16,
			BloomSize:      100000,
			BloomFalseProb: 0.01,
		},
	}

	if configPath == "" {
		for _, p := range []string{"configs/neuro.yaml", "neuro.yaml"} {
			data, err := os.ReadFile(p)
			if err == nil {
				if err := yaml.Unmarshal(data, cfg); err != nil {
					return cfg, err
				}
				applyStorageDefaults(cfg)
				return cfg, nil
			}
		}
		applyStorageDefaults(cfg)
		return cfg, nil // no file found: use defaults
	}

	data, err := os.ReadFile(configPath)
	if err != nil {
		return cfg, err
	}

	if err := yaml.Unmarshal(data, cfg); err != nil {
		return cfg, err
	}

	applyStorageDefaults(cfg)
	return cfg, nil
}

func applyStorageDefaults(cfg *Config) {
	if cfg.Storage.MemTableFlushThreshold <= 0 {
		cfg.Storage.MemTableFlushThreshold = 2000
	}
	if cfg.Storage.CompactionThreshold <= 0 {
		cfg.Storage.CompactionThreshold = 4
	}
	if cfg.Storage.WalBatchSize <= 0 {
		cfg.Storage.WalBatchSize = 500
	}
	if cfg.System.ShardCount <= 0 {
		cfg.System.ShardCount = 16
	}
	if cfg.System.BloomSize == 0 {
		cfg.System.BloomSize = 100000
	}
	if cfg.System.BloomFalseProb <= 0 || cfg.System.BloomFalseProb >= 1 {
		cfg.System.BloomFalseProb = 0.01
	}
}
