package config

import (
	"flag"
	"os"
	"strconv"
	"time"
)

// Format is the write format for records.
type Format string

const (
	// FormatJSONL is the default newline-delimited JSON write format.
	FormatJSONL Format = "jsonl"
	// FormatCSV is the comma-separated values write format.
	FormatCSV Format = "csv"
)

// Config holds all runtime configuration.
type Config struct {
	HTTPAddr        string
	MCPAddr         string
	DataDir         string
	SchemaPath      string
	Format          Format
	ChannelCapacity int
	SyncInterval    time.Duration
	SyncRecordCount int
	ShutdownTimeout time.Duration
	MCPStdio        bool
	Rebuild         bool
}

// Load reads configuration from flags and environment variables.
// Flags take precedence over environment variables.
func Load() *Config {
	cfg := &Config{
		HTTPAddr:        envOr("HTTP_ADDR", ":8080"),
		MCPAddr:         envOr("MCP_ADDR", ":8081"),
		DataDir:         envOr("DATA_DIR", "./data"),
		SchemaPath:      envOr("SCHEMA_PATH", "./schema.json"),
		Format:          Format(envOr("FORMAT", "jsonl")),
		ChannelCapacity: envIntOr("CHANNEL_CAPACITY", 10_000),
		SyncInterval:    envDurOr("SYNC_INTERVAL", 30*time.Second),
		SyncRecordCount: envIntOr("SYNC_RECORD_COUNT", 1000),
		ShutdownTimeout: envDurOr("SHUTDOWN_TIMEOUT", 30*time.Second),
	}

	flag.StringVar(&cfg.HTTPAddr, "http-addr", cfg.HTTPAddr, "HTTP server listen address")
	flag.StringVar(&cfg.MCPAddr, "mcp-addr", cfg.MCPAddr, "MCP SSE server listen address")
	flag.StringVar(&cfg.DataDir, "data-dir", cfg.DataDir, "Directory for data files")
	flag.StringVar(&cfg.SchemaPath, "schema", cfg.SchemaPath, "Path to schema.json")
	flag.StringVar((*string)(&cfg.Format), "format", string(cfg.Format), "Write format: jsonl or csv")
	flag.IntVar(&cfg.ChannelCapacity, "channel-capacity", cfg.ChannelCapacity, "Ingest channel buffer size")
	flag.DurationVar(&cfg.SyncInterval, "sync-interval", cfg.SyncInterval, "Automatic sync interval")
	flag.IntVar(&cfg.SyncRecordCount, "sync-record-count", cfg.SyncRecordCount, "Trigger sync every N records")
	flag.DurationVar(&cfg.ShutdownTimeout, "shutdown-timeout", cfg.ShutdownTimeout, "Graceful shutdown timeout")
	flag.BoolVar(&cfg.MCPStdio, "mcp-stdio", false, "Run MCP server over stdio instead of HTTP/SSE")
	flag.BoolVar(&cfg.Rebuild, "rebuild", false, "Rebuild all Parquet files from source JSONL/CSV on startup")
	flag.Parse()

	return cfg
}

func envOr(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

func envIntOr(key string, def int) int {
	if v := os.Getenv(key); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			return n
		}
	}
	return def
}

func envDurOr(key string, def time.Duration) time.Duration {
	if v := os.Getenv(key); v != "" {
		if d, err := time.ParseDuration(v); err == nil {
			return d
		}
	}
	return def
}
