package config

import (
	"flag"
	"os"

	"es-to-s3-dumper/internal/logger"
)

type Config struct {
	// es
	EsUrl         string
	EsUsername    string
	EsPassword    string
	IndexName     string
	ScrollSize    int
	ScrollTimeout string
	MaxTimeout    int
	// s3
	S3Region    string
	S3Bucket    string
	S3Path      string
	S3AccessKey string
	S3SecretKey string
	// dumper config
	MaxFileSize int
	MaxDocs     int
	Debug       bool
	MaxUploads  int
}

func LoadConfig() *Config {
	log := logger.Logger
	var cfg Config

	// es
	flag.StringVar(&cfg.EsUrl, "es-url", "https://localhost:9200", "Elasticsearch URL")
	flag.StringVar(&cfg.EsUsername, "es-username", "", "Basic auth username")
	flag.StringVar(&cfg.EsPassword, "es-password", "", "Basic auth password")
	flag.StringVar(&cfg.IndexName, "index-name", "", "Index name to extract")
	flag.IntVar(&cfg.ScrollSize, "scroll-size", 10000, "Size of scroll")
	flag.StringVar(&cfg.ScrollTimeout, "scroll-timeout", "5m", "Scroll timeout")
	flag.IntVar(&cfg.MaxTimeout, "max-timeout", 60, "Timeout for http requests to Elasticsearch")

	// s3
	flag.StringVar(&cfg.S3Region, "s3-region", "us-west-2", "Region S3 bucket resides in")
	flag.StringVar(&cfg.S3Bucket, "s3-bucket", "", "S3 bucket to dump objects into")
	flag.StringVar(&cfg.S3Path, "s3-path", "", "S3 path to dump objects into")
	flag.StringVar(&cfg.S3AccessKey, "s3-access-key", "", "S3 access key")
	flag.StringVar(&cfg.S3SecretKey, "s3-secret-key", "", "S3 secret key")

	// dumper
	flag.IntVar(&cfg.MaxFileSize, "max-file-size", 32*1024*1024, "Maximum file size before splitting")
	flag.IntVar(&cfg.MaxDocs, "max-docs", 1_000_000, "Maximum docs before splitting")
	flag.BoolVar(&cfg.Debug, "debug", false, "Enable debug logging")
	flag.IntVar(&cfg.MaxUploads, "max-uploads", 2, "Max background uploads to perform")

	flag.Parse()

	// optional env vars
	if cfg.EsUsername == "" {
		cfg.EsUsername = os.Getenv("ES_USERNAME")
	}

	if cfg.EsPassword == "" {
		cfg.EsPassword = os.Getenv("ES_PASSWORD")
	}

	if cfg.S3AccessKey == "" {
		cfg.S3AccessKey = os.Getenv("S3_ACCESS_KEY")
	}

	if cfg.S3SecretKey == "" {
		cfg.S3SecretKey = os.Getenv("S3_SECRET_KEY")
	}

	if cfg.S3Bucket == "" {
		log.Fatalln("S3 Bucket required!")
	}

	return &cfg
}
