// Package config reads the server's settings from environment variables.
package config

import (
	"errors"
	"fmt"
	"os"
	"strconv"
	"time"

	"github.com/daniel-mutuku/textract-bulk-onboarding/internal/storage"
)

type Config struct {
	Bucket        string        // UPLOADS_BUCKET, required
	Region        string        // AWS_REGION
	PartSize      int64         // PART_SIZE_MB, in bytes
	MaxSize       int64         // MAX_UPLOAD_GB, in bytes
	PresignTTL    time.Duration // PRESIGN_TTL
	Addr          string        // ADDR
	AllowedOrigin string        // ALLOWED_ORIGIN; empty means same-origin only
	PipelineTable string        // PIPELINE_TABLE; empty disables the batch status endpoints
	DocsBucket    string        // DOCUMENTS_BUCKET; empty disables document previews
}

// AWS_PROFILE is not read here: the SDK's default credential chain picks it up.

// Load reads the config from the process environment.
func Load() (Config, error) {
	return load(os.Getenv)
}

func load(getenv func(string) string) (Config, error) {
	cfg := Config{
		Bucket:        getenv("UPLOADS_BUCKET"),
		Region:        or(getenv("AWS_REGION"), "eu-west-1"),
		Addr:          or(getenv("ADDR"), ":5173"),
		AllowedOrigin: getenv("ALLOWED_ORIGIN"),
		PipelineTable: getenv("PIPELINE_TABLE"),
		DocsBucket:    getenv("DOCUMENTS_BUCKET"),
	}
	var errs []error
	if cfg.Bucket == "" {
		errs = append(errs, errors.New("UPLOADS_BUCKET is required"))
	}

	partMB, err := intVar(getenv, "PART_SIZE_MB", 10)
	errs = append(errs, err)
	cfg.PartSize = partMB << 20
	if err == nil && cfg.PartSize < storage.MinPartSize {
		errs = append(errs, fmt.Errorf("PART_SIZE_MB must be at least 5 (S3's minimum part size), got %d", partMB))
	}

	maxGB, err := intVar(getenv, "MAX_UPLOAD_GB", 50)
	errs = append(errs, err)
	cfg.MaxSize = maxGB << 30
	if err == nil && maxGB <= 0 {
		errs = append(errs, fmt.Errorf("MAX_UPLOAD_GB must be positive, got %d", maxGB))
	}

	cfg.PresignTTL = 15 * time.Minute
	if v := getenv("PRESIGN_TTL"); v != "" {
		ttl, err := time.ParseDuration(v)
		switch {
		case err != nil:
			errs = append(errs, fmt.Errorf("PRESIGN_TTL: %w", err))
		case ttl <= 0 || ttl > 7*24*time.Hour:
			// SigV4 presigned URLs can live at most 7 days.
			errs = append(errs, fmt.Errorf("PRESIGN_TTL must be between 1s and 168h, got %s", ttl))
		default:
			cfg.PresignTTL = ttl
		}
	}

	return cfg, errors.Join(errs...)
}

func intVar(getenv func(string) string, name string, def int64) (int64, error) {
	v := getenv(name)
	if v == "" {
		return def, nil
	}
	n, err := strconv.ParseInt(v, 10, 64)
	if err != nil {
		return 0, fmt.Errorf("%s must be a whole number, got %q", name, v)
	}
	return n, nil
}

func or(v, def string) string {
	if v == "" {
		return def
	}
	return v
}
