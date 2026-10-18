package config

import (
	"testing"
	"time"
)

func env(vars map[string]string) func(string) string {
	return func(k string) string { return vars[k] }
}

func TestLoadDefaults(t *testing.T) {
	cfg, err := load(env(map[string]string{"UPLOADS_BUCKET": "b"}))
	if err != nil {
		t.Fatal(err)
	}
	want := Config{
		Bucket:     "b",
		Region:     "eu-west-1",
		PartSize:   10 << 20,
		MaxSize:    50 << 30,
		PresignTTL: 15 * time.Minute,
		Addr:       ":5173",
	}
	if cfg != want {
		t.Errorf("got %+v, want %+v", cfg, want)
	}
}

func TestLoadOverrides(t *testing.T) {
	cfg, err := load(env(map[string]string{
		"UPLOADS_BUCKET": "b",
		"AWS_REGION":     "us-east-1",
		"PART_SIZE_MB":   "64",
		"MAX_UPLOAD_GB":  "5",
		"PRESIGN_TTL":    "1h",
		"ADDR":           ":8080",
		"ALLOWED_ORIGIN": "http://localhost:3000",
	}))
	if err != nil {
		t.Fatal(err)
	}
	if cfg.PartSize != 64<<20 || cfg.MaxSize != 5<<30 || cfg.PresignTTL != time.Hour ||
		cfg.Region != "us-east-1" || cfg.Addr != ":8080" || cfg.AllowedOrigin != "http://localhost:3000" {
		t.Errorf("got %+v", cfg)
	}
}

func TestLoadRejects(t *testing.T) {
	tests := map[string]map[string]string{
		"missing bucket":         {},
		"part size too small":    {"UPLOADS_BUCKET": "b", "PART_SIZE_MB": "4"},
		"part size not a number": {"UPLOADS_BUCKET": "b", "PART_SIZE_MB": "ten"},
		"zero max size":          {"UPLOADS_BUCKET": "b", "MAX_UPLOAD_GB": "0"},
		"bad ttl":                {"UPLOADS_BUCKET": "b", "PRESIGN_TTL": "soon"},
		"ttl over 7 days":        {"UPLOADS_BUCKET": "b", "PRESIGN_TTL": "200h"},
	}
	for name, vars := range tests {
		if _, err := load(env(vars)); err == nil {
			t.Errorf("%s: expected an error", name)
		}
	}
}
