// Command server runs the upload API.
package main

import (
	"context"
	"errors"
	"io/fs"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/joho/godotenv"

	"github.com/daniel-mutuku/textract-bulk-onboarding/internal/batch/dynamostore"
	"github.com/daniel-mutuku/textract-bulk-onboarding/internal/config"
	"github.com/daniel-mutuku/textract-bulk-onboarding/internal/status"
	"github.com/daniel-mutuku/textract-bulk-onboarding/internal/storage"
	"github.com/daniel-mutuku/textract-bulk-onboarding/internal/upload"
)

func main() {
	if err := run(); err != nil {
		slog.Error("server stopped", "err", err)
		os.Exit(1)
	}
}

func run() error {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	// Load .env if present. Variables already set in the shell win over the file.
	if err := godotenv.Load(); err != nil && !errors.Is(err, fs.ErrNotExist) {
		return err
	}
	cfg, err := config.Load()
	if err != nil {
		return err
	}

	// The default chain reads AWS_PROFILE, ~/.aws/config and friends; no keys in code.
	awsCfg, err := awsconfig.LoadDefaultConfig(ctx, awsconfig.WithRegion(cfg.Region))
	if err != nil {
		return err
	}
	s3Client := s3.NewFromConfig(awsCfg)
	store := storage.NewS3Store(s3Client, cfg.Bucket, cfg.PresignTTL)

	checkCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	err = store.CheckBucket(checkCtx)
	cancel()
	if err != nil {
		return errors.New("cannot reach bucket " + cfg.Bucket + " (check UPLOADS_BUCKET, AWS_PROFILE and that Terraform was applied): " + err.Error())
	}

	mux := http.NewServeMux()
	upload.New(store, upload.Options{PartSize: cfg.PartSize, MaxSize: cfg.MaxSize}).Register(mux)
	if cfg.PipelineTable != "" {
		var files status.DocumentFiles
		if cfg.DocsBucket != "" {
			files = storage.NewDocumentLinks(s3Client, cfg.DocsBucket, 5*time.Minute)
		} else {
			slog.Warn("DOCUMENTS_BUCKET not set; document previews are disabled")
		}
		status.New(dynamostore.New(dynamodb.NewFromConfig(awsCfg), cfg.PipelineTable), files).Register(mux)
	} else {
		slog.Warn("PIPELINE_TABLE not set; batch status endpoints are disabled")
	}
	// The page and the API share an origin, so the API needs no CORS.
	mux.Handle("GET /", http.FileServer(http.Dir("web")))

	srv := &http.Server{
		Addr:              cfg.Addr,
		Handler:           withCORS(cfg.AllowedOrigin, mux),
		ReadHeaderTimeout: 10 * time.Second,
		ReadTimeout:       30 * time.Second, // request bodies are small JSON; file bytes never come here
		WriteTimeout:      30 * time.Second,
	}
	go func() {
		<-ctx.Done()
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		srv.Shutdown(shutdownCtx)
	}()

	slog.Info("listening", "addr", cfg.Addr, "bucket", cfg.Bucket, "table", cfg.PipelineTable, "region", cfg.Region,
		"partSizeMB", cfg.PartSize>>20, "maxUploadGB", cfg.MaxSize>>30)
	if err := srv.ListenAndServe(); !errors.Is(err, http.ErrServerClosed) {
		return err
	}
	return nil
}

// withCORS allows the API to be called from a frontend on another origin.
// When the frontend is served by this server, origin is empty and this is a no-op.
func withCORS(origin string, next http.Handler) http.Handler {
	if origin == "" {
		return next
	}
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Origin") == origin {
			w.Header().Set("Access-Control-Allow-Origin", origin)
			w.Header().Set("Vary", "Origin")
			if r.Method == http.MethodOptions {
				w.Header().Set("Access-Control-Allow-Methods", "POST")
				w.Header().Set("Access-Control-Allow-Headers", "Content-Type")
				w.WriteHeader(http.StatusNoContent)
				return
			}
		}
		next.ServeHTTP(w, r)
	})
}
