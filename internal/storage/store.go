// Package storage hides S3 behind a small interface so the HTTP layer can be
// tested with an in-memory fake.
package storage

import (
	"context"
	"errors"
)

// S3 limits for multipart uploads.
const (
	MinPartSize = 5 << 20 // every part except the last must be at least 5 MiB
	MaxParts    = 10_000
)

var (
	// ErrNoSuchUpload means the upload ID is unknown, or was already completed or aborted.
	ErrNoSuchUpload = errors.New("no such upload")
	// ErrInvalidPart means a submitted part number or ETag does not match what S3 stored.
	ErrInvalidPart = errors.New("invalid part")
)

// Part identifies one uploaded part. The ETag is what S3 returned from the part PUT.
type Part struct {
	PartNumber int32  `json:"partNumber"`
	ETag       string `json:"etag"`
}

// MultipartStore coordinates multipart uploads. It never sees file bytes:
// the browser PUTs parts straight to the presigned URLs.
type MultipartStore interface {
	// Create opens a multipart upload for key and returns its upload ID.
	Create(ctx context.Context, key, contentType string, meta map[string]string) (uploadID string, err error)
	// PresignPart returns a URL the browser can PUT one part's bytes to.
	PresignPart(ctx context.Context, key, uploadID string, partNumber int32) (url string, err error)
	// Complete assembles the parts, in ascending part-number order, into the final object.
	Complete(ctx context.Context, key, uploadID string, parts []Part) error
	// Abort discards the upload and every part stored so far.
	Abort(ctx context.Context, key, uploadID string) error
}
