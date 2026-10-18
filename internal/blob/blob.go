// Package blob is the pipeline's view of object storage (S3 in production).
package blob

import (
	"context"
	"errors"
	"io"
)

var ErrNotFound = errors.New("object not found")

// Object gives random access to a stored object. archive/zip needs that,
// because a zip's table of contents (the central directory) is at the END of
// the file: the reader jumps there first, then to each entry's offset.
type Object struct {
	io.ReaderAt
	Size     int64
	Metadata map[string]string // user metadata; on S3, the x-amz-meta-* headers
}

type Store interface {
	// Open returns ErrNotFound if the object does not exist.
	Open(ctx context.Context, bucket, key string) (Object, error)
	// Put writes a whole object. Documents are at most 10 MB, so a byte slice is fine.
	Put(ctx context.Context, bucket, key string, body []byte, contentType string) error
}
