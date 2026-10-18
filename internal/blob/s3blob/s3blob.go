// Package s3blob is the S3 implementation of blob.Store.
//
// Open doesn't download anything. It returns an io.ReaderAt that fetches
// byte ranges on demand with ranged GETs (HTTP "Range: bytes=start-end"), so
// archive/zip can jump to the central directory at the end of a multi-GB zip
// and then to each entry, reading only what it needs.
package s3blob

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"sync"
	"sync/atomic"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"

	"github.com/daniel-mutuku/textract-bulk-onboarding/internal/blob"
)

const (
	// Each ranged GET fetches one 8 MiB block. archive/zip reads in small
	// pieces (a few KB at a time while decompressing); fetching each of those
	// separately would mean thousands of requests. Big blocks turn a 1 GiB
	// chunk into ~128 GETs.
	defaultBlockSize = 8 << 20
	// Blocks kept in memory: the end of the file (central directory) plus
	// the block currently being decompressed, with room to spare.
	cachedBlocks = 4
)

type Store struct {
	client    *s3.Client
	blockSize int64
}

func New(client *s3.Client) *Store {
	return &Store{client: client, blockSize: defaultBlockSize}
}

var _ blob.Store = (*Store)(nil)

func (s *Store) Open(ctx context.Context, bucket, key string) (blob.Object, error) {
	// HeadObject returns size, ETag and user metadata without the body.
	head, err := s.client.HeadObject(ctx, &s3.HeadObjectInput{Bucket: &bucket, Key: &key})
	if err != nil {
		var nf *types.NotFound
		if errors.As(err, &nf) {
			return blob.Object{}, blob.ErrNotFound
		}
		return blob.Object{}, err
	}
	r := &rangeReader{
		ctx: ctx, client: s.client, bucket: bucket, key: key,
		size: aws.ToInt64(head.ContentLength), etag: aws.ToString(head.ETag),
		blockSize: s.blockSize, blocks: map[int64][]byte{},
	}
	return blob.Object{ReaderAt: r, Size: r.size, Metadata: head.Metadata}, nil
}

func (s *Store) Put(ctx context.Context, bucket, key string, body []byte, contentType string) error {
	_, err := s.client.PutObject(ctx, &s3.PutObjectInput{
		Bucket:        &bucket,
		Key:           &key,
		Body:          bytes.NewReader(body),
		ContentLength: aws.Int64(int64(len(body))),
		ContentType:   &contentType,
	})
	return err
}

// rangeReader implements io.ReaderAt over an S3 object, block by block.
type rangeReader struct {
	ctx       context.Context
	client    *s3.Client
	bucket    string
	key       string
	size      int64
	etag      string
	blockSize int64

	mu     sync.Mutex
	blocks map[int64][]byte // block index -> bytes
	order  []int64          // least recently used first
	gets   atomic.Int64     // ranged GETs made, for tests and logs
}

func (r *rangeReader) ReadAt(p []byte, off int64) (int, error) {
	if off < 0 {
		return 0, errors.New("s3blob: negative offset")
	}
	n := 0
	for n < len(p) {
		pos := off + int64(n)
		if pos >= r.size {
			return n, io.EOF
		}
		idx := pos / r.blockSize
		block, err := r.block(idx)
		if err != nil {
			return n, err
		}
		n += copy(p[n:], block[pos-idx*r.blockSize:])
	}
	return n, nil
}

func (r *rangeReader) block(idx int64) ([]byte, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if b, ok := r.blocks[idx]; ok {
		r.touch(idx)
		return b, nil
	}

	start := idx * r.blockSize
	end := min(start+r.blockSize, r.size) - 1
	r.gets.Add(1)
	out, err := r.client.GetObject(r.ctx, &s3.GetObjectInput{
		Bucket: &r.bucket,
		Key:    &r.key,
		Range:  aws.String(fmt.Sprintf("bytes=%d-%d", start, end)),
		// Fail rather than mix bytes from two versions if the object is
		// replaced while we are reading it (412 Precondition Failed).
		IfMatch: &r.etag,
	}, func(o *s3.Options) {
		// S3's checksum covers the whole object, so a byte range can't be
		// validated against it; without this the SDK logs that it skipped
		// the check on every single GET.
		o.DisableLogOutputChecksumValidationSkipped = true
	})
	if err != nil {
		return nil, fmt.Errorf("get %s bytes %d-%d: %w", r.key, start, end, err)
	}
	defer out.Body.Close()
	b := make([]byte, end-start+1)
	if _, err := io.ReadFull(out.Body, b); err != nil {
		return nil, fmt.Errorf("read %s bytes %d-%d: %w", r.key, start, end, err)
	}

	r.blocks[idx] = b
	r.order = append(r.order, idx)
	if len(r.order) > cachedBlocks {
		delete(r.blocks, r.order[0])
		r.order = r.order[1:]
	}
	return b, nil
}

// touch marks a cached block as most recently used.
func (r *rangeReader) touch(idx int64) {
	for i, v := range r.order {
		if v == idx {
			r.order = append(append(r.order[:i:i], r.order[i+1:]...), idx)
			return
		}
	}
}
