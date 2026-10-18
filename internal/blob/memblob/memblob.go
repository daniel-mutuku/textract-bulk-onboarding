// Package memblob is an in-memory blob.Store for tests and local runs.
package memblob

import (
	"bytes"
	"context"
	"io"
	"maps"
	"sync"

	"github.com/daniel-mutuku/textract-bulk-onboarding/internal/blob"
)

type object struct {
	r           io.ReaderAt
	size        int64
	meta        map[string]string
	contentType string
}

type Store struct {
	mu      sync.Mutex
	objects map[string]object // "bucket/key" -> object
}

func New() *Store {
	return &Store{objects: map[string]object{}}
}

var _ blob.Store = (*Store)(nil)

func (s *Store) Open(_ context.Context, bucket, key string) (blob.Object, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	o, ok := s.objects[bucket+"/"+key]
	if !ok {
		return blob.Object{}, blob.ErrNotFound
	}
	return blob.Object{ReaderAt: o.r, Size: o.size, Metadata: maps.Clone(o.meta)}, nil
}

func (s *Store) Put(_ context.Context, bucket, key string, body []byte, contentType string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	data := bytes.Clone(body)
	s.objects[bucket+"/"+key] = object{r: bytes.NewReader(data), size: int64(len(data)), contentType: contentType}
	return nil
}

// Add stores an object backed by any ReaderAt, such as an open local file,
// with user metadata. It is how tests and cmd/dev place an "uploaded" zip.
func (s *Store) Add(bucket, key string, r io.ReaderAt, size int64, meta map[string]string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.objects[bucket+"/"+key] = object{r: r, size: size, meta: meta}
}

// Get returns a stored object's bytes and content type.
func (s *Store) Get(bucket, key string) ([]byte, string, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	o, ok := s.objects[bucket+"/"+key]
	if !ok {
		return nil, "", false
	}
	data := make([]byte, o.size)
	o.r.ReadAt(data, 0)
	return data, o.contentType, true
}

// Keys lists the keys stored in a bucket.
func (s *Store) Keys(bucket string) []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	var keys []string
	for k := range s.objects {
		if len(k) > len(bucket) && k[:len(bucket)+1] == bucket+"/" {
			keys = append(keys, k[len(bucket)+1:])
		}
	}
	return keys
}
