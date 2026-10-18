// Package memstore is an in-memory batch.Store for tests and local runs.
// A single mutex makes each method atomic, which is the guarantee the
// DynamoDB store gets from conditional writes and transactions.
package memstore

import (
	"context"
	"fmt"
	"maps"
	"slices"
	"sync"
	"time"

	"github.com/daniel-mutuku/textract-bulk-onboarding/internal/batch"
)

type Store struct {
	mu      sync.Mutex
	batches map[string]*batch.Batch
	docs    map[string]map[int]*batch.Document // batch ID -> seq -> document
	chunks  map[string]map[int]bool            // batch ID -> finished chunks
	now     func() time.Time
}

func New() *Store {
	return &Store{
		batches: map[string]*batch.Batch{},
		docs:    map[string]map[int]*batch.Document{},
		chunks:  map[string]map[int]bool{},
		now:     time.Now,
	}
}

var _ batch.Store = (*Store)(nil)

func (s *Store) CreateBatch(_ context.Context, b batch.Batch) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.batches[b.ID]; ok {
		return batch.ErrExists
	}
	now := s.now()
	b.CreatedAt, b.UpdatedAt = now, now
	s.batches[b.ID] = &b
	s.docs[b.ID] = map[int]*batch.Document{}
	s.chunks[b.ID] = map[int]bool{}
	return nil
}

func (s *Store) GetBatch(_ context.Context, id string) (batch.Batch, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	b, ok := s.batches[id]
	if !ok {
		return batch.Batch{}, batch.ErrNotFound
	}
	return *b, nil
}

func (s *Store) FinishChunk(_ context.Context, batchID string, chunk int) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	b, ok := s.batches[batchID]
	if !ok {
		return batch.ErrNotFound
	}
	if s.chunks[batchID][chunk] {
		return nil // a retried chunk is not counted twice
	}
	s.chunks[batchID][chunk] = true
	b.ChunksDone++
	b.UpdatedAt = s.now()
	return nil
}

func (s *Store) CreateDocument(_ context.Context, d batch.Document) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	b, ok := s.batches[d.BatchID]
	if !ok {
		return batch.ErrNotFound
	}
	if _, ok := s.docs[d.BatchID][d.Seq]; ok {
		return batch.ErrExists
	}
	switch d.Status {
	case batch.StatusPending:
	case batch.StatusSkipped:
		b.Skipped++
	case batch.StatusFailed:
		b.Failed++
	default:
		return fmt.Errorf("a new document cannot start as %q", d.Status)
	}
	d.Fields = maps.Clone(d.Fields)
	s.docs[d.BatchID][d.Seq] = &d
	b.UpdatedAt = s.now()
	return nil
}

func (s *Store) GetDocument(_ context.Context, batchID string, seq int) (batch.Document, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	d, ok := s.docs[batchID][seq]
	if !ok {
		return batch.Document{}, batch.ErrNotFound
	}
	return copyDoc(d), nil
}

func (s *Store) ListDocuments(_ context.Context, batchID string, afterSeq, limit int) ([]batch.Document, int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	seqs := slices.Sorted(maps.Keys(s.docs[batchID]))
	var out []batch.Document
	for _, seq := range seqs {
		if seq <= afterSeq {
			continue
		}
		if len(out) == limit {
			return out, out[len(out)-1].Seq, nil
		}
		out = append(out, copyDoc(s.docs[batchID][seq]))
	}
	return out, 0, nil
}

func (s *Store) CompleteDocument(_ context.Context, batchID string, seq int, from batch.Status, e batch.Extraction) error {
	if from != batch.StatusPending && from != batch.StatusFailed {
		return fmt.Errorf("cannot complete a document from %q", from)
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	d, ok := s.docs[batchID][seq]
	if !ok {
		return batch.ErrNotFound
	}
	if d.Status != from {
		return batch.ErrConflict
	}
	b := s.batches[batchID]
	b.Processed++
	if from == batch.StatusFailed {
		b.Failed-- // a dead-letter replay that now succeeds moves between counters
	}
	d.Status = batch.StatusExtracted
	d.Reason = ""
	d.Fields = maps.Clone(e.Fields)
	d.RawKey = e.RawKey
	d.ExtractedAt = e.At
	b.UpdatedAt = s.now()
	return nil
}

func (s *Store) FailDocument(_ context.Context, batchID string, seq int, reason string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	d, ok := s.docs[batchID][seq]
	if !ok {
		return batch.ErrNotFound
	}
	if d.Status != batch.StatusPending {
		return batch.ErrConflict
	}
	d.Status = batch.StatusFailed
	d.Reason = reason
	b := s.batches[batchID]
	b.Failed++
	b.UpdatedAt = s.now()
	return nil
}

// copyDoc returns a copy whose Fields map the caller can't use to change ours.
func copyDoc(d *batch.Document) batch.Document {
	c := *d
	c.Fields = maps.Clone(d.Fields)
	return c
}
