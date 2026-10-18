// Package storetest is a contract test suite every batch.Store must pass. The
// in-memory fake runs it in its tests and the DynamoDB store runs the same
// suite against a real table, so the fake can't quietly drift from the real thing.
package storetest

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/daniel-mutuku/textract-bulk-onboarding/internal/batch"
)

// Run runs the suite. newStore is called once per subtest.
func Run(t *testing.T, newStore func(t *testing.T) batch.Store) {
	tests := []struct {
		name string
		fn   func(t *testing.T, s batch.Store)
	}{
		{"BatchRoundTrip", testBatchRoundTrip},
		{"FinishChunkCountsOnce", testFinishChunkCountsOnce},
		{"CreateDocumentCounters", testCreateDocumentCounters},
		{"ListDocumentsPages", testListDocumentsPages},
		{"CompleteDocument", testCompleteDocument},
		{"CompleteFromFailed", testCompleteFromFailed},
		{"FailDocument", testFailDocument},
		{"ConcurrentCompleteCountsOnce", testConcurrentCompleteCountsOnce},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) { tt.fn(t, newStore(t)) })
	}
}

func newBatch(t *testing.T, s batch.Store) batch.Batch {
	t.Helper()
	b := batch.Batch{
		ID:               uuid.NewString(),
		SourceKey:        "uploads/x/upload.zip",
		OriginalFilename: "forms.zip",
		Total:            30,
		ChunkCount:       2,
	}
	if err := s.CreateBatch(context.Background(), b); err != nil {
		t.Fatalf("CreateBatch: %v", err)
	}
	return b
}

func pendingDoc(batchID string, seq int) batch.Document {
	return batch.Document{
		BatchID:     batchID,
		Seq:         seq,
		Filename:    fmt.Sprintf("forms/form-%d.pdf", seq),
		Key:         fmt.Sprintf("documents/%s/%06d-form-%d.pdf", batchID, seq, seq),
		Size:        1234,
		ContentType: "application/pdf",
		Status:      batch.StatusPending,
	}
}

func mustCreateDoc(t *testing.T, s batch.Store, d batch.Document) {
	t.Helper()
	if err := s.CreateDocument(context.Background(), d); err != nil {
		t.Fatalf("CreateDocument(seq %d): %v", d.Seq, err)
	}
}

func getBatch(t *testing.T, s batch.Store, id string) batch.Batch {
	t.Helper()
	b, err := s.GetBatch(context.Background(), id)
	if err != nil {
		t.Fatalf("GetBatch: %v", err)
	}
	return b
}

func wantCounts(t *testing.T, b batch.Batch, processed, failed, skipped int) {
	t.Helper()
	if b.Processed != processed || b.Failed != failed || b.Skipped != skipped {
		t.Errorf("counts processed/failed/skipped = %d/%d/%d, want %d/%d/%d",
			b.Processed, b.Failed, b.Skipped, processed, failed, skipped)
	}
}

func testBatchRoundTrip(t *testing.T, s batch.Store) {
	ctx := context.Background()
	b := newBatch(t, s)

	got := getBatch(t, s, b.ID)
	if got.SourceKey != b.SourceKey || got.OriginalFilename != b.OriginalFilename ||
		got.Total != 30 || got.ChunkCount != 2 || got.ChunksDone != 0 {
		t.Errorf("got %+v", got)
	}
	if got.CreatedAt.IsZero() || got.UpdatedAt.IsZero() {
		t.Error("timestamps not set")
	}
	failed := batch.Batch{ID: uuid.NewString(), Error: "zip: not a valid zip file"}
	if err := s.CreateBatch(ctx, failed); err != nil {
		t.Fatal(err)
	}
	if got := getBatch(t, s, failed.ID); got.Error != failed.Error {
		t.Errorf("Error = %q, want %q", got.Error, failed.Error)
	}
	if err := s.CreateBatch(ctx, b); !errors.Is(err, batch.ErrExists) {
		t.Errorf("duplicate CreateBatch: got %v, want ErrExists", err)
	}
	if _, err := s.GetBatch(ctx, uuid.NewString()); !errors.Is(err, batch.ErrNotFound) {
		t.Errorf("unknown batch: got %v, want ErrNotFound", err)
	}
}

func testFinishChunkCountsOnce(t *testing.T, s batch.Store) {
	ctx := context.Background()
	b := newBatch(t, s)
	for _, chunk := range []int{1, 1, 2, 1} { // chunk 1 retried twice
		if err := s.FinishChunk(ctx, b.ID, chunk); err != nil {
			t.Fatalf("FinishChunk(%d): %v", chunk, err)
		}
	}
	if got := getBatch(t, s, b.ID).ChunksDone; got != 2 {
		t.Errorf("ChunksDone = %d, want 2", got)
	}
	if err := s.FinishChunk(ctx, uuid.NewString(), 1); !errors.Is(err, batch.ErrNotFound) {
		t.Errorf("unknown batch: got %v, want ErrNotFound", err)
	}
}

func testCreateDocumentCounters(t *testing.T, s batch.Store) {
	ctx := context.Background()
	b := newBatch(t, s)

	mustCreateDoc(t, s, pendingDoc(b.ID, 1))
	skipped := batch.Document{BatchID: b.ID, Seq: 2, Filename: "notes.txt", Status: batch.StatusSkipped, Reason: "not a PDF or image"}
	mustCreateDoc(t, s, skipped)
	failed := batch.Document{BatchID: b.ID, Seq: 3, Filename: "bad.pdf", Status: batch.StatusFailed, Reason: "corrupt entry"}
	mustCreateDoc(t, s, failed)
	wantCounts(t, getBatch(t, s, b.ID), 0, 1, 1)

	// A retried unzip chunk recreates the same documents: nothing changes.
	for _, d := range []batch.Document{pendingDoc(b.ID, 1), skipped, failed} {
		if err := s.CreateDocument(ctx, d); !errors.Is(err, batch.ErrExists) {
			t.Errorf("duplicate seq %d: got %v, want ErrExists", d.Seq, err)
		}
	}
	wantCounts(t, getBatch(t, s, b.ID), 0, 1, 1)

	got, err := s.GetDocument(ctx, b.ID, 2)
	if err != nil {
		t.Fatal(err)
	}
	if got.Status != batch.StatusSkipped || got.Reason != "not a PDF or image" || got.Filename != "notes.txt" {
		t.Errorf("got %+v", got)
	}
	if _, err := s.GetDocument(ctx, b.ID, 99); !errors.Is(err, batch.ErrNotFound) {
		t.Errorf("unknown document: got %v, want ErrNotFound", err)
	}
	if err := s.CreateDocument(ctx, pendingDoc(uuid.NewString(), 1)); !errors.Is(err, batch.ErrNotFound) {
		t.Errorf("document for unknown batch: got %v, want ErrNotFound", err)
	}
}

func testListDocumentsPages(t *testing.T, s batch.Store) {
	ctx := context.Background()
	b := newBatch(t, s)
	other := newBatch(t, s)
	// Created out of order, and past 9 so string-vs-number ordering would show.
	for _, seq := range []int{12, 3, 1, 10, 2, 11} {
		mustCreateDoc(t, s, pendingDoc(b.ID, seq))
	}
	mustCreateDoc(t, s, pendingDoc(other.ID, 4))

	var seqs []int
	after := 0
	for range 10 {
		page, next, err := s.ListDocuments(ctx, b.ID, after, 4)
		if err != nil {
			t.Fatal(err)
		}
		for _, d := range page {
			seqs = append(seqs, d.Seq)
		}
		if next == 0 {
			break
		}
		after = next
	}
	if fmt.Sprint(seqs) != "[1 2 3 10 11 12]" {
		t.Errorf("listed seqs %v, want [1 2 3 10 11 12]", seqs)
	}

	page, next, err := s.ListDocuments(ctx, uuid.NewString(), 0, 10)
	if err != nil || len(page) != 0 || next != 0 {
		t.Errorf("unknown batch: got %d docs, next %d, err %v; want empty", len(page), next, err)
	}
}

func extraction() batch.Extraction {
	return batch.Extraction{
		Fields: map[string]batch.Field{
			"surname":       {Value: "Mutuku", Confidence: 97.5},
			"mobile_number": {Value: "0712 345 678", Confidence: 88},
		},
		RawKey: "textract/b/000001.json",
		At:     time.Date(2026, 10, 17, 9, 30, 0, 0, time.UTC),
	}
}

func testCompleteDocument(t *testing.T, s batch.Store) {
	ctx := context.Background()
	b := newBatch(t, s)
	mustCreateDoc(t, s, pendingDoc(b.ID, 1))

	if err := s.CompleteDocument(ctx, b.ID, 1, batch.StatusPending, extraction()); err != nil {
		t.Fatalf("CompleteDocument: %v", err)
	}
	wantCounts(t, getBatch(t, s, b.ID), 1, 0, 0)

	got, err := s.GetDocument(ctx, b.ID, 1)
	if err != nil {
		t.Fatal(err)
	}
	if got.Status != batch.StatusExtracted || got.RawKey != "textract/b/000001.json" ||
		got.Fields["surname"] != (batch.Field{Value: "Mutuku", Confidence: 97.5}) ||
		got.Fields["mobile_number"] != (batch.Field{Value: "0712 345 678", Confidence: 88}) ||
		!got.ExtractedAt.Equal(extraction().At) {
		t.Errorf("got %+v", got)
	}

	// A redelivered message for the same document must not count it again.
	if err := s.CompleteDocument(ctx, b.ID, 1, batch.StatusPending, extraction()); !errors.Is(err, batch.ErrConflict) {
		t.Errorf("second complete: got %v, want ErrConflict", err)
	}
	wantCounts(t, getBatch(t, s, b.ID), 1, 0, 0)

	if err := s.CompleteDocument(ctx, b.ID, 99, batch.StatusPending, extraction()); err == nil {
		t.Error("completing an unknown document should fail")
	}
}

func testCompleteFromFailed(t *testing.T, s batch.Store) {
	ctx := context.Background()
	b := newBatch(t, s)
	mustCreateDoc(t, s, pendingDoc(b.ID, 1))
	if err := s.FailDocument(ctx, b.ID, 1, "Textract throttled"); err != nil {
		t.Fatal(err)
	}
	wantCounts(t, getBatch(t, s, b.ID), 0, 1, 0)

	// Replayed from the dead-letter queue and succeeded: moves from failed to processed.
	if err := s.CompleteDocument(ctx, b.ID, 1, batch.StatusFailed, extraction()); err != nil {
		t.Fatalf("CompleteDocument from failed: %v", err)
	}
	wantCounts(t, getBatch(t, s, b.ID), 1, 0, 0)
	if got, _ := s.GetDocument(ctx, b.ID, 1); got.Reason != "" {
		t.Errorf("reason %q should be cleared", got.Reason)
	}
}

func testFailDocument(t *testing.T, s batch.Store) {
	ctx := context.Background()
	b := newBatch(t, s)
	mustCreateDoc(t, s, pendingDoc(b.ID, 1))

	if err := s.FailDocument(ctx, b.ID, 1, "unsupported document"); err != nil {
		t.Fatalf("FailDocument: %v", err)
	}
	got, _ := s.GetDocument(ctx, b.ID, 1)
	if got.Status != batch.StatusFailed || got.Reason != "unsupported document" {
		t.Errorf("got %+v", got)
	}
	if err := s.FailDocument(ctx, b.ID, 1, "again"); !errors.Is(err, batch.ErrConflict) {
		t.Errorf("second fail: got %v, want ErrConflict", err)
	}
	// A pending-to-extracted write that lost the race also conflicts.
	if err := s.CompleteDocument(ctx, b.ID, 1, batch.StatusPending, extraction()); !errors.Is(err, batch.ErrConflict) {
		t.Errorf("complete after fail: got %v, want ErrConflict", err)
	}
	wantCounts(t, getBatch(t, s, b.ID), 0, 1, 0)
}

// Simulates SQS delivering the same message to many workers at once.
func testConcurrentCompleteCountsOnce(t *testing.T, s batch.Store) {
	ctx := context.Background()
	b := newBatch(t, s)
	mustCreateDoc(t, s, pendingDoc(b.ID, 1))

	const workers = 10
	var wg sync.WaitGroup
	errs := make(chan error, workers)
	for range workers {
		wg.Go(func() {
			errs <- s.CompleteDocument(ctx, b.ID, 1, batch.StatusPending, extraction())
		})
	}
	wg.Wait()
	close(errs)

	wins := 0
	for err := range errs {
		switch {
		case err == nil:
			wins++
		case errors.Is(err, batch.ErrConflict):
		default:
			t.Errorf("unexpected error: %v", err)
		}
	}
	if wins != 1 {
		t.Errorf("%d workers succeeded, want exactly 1", wins)
	}
	wantCounts(t, getBatch(t, s, b.ID), 1, 0, 0)
}
