package extract_test

import (
	"archive/zip"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/daniel-mutuku/textract-bulk-onboarding/internal/batch"
	"github.com/daniel-mutuku/textract-bulk-onboarding/internal/batch/memstore"
	"github.com/daniel-mutuku/textract-bulk-onboarding/internal/blob/memblob"
	"github.com/daniel-mutuku/textract-bulk-onboarding/internal/extract"
	"github.com/daniel-mutuku/textract-bulk-onboarding/internal/extract/faketextract"
	"github.com/daniel-mutuku/textract-bulk-onboarding/internal/job"
	"github.com/daniel-mutuku/textract-bulk-onboarding/internal/queue/memqueue"
	"github.com/daniel-mutuku/textract-bulk-onboarding/internal/unzip"
)

const (
	batchID   = "6f1c2b9e-8d3a-4c5e-9f70-1a2b3c4d5e6f"
	docBucket = "documents-bucket"
	docKey    = "documents/" + batchID + "/000001-form.jpeg"
)

var (
	now    = time.Date(2026, 10, 17, 9, 0, 0, 0, time.UTC)
	answer = faketextract.Response(map[string][]faketextract.Answer{"full_name": {{Text: "John Doe", Confidence: 98}}})
)

type env struct {
	store    *memstore.Store
	blobs    *memblob.Store
	textract *faketextract.Analyzer
	worker   *extract.Worker
}

// newEnv sets up one batch with one pending document, and a Textract fake
// that answers with respond.
func newEnv(t *testing.T, respond func(call int) ([]byte, error)) *env {
	t.Helper()
	ctx := context.Background()
	e := &env{store: memstore.New(), blobs: memblob.New()}
	e.textract = &faketextract.Analyzer{Respond: func(_, _ string, call int) ([]byte, error) { return respond(call) }}
	e.worker = &extract.Worker{
		Store: e.store, Blobs: e.blobs, Textract: e.textract,
		Queries: extract.Queries, MaxAttempts: 5, Now: func() time.Time { return now },
	}
	if err := e.store.CreateBatch(ctx, batch.Batch{ID: batchID, Total: 1, ChunkCount: 1}); err != nil {
		t.Fatal(err)
	}
	if err := e.store.CreateDocument(ctx, batch.Document{BatchID: batchID, Seq: 1, Key: docKey, Status: batch.StatusPending}); err != nil {
		t.Fatal(err)
	}
	if err := e.store.FinishChunk(ctx, batchID, 1); err != nil {
		t.Fatal(err)
	}
	return e
}

func message(receiveCount int) extract.Message {
	body, _ := json.Marshal(job.Document{V: job.Version, BatchID: batchID, Seq: 1, Bucket: docBucket, Key: docKey})
	return extract.Message{Body: body, ReceiveCount: receiveCount}
}

func (e *env) doc(t *testing.T) batch.Document {
	t.Helper()
	d, err := e.store.GetDocument(context.Background(), batchID, 1)
	if err != nil {
		t.Fatal(err)
	}
	return d
}

func (e *env) batch(t *testing.T) batch.Batch {
	t.Helper()
	b, err := e.store.GetBatch(context.Background(), batchID)
	if err != nil {
		t.Fatal(err)
	}
	return b
}

func TestExtractsAndCounts(t *testing.T) {
	e := newEnv(t, func(int) ([]byte, error) { return answer, nil })

	if out := e.worker.Handle(context.Background(), message(1)); out.Retry {
		t.Fatalf("got %+v, want done", out)
	}
	d := e.doc(t)
	if d.Status != batch.StatusExtracted || d.Fields["full_name"] != (batch.Field{Value: "John Doe", Confidence: 98}) ||
		d.RawKey != extract.RawKey(batchID, 1) || !d.ExtractedAt.Equal(now) {
		t.Errorf("document %+v", d)
	}
	if raw, ct, ok := e.blobs.Get(docBucket, extract.RawKey(batchID, 1)); !ok || !bytes.Equal(raw, answer) || ct != "application/json" {
		t.Errorf("raw response saved=%v type=%q", ok, ct)
	}
	if b := e.batch(t); b.Processed != 1 || b.State() != batch.StateCompleted {
		t.Errorf("batch %+v (state %s)", b, b.State())
	}
}

func TestDuplicateMessageSkipsTextract(t *testing.T) {
	e := newEnv(t, func(int) ([]byte, error) { return answer, nil })
	ctx := context.Background()
	for range 3 { // SQS delivers the same message three times
		if out := e.worker.Handle(ctx, message(1)); out.Retry {
			t.Fatalf("got %+v", out)
		}
	}
	if n := e.textract.Calls(docKey); n != 1 {
		t.Errorf("Textract called %d times, want 1", n)
	}
	if b := e.batch(t); b.Processed != 1 {
		t.Errorf("processed = %d, want 1", b.Processed)
	}
}

func TestRetryReusesSavedResponse(t *testing.T) {
	e := newEnv(t, func(int) ([]byte, error) { return answer, nil })
	// An earlier attempt got as far as saving the raw response, then died.
	if err := e.blobs.Put(context.Background(), docBucket, extract.RawKey(batchID, 1), answer, "application/json"); err != nil {
		t.Fatal(err)
	}
	if out := e.worker.Handle(context.Background(), message(2)); out.Retry {
		t.Fatalf("got %+v", out)
	}
	if n := e.textract.Calls(docKey); n != 0 {
		t.Errorf("Textract called %d times; the saved response should have been reused", n)
	}
	if d := e.doc(t); d.Status != batch.StatusExtracted || d.Fields["full_name"].Value != "John Doe" {
		t.Errorf("document %+v", d)
	}
}

func TestTransientErrorBacksOff(t *testing.T) {
	throttled := errors.New("ThrottlingException: Rate exceeded")
	e := newEnv(t, func(int) ([]byte, error) { return nil, throttled })

	for attempt := 1; attempt <= 4; attempt++ {
		out := e.worker.Handle(context.Background(), message(attempt))
		if !out.Retry {
			t.Fatalf("attempt %d: got %+v, want retry", attempt, out)
		}
		// Equal jitter: the delay is between half and all of 30s * 2^(attempt-1).
		full := 30 * time.Second << (attempt - 1)
		if out.Delay < full/2 || out.Delay > full {
			t.Errorf("attempt %d: delay %s outside [%s, %s]", attempt, out.Delay, full/2, full)
		}
	}
	if d := e.doc(t); d.Status != batch.StatusPending {
		t.Errorf("status %s, want still pending while retrying", d.Status)
	}
}

func TestLastAttemptFailsDocumentAndSendsToDLQ(t *testing.T) {
	e := newEnv(t, func(int) ([]byte, error) { return nil, errors.New("InternalServerError") })

	out := e.worker.Handle(context.Background(), message(5)) // MaxAttempts
	if !out.Retry || out.Delay != 0 {
		t.Errorf("got %+v, want retry with no delay so SQS moves it to the DLQ", out)
	}
	d := e.doc(t)
	if d.Status != batch.StatusFailed || !strings.Contains(d.Reason, "gave up after 5 attempts") {
		t.Errorf("document %+v", d)
	}
	if b := e.batch(t); b.Failed != 1 || b.State() != batch.StateCompleted {
		t.Errorf("batch %+v: a document in the DLQ must not leave the batch stuck", b)
	}
}

func TestPermanentErrorFailsWithoutRetry(t *testing.T) {
	e := newEnv(t, func(int) ([]byte, error) {
		return nil, extract.Permanent(errors.New("UnsupportedDocumentException: multi-page PDF"))
	})
	if out := e.worker.Handle(context.Background(), message(1)); out.Retry {
		t.Errorf("got %+v, want done", out)
	}
	if d := e.doc(t); d.Status != batch.StatusFailed || !strings.Contains(d.Reason, "UnsupportedDocument") {
		t.Errorf("document %+v", d)
	}
}

func TestReplayFromDLQSucceeds(t *testing.T) {
	calls := 0
	e := newEnv(t, func(int) ([]byte, error) {
		calls++
		if calls == 1 {
			return nil, errors.New("InternalServerError")
		}
		return answer, nil
	})
	ctx := context.Background()
	e.worker.Handle(ctx, message(5)) // gives up; document failed, message to DLQ
	// Later the DLQ is redriven to the source queue: a fresh receive count.
	if out := e.worker.Handle(ctx, message(1)); out.Retry {
		t.Fatalf("replay: got %+v", out)
	}
	if b := e.batch(t); b.Processed != 1 || b.Failed != 0 {
		t.Errorf("batch %+v, want the failure moved to processed", b)
	}
}

func TestMalformedAndOrphanMessagesAreDropped(t *testing.T) {
	e := newEnv(t, func(int) ([]byte, error) { return answer, nil })
	ctx := context.Background()
	if out := e.worker.Handle(ctx, extract.Message{Body: []byte("{not json"), ReceiveCount: 1}); out.Retry {
		t.Error("malformed message should be dropped")
	}
	orphan, _ := json.Marshal(job.Document{V: 1, BatchID: batchID, Seq: 99, Bucket: docBucket, Key: "x"})
	if out := e.worker.Handle(ctx, extract.Message{Body: orphan, ReceiveCount: 1}); out.Retry {
		t.Error("message for an unknown document should be dropped")
	}
}

// The full local loop: unzip the sample forms, then drain the queue through
// the worker with Textract replaying the real responses for each form.
func TestQueueExtractStoreIncrement(t *testing.T) {
	forms, _ := filepath.Glob("../../testdata/forms/*.jpeg")
	if len(forms) == 0 {
		t.Skip("no sample forms")
	}
	ctx := context.Background()
	store, blobs, chunks, docs := memstore.New(), memblob.New(), memqueue.New(), memqueue.New()

	// Zip the forms and "upload" the zip.
	var buf bytes.Buffer
	zw := zip.NewWriter(&buf)
	for _, f := range forms {
		data, _ := os.ReadFile(f)
		w, _ := zw.Create("scans/" + filepath.Base(f))
		w.Write(data)
	}
	zw.Close()
	key := "uploads/" + batchID + "/upload.zip"
	blobs.Add("uploads", key, bytes.NewReader(buf.Bytes()), int64(buf.Len()), nil)

	lim := unzip.DefaultLimits()
	lim.MaxChunkEntries = 2
	if err := (&unzip.Planner{Blobs: blobs, Store: store, Chunks: chunks, Limits: lim}).Plan(ctx, "uploads", key); err != nil {
		t.Fatal(err)
	}
	chunker := &unzip.Chunker{Blobs: blobs, Store: store, Documents: docs, DocumentsBucket: docBucket, Limits: lim}
	for _, body := range chunks.Drain() {
		if err := chunker.HandleMessage(ctx, body); err != nil {
			t.Fatal(err)
		}
	}

	// The fake answers each document with the real response for that form
	// (documents/{batch}/00000N-N.jpeg -> testdata/textract/N.json), and
	// throttles every first attempt to exercise the retry path.
	textract := &faketextract.Analyzer{Respond: func(_, key string, call int) ([]byte, error) {
		if call == 1 {
			return nil, errors.New("ProvisionedThroughputExceededException")
		}
		form := strings.TrimSuffix(key[strings.LastIndex(key, "-")+1:], ".jpeg")
		return os.ReadFile("../../testdata/textract/" + form + ".json")
	}}
	worker := &extract.Worker{Store: store, Blobs: blobs, Textract: textract, Queries: extract.Queries, MaxAttempts: 5, Now: time.Now}

	type delivery struct {
		body  []byte
		count int
	}
	var pending []delivery
	for _, body := range docs.Drain() {
		pending = append(pending, delivery{body, 1})
	}
	for len(pending) > 0 {
		d := pending[0]
		pending = pending[1:]
		if out := worker.Handle(ctx, extract.Message{Body: d.body, ReceiveCount: d.count}); out.Retry {
			pending = append(pending, delivery{d.body, d.count + 1}) // SQS redelivers after the delay
		}
	}

	b, _ := store.GetBatch(ctx, batchID)
	if b.State() != batch.StateCompleted || b.Processed != len(forms) || b.Failed != 0 {
		t.Fatalf("batch %+v (state %s)", b, b.State())
	}
	page, _, _ := store.ListDocuments(ctx, batchID, 0, 10)
	for _, d := range page {
		form := strings.TrimSuffix(filepath.Base(d.Filename), ".jpeg")
		if got, want := d.Fields["full_name"].Value, wantForms[form]["full_name"]; got != want {
			t.Errorf("%s: full_name %q, want %q", d.Filename, got, want)
		}
	}
}
