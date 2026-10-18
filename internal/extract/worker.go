package extract

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"math/rand/v2"
	"time"

	"github.com/daniel-mutuku/textract-bulk-onboarding/internal/batch"
	"github.com/daniel-mutuku/textract-bulk-onboarding/internal/blob"
	"github.com/daniel-mutuku/textract-bulk-onboarding/internal/job"
)

// Message is one SQS message as the worker needs it.
type Message struct {
	Body []byte
	// ReceiveCount is SQS's ApproximateReceiveCount: 1 on first delivery, +1
	// each time the message comes back. It drives backoff and the last-attempt check.
	ReceiveCount int
}

// Outcome tells the caller (the Lambda handler, or cmd/dev) what to do with
// the message. Retry false: delete it. Retry true: leave it on the queue and
// make it visible again after Delay.
type Outcome struct {
	Retry bool
	Delay time.Duration
}

var done = Outcome{}

const (
	backoffBase = 30 * time.Second
	backoffCap  = 15 * time.Minute
)

type Worker struct {
	Store    batch.Store
	Blobs    blob.Store
	Textract Analyzer
	Queries  []Query
	// MaxAttempts must equal the queue's maxReceiveCount: on that attempt the
	// worker records the document as failed before SQS moves the message to
	// the dead-letter queue, so the batch can still finish.
	MaxAttempts int
	Now         func() time.Time
}

// RawKey is where a document's raw Textract response is saved.
func RawKey(batchID string, seq int) string {
	return fmt.Sprintf("textract/%s/%06d.json", batchID, seq)
}

// Handle processes one document message.
//
// Exactly-once, in three layers:
//  1. A document that is already extracted (or skipped) is acknowledged
//     without calling Textract: the common duplicate costs one read.
//  2. The raw response is saved to S3 before the record is updated. If a
//     retry finds it there, it is re-mapped instead of paying Textract again.
//  3. CompleteDocument only succeeds if the status is still what step 1 saw,
//     and moves the batch counter in the same atomic write. If two workers race
//     past step 1, one wins and the other gets ErrConflict: counted once.
func (w *Worker) Handle(ctx context.Context, m Message) Outcome {
	var j job.Document
	if err := json.Unmarshal(m.Body, &j); err != nil || j.BatchID == "" {
		// Retrying cannot fix a malformed message.
		slog.Error("dropping malformed document message", "body", string(m.Body), "err", err)
		return done
	}
	log := slog.With("batch", j.BatchID, "seq", j.Seq, "attempt", m.ReceiveCount)

	doc, err := w.Store.GetDocument(ctx, j.BatchID, j.Seq)
	if errors.Is(err, batch.ErrNotFound) {
		log.Warn("dropping message for unknown document")
		return done
	}
	if err != nil {
		log.Error("load document", "err", err)
		return w.retry(m)
	}
	switch doc.Status {
	case batch.StatusPending, batch.StatusFailed: // failed: replayed from the dead-letter queue
	default:
		log.Info("already done, skipping duplicate", "status", doc.Status)
		return done
	}
	from := doc.Status

	rawKey := RawKey(j.BatchID, j.Seq)
	raw, reused, err := w.analyze(ctx, j, rawKey)
	if err != nil {
		return w.failure(ctx, log, j, m, from, err)
	}
	fields, err := MapFields(raw)
	if err != nil {
		return w.failure(ctx, log, j, m, from, Permanent(err))
	}
	if !reused {
		if err := w.Blobs.Put(ctx, j.Bucket, rawKey, raw, "application/json"); err != nil {
			log.Error("save raw response", "err", err)
			return w.retry(m)
		}
	}

	err = w.Store.CompleteDocument(ctx, j.BatchID, j.Seq, from, batch.Extraction{
		Fields: fields,
		RawKey: rawKey,
		At:     w.Now(),
	})
	switch {
	case errors.Is(err, batch.ErrConflict):
		log.Info("another worker finished this document first")
		return done
	case err != nil:
		log.Error("record extraction", "err", err)
		return w.retry(m)
	}
	log.Info("extracted", "fields", len(fields), "reusedRaw", reused)
	return done
}

// analyze returns the Textract response, reusing one saved by an earlier attempt.
func (w *Worker) analyze(ctx context.Context, j job.Document, rawKey string) ([]byte, bool, error) {
	obj, err := w.Blobs.Open(ctx, j.Bucket, rawKey)
	if err == nil {
		raw, err := io.ReadAll(io.NewSectionReader(obj, 0, obj.Size))
		return raw, true, err
	}
	if !errors.Is(err, blob.ErrNotFound) {
		return nil, false, err
	}
	raw, err := w.Textract.Analyze(ctx, j.Bucket, j.Key, w.Queries)
	return raw, false, err
}

// failure decides between retrying and giving up.
func (w *Worker) failure(ctx context.Context, log *slog.Logger, j job.Document, m Message, from batch.Status, cause error) Outcome {
	permanent := isPermanent(cause)
	last := m.ReceiveCount >= w.MaxAttempts
	if !permanent && !last {
		out := w.retry(m)
		log.Warn("extraction failed, will retry", "err", cause, "in", out.Delay.Round(time.Second))
		return out
	}

	reason := cause.Error()
	if !permanent {
		reason = fmt.Sprintf("gave up after %d attempts: %s", m.ReceiveCount, reason)
	}
	if from == batch.StatusPending {
		err := w.Store.FailDocument(ctx, j.BatchID, j.Seq, reason)
		if err != nil && !errors.Is(err, batch.ErrConflict) {
			log.Error("record failure", "err", err)
			return w.retry(m)
		}
	}
	if permanent {
		log.Warn("document cannot be extracted", "reason", reason)
		return done // recorded with its reason; nothing left to retry
	}
	// Out of attempts: make it visible at once so SQS moves it to the DLQ,
	// where it waits for inspection and replay.
	log.Error("giving up, sending to dead-letter queue", "reason", reason)
	return Outcome{Retry: true}
}

// retry backs off exponentially by attempt: ~30s, 1m, 2m, 4m… capped at 15m.
// Half the delay is random ("equal jitter"), so documents that failed together,
// say during a Textract throttling spike, don't all come back together.
func (w *Worker) retry(m Message) Outcome {
	d := min(backoffCap, backoffBase<<min(max(m.ReceiveCount-1, 0), 10))
	return Outcome{Retry: true, Delay: d/2 + rand.N(d/2+1)}
}
