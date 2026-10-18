package unzip

import (
	"archive/zip"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"path"
	"strings"

	"github.com/daniel-mutuku/textract-bulk-onboarding/internal/batch"
	"github.com/daniel-mutuku/textract-bulk-onboarding/internal/blob"
	"github.com/daniel-mutuku/textract-bulk-onboarding/internal/job"
	"github.com/daniel-mutuku/textract-bulk-onboarding/internal/queue"
)

type Chunker struct {
	Blobs           blob.Store
	Store           batch.Store
	Documents       queue.Sender // the extraction queue
	DocumentsBucket string
	Limits          Limits
}

// HandleMessage processes one chunk message body.
func (c *Chunker) HandleMessage(ctx context.Context, body []byte) error {
	var j job.Chunk
	if err := json.Unmarshal(body, &j); err != nil {
		return fmt.Errorf("bad chunk message: %w", err)
	}
	return c.Process(ctx, j)
}

// Process unpacks documents j.FirstSeq..j.LastSeq. For each entry it either
// records a skip, or writes the document to S3, records it as pending, and
// enqueues it for extraction, in that order: a record only exists once its
// file does, and a message only once its record does.
//
// Running it twice is safe. Documents that already exist are not rewritten;
// pending ones are enqueued again in case the earlier run died before
// sending, and the extract worker ignores duplicates. Counters only move in
// CreateDocument and FinishChunk, which count each document and chunk once.
func (c *Chunker) Process(ctx context.Context, j job.Chunk) error {
	obj, err := c.Blobs.Open(ctx, j.Bucket, j.Key)
	if err != nil {
		return err
	}
	zr, err := zip.NewReader(obj, obj.Size)
	if err != nil {
		return fmt.Errorf("open zip: %w", err) // the planner read it fine, so treat as transient
	}
	files := documentEntries(zr)
	if j.FirstSeq < 1 || j.LastSeq > len(files) || j.FirstSeq > j.LastSeq {
		return fmt.Errorf("chunk %d range %d-%d is outside the zip's %d documents", j.Chunk, j.FirstSeq, j.LastSeq, len(files))
	}

	var pending [][]byte
	flush := func() error {
		if len(pending) == 0 {
			return nil
		}
		err := c.Documents.Send(ctx, pending...)
		pending = pending[:0]
		return err
	}

	for seq := j.FirstSeq; seq <= j.LastSeq; seq++ {
		enqueue, err := c.processEntry(ctx, j, seq, files[seq-1])
		if err != nil {
			return fmt.Errorf("seq %d (%s): %w", seq, files[seq-1].Name, err)
		}
		if enqueue != nil {
			pending = append(pending, enqueue)
		}
		if len(pending) == 10 { // one SQS SendMessageBatch
			if err := flush(); err != nil {
				return err
			}
		}
	}
	if err := flush(); err != nil {
		return err
	}
	if err := c.Store.FinishChunk(ctx, j.BatchID, j.Chunk); err != nil {
		return err
	}
	slog.Info("unpacked chunk", "batch", j.BatchID, "chunk", j.Chunk, "first", j.FirstSeq, "last", j.LastSeq)
	return nil
}

// processEntry handles one entry and returns the extraction message to send,
// or nil if the entry is not (or no longer) pending.
func (c *Chunker) processEntry(ctx context.Context, j job.Chunk, seq int, f *zip.File) ([]byte, error) {
	existing, err := c.Store.GetDocument(ctx, j.BatchID, seq)
	switch {
	case err == nil:
		if existing.Status == batch.StatusPending {
			return c.message(existing), nil
		}
		return nil, nil
	case !errors.Is(err, batch.ErrNotFound):
		return nil, err
	}

	doc, data := c.read(j.BatchID, seq, f)
	if doc.Status == batch.StatusPending {
		if err := c.Blobs.Put(ctx, c.DocumentsBucket, doc.Key, data, doc.ContentType); err != nil {
			return nil, err
		}
	}
	err = c.Store.CreateDocument(ctx, doc)
	if err != nil && !errors.Is(err, batch.ErrExists) { // exists: a concurrent duplicate got there first
		return nil, err
	}
	if doc.Status != batch.StatusPending {
		return nil, nil
	}
	return c.message(doc), nil
}

// read classifies an entry and, for a real document, returns its bytes.
func (c *Chunker) read(batchID string, seq int, f *zip.File) (batch.Document, []byte) {
	doc := batch.Document{
		BatchID:  batchID,
		Seq:      seq,
		Filename: f.Name,
		Size:     int64(f.UncompressedSize64),
	}
	skip := func(reason string) (batch.Document, []byte) {
		doc.Status, doc.Reason = batch.StatusSkipped, reason
		return doc, nil
	}
	fail := func(reason string) (batch.Document, []byte) {
		doc.Status, doc.Reason = batch.StatusFailed, reason
		return doc, nil
	}

	if reason := skipReason(f, c.Limits.MaxDocumentBytes); reason != "" {
		return skip(reason)
	}
	rc, err := f.Open()
	if err != nil {
		return fail("cannot read entry: " + err.Error()) // e.g. an unsupported compression method
	}
	defer rc.Close()
	// Never trust the size in the header: read at most one byte past the limit.
	data, err := io.ReadAll(io.LimitReader(rc, c.Limits.MaxDocumentBytes+1))
	if err != nil {
		// archive/zip verifies each entry's CRC-32 at EOF, so a damaged entry fails here.
		return fail("corrupt entry: " + err.Error())
	}
	if int64(len(data)) > c.Limits.MaxDocumentBytes {
		return skip(fmt.Sprintf("larger than its header claims and over the %d MB limit", c.Limits.MaxDocumentBytes>>20))
	}
	contentType, ok := sniff(f.Name, data)
	if !ok {
		ext := strings.ToUpper(strings.TrimPrefix(path.Ext(f.Name), "."))
		return skip(fmt.Sprintf("content is not a valid %s file", ext))
	}
	doc.Status = batch.StatusPending
	doc.Key = documentKey(batchID, seq, f.Name)
	doc.ContentType = contentType
	doc.Size = int64(len(data))
	return doc, data
}

func (c *Chunker) message(d batch.Document) []byte {
	b, _ := json.Marshal(job.Document{
		V:       job.Version,
		BatchID: d.BatchID,
		Seq:     d.Seq,
		Bucket:  c.DocumentsBucket,
		Key:     d.Key,
	})
	return b
}
