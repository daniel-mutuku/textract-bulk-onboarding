// Package unzip turns one uploaded zip into many independent per-document
// jobs, in two stages so no single Lambda invocation has to process the
// whole zip within the 15-minute limit:
//
//   - Planner (unzip-plan Lambda) reads only the zip's central directory,
//     creates the batch, and splits the entries into chunks.
//   - Chunker (unzip-chunk Lambda, many in parallel) unpacks one chunk:
//     filters entries, writes documents to S3, records them, and enqueues them.
package unzip

import (
	"archive/zip"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"

	"github.com/daniel-mutuku/textract-bulk-onboarding/internal/batch"
	"github.com/daniel-mutuku/textract-bulk-onboarding/internal/blob"
	"github.com/daniel-mutuku/textract-bulk-onboarding/internal/job"
	"github.com/daniel-mutuku/textract-bulk-onboarding/internal/queue"
)

// Limits shape how work is split. The defaults keep a chunk to at most 250
// documents and 1 GiB of compressed bytes to read, a few minutes of work, far
// inside Lambda's 15-minute ceiling whatever the zip's total size.
type Limits struct {
	MaxChunkEntries  int
	MaxChunkBytes    int64
	MaxDocumentBytes int64 // synchronous Textract accepts up to 10 MB from S3
}

func DefaultLimits() Limits {
	return Limits{
		MaxChunkEntries:  250,
		MaxChunkBytes:    1 << 30,
		MaxDocumentBytes: 10 << 20,
	}
}

type Planner struct {
	Blobs  blob.Store
	Store  batch.Store
	Chunks queue.Sender
	Limits Limits
}

// HandleS3Event plans every new zip named in one S3 event notification.
func (p *Planner) HandleS3Event(ctx context.Context, body []byte) error {
	refs, err := ParseS3Event(body)
	if err != nil {
		return err
	}
	for _, ref := range refs {
		if _, ok := BatchIDFromKey(ref.Key); !ok {
			slog.Warn("ignoring object that is not an upload", "bucket", ref.Bucket, "key", ref.Key)
			continue
		}
		if err := p.Plan(ctx, ref.Bucket, ref.Key); err != nil {
			return fmt.Errorf("plan %s: %w", ref.Key, err)
		}
	}
	return nil
}

// Plan creates the batch for one uploaded zip and enqueues its chunks.
//
// It is safe to run twice on the same zip (SQS delivers at least once): the
// batch already existing is fine, and the chunk messages it sends again are
// handled idempotently by the Chunker.
func (p *Planner) Plan(ctx context.Context, bucket, key string) error {
	batchID, ok := BatchIDFromKey(key)
	if !ok {
		return fmt.Errorf("key %q is not an upload", key)
	}
	obj, err := p.Blobs.Open(ctx, bucket, key)
	if err != nil {
		return err // transient (or the object is gone); let SQS retry
	}
	b := batch.Batch{
		ID:               batchID,
		SourceKey:        key,
		OriginalFilename: obj.Metadata["original-filename"],
	}

	// Reads the central directory: on S3 that is a few ranged GETs at the end
	// of the object, no matter how large the zip is.
	zr, err := zip.NewReader(obj, obj.Size)
	if err != nil {
		// Retrying won't fix a corrupt zip, so record it and succeed.
		b.Error = "the upload is not a readable zip file: " + err.Error()
		slog.Warn("unreadable zip", "batch", batchID, "err", err)
		return ignoreExists(p.Store.CreateBatch(ctx, b))
	}

	files := documentEntries(zr)
	chunks := planChunks(files, p.Limits)
	b.Total = len(files)
	b.ChunkCount = len(chunks)
	if err := ignoreExists(p.Store.CreateBatch(ctx, b)); err != nil {
		return err
	}

	bodies := make([][]byte, len(chunks))
	for i, c := range chunks {
		c.V, c.BatchID, c.Bucket, c.Key, c.Chunk = job.Version, batchID, bucket, key, i+1
		bodies[i], _ = json.Marshal(c)
	}
	if err := p.Chunks.Send(ctx, bodies...); err != nil {
		return err
	}
	slog.Info("planned batch", "batch", batchID, "documents", len(files), "chunks", len(chunks))
	return nil
}

// planChunks splits seqs 1..len(files) into consecutive ranges. A chunk closes
// when it reaches MaxChunkEntries, or when the next entry would push the bytes
// it must read past MaxChunkBytes. Entries that will be skipped cost no bytes:
// the chunk never reads them, it only records them.
func planChunks(files []*zip.File, lim Limits) []job.Chunk {
	var chunks []job.Chunk
	var cur job.Chunk
	var curBytes int64
	for i, f := range files {
		seq := i + 1
		var cost int64
		if skipReason(f, lim.MaxDocumentBytes) == "" {
			cost = int64(f.CompressedSize64)
		}
		n := seq - cur.FirstSeq
		if cur.FirstSeq != 0 && (n >= lim.MaxChunkEntries || curBytes+cost > lim.MaxChunkBytes) {
			chunks = append(chunks, cur)
			cur = job.Chunk{}
		}
		if cur.FirstSeq == 0 {
			cur.FirstSeq, curBytes = seq, 0
		}
		cur.LastSeq = seq
		curBytes += cost
	}
	if cur.FirstSeq != 0 {
		chunks = append(chunks, cur)
	}
	return chunks
}

func ignoreExists(err error) error {
	if errors.Is(err, batch.ErrExists) {
		return nil
	}
	return err
}
