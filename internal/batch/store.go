package batch

import "context"

// Store holds batches and their documents.
//
// Every method that changes a document's status also updates the batch's
// counters in the same atomic write. A counter therefore moves exactly once
// per document, however many times SQS redelivers a message.
type Store interface {
	// CreateBatch stores a new batch. It returns ErrExists if the ID is taken;
	// a retried planner treats that as success.
	CreateBatch(ctx context.Context, b Batch) error
	// GetBatch returns ErrNotFound for an unknown ID.
	GetBatch(ctx context.Context, id string) (Batch, error)
	// FinishChunk records that unzip chunk n is done. Each chunk is counted
	// once, however many times it is retried.
	FinishChunk(ctx context.Context, batchID string, chunk int) error

	// CreateDocument stores a new document. If it starts as skipped or failed,
	// the matching batch counter goes up in the same atomic write. It returns
	// ErrExists, changing nothing, if the document is already there, and
	// ErrNotFound if the batch is missing.
	CreateDocument(ctx context.Context, d Document) error
	// GetDocument returns ErrNotFound for an unknown document.
	GetDocument(ctx context.Context, batchID string, seq int) (Document, error)
	// ListDocuments returns up to limit documents with Seq > afterSeq in seq
	// order, plus a cursor for the next page. The cursor is 0 when there is
	// nothing more; it may also point at an empty final page.
	ListDocuments(ctx context.Context, batchID string, afterSeq, limit int) ([]Document, int, error)

	// CompleteDocument marks a document extracted and counts it as processed,
	// only if its status is still `from` (pending, or failed for a replay from
	// the dead-letter queue). Otherwise it returns ErrConflict and changes nothing.
	CompleteDocument(ctx context.Context, batchID string, seq int, from Status, e Extraction) error
	// FailDocument marks a pending document failed and counts it, with the
	// same guarantee: ErrConflict if it is no longer pending.
	FailDocument(ctx context.Context, batchID string, seq int, reason string) error
}
