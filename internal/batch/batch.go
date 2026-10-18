// Package batch defines the records the processing pipeline keeps, and the
// Store interface that holds them. Nothing here depends on a particular
// database, so DynamoDB can later be swapped for SQLite.
package batch

import (
	"errors"
	"time"
)

// Status is where a single document is in the pipeline.
type Status string

const (
	StatusPending   Status = "pending"   // written to S3 and queued for extraction
	StatusExtracted Status = "extracted" // Textract succeeded; Fields is filled
	StatusFailed    Status = "failed"    // extraction gave up; Reason says why
	StatusSkipped   Status = "skipped"   // filtered out while unzipping; Reason says why
)

// State is where a whole batch is. It is derived from the counters, never stored.
type State string

const (
	StateUnpacking  State = "unpacking"  // unzip chunks are still running
	StateProcessing State = "processing" // all documents created; some still pending
	StateCompleted  State = "completed"  // every document is extracted, failed or skipped
	StateFailed     State = "failed"     // the zip itself could not be read; see Batch.Error
)

// Batch is one uploaded zip.
type Batch struct {
	ID               string // the upload ID, so the browser knows it before processing starts
	SourceKey        string // S3 key of the zip
	OriginalFilename string
	Total            int // documents in the zip, directory entries excluded
	ChunkCount       int // how many unzip chunks the planner split the zip into
	ChunksDone       int
	Processed        int
	Failed           int
	Skipped          int
	Error            string // set when the zip itself could not be read
	CreatedAt        time.Time
	UpdatedAt        time.Time
}

// Pending is how many documents still await extraction.
func (b Batch) Pending() int {
	return max(0, b.Total-b.Processed-b.Failed-b.Skipped)
}

// State is computed rather than stored. A stored "completed" would need some
// worker to notice it had finished the last document and write it, which is
// a second write that can race; a computed one can never disagree with the counts.
func (b Batch) State() State {
	switch {
	case b.Error != "":
		return StateFailed
	case b.ChunksDone < b.ChunkCount:
		return StateUnpacking
	case b.Pending() > 0:
		return StateProcessing
	default:
		return StateCompleted
	}
}

// Document is one entry from the zip.
type Document struct {
	BatchID     string
	Seq         int    // position in the zip, from 1; stable across retries
	Filename    string // path inside the zip
	Key         string // S3 key in the documents bucket; empty when skipped
	Size        int64
	ContentType string
	Status      Status
	Reason      string           // why it was skipped or failed
	Fields      map[string]Field // extracted values, by field name
	RawKey      string           // S3 key of the raw Textract response
	ExtractedAt time.Time
}

// Field is one extracted value. Confidence is 0–100, as Textract reports it.
type Field struct {
	Value      string
	Confidence float64
}

// Extraction is what a successful Textract call produces for a document.
type Extraction struct {
	Fields map[string]Field
	RawKey string
	At     time.Time
}

var (
	ErrNotFound = errors.New("not found")
	ErrExists   = errors.New("already exists")
	// ErrConflict means the record was not in the state the write expected,
	// usually because another worker got there first.
	ErrConflict = errors.New("conflict")
)
