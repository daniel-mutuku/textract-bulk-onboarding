// Package job defines the messages that travel between pipeline stages on SQS.
// They are deliberately small: identifiers only. The datastore is the single
// source of truth, and an SQS message body is capped at 256 KB anyway.
package job

// Version is bumped if a message shape changes incompatibly, so a consumer
// can tell old messages (still in a queue or DLQ) from new ones.
const Version = 1

// Chunk asks an unzip-chunk Lambda to unpack documents FirstSeq..LastSeq
// (inclusive) of one zip.
type Chunk struct {
	V        int    `json:"v"`
	BatchID  string `json:"batchId"`
	Bucket   string `json:"bucket"`
	Key      string `json:"key"`
	Chunk    int    `json:"chunk"` // 1-based, for FinishChunk
	FirstSeq int    `json:"firstSeq"`
	LastSeq  int    `json:"lastSeq"`
}

// Document asks the extract Lambda to run Textract on one document.
type Document struct {
	V       int    `json:"v"`
	BatchID string `json:"batchId"`
	Seq     int    `json:"seq"`
	Bucket  string `json:"bucket"`
	Key     string `json:"key"`
}
