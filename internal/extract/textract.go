package extract

import (
	"context"
	"errors"
)

// Analyzer runs Textract AnalyzeDocument with Queries on a document in S3 and
// returns the raw JSON response. The response is returned raw, rather than
// parsed, so it can be saved as-is and re-mapped later without paying for a
// second Textract call.
type Analyzer interface {
	Analyze(ctx context.Context, bucket, key string, queries []Query) ([]byte, error)
}

// PermanentError marks a failure retrying cannot fix, such as a document
// Textract can't read. Any other error is treated as transient and retried.
type PermanentError struct {
	Err error
}

func (e *PermanentError) Error() string { return e.Err.Error() }
func (e *PermanentError) Unwrap() error { return e.Err }

// Permanent wraps err as a PermanentError.
func Permanent(err error) error {
	return &PermanentError{Err: err}
}

func isPermanent(err error) bool {
	var p *PermanentError
	return errors.As(err, &p)
}
