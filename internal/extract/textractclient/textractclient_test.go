package textractclient

import (
	"context"
	"errors"
	"os"
	"testing"

	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/textract"
	"github.com/google/uuid"

	"github.com/daniel-mutuku/textract-bulk-onboarding/internal/awstest"
	"github.com/daniel-mutuku/textract-bulk-onboarding/internal/blob/s3blob"
	"github.com/daniel-mutuku/textract-bulk-onboarding/internal/extract"
)

// One real AnalyzeDocument call (about $0.015) on sample form 1, read from S3.
// It proves the SDK's marshalled response parses with MapFields exactly like
// the CLI-captured fixtures do.
func TestRealAnalyze(t *testing.T) {
	cfg := awstest.Config(t)
	bucket := awstest.DocumentsBucket(t)
	s3c := s3.NewFromConfig(cfg)
	prefix := "test/" + uuid.NewString() + "/"
	awstest.CleanupPrefix(t, s3c, bucket, prefix)
	ctx := context.Background()

	data, err := os.ReadFile("../../../testdata/forms/1.jpeg")
	if err != nil {
		t.Skip("no sample form")
	}
	key := prefix + "1.jpeg"
	if err := s3blob.New(s3c).Put(ctx, bucket, key, data, "image/jpeg"); err != nil {
		t.Fatal(err)
	}

	c := New(textract.NewFromConfig(cfg))
	raw, err := c.Analyze(ctx, bucket, key, extract.Queries)
	if err != nil {
		t.Fatal(err)
	}
	fields, err := extract.MapFields(raw)
	if err != nil {
		t.Fatal(err)
	}
	for alias, want := range map[string]string{"full_name": "John Doe", "account_number": "0101010101", "town": "Nairobi"} {
		if got := fields[alias].Value; got != want {
			t.Errorf("%s = %q, want %q", alias, got, want)
		}
	}
	t.Logf("%d fields from a %d KB response", len(fields), len(raw)>>10)

	// A document that isn't an image is a permanent failure, not a retry.
	if err := s3blob.New(s3c).Put(ctx, bucket, prefix+"bad.jpeg", []byte("not an image"), "image/jpeg"); err != nil {
		t.Fatal(err)
	}
	_, err = c.Analyze(ctx, bucket, prefix+"bad.jpeg", extract.Queries)
	var perm *extract.PermanentError
	if !errors.As(err, &perm) {
		t.Errorf("bad document: got %v, want a permanent error", err)
	}
	t.Logf("bad document: %v", err)
}
