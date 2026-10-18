package s3blob

import (
	"archive/zip"
	"bytes"
	"context"
	"errors"
	"io"
	"os"
	"path/filepath"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/google/uuid"

	"github.com/daniel-mutuku/textract-bulk-onboarding/internal/awstest"
	"github.com/daniel-mutuku/textract-bulk-onboarding/internal/blob"
)

// Reads a zip of the sample forms straight from S3 through the ranged
// ReaderAt, and checks every entry against the local files.
func TestRealRangedZipRead(t *testing.T) {
	client := s3.NewFromConfig(awstest.Config(t))
	bucket := awstest.DocumentsBucket(t)
	prefix := "test/" + uuid.NewString() + "/"
	awstest.CleanupPrefix(t, client, bucket, prefix)
	ctx := context.Background()

	paths, _ := filepath.Glob("../../../testdata/forms/*.jpeg")
	if len(paths) == 0 {
		t.Skip("no sample forms")
	}
	want := map[string][]byte{}
	var buf bytes.Buffer
	zw := zip.NewWriter(&buf)
	for _, p := range paths {
		data, _ := os.ReadFile(p)
		want[filepath.Base(p)] = data
		w, _ := zw.CreateHeader(&zip.FileHeader{Name: filepath.Base(p), Method: zip.Store})
		w.Write(data)
	}
	zw.Close()

	key := prefix + "forms.zip"
	_, err := client.PutObject(ctx, &s3.PutObjectInput{
		Bucket: &bucket, Key: &key, Body: bytes.NewReader(buf.Bytes()),
		Metadata: map[string]string{"original-filename": "forms.zip"},
	})
	if err != nil {
		t.Fatal(err)
	}

	for _, blockSize := range []int64{defaultBlockSize, 256 << 10} {
		s := &Store{client: client, blockSize: blockSize}
		obj, err := s.Open(ctx, bucket, key)
		if err != nil {
			t.Fatal(err)
		}
		if obj.Size != int64(buf.Len()) || obj.Metadata["original-filename"] != "forms.zip" {
			t.Errorf("size %d metadata %v", obj.Size, obj.Metadata)
		}
		zr, err := zip.NewReader(obj, obj.Size)
		if err != nil {
			t.Fatal(err)
		}
		for _, f := range zr.File {
			rc, _ := f.Open()
			got, err := io.ReadAll(rc)
			rc.Close()
			if err != nil || !bytes.Equal(got, want[f.Name]) {
				t.Errorf("%s: err %v, bytes match %v", f.Name, err, bytes.Equal(got, want[f.Name]))
			}
		}
		t.Logf("%.1f MB zip, %d KB blocks: %d ranged GETs", float64(obj.Size)/(1<<20), blockSize>>10, obj.ReaderAt.(*rangeReader).gets.Load())
	}
}

func TestRealPutAndNotFound(t *testing.T) {
	client := s3.NewFromConfig(awstest.Config(t))
	bucket := awstest.DocumentsBucket(t)
	prefix := "test/" + uuid.NewString() + "/"
	awstest.CleanupPrefix(t, client, bucket, prefix)
	ctx := context.Background()
	s := New(client)

	if _, err := s.Open(ctx, bucket, prefix+"missing.json"); !errors.Is(err, blob.ErrNotFound) {
		t.Errorf("missing object: got %v, want blob.ErrNotFound", err)
	}
	if err := s.Put(ctx, bucket, prefix+"raw.json", []byte(`{"Blocks":[]}`), "application/json"); err != nil {
		t.Fatal(err)
	}
	head, err := client.HeadObject(ctx, &s3.HeadObjectInput{Bucket: &bucket, Key: aws.String(prefix + "raw.json")})
	if err != nil || aws.ToString(head.ContentType) != "application/json" {
		t.Errorf("head: %v, content type %q", err, aws.ToString(head.ContentType))
	}
}
