package storage

import (
	"context"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

// DocumentLinks signs short-lived GET URLs for documents in a private bucket:
// the read-side twin of the presigned part URLs used for uploading.
type DocumentLinks struct {
	presign *s3.PresignClient
	bucket  string
	ttl     time.Duration
}

func NewDocumentLinks(client *s3.Client, bucket string, ttl time.Duration) *DocumentLinks {
	return &DocumentLinks{presign: s3.NewPresignClient(client), bucket: bucket, ttl: ttl}
}

func (l *DocumentLinks) URL(ctx context.Context, key string) (string, error) {
	req, err := l.presign.PresignGetObject(ctx, &s3.GetObjectInput{
		Bucket: &l.bucket,
		Key:    &key,
		// Display in the browser rather than download.
		ResponseContentDisposition: aws.String("inline"),
	}, s3.WithPresignExpires(l.ttl))
	if err != nil {
		return "", err
	}
	return req.URL, nil
}
