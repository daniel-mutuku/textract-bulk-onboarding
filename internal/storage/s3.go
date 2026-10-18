package storage

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/aws/smithy-go"
)

// S3Store implements MultipartStore against a real bucket.
type S3Store struct {
	client     *s3.Client
	presign    *s3.PresignClient
	bucket     string
	presignTTL time.Duration
}

func NewS3Store(client *s3.Client, bucket string, presignTTL time.Duration) *S3Store {
	return &S3Store{
		client:     client,
		presign:    s3.NewPresignClient(client),
		bucket:     bucket,
		presignTTL: presignTTL,
	}
}

// CheckBucket confirms the bucket exists and the credentials can reach it.
func (s *S3Store) CheckBucket(ctx context.Context) error {
	_, err := s.client.HeadBucket(ctx, &s3.HeadBucketInput{Bucket: &s.bucket})
	return err
}

func (s *S3Store) Create(ctx context.Context, key, contentType string, meta map[string]string) (string, error) {
	out, err := s.client.CreateMultipartUpload(ctx, &s3.CreateMultipartUploadInput{
		Bucket:      &s.bucket,
		Key:         &key,
		ContentType: &contentType,
		Metadata:    meta, // stored as x-amz-meta-* headers on the final object
	})
	if err != nil {
		return "", mapError(err)
	}
	return *out.UploadId, nil
}

// PresignPart signs an UploadPart request locally with our credentials. It
// makes no network call, so an unknown upload ID is only caught later, when
// the browser's PUT to the URL gets a 404 from S3.
func (s *S3Store) PresignPart(ctx context.Context, key, uploadID string, partNumber int32) (string, error) {
	req, err := s.presign.PresignUploadPart(ctx, &s3.UploadPartInput{
		Bucket:     &s.bucket,
		Key:        &key,
		UploadId:   &uploadID,
		PartNumber: aws.Int32(partNumber),
	}, s3.WithPresignExpires(s.presignTTL))
	if err != nil {
		return "", err
	}
	return req.URL, nil
}

func (s *S3Store) Complete(ctx context.Context, key, uploadID string, parts []Part) error {
	completed := make([]types.CompletedPart, len(parts))
	for i, p := range parts {
		completed[i] = types.CompletedPart{PartNumber: aws.Int32(p.PartNumber), ETag: aws.String(p.ETag)}
	}
	_, err := s.client.CompleteMultipartUpload(ctx, &s3.CompleteMultipartUploadInput{
		Bucket:          &s.bucket,
		Key:             &key,
		UploadId:        &uploadID,
		MultipartUpload: &types.CompletedMultipartUpload{Parts: completed},
	})
	return mapError(err)
}

func (s *S3Store) Abort(ctx context.Context, key, uploadID string) error {
	_, err := s.client.AbortMultipartUpload(ctx, &s3.AbortMultipartUploadInput{
		Bucket:   &s.bucket,
		Key:      &key,
		UploadId: &uploadID,
	})
	return mapError(err)
}

// mapError turns the S3 error codes the handler cares about into this
// package's errors, keeping the original for logs.
func mapError(err error) error {
	var apiErr smithy.APIError
	if err == nil || !errors.As(err, &apiErr) {
		return err
	}
	switch apiErr.ErrorCode() {
	case "NoSuchUpload":
		return fmt.Errorf("%w: %v", ErrNoSuchUpload, err)
	case "InvalidPart", "InvalidPartOrder", "EntityTooSmall":
		return fmt.Errorf("%w: %v", ErrInvalidPart, err)
	}
	return err
}
