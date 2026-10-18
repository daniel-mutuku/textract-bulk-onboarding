// Package awstest holds helpers for tests that run against real AWS. They are
// opt-in, because they need credentials and cost (a little) money:
//
//	AWS_TEST=1 AWS_PROFILE=textract-onboarding \
//	DOCUMENTS_BUCKET=$(terraform -chdir=infra output -raw documents_bucket) \
//	go test ./... -run Real -v
package awstest

import (
	"context"
	"os"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
)

// Config skips the test unless AWS_TEST is set, then loads AWS config the
// usual way (AWS_PROFILE, AWS_REGION, defaulting to eu-west-1).
func Config(t *testing.T) aws.Config {
	t.Helper()
	if os.Getenv("AWS_TEST") == "" {
		t.Skip("set AWS_TEST=1 to run against real AWS")
	}
	region := os.Getenv("AWS_REGION")
	if region == "" {
		region = "eu-west-1"
	}
	cfg, err := config.LoadDefaultConfig(context.Background(), config.WithRegion(region))
	if err != nil {
		t.Fatal(err)
	}
	return cfg
}

// DocumentsBucket returns the bucket tests may write scratch objects to.
func DocumentsBucket(t *testing.T) string {
	t.Helper()
	b := os.Getenv("DOCUMENTS_BUCKET")
	if b == "" {
		t.Skip("set DOCUMENTS_BUCKET (terraform -chdir=infra output -raw documents_bucket)")
	}
	return b
}

// CleanupPrefix deletes every version of every object under prefix when the
// test ends. The documents bucket is versioned, so a plain delete would only
// add a delete marker and leave the bytes (and their storage cost) behind.
func CleanupPrefix(t *testing.T, client *s3.Client, bucket, prefix string) {
	t.Helper()
	t.Cleanup(func() {
		ctx := context.Background()
		out, err := client.ListObjectVersions(ctx, &s3.ListObjectVersionsInput{Bucket: &bucket, Prefix: &prefix})
		if err != nil {
			t.Errorf("list versions under %s: %v", prefix, err)
			return
		}
		var ids []types.ObjectIdentifier
		for _, v := range out.Versions {
			ids = append(ids, types.ObjectIdentifier{Key: v.Key, VersionId: v.VersionId})
		}
		for _, m := range out.DeleteMarkers {
			ids = append(ids, types.ObjectIdentifier{Key: m.Key, VersionId: m.VersionId})
		}
		if len(ids) == 0 {
			return
		}
		if _, err := client.DeleteObjects(ctx, &s3.DeleteObjectsInput{Bucket: &bucket, Delete: &types.Delete{Objects: ids}}); err != nil {
			t.Errorf("delete test objects under %s: %v", prefix, err)
		}
	})
}
