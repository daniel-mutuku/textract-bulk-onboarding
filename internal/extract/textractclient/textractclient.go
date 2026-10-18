// Package textractclient is the Amazon Textract implementation of extract.Analyzer.
package textractclient

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/textract"
	"github.com/aws/aws-sdk-go-v2/service/textract/types"
	"github.com/aws/smithy-go"

	"github.com/daniel-mutuku/textract-bulk-onboarding/internal/extract"
)

type Client struct {
	client *textract.Client
}

func New(client *textract.Client) *Client {
	return &Client{client: client}
}

var _ extract.Analyzer = (*Client)(nil)

// Analyze calls AnalyzeDocument with the Queries feature on a document in S3.
//
// The document is passed by reference (S3Object), not as bytes: Textract
// reads it from S3 itself, inside AWS, so the Lambda never downloads it.
// Textract reads it with the CALLER's permissions, which is why the extract
// Lambda's role needs s3:GetObject on the documents bucket.
func (c *Client) Analyze(ctx context.Context, bucket, key string, queries []extract.Query) ([]byte, error) {
	qs := make([]types.Query, len(queries))
	for i, q := range queries {
		qs[i] = types.Query{Text: aws.String(q.Text), Alias: aws.String(q.Alias)}
	}
	out, err := c.client.AnalyzeDocument(ctx, &textract.AnalyzeDocumentInput{
		Document:      &types.Document{S3Object: &types.S3Object{Bucket: &bucket, Name: &key}},
		FeatureTypes:  []types.FeatureType{types.FeatureTypeQueries},
		QueriesConfig: &types.QueriesConfig{Queries: qs},
	})
	if err != nil {
		return nil, classify(err)
	}
	// The SDK's structs marshal with the API's field names (Blocks, BlockType,
	// Id, Relationships...), so the saved JSON reads the same as the CLI's.
	return json.Marshal(out)
}

// Errors retrying can't fix: the document itself is the problem.
var permanentCodes = map[string]string{
	"UnsupportedDocumentException": "unsupported document (format, or a PDF/TIFF with more than one page)",
	"BadDocumentException":         "Textract could not read the document (corrupt or unreadable image)",
	"DocumentTooLargeException":    "document too large for synchronous Textract",
	"InvalidParameterException":    "invalid request (for example, the image is too small)",
	"InvalidS3ObjectException":     "Textract could not open the S3 object",
}

// classify marks document problems permanent. Everything else is retryable:
//   - ThrottlingException, ProvisionedThroughputExceededException: over the
//     account's Textract TPS quota. The SDK already retried a few times with
//     backoff; the worker's longer SQS backoff takes over from there.
//   - InternalServerError: Textract's side.
//   - AccessDeniedException: a configuration mistake (IAM). Retrying lets the
//     messages land in the DLQ, to be redriven once the policy is fixed,
//     instead of marking every document failed.
func classify(err error) error {
	var apiErr smithy.APIError
	if errors.As(err, &apiErr) {
		if reason, ok := permanentCodes[apiErr.ErrorCode()]; ok {
			return extract.Permanent(fmt.Errorf("%s: %s", apiErr.ErrorCode(), reason))
		}
	}
	return err
}
