// Command unzip-chunk is the Lambda behind the unzip-chunks queue. Each
// invocation unpacks one chunk of a zip (at most 250 documents) with ranged
// reads, writes the documents to S3, records them, and queues them for
// extraction. Many run in parallel, one per chunk.
package main

import (
	"context"

	"github.com/aws/aws-lambda-go/events"
	"github.com/aws/aws-lambda-go/lambda"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/sqs"

	"github.com/daniel-mutuku/textract-bulk-onboarding/internal/batch/dynamostore"
	"github.com/daniel-mutuku/textract-bulk-onboarding/internal/blob/s3blob"
	"github.com/daniel-mutuku/textract-bulk-onboarding/internal/lambdakit"
	"github.com/daniel-mutuku/textract-bulk-onboarding/internal/queue/sqsqueue"
	"github.com/daniel-mutuku/textract-bulk-onboarding/internal/unzip"
)

func main() {
	cfg := lambdakit.Boot(context.Background())
	sqsClient := sqs.NewFromConfig(cfg)
	chunker := &unzip.Chunker{
		Blobs:           s3blob.New(s3.NewFromConfig(cfg)),
		Store:           dynamostore.New(dynamodb.NewFromConfig(cfg), lambdakit.Env("PIPELINE_TABLE")),
		Documents:       sqsqueue.New(sqsClient, lambdakit.Env("DOCUMENTS_QUEUE_URL")),
		DocumentsBucket: lambdakit.Env("DOCUMENTS_BUCKET"),
		Limits:          unzip.DefaultLimits(),
	}
	handle := lambdakit.FromError("chunk", chunker.HandleMessage)
	vis := lambdakit.SQSVisibility{Client: sqsClient}

	lambda.Start(func(ctx context.Context, ev events.SQSEvent) (events.SQSEventResponse, error) {
		return lambdakit.HandleSQS(ctx, ev, vis, handle), nil
	})
}
