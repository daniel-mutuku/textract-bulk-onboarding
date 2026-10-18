// Command unzip-plan is the Lambda behind the zip-events queue. For each S3
// "object created" event it reads the zip's central directory, creates the
// batch, and enqueues one message per unzip chunk.
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
	// Everything above lambda.Start runs once per cold start and is reused
	// by every invocation this instance serves.
	cfg := lambdakit.Boot(context.Background())
	sqsClient := sqs.NewFromConfig(cfg)
	planner := &unzip.Planner{
		Blobs:  s3blob.New(s3.NewFromConfig(cfg)),
		Store:  dynamostore.New(dynamodb.NewFromConfig(cfg), lambdakit.Env("PIPELINE_TABLE")),
		Chunks: sqsqueue.New(sqsClient, lambdakit.Env("CHUNKS_QUEUE_URL")),
		Limits: unzip.DefaultLimits(),
	}
	handle := lambdakit.FromError("plan", planner.HandleS3Event)
	vis := lambdakit.SQSVisibility{Client: sqsClient}

	lambda.Start(func(ctx context.Context, ev events.SQSEvent) (events.SQSEventResponse, error) {
		return lambdakit.HandleSQS(ctx, ev, vis, handle), nil
	})
}
