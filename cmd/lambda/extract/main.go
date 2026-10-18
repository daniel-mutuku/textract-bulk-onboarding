// Command extract is the Lambda behind the documents queue. Each message is
// one document: run Textract on it, map the answers to fields, and record
// the result exactly once. The event source mapping's maximum_concurrency
// caps how many run at once, which is what keeps Textract under its quota.
package main

import (
	"context"
	"strconv"
	"time"

	"github.com/aws/aws-lambda-go/events"
	"github.com/aws/aws-lambda-go/lambda"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/aws/aws-sdk-go-v2/service/textract"

	"github.com/daniel-mutuku/textract-bulk-onboarding/internal/batch/dynamostore"
	"github.com/daniel-mutuku/textract-bulk-onboarding/internal/blob/s3blob"
	"github.com/daniel-mutuku/textract-bulk-onboarding/internal/extract"
	"github.com/daniel-mutuku/textract-bulk-onboarding/internal/extract/textractclient"
	"github.com/daniel-mutuku/textract-bulk-onboarding/internal/lambdakit"
)

func main() {
	cfg := lambdakit.Boot(context.Background())
	maxAttempts, err := strconv.Atoi(lambdakit.Env("MAX_ATTEMPTS"))
	if err != nil {
		lambdakit.Fatal("MAX_ATTEMPTS must be a number", err)
	}
	worker := &extract.Worker{
		Store:       dynamostore.New(dynamodb.NewFromConfig(cfg), lambdakit.Env("PIPELINE_TABLE")),
		Blobs:       s3blob.New(s3.NewFromConfig(cfg)),
		Textract:    textractclient.New(textract.NewFromConfig(cfg)),
		Queries:     extract.Queries,
		MaxAttempts: maxAttempts, // Terraform sets it from the queue's maxReceiveCount
		Now:         time.Now,
	}
	handle := func(ctx context.Context, m lambdakit.Message) lambdakit.Result {
		out := worker.Handle(ctx, extract.Message{Body: m.Body, ReceiveCount: m.ReceiveCount})
		return lambdakit.Result{Retry: out.Retry, Delay: out.Delay}
	}
	vis := lambdakit.SQSVisibility{Client: sqs.NewFromConfig(cfg)}

	lambda.Start(func(ctx context.Context, ev events.SQSEvent) (events.SQSEventResponse, error) {
		return lambdakit.HandleSQS(ctx, ev, vis, handle), nil
	})
}
