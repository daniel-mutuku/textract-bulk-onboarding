package lambdakit

import (
	"context"
	"log/slog"
	"os"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
)

// Boot does the per-cold-start setup every function shares: JSON logs, which
// CloudWatch Logs Insights can query by field, and AWS config. Lambda sets
// AWS_REGION and the execution role's temporary credentials as environment
// variables, and the SDK's default chain reads both, so no keys or region
// appear in code.
func Boot(ctx context.Context) aws.Config {
	slog.SetDefault(slog.New(slog.NewJSONHandler(os.Stdout, nil)))
	cfg, err := config.LoadDefaultConfig(ctx)
	if err != nil {
		Fatal("load AWS config", err)
	}
	return cfg
}

// Env returns a required environment variable, set on the function by Terraform.
func Env(name string) string {
	v := os.Getenv(name)
	if v == "" {
		Fatal("missing environment variable", nil, "name", name)
	}
	return v
}

// Fatal logs and exits. During init, Lambda reports the function as failed
// and shows this log line in CloudWatch.
func Fatal(msg string, err error, args ...any) {
	slog.Error(msg, append(args, "err", err)...)
	os.Exit(1)
}

// SQSVisibility implements Visibility with the SQS API.
type SQSVisibility struct {
	Client *sqs.Client
}

func (v SQSVisibility) ChangeVisibility(ctx context.Context, queueURL, receiptHandle string, d time.Duration) error {
	// The receipt handle identifies this particular receive of the message,
	// not the message itself; it is what proves we currently hold it.
	_, err := v.Client.ChangeMessageVisibility(ctx, &sqs.ChangeMessageVisibilityInput{
		QueueUrl:          &queueURL,
		ReceiptHandle:     &receiptHandle,
		VisibilityTimeout: int32(min(d, 12*time.Hour) / time.Second), // SQS allows at most 12 hours
	})
	return err
}
