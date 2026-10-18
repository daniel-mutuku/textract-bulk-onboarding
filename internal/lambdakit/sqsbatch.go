// Package lambdakit is the glue between AWS Lambda and the pipeline's
// handlers: it turns an SQS event batch into handler calls, and the results
// back into what Lambda's SQS integration understands.
package lambdakit

import (
	"context"
	"fmt"
	"log/slog"
	"math/rand/v2"
	"strconv"
	"strings"
	"time"

	"github.com/aws/aws-lambda-go/events"
)

// Message is one SQS record, reduced to what handlers need.
type Message struct {
	ID           string
	Body         []byte
	ReceiveCount int // SQS's ApproximateReceiveCount: 1 on first delivery
}

// Result says what to do with a message once handled.
type Result struct {
	Retry bool
	Delay time.Duration // with Retry: how long until SQS delivers it again
}

// Func handles one message.
type Func func(ctx context.Context, m Message) Result

// Visibility changes how long a received message stays hidden.
type Visibility interface {
	ChangeVisibility(ctx context.Context, queueURL, receiptHandle string, d time.Duration) error
}

// HandleSQS processes a batch of SQS records and returns a partial batch
// response.
//
// How Lambda's SQS integration (the event source mapping) treats the result:
//   - Records listed in BatchItemFailures stay on the queue and are retried.
//   - Every other record is deleted from the queue by Lambda.
//
// That only works when the mapping has function_response_types =
// ["ReportBatchItemFailures"]. Without it, Lambda would read the response as
// plain success and delete everything, including the failures.
//
// Backoff: SQS has no per-message retry delay, but a message's visibility
// timeout can be changed after it is received. Setting it to Result.Delay
// before reporting the failure makes SQS deliver the message again after
// that delay instead of after the queue's full visibility timeout.
func HandleSQS(ctx context.Context, ev events.SQSEvent, vis Visibility, fn Func) events.SQSEventResponse {
	var resp events.SQSEventResponse
	for _, rec := range ev.Records {
		fail := func() {
			resp.BatchItemFailures = append(resp.BatchItemFailures, events.SQSBatchItemFailure{ItemIdentifier: rec.MessageId})
		}
		// Out of time: hand the rest back untouched rather than start work
		// that Lambda will kill halfway through.
		if ctx.Err() != nil {
			fail()
			continue
		}
		count, _ := strconv.Atoi(rec.Attributes["ApproximateReceiveCount"])
		res := fn(ctx, Message{ID: rec.MessageId, Body: []byte(rec.Body), ReceiveCount: count})
		if !res.Retry {
			continue
		}
		fail()
		url, err := QueueURL(rec.EventSourceARN)
		if err == nil {
			err = vis.ChangeVisibility(ctx, url, rec.ReceiptHandle, res.Delay)
		}
		if err != nil {
			// Not fatal: the message still comes back, after the queue's
			// default visibility timeout instead of the backoff delay.
			slog.Warn("could not set retry delay", "message", rec.MessageId, "err", err)
		}
	}
	return resp
}

// FromError adapts a handler that returns an error: success deletes the
// message; an error retries it with exponential backoff.
func FromError(name string, h func(ctx context.Context, body []byte) error) Func {
	return func(ctx context.Context, m Message) Result {
		err := h(ctx, m.Body)
		if err == nil {
			return Result{}
		}
		delay := Backoff(m.ReceiveCount)
		slog.Error(name+" failed, will retry", "message", m.ID, "attempt", m.ReceiveCount, "in", delay.Round(time.Second), "err", err)
		return Result{Retry: true, Delay: delay}
	}
}

const (
	backoffBase = 30 * time.Second
	backoffCap  = 15 * time.Minute
)

// Backoff is ~30s, 1m, 2m, 4m… capped at 15m, half of it random so messages
// that failed together don't come back together.
func Backoff(receiveCount int) time.Duration {
	d := min(backoffCap, backoffBase<<min(max(receiveCount-1, 0), 10))
	return d/2 + rand.N(d/2+1)
}

// QueueURL turns an SQS queue ARN (which events carry) into the queue URL
// (which the SQS API takes): arn:aws:sqs:eu-west-1:123456789012:name
// becomes https://sqs.eu-west-1.amazonaws.com/123456789012/name.
func QueueURL(arn string) (string, error) {
	parts := strings.Split(arn, ":")
	if len(parts) != 6 || parts[0] != "arn" || parts[2] != "sqs" {
		return "", fmt.Errorf("not an SQS queue ARN: %q", arn)
	}
	region, account, name := parts[3], parts[4], parts[5]
	return fmt.Sprintf("https://sqs.%s.amazonaws.com/%s/%s", region, account, name), nil
}
