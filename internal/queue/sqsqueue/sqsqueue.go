// Package sqsqueue is the SQS implementation of queue.Sender.
package sqsqueue

import (
	"context"
	"fmt"
	"strconv"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/aws/aws-sdk-go-v2/service/sqs/types"

	"github.com/daniel-mutuku/textract-bulk-onboarding/internal/queue"
)

const (
	batchSize    = 10 // SendMessageBatch's limit
	sendAttempts = 3
)

type Queue struct {
	client *sqs.Client
	url    string
}

func New(client *sqs.Client, url string) *Queue {
	return &Queue{client: client, url: url}
}

var _ queue.Sender = (*Queue)(nil)

// Send enqueues bodies in batches of 10.
//
// A batch call can partly succeed: the request returns 200, and the response
// lists which entries were sent (Successful) and which were not (Failed).
// Checking only the call's error would silently drop messages, so failed
// entries are resent on their own. An entry SQS blames on the sender (bad
// body, too large) will never succeed and is reported at once.
func (q *Queue) Send(ctx context.Context, bodies ...[]byte) error {
	for start := 0; start < len(bodies); start += batchSize {
		if err := q.sendBatch(ctx, bodies[start:min(start+batchSize, len(bodies))]); err != nil {
			return err
		}
	}
	return nil
}

func (q *Queue) sendBatch(ctx context.Context, bodies [][]byte) error {
	pending := map[string][]byte{}
	for i, b := range bodies {
		pending[strconv.Itoa(i)] = b
	}
	for attempt := 1; ; attempt++ {
		entries := make([]types.SendMessageBatchRequestEntry, 0, len(pending))
		for id, b := range pending {
			entries = append(entries, types.SendMessageBatchRequestEntry{Id: aws.String(id), MessageBody: aws.String(string(b))})
		}
		out, err := q.client.SendMessageBatch(ctx, &sqs.SendMessageBatchInput{QueueUrl: &q.url, Entries: entries})
		if err != nil {
			return err // the SDK has already retried transient failures of the call itself
		}
		for _, s := range out.Successful {
			delete(pending, aws.ToString(s.Id))
		}
		for _, f := range out.Failed {
			if f.SenderFault {
				return fmt.Errorf("sqs rejected message: %s: %s", aws.ToString(f.Code), aws.ToString(f.Message))
			}
		}
		if len(pending) == 0 {
			return nil
		}
		if attempt == sendAttempts {
			reason := "no reason given"
			if len(out.Failed) > 0 {
				reason = aws.ToString(out.Failed[0].Code) + ": " + aws.ToString(out.Failed[0].Message)
			}
			return fmt.Errorf("%d messages not sent after %d attempts: %s", len(pending), attempt, reason)
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(time.Duration(attempt) * 200 * time.Millisecond):
		}
	}
}
