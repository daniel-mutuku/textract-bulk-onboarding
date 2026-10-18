package lambdakit

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/aws/aws-lambda-go/events"
)

const queueARN = "arn:aws:sqs:eu-west-1:123456789012:textract-onboarding-documents"

type visCall struct {
	url, handle string
	delay       time.Duration
}

type fakeVisibility struct {
	calls []visCall
	err   error
}

func (f *fakeVisibility) ChangeVisibility(_ context.Context, url, handle string, d time.Duration) error {
	f.calls = append(f.calls, visCall{url, handle, d})
	return f.err
}

func record(id string, receiveCount int) events.SQSMessage {
	return events.SQSMessage{
		MessageId:      id,
		ReceiptHandle:  "handle-" + id,
		Body:           `{"seq":1}`,
		EventSourceARN: queueARN,
		Attributes:     map[string]string{"ApproximateReceiveCount": fmt.Sprint(receiveCount)},
	}
}

func failedIDs(resp events.SQSEventResponse) []string {
	var ids []string
	for _, f := range resp.BatchItemFailures {
		ids = append(ids, f.ItemIdentifier)
	}
	return ids
}

func TestHandleSQSPartialBatch(t *testing.T) {
	ev := events.SQSEvent{Records: []events.SQSMessage{record("ok", 1), record("later", 2), record("dlq", 5)}}
	vis := &fakeVisibility{}
	var seen []Message

	resp := HandleSQS(context.Background(), ev, vis, func(_ context.Context, m Message) Result {
		seen = append(seen, m)
		switch m.ID {
		case "later":
			return Result{Retry: true, Delay: 90 * time.Second}
		case "dlq":
			return Result{Retry: true} // no delay: visible at once, so SQS moves it to the DLQ
		}
		return Result{}
	})

	if got := fmt.Sprint(failedIDs(resp)); got != "[later dlq]" {
		t.Errorf("batch item failures %s, want [later dlq]; \"ok\" must be left for Lambda to delete", got)
	}
	if len(seen) != 3 || seen[1].ReceiveCount != 2 || string(seen[0].Body) != `{"seq":1}` {
		t.Errorf("handler saw %+v", seen)
	}
	url := "https://sqs.eu-west-1.amazonaws.com/123456789012/textract-onboarding-documents"
	want := []visCall{{url, "handle-later", 90 * time.Second}, {url, "handle-dlq", 0}}
	if fmt.Sprint(vis.calls) != fmt.Sprint(want) {
		t.Errorf("visibility calls %v, want %v", vis.calls, want)
	}
}

func TestHandleSQSVisibilityErrorStillRetries(t *testing.T) {
	vis := &fakeVisibility{err: errors.New("AccessDenied")}
	resp := HandleSQS(context.Background(), events.SQSEvent{Records: []events.SQSMessage{record("a", 1)}}, vis,
		func(context.Context, Message) Result { return Result{Retry: true, Delay: time.Minute} })
	if got := fmt.Sprint(failedIDs(resp)); got != "[a]" {
		t.Errorf("failures %s, want [a]", got)
	}
}

func TestHandleSQSStopsWhenOutOfTime(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	calls := 0
	ev := events.SQSEvent{Records: []events.SQSMessage{record("a", 1), record("b", 1), record("c", 1)}}
	resp := HandleSQS(ctx, ev, &fakeVisibility{}, func(context.Context, Message) Result {
		calls++
		cancel() // the deadline passes during the first message
		return Result{}
	})
	if calls != 1 || fmt.Sprint(failedIDs(resp)) != "[b c]" {
		t.Errorf("calls %d, failures %v; want 1 call and [b c] handed back", calls, failedIDs(resp))
	}
}

func TestFromError(t *testing.T) {
	fn := FromError("test", func(_ context.Context, body []byte) error {
		if string(body) == "bad" {
			return errors.New("boom")
		}
		return nil
	})
	if res := fn(context.Background(), Message{Body: []byte("good"), ReceiveCount: 1}); res.Retry {
		t.Errorf("success: %+v", res)
	}
	res := fn(context.Background(), Message{Body: []byte("bad"), ReceiveCount: 3})
	if !res.Retry || res.Delay < 60*time.Second || res.Delay > 120*time.Second {
		t.Errorf("third attempt: %+v, want a retry in 1-2 minutes", res)
	}
}

func TestBackoffCaps(t *testing.T) {
	for n := 1; n <= 40; n++ {
		if d := Backoff(n); d <= 0 || d > 15*time.Minute {
			t.Errorf("Backoff(%d) = %s", n, d)
		}
	}
}

func TestQueueURL(t *testing.T) {
	url, err := QueueURL(queueARN)
	if err != nil || url != "https://sqs.eu-west-1.amazonaws.com/123456789012/textract-onboarding-documents" {
		t.Errorf("got %q, %v", url, err)
	}
	if _, err := QueueURL("arn:aws:s3:::bucket"); err == nil {
		t.Error("an S3 ARN should be rejected")
	}
}
