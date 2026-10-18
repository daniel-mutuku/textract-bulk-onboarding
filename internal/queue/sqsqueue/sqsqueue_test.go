package sqsqueue

import (
	"context"
	"fmt"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/google/uuid"

	"github.com/daniel-mutuku/textract-bulk-onboarding/internal/awstest"
)

// Sends 23 messages (batches of 10, 10, 3) to a temporary queue and reads
// them all back.
func TestRealSend(t *testing.T) {
	client := sqs.NewFromConfig(awstest.Config(t))
	ctx := context.Background()

	name := "textract-onboarding-test-" + uuid.NewString()[:8]
	created, err := client.CreateQueue(ctx, &sqs.CreateQueueInput{
		QueueName: &name,
		Tags:      map[string]string{"Project": "textract-onboarding"},
	})
	if err != nil {
		t.Fatal(err)
	}
	url := aws.ToString(created.QueueUrl)
	t.Cleanup(func() { client.DeleteQueue(context.Background(), &sqs.DeleteQueueInput{QueueUrl: &url}) })

	want := map[string]bool{}
	var bodies [][]byte
	for i := range 23 {
		b := fmt.Sprintf(`{"seq":%d}`, i+1)
		want[b] = true
		bodies = append(bodies, []byte(b))
	}
	if err := New(client, url).Send(ctx, bodies...); err != nil {
		t.Fatal(err)
	}

	got := map[string]bool{}
	for range 10 {
		out, err := client.ReceiveMessage(ctx, &sqs.ReceiveMessageInput{
			QueueUrl: &url, MaxNumberOfMessages: 10, WaitTimeSeconds: 2,
		})
		if err != nil {
			t.Fatal(err)
		}
		for _, m := range out.Messages {
			got[aws.ToString(m.Body)] = true
		}
		if len(got) == len(want) {
			break
		}
	}
	if len(got) != len(want) {
		t.Errorf("received %d distinct messages, want %d", len(got), len(want))
	}
}
