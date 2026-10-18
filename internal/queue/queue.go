// Package queue is the pipeline's view of a message queue (SQS in production).
//
// Only sending lives here. Receiving is done by AWS: a Lambda event source
// mapping polls the queue and invokes the function with a batch of messages,
// so our code never calls ReceiveMessage itself.
package queue

import "context"

type Sender interface {
	// Send enqueues one message per body. SQS accepts at most 10 per
	// SendMessageBatch call; implementations split larger sends.
	Send(ctx context.Context, bodies ...[]byte) error
}
