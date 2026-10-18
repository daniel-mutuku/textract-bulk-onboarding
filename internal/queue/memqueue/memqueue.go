// Package memqueue is an in-memory queue.Sender for tests and local runs.
package memqueue

import (
	"bytes"
	"context"
	"sync"

	"github.com/daniel-mutuku/textract-bulk-onboarding/internal/queue"
)

type Queue struct {
	mu   sync.Mutex
	msgs [][]byte
}

func New() *Queue {
	return &Queue{}
}

var _ queue.Sender = (*Queue)(nil)

func (q *Queue) Send(_ context.Context, bodies ...[]byte) error {
	q.mu.Lock()
	defer q.mu.Unlock()
	for _, b := range bodies {
		q.msgs = append(q.msgs, bytes.Clone(b))
	}
	return nil
}

// Drain removes and returns every queued message, oldest first.
func (q *Queue) Drain() [][]byte {
	q.mu.Lock()
	defer q.mu.Unlock()
	msgs := q.msgs
	q.msgs = nil
	return msgs
}

func (q *Queue) Len() int {
	q.mu.Lock()
	defer q.mu.Unlock()
	return len(q.msgs)
}
