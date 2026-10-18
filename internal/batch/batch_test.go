package batch

import "testing"

func TestState(t *testing.T) {
	tests := []struct {
		name string
		b    Batch
		want State
	}{
		{"chunks still running", Batch{Total: 10, ChunkCount: 3, ChunksDone: 2, Processed: 4}, StateUnpacking},
		{"documents pending", Batch{Total: 10, ChunkCount: 3, ChunksDone: 3, Processed: 4, Skipped: 1}, StateProcessing},
		{"all accounted for", Batch{Total: 10, ChunkCount: 3, ChunksDone: 3, Processed: 7, Failed: 1, Skipped: 2}, StateCompleted},
		{"empty zip", Batch{}, StateCompleted},
		{"unreadable zip", Batch{Error: "zip: not a valid zip file"}, StateFailed},
	}
	for _, tt := range tests {
		if got := tt.b.State(); got != tt.want {
			t.Errorf("%s: State() = %s, want %s", tt.name, got, tt.want)
		}
	}
}

func TestPending(t *testing.T) {
	b := Batch{Total: 10, Processed: 6, Failed: 1, Skipped: 2}
	if got := b.Pending(); got != 1 {
		t.Errorf("Pending() = %d, want 1", got)
	}
}
