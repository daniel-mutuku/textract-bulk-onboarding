package memstore

import (
	"testing"

	"github.com/daniel-mutuku/textract-bulk-onboarding/internal/batch"
	"github.com/daniel-mutuku/textract-bulk-onboarding/internal/batch/storetest"
)

func TestContract(t *testing.T) {
	storetest.Run(t, func(*testing.T) batch.Store { return New() })
}
