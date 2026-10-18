package main

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"log/slog"
	"math/rand/v2"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/daniel-mutuku/textract-bulk-onboarding/internal/blob/memblob"
	"github.com/daniel-mutuku/textract-bulk-onboarding/internal/extract"
	"github.com/daniel-mutuku/textract-bulk-onboarding/internal/extract/faketextract"
)

type workerOptions struct {
	concurrency  int
	latency      time.Duration
	failRate     float64
	backoffScale float64
	maxAttempts  int
}

// runWorkers stands in for SQS + the extract Lambda's event source mapping:
// `concurrency` goroutines take messages (the maximum_concurrency setting),
// and a retried message comes back after its backoff delay, scaled down so
// local runs don't wait minutes. Past maxAttempts it "moves to the DLQ".
func runWorkers(ctx context.Context, w *extract.Worker, bodies [][]byte, opt workerOptions) {
	type delivery struct {
		body  []byte
		count int
	}
	queue := make(chan delivery, len(bodies))
	var outstanding sync.WaitGroup
	outstanding.Add(len(bodies))
	for _, b := range bodies {
		queue <- delivery{b, 1}
	}

	for range opt.concurrency {
		go func() {
			for {
				select {
				case <-ctx.Done():
					return
				case d := <-queue:
					out := w.Handle(ctx, extract.Message{Body: d.body, ReceiveCount: d.count})
					switch {
					case !out.Retry:
						outstanding.Done()
					case d.count >= opt.maxAttempts:
						slog.Warn("moved to dead-letter queue", "attempts", d.count)
						outstanding.Done()
					default:
						delay := time.Duration(float64(out.Delay) * opt.backoffScale)
						time.AfterFunc(delay, func() { queue <- delivery{d.body, d.count + 1} })
					}
				}
			}
		}()
	}
	outstanding.Wait()
	fmt.Println("\nextraction finished; the status API keeps serving the results")
}

// devTextract replays the real responses in testdata/textract for the sample
// forms (matched by file content), and makes up plausible answers for
// anything else. It can add latency and simulated throttling.
func devTextract(blobs *memblob.Store, opt workerOptions) *faketextract.Analyzer {
	fixtures := map[[32]byte]string{}
	forms, _ := filepath.Glob("testdata/forms/*.jpeg")
	for _, f := range forms {
		data, err := os.ReadFile(f)
		resp := filepath.Join("testdata/textract", strings.TrimSuffix(filepath.Base(f), ".jpeg")+".json")
		if _, statErr := os.Stat(resp); err == nil && statErr == nil {
			fixtures[sha256.Sum256(data)] = resp
		}
	}

	return &faketextract.Analyzer{Respond: func(bucket, key string, _ int) ([]byte, error) {
		time.Sleep(opt.latency)
		if rand.Float64() < opt.failRate {
			return nil, errors.New("ThrottlingException: Rate exceeded (simulated)")
		}
		data, _, ok := blobs.Get(bucket, key)
		if !ok {
			return nil, extract.Permanent(errors.New("InvalidS3ObjectException: object not found"))
		}
		if path, ok := fixtures[sha256.Sum256(data)]; ok {
			return os.ReadFile(path)
		}
		return syntheticResponse(), nil
	}}
}

func syntheticResponse() []byte {
	names := []string{"Grace Wanjiru", "Brian Otieno", "Faith Chebet", "Kevin Mutua", "Aisha Hassan"}
	towns := []string{"Nairobi", "Mombasa", "Eldoret", "Nakuru", "Kisumu"}
	pick := func(xs []string) string { return xs[rand.N(len(xs))] }
	conf := func() float64 { return float64(60+rand.N(40)) + rand.Float64() }
	name := pick(names)
	a := func(v string) []faketextract.Answer { return []faketextract.Answer{{Text: v, Confidence: conf()}} }
	return faketextract.Response(map[string][]faketextract.Answer{
		"member_number":         a(fmt.Sprintf("KE/2026/IND/%03d", rand.N(1000))),
		"full_name":             a(name),
		"date_of_birth":         a(fmt.Sprintf("%02d/%02d/19%02d", 1+rand.N(28), 1+rand.N(12), 60+rand.N(40))),
		"id_passport_number":    a(fmt.Sprintf("%08d", rand.N(100_000_000))),
		"kra_pin":               a(fmt.Sprintf("A%09dZ", rand.N(1_000_000_000))),
		"mobile_number":         a(fmt.Sprintf("07%08d", rand.N(100_000_000))),
		"email":                 a(strings.ToLower(strings.ReplaceAll(name, " ", ".")) + "@example.com"),
		"postal_address":        a(fmt.Sprintf("P.O. Box %d", 100+rand.N(9000))),
		"town":                  a(pick(towns)),
		"occupation":            a(pick([]string{"Teacher", "Nurse", "Engineer", "Accountant", "Farmer"})),
		"account_name":          a(name),
		"account_number":        a(fmt.Sprintf("%012d", rand.N(1_000_000_000_000))),
		"bank_name":             a(pick([]string{"Equity Bank", "KCB Bank", "Co-operative Bank", "NCBA Bank"})),
		"bank_branch":           a(pick(towns)),
		"total_amount_invested": a(fmt.Sprintf("%d,000", 5+rand.N(500))),
	})
}
