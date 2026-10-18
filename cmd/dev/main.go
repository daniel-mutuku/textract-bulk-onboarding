// Command dev runs the pipeline in one process against in-memory fakes, so it
// works with no AWS account. The fakes live in memory, which is why every
// stage has to run in this one process rather than as separate programs.
//
//	go run ./cmd/dev -zip forms.zip   unzip and extract a local zip with the real pipeline code
//	go run ./cmd/dev                  serve a demo batch that advances on a timer
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"time"

	"github.com/google/uuid"

	"github.com/daniel-mutuku/textract-bulk-onboarding/internal/batch"
	"github.com/daniel-mutuku/textract-bulk-onboarding/internal/batch/memstore"
	"github.com/daniel-mutuku/textract-bulk-onboarding/internal/blob/memblob"
	"github.com/daniel-mutuku/textract-bulk-onboarding/internal/extract"
	"github.com/daniel-mutuku/textract-bulk-onboarding/internal/queue/memqueue"
	"github.com/daniel-mutuku/textract-bulk-onboarding/internal/status"
	"github.com/daniel-mutuku/textract-bulk-onboarding/internal/unzip"
)

const demoBatchID = "00000000-0000-4000-8000-000000000001"

func main() {
	addr := flag.String("addr", ":5180", "address to listen on")
	zipPath := flag.String("zip", "", "a local zip to run through the pipeline")
	chunkEntries := flag.Int("chunk-entries", unzip.DefaultLimits().MaxChunkEntries, "max documents per unzip chunk")
	wopt := workerOptions{maxAttempts: 5}
	flag.IntVar(&wopt.concurrency, "workers", 4, "documents extracted at once (the Lambda's maximum_concurrency)")
	flag.DurationVar(&wopt.latency, "latency", 1500*time.Millisecond, "simulated Textract time per document")
	flag.Float64Var(&wopt.failRate, "fail-rate", 0, "fraction of Textract calls that fail with throttling, e.g. 0.2")
	flag.Float64Var(&wopt.backoffScale, "backoff-scale", 0.02, "retry delays are multiplied by this locally (0.02: 30s becomes 0.6s)")
	docs := flag.Int("docs", 40, "documents in the demo batch")
	every := flag.Duration("every", time.Second, "how often the demo finishes a document")
	flag.Parse()

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	defer stop()
	store := memstore.New()
	var id string
	var files *devFiles // only the -zip mode has real files to show
	var err error
	if *zipPath != "" {
		id, files, err = runZip(ctx, store, *zipPath, *chunkEntries, wopt)
	} else {
		id, err = startDemo(ctx, store, *docs, *every)
	}
	if err == nil {
		err = serve(ctx, store, *addr, id, files)
	}
	if err != nil {
		slog.Error("dev stopped", "err", err)
		os.Exit(1)
	}
}

const (
	uploadsBucket   = "local-uploads"
	documentsBucket = "local-documents"
)

// runZip does locally what S3, SQS and the two unzip Lambdas do in AWS.
func runZip(ctx context.Context, store *memstore.Store, path string, chunkEntries int, wopt workerOptions) (string, *devFiles, error) {
	f, err := os.Open(path)
	if err != nil {
		return "", nil, err
	}
	info, err := f.Stat()
	if err != nil {
		return "", nil, err
	}
	// The zip is read in place from disk, the way the S3 store reads it with ranged GETs.
	blobs := memblob.New()
	id := uuid.NewString()
	key := "uploads/" + id + "/upload.zip"
	blobs.Add(uploadsBucket, key, f, info.Size(), map[string]string{"original-filename": filepath.Base(path)})

	lim := unzip.DefaultLimits()
	lim.MaxChunkEntries = chunkEntries
	chunks, docs := memqueue.New(), memqueue.New()
	planner := &unzip.Planner{Blobs: blobs, Store: store, Chunks: chunks, Limits: lim}
	chunker := &unzip.Chunker{Blobs: blobs, Store: store, Documents: docs, DocumentsBucket: documentsBucket, Limits: lim}

	start := time.Now()
	if err := planner.Plan(ctx, uploadsBucket, key); err != nil {
		return "", nil, err
	}
	// In AWS each chunk message invokes its own Lambda, in parallel.
	for _, body := range chunks.Drain() {
		if err := chunker.HandleMessage(ctx, body); err != nil {
			return "", nil, err
		}
	}

	b, err := store.GetBatch(ctx, id)
	if err != nil {
		return "", nil, err
	}
	fmt.Printf("unpacked %s in %s\n", filepath.Base(path), time.Since(start).Round(time.Millisecond))
	fmt.Printf("  batch      %s (%s)\n", id, b.State())
	fmt.Printf("  documents  %d total: %d pending, %d skipped, %d failed\n", b.Total, b.Pending(), b.Skipped, b.Failed)
	fmt.Printf("  chunks     %d\n", b.ChunkCount)
	fmt.Printf("  queued     %d extraction messages\n\n", docs.Len())
	printSkipped(ctx, store, id)

	worker := &extract.Worker{
		Store:       store,
		Blobs:       blobs,
		Textract:    devTextract(blobs, wopt),
		Queries:     extract.Queries,
		MaxAttempts: wopt.maxAttempts,
		Now:         time.Now,
	}
	go runWorkers(ctx, worker, docs.Drain(), wopt)
	fmt.Printf("extracting with %d workers in the background (fake Textract, %s each)\n\n", wopt.concurrency, wopt.latency)
	return id, &devFiles{blobs: blobs}, nil
}

func printSkipped(ctx context.Context, store *memstore.Store, id string) {
	after := 0
	for {
		page, next, err := store.ListDocuments(ctx, id, after, 200)
		if err != nil {
			return
		}
		for _, d := range page {
			if d.Status == batch.StatusSkipped || d.Status == batch.StatusFailed {
				fmt.Printf("  %-7s %4d  %-40s %s\n", d.Status, d.Seq, d.Filename, d.Reason)
			}
		}
		if next == 0 {
			fmt.Println()
			return
		}
		after = next
	}
}

func startDemo(ctx context.Context, store *memstore.Store, docs int, every time.Duration) (string, error) {
	if err := seedDemo(ctx, store, docs); err != nil {
		return "", err
	}
	go advanceDemo(ctx, store, docs, every)
	fmt.Printf("demo batch of %d documents, one finishing every %s\n\n", docs, every)
	return demoBatchID, nil
}

// serve runs the status API (and the page) until ctx ends.
func serve(ctx context.Context, store batch.Store, addr, batchID string, files *devFiles) error {
	mux := http.NewServeMux()
	if files != nil {
		status.New(store, files).Register(mux)
		mux.Handle("GET /dev/files/{key...}", files)
	} else {
		status.New(store, nil).Register(mux)
	}
	mux.Handle("GET /", http.FileServer(http.Dir("web")))

	srv := &http.Server{Addr: addr, Handler: mux, ReadHeaderTimeout: 10 * time.Second}
	go func() {
		<-ctx.Done()
		srv.Shutdown(context.Background())
	}()

	base := "http://localhost" + addr
	fmt.Printf("dev server on %s (in-memory store)\n\n", base)
	fmt.Printf("  curl -s %s/api/batches/%s | jq\n", base, batchID)
	fmt.Printf("  curl -s '%s/api/batches/%s/documents?limit=5' | jq\n\n", base, batchID)
	if err := srv.ListenAndServe(); !errors.Is(err, http.ErrServerClosed) {
		return err
	}
	return nil
}

// seedDemo creates a batch the way the unzip stage will: every 10th entry is
// skipped, the rest are pending, and the single chunk is still running.
func seedDemo(ctx context.Context, store batch.Store, n int) error {
	err := store.CreateBatch(ctx, batch.Batch{
		ID:               demoBatchID,
		SourceKey:        "uploads/" + demoBatchID + "/upload.zip",
		OriginalFilename: "cic-applications-demo.zip",
		Total:            n,
		ChunkCount:       1,
	})
	if err != nil {
		return err
	}
	for seq := 1; seq <= n; seq++ {
		d := batch.Document{
			BatchID:     demoBatchID,
			Seq:         seq,
			Filename:    fmt.Sprintf("applications/form-%03d.pdf", seq),
			Key:         fmt.Sprintf("documents/%s/%06d-form-%03d.pdf", demoBatchID, seq, seq),
			Size:        180_000,
			ContentType: "application/pdf",
			Status:      batch.StatusPending,
		}
		if seq%10 == 0 {
			d.Filename = fmt.Sprintf("applications/readme-%d.txt", seq)
			d.Key, d.Size, d.ContentType = "", 512, "text/plain"
			d.Status, d.Reason = batch.StatusSkipped, "not a PDF or image"
		}
		if err := store.CreateDocument(ctx, d); err != nil {
			return err
		}
	}
	return nil
}

// advanceDemo finishes the chunk, then one document per tick, failing every 13th.
func advanceDemo(ctx context.Context, store batch.Store, n int, every time.Duration) {
	ticker := time.NewTicker(every)
	defer ticker.Stop()
	seq := 0
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
		if seq == 0 {
			store.FinishChunk(ctx, demoBatchID, 1)
		}
		for seq++; seq <= n && seq%10 == 0; seq++ {
		}
		if seq > n {
			return
		}
		if seq%13 == 0 {
			store.FailDocument(ctx, demoBatchID, seq, "Textract: UnsupportedDocumentException")
			continue
		}
		store.CompleteDocument(ctx, demoBatchID, seq, batch.StatusPending, batch.Extraction{
			Fields: demoFields(seq),
			RawKey: fmt.Sprintf("textract/%s/%06d.json", demoBatchID, seq),
			At:     time.Now(),
		})
	}
}

func demoFields(seq int) map[string]batch.Field {
	return map[string]batch.Field{
		"member_number":         {Value: fmt.Sprintf("M%05d", 40000+seq), Confidence: 91},
		"surname":               {Value: "Wanjiru", Confidence: 98.2},
		"other_names":           {Value: "Grace Achieng", Confidence: 96.4},
		"date_of_birth":         {Value: "14/03/1990", Confidence: 93.1},
		"id_passport_number":    {Value: fmt.Sprintf("%08d", 28000000+seq), Confidence: 95.7},
		"kra_pin":               {Value: "A012345678Z", Confidence: 89.9},
		"mobile_number":         {Value: "0712 345 678", Confidence: 94.3},
		"email":                 {Value: "grace.w@example.com", Confidence: 87.6},
		"postal_address":        {Value: "P.O. Box 1234", Confidence: 90.2},
		"occupation":            {Value: "Accountant", Confidence: 92.8},
		"bank_name":             {Value: "Equity Bank", Confidence: 97.1},
		"bank_branch":           {Value: "Westlands", Confidence: 95},
		"account_name":          {Value: "Grace A. Wanjiru", Confidence: 93.4},
		"account_number":        {Value: "0180291234567", Confidence: 72.5},
		"total_amount_invested": {Value: "50,000", Confidence: 88.8},
	}
}
