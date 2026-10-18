package unzip

import (
	"archive/zip"
	"bytes"
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"

	"github.com/daniel-mutuku/textract-bulk-onboarding/internal/batch"
	"github.com/daniel-mutuku/textract-bulk-onboarding/internal/batch/memstore"
	"github.com/daniel-mutuku/textract-bulk-onboarding/internal/blob/memblob"
	"github.com/daniel-mutuku/textract-bulk-onboarding/internal/job"
	"github.com/daniel-mutuku/textract-bulk-onboarding/internal/queue/memqueue"
)

const (
	uploadsBucket   = "uploads-bucket"
	documentsBucket = "documents-bucket"
	testBatchID     = "6f1c2b9e-8d3a-4c5e-9f70-1a2b3c4d5e6f"
	testKey         = "uploads/" + testBatchID + "/upload.zip"
)

var (
	pdfBytes  = []byte("%PDF-1.7\n1 0 obj << /Type /Catalog >> endobj\n%%EOF\n")
	pngBytes  = append([]byte("\x89PNG\r\n\x1a\n"), bytes.Repeat([]byte{1}, 64)...)
	jpegBytes = append([]byte{0xFF, 0xD8, 0xFF, 0xE0}, bytes.Repeat([]byte{2}, 64)...)
)

type entry struct {
	name   string
	data   []byte
	stored bool   // no compression, so tests can corrupt the raw bytes
	flags  uint16 // e.g. 0x1 marks an entry as encrypted
}

func buildZip(t *testing.T, entries []entry) []byte {
	t.Helper()
	var buf bytes.Buffer
	zw := zip.NewWriter(&buf)
	for _, e := range entries {
		h := &zip.FileHeader{Name: e.name, Method: zip.Deflate, Flags: e.flags}
		if e.stored {
			h.Method = zip.Store
		}
		w, err := zw.CreateHeader(h)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := w.Write(e.data); err != nil {
			t.Fatal(err)
		}
	}
	if err := zw.Close(); err != nil {
		t.Fatal(err)
	}
	return buf.Bytes()
}

type pipeline struct {
	blobs   *memblob.Store
	store   *memstore.Store
	chunks  *memqueue.Queue
	docs    *memqueue.Queue
	planner *Planner
	chunker *Chunker
}

func newPipeline(lim Limits) *pipeline {
	p := &pipeline{blobs: memblob.New(), store: memstore.New(), chunks: memqueue.New(), docs: memqueue.New()}
	p.planner = &Planner{Blobs: p.blobs, Store: p.store, Chunks: p.chunks, Limits: lim}
	p.chunker = &Chunker{Blobs: p.blobs, Store: p.store, Documents: p.docs, DocumentsBucket: documentsBucket, Limits: lim}
	return p
}

func (p *pipeline) upload(zipBytes []byte) {
	p.blobs.Add(uploadsBucket, testKey, bytes.NewReader(zipBytes), int64(len(zipBytes)),
		map[string]string{"original-filename": "applications.zip"})
}

// runChunks processes every queued chunk message and returns them.
func (p *pipeline) runChunks(t *testing.T) []job.Chunk {
	t.Helper()
	var chunks []job.Chunk
	for _, body := range p.chunks.Drain() {
		var j job.Chunk
		if err := json.Unmarshal(body, &j); err != nil {
			t.Fatal(err)
		}
		chunks = append(chunks, j)
		if err := p.chunker.HandleMessage(context.Background(), body); err != nil {
			t.Fatalf("chunk %d: %v", j.Chunk, err)
		}
	}
	return chunks
}

func (p *pipeline) batch(t *testing.T) batch.Batch {
	t.Helper()
	b, err := p.store.GetBatch(context.Background(), testBatchID)
	if err != nil {
		t.Fatal(err)
	}
	return b
}

func (p *pipeline) doc(t *testing.T, seq int) batch.Document {
	t.Helper()
	d, err := p.store.GetDocument(context.Background(), testBatchID, seq)
	if err != nil {
		t.Fatalf("seq %d: %v", seq, err)
	}
	return d
}

func docMessages(t *testing.T, q *memqueue.Queue) []job.Document {
	t.Helper()
	var out []job.Document
	for _, body := range q.Drain() {
		var d job.Document
		if err := json.Unmarshal(body, &d); err != nil {
			t.Fatal(err)
		}
		out = append(out, d)
	}
	return out
}

// mixedZip has one of every kind of entry the filter must handle.
func mixedZip(t *testing.T) []byte {
	big := append(slices.Clone(pdfBytes), make([]byte, 11<<20)...) // compresses to almost nothing
	return buildZip(t, []entry{
		{name: "forms/"}, // directory: not a document at all
		{name: "forms/form 1.pdf", data: pdfBytes},            // seq 1
		{name: "forms/form-2.PNG", data: pngBytes},            // seq 2
		{name: "forms/form-3.jpeg", data: jpegBytes},          // seq 3
		{name: "forms/notes.txt", data: []byte("hello")},      // seq 4
		{name: "forms/fake.pdf", data: []byte("not a pdf")},   // seq 5
		{name: "forms/huge.pdf", data: big},                   // seq 6
		{name: "forms/empty.pdf", data: nil},                  // seq 7
		{name: "__MACOSX/forms/._form-1.pdf", data: pdfBytes}, // seq 8
		{name: "forms/.DS_Store", data: []byte{0, 0, 0, 1}},   // seq 9
		{name: "more.zip", data: []byte("PK")},                // seq 10
		{name: "locked.pdf", data: pdfBytes, flags: 0x1},      // seq 11
		{name: "README", data: []byte("x")},                   // seq 12
	})
}

func TestPlanAndUnpack(t *testing.T) {
	lim := DefaultLimits()
	lim.MaxChunkEntries = 5
	p := newPipeline(lim)
	p.upload(mixedZip(t))

	if err := p.planner.Plan(context.Background(), uploadsBucket, testKey); err != nil {
		t.Fatal(err)
	}
	b := p.batch(t)
	if b.Total != 12 || b.ChunkCount != 3 || b.OriginalFilename != "applications.zip" || b.State() != batch.StateUnpacking {
		t.Fatalf("after plan: %+v (state %s)", b, b.State())
	}

	chunks := p.runChunks(t)
	ranges := make([]string, len(chunks))
	for i, c := range chunks {
		ranges[i] = strings.Join([]string{itoa(c.FirstSeq), itoa(c.LastSeq)}, "-")
	}
	if got := strings.Join(ranges, " "); got != "1-5 6-10 11-12" {
		t.Errorf("chunk ranges %q, want \"1-5 6-10 11-12\"", got)
	}

	b = p.batch(t)
	if b.ChunksDone != 3 || b.Skipped != 9 || b.Failed != 0 || b.Pending() != 3 || b.State() != batch.StateProcessing {
		t.Errorf("after unpack: %+v (state %s)", b, b.State())
	}

	wantReasons := map[int]string{
		4:  "unsupported file type .txt",
		5:  "content is not a valid PDF file",
		6:  "over the 10 MB limit",
		7:  "empty file",
		8:  "macOS metadata file",
		9:  "hidden system file",
		10: "nested zip",
		11: "password-protected",
		12: "no file extension",
	}
	for seq, want := range wantReasons {
		d := p.doc(t, seq)
		if d.Status != batch.StatusSkipped || !strings.Contains(d.Reason, want) || d.Key != "" {
			t.Errorf("seq %d (%s): status %s, reason %q; want skipped with %q", seq, d.Filename, d.Status, d.Reason, want)
		}
	}

	wantDocs := []struct {
		seq         int
		key         string
		contentType string
		data        []byte
	}{
		{1, "documents/" + testBatchID + "/000001-form_1.pdf", "application/pdf", pdfBytes},
		{2, "documents/" + testBatchID + "/000002-form-2.PNG", "image/png", pngBytes},
		{3, "documents/" + testBatchID + "/000003-form-3.jpeg", "image/jpeg", jpegBytes},
	}
	msgs := docMessages(t, p.docs)
	if len(msgs) != 3 {
		t.Fatalf("%d extraction messages, want 3", len(msgs))
	}
	for i, w := range wantDocs {
		d := p.doc(t, w.seq)
		if d.Status != batch.StatusPending || d.Key != w.key || d.ContentType != w.contentType || d.Size != int64(len(w.data)) {
			t.Errorf("seq %d: %+v", w.seq, d)
		}
		data, ct, ok := p.blobs.Get(documentsBucket, w.key)
		if !ok || !bytes.Equal(data, w.data) || ct != w.contentType {
			t.Errorf("seq %d: stored object ok=%v type=%q matches=%v", w.seq, ok, ct, bytes.Equal(data, w.data))
		}
		want := job.Document{V: job.Version, BatchID: testBatchID, Seq: w.seq, Bucket: documentsBucket, Key: w.key}
		if msgs[i] != want {
			t.Errorf("message %d = %+v, want %+v", i, msgs[i], want)
		}
	}
}

func TestChunkRetryIsIdempotent(t *testing.T) {
	p := newPipeline(DefaultLimits())
	p.upload(mixedZip(t))
	if err := p.planner.Plan(context.Background(), uploadsBucket, testKey); err != nil {
		t.Fatal(err)
	}
	chunkMsgs := p.chunks.Drain()
	if len(chunkMsgs) != 1 {
		t.Fatalf("%d chunks, want 1", len(chunkMsgs))
	}
	for range 2 { // SQS redelivers the chunk message
		if err := p.chunker.HandleMessage(context.Background(), chunkMsgs[0]); err != nil {
			t.Fatal(err)
		}
	}
	b := p.batch(t)
	if b.ChunksDone != 1 || b.Skipped != 9 || b.Pending() != 3 {
		t.Errorf("after retry: %+v", b)
	}
	// Pending documents are enqueued again; the extract worker ignores duplicates.
	if got := len(docMessages(t, p.docs)); got != 6 {
		t.Errorf("%d extraction messages, want 6 (3 per run)", got)
	}

	// Once a document is extracted, a retried chunk leaves it alone.
	ctx := context.Background()
	if err := p.store.CompleteDocument(ctx, testBatchID, 1, batch.StatusPending, batch.Extraction{}); err != nil {
		t.Fatal(err)
	}
	if err := p.chunker.HandleMessage(ctx, chunkMsgs[0]); err != nil {
		t.Fatal(err)
	}
	if got := len(docMessages(t, p.docs)); got != 2 {
		t.Errorf("%d extraction messages after doc 1 was extracted, want 2", got)
	}
}

func TestPlanRetryKeepsBatch(t *testing.T) {
	p := newPipeline(DefaultLimits())
	p.upload(mixedZip(t))
	ctx := context.Background()
	for range 2 {
		if err := p.planner.Plan(ctx, uploadsBucket, testKey); err != nil {
			t.Fatal(err)
		}
	}
	if b := p.batch(t); b.Total != 12 || b.ChunkCount != 1 {
		t.Errorf("%+v", b)
	}
}

func TestCorruptEntryIsFailed(t *testing.T) {
	zipBytes := buildZip(t, []entry{{name: "scan.pdf", data: pdfBytes, stored: true}})
	// Flip a byte inside the stored content; the CRC-32 check catches it.
	i := bytes.Index(zipBytes, []byte("Catalog"))
	zipBytes[i] ^= 0xFF

	p := newPipeline(DefaultLimits())
	p.upload(zipBytes)
	if err := p.planner.Plan(context.Background(), uploadsBucket, testKey); err != nil {
		t.Fatal(err)
	}
	p.runChunks(t)
	d := p.doc(t, 1)
	if d.Status != batch.StatusFailed || !strings.Contains(d.Reason, "checksum") {
		t.Errorf("got status %s, reason %q", d.Status, d.Reason)
	}
	if b := p.batch(t); b.Failed != 1 || b.State() != batch.StateCompleted {
		t.Errorf("%+v", b)
	}
	if _, _, ok := p.blobs.Get(documentsBucket, documentKey(testBatchID, 1, "scan.pdf")); ok {
		t.Error("a corrupt entry should not be written to the documents bucket")
	}
}

func TestNotAZip(t *testing.T) {
	p := newPipeline(DefaultLimits())
	p.upload([]byte("this is not a zip file at all"))
	if err := p.planner.Plan(context.Background(), uploadsBucket, testKey); err != nil {
		t.Fatalf("an unreadable zip should be recorded, not retried: %v", err)
	}
	b := p.batch(t)
	if b.State() != batch.StateFailed || !strings.Contains(b.Error, "not a readable zip") || p.chunks.Len() != 0 {
		t.Errorf("%+v", b)
	}
}

func TestChunksSplitByBytes(t *testing.T) {
	var entries []entry
	for i := range 6 {
		data := append(slices.Clone(pdfBytes), bytes.Repeat([]byte{byte(i)}, 1000)...)
		entries = append(entries, entry{name: "f" + itoa(i) + ".pdf", data: data, stored: true})
	}
	entries = append(entries, entry{name: "skip.txt", data: bytes.Repeat([]byte("x"), 5000), stored: true})
	zr, err := zip.NewReader(bytes.NewReader(buildZip(t, entries)), int64(len(buildZip(t, entries))))
	if err != nil {
		t.Fatal(err)
	}
	lim := DefaultLimits()
	lim.MaxChunkBytes = 2500 // fits two ~1 KB documents
	chunks := planChunks(documentEntries(zr), lim)
	var got []string
	for _, c := range chunks {
		got = append(got, itoa(c.FirstSeq)+"-"+itoa(c.LastSeq))
	}
	// The skipped .txt costs no bytes, so it rides along in the last chunk.
	if strings.Join(got, " ") != "1-2 3-4 5-7" {
		t.Errorf("chunks %v, want [1-2 3-4 5-7]", got)
	}
}

func TestHandleS3Event(t *testing.T) {
	p := newPipeline(DefaultLimits())
	p.upload(mixedZip(t))
	ctx := context.Background()

	// What S3 actually sends, abridged.
	event := `{"Records":[{"eventVersion":"2.1","eventSource":"aws:s3","awsRegion":"eu-west-1",
		"eventName":"ObjectCreated:CompleteMultipartUpload",
		"s3":{"bucket":{"name":"` + uploadsBucket + `"},
		      "object":{"key":"uploads/` + testBatchID + `/upload.zip","size":1234}}}]}`
	if err := p.planner.HandleS3Event(ctx, []byte(event)); err != nil {
		t.Fatal(err)
	}
	if p.chunks.Len() != 1 {
		t.Errorf("%d chunk messages, want 1", p.chunks.Len())
	}

	test := `{"Service":"Amazon S3","Event":"s3:TestEvent","Time":"2026-10-17T10:00:00.000Z","Bucket":"x"}`
	if err := p.planner.HandleS3Event(ctx, []byte(test)); err != nil {
		t.Errorf("test event: %v", err)
	}
	other := `{"Records":[{"s3":{"bucket":{"name":"b"},"object":{"key":"somewhere/else.zip"}}}]}`
	if err := p.planner.HandleS3Event(ctx, []byte(other)); err != nil {
		t.Errorf("non-upload key should be ignored: %v", err)
	}
}

func TestParseS3EventDecodesKeys(t *testing.T) {
	refs, err := ParseS3Event([]byte(`{"Records":[{"s3":{"bucket":{"name":"b"},"object":{"key":"a/my+file%281%29.zip","size":5}}}]}`))
	if err != nil {
		t.Fatal(err)
	}
	if len(refs) != 1 || refs[0].Key != "a/my file(1).zip" || refs[0].Size != 5 {
		t.Errorf("%+v", refs)
	}
}

func TestBatchIDFromKey(t *testing.T) {
	tests := map[string]bool{
		"uploads/" + testBatchID + "/upload.zip":       true,
		"uploads/not-a-uuid/upload.zip":                false,
		"uploads/" + testBatchID + "/other.zip":        false,
		"documents/" + testBatchID + "/upload.zip":     false,
		"uploads/" + testBatchID + "/upload.zip/extra": false,
	}
	for key, want := range tests {
		id, ok := BatchIDFromKey(key)
		if ok != want || (ok && id != testBatchID) {
			t.Errorf("%s: got (%q, %v), want ok=%v", key, id, ok, want)
		}
	}
}

func TestSafeName(t *testing.T) {
	long := strings.Repeat("a", 150) + ".pdf"
	tests := map[string]string{
		"form 1.pdf":          "form_1.pdf",
		"Ombi_la_Mteja.jpg":   "Ombi_la_Mteja.jpg",
		"fomu ya mteja ✓.png": "fomu_ya_mteja__.png",
		long:                  strings.Repeat("a", 96) + ".pdf",
	}
	for in, want := range tests {
		if got := safeName(in); got != want {
			t.Errorf("safeName(%q) = %q, want %q", in, got, want)
		}
	}
}

// The sample forms in testdata/forms go through the real filter untouched.
func TestSampleForms(t *testing.T) {
	paths, _ := filepath.Glob("../../testdata/forms/*.jpeg")
	if len(paths) == 0 {
		t.Skip("no sample forms in testdata/forms")
	}
	var entries []entry
	for _, p := range paths {
		data, err := os.ReadFile(p)
		if err != nil {
			t.Fatal(err)
		}
		entries = append(entries, entry{name: "scans/" + filepath.Base(p), data: data})
	}
	p := newPipeline(DefaultLimits())
	p.upload(buildZip(t, entries))
	if err := p.planner.Plan(context.Background(), uploadsBucket, testKey); err != nil {
		t.Fatal(err)
	}
	p.runChunks(t)
	b := p.batch(t)
	if b.Total != len(paths) || b.Pending() != len(paths) || b.Skipped != 0 {
		t.Errorf("%+v", b)
	}
}

func itoa(n int) string {
	b, _ := json.Marshal(n)
	return string(b)
}
