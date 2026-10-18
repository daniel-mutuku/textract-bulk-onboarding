package status

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/daniel-mutuku/textract-bulk-onboarding/internal/batch"
	"github.com/daniel-mutuku/textract-bulk-onboarding/internal/batch/memstore"
)

const batchID1 = "6f1c2b9e-8d3a-4c5e-9f70-1a2b3c4d5e6f"

func setup(t *testing.T) (*memstore.Store, http.Handler) {
	t.Helper()
	ctx := context.Background()
	store := memstore.New()
	if err := store.CreateBatch(ctx, batch.Batch{ID: batchID1, OriginalFilename: "forms.zip", Total: 5, ChunkCount: 1}); err != nil {
		t.Fatal(err)
	}
	for seq := 1; seq <= 4; seq++ {
		if err := store.CreateDocument(ctx, batch.Document{BatchID: batchID1, Seq: seq, Filename: "f.pdf", Key: fmt.Sprintf("documents/b/%06d-f.pdf", seq), ContentType: "application/pdf", Status: batch.StatusPending}); err != nil {
			t.Fatal(err)
		}
	}
	if err := store.CreateDocument(ctx, batch.Document{BatchID: batchID1, Seq: 5, Filename: "notes.txt", Status: batch.StatusSkipped, Reason: "not a PDF or image"}); err != nil {
		t.Fatal(err)
	}
	if err := store.CompleteDocument(ctx, batchID1, 1, batch.StatusPending, batch.Extraction{
		Fields: map[string]batch.Field{"surname": {Value: "Mutuku", Confidence: 97.5}},
		RawKey: "textract/raw.json",
		At:     time.Date(2026, 10, 17, 9, 0, 0, 0, time.UTC),
	}); err != nil {
		t.Fatal(err)
	}
	if err := store.FailDocument(ctx, batchID1, 2, "unsupported document"); err != nil {
		t.Fatal(err)
	}

	mux := http.NewServeMux()
	New(store, fakeFiles{}).Register(mux)
	return store, mux
}

type fakeFiles struct{}

func (fakeFiles) URL(_ context.Context, key string) (string, error) {
	return "https://files.example/" + key + "?signature=x", nil
}

func get(t *testing.T, h http.Handler, url string) *httptest.ResponseRecorder {
	t.Helper()
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, url, nil))
	return rec
}

func decode[T any](t *testing.T, rec *httptest.ResponseRecorder) T {
	t.Helper()
	var v T
	if err := json.Unmarshal(rec.Body.Bytes(), &v); err != nil {
		t.Fatalf("decode %q: %v", rec.Body, err)
	}
	return v
}

func TestGetBatch(t *testing.T) {
	store, h := setup(t)

	rec := get(t, h, "/api/batches/"+batchID1)
	if rec.Code != http.StatusOK {
		t.Fatalf("status %d, body %s", rec.Code, rec.Body)
	}
	if rec.Header().Get("Cache-Control") != "no-store" {
		t.Error("polled response should not be cacheable")
	}
	got := decode[batchResponse](t, rec)
	if got.State != batch.StateUnpacking || got.Total != 5 || got.Processed != 1 || got.Failed != 1 ||
		got.Skipped != 1 || got.Pending != 2 || got.Chunks != (chunksResponse{Done: 0, Total: 1}) {
		t.Errorf("got %+v", got)
	}

	// Once the only chunk finishes, the derived state moves on.
	if err := store.FinishChunk(context.Background(), batchID1, 1); err != nil {
		t.Fatal(err)
	}
	if got := decode[batchResponse](t, get(t, h, "/api/batches/"+batchID1)); got.State != batch.StateProcessing {
		t.Errorf("state %s, want processing", got.State)
	}
}

func TestGetBatchErrors(t *testing.T) {
	_, h := setup(t)
	if rec := get(t, h, "/api/batches/00000000-0000-4000-8000-000000000000"); rec.Code != http.StatusNotFound {
		t.Errorf("unknown batch: status %d, want 404", rec.Code)
	}
	if rec := get(t, h, "/api/batches/not-a-uuid"); rec.Code != http.StatusBadRequest {
		t.Errorf("bad id: status %d, want 400", rec.Code)
	}
}

func TestListDocuments(t *testing.T) {
	_, h := setup(t)

	page1 := decode[listResponse](t, get(t, h, "/api/batches/"+batchID1+"/documents?limit=2"))
	if len(page1.Documents) != 2 || page1.Next == nil || *page1.Next != 2 {
		t.Fatalf("page 1: %+v", page1)
	}
	d := page1.Documents[0]
	if d.Seq != 1 || d.Status != batch.StatusExtracted || d.Fields["surname"] != (fieldResponse{"Mutuku", 97.5}) || d.ExtractedAt == nil {
		t.Errorf("doc 1: %+v", d)
	}
	if d := page1.Documents[1]; d.Status != batch.StatusFailed || d.Reason != "unsupported document" {
		t.Errorf("doc 2: %+v", d)
	}

	page2 := decode[listResponse](t, get(t, h, "/api/batches/"+batchID1+"/documents?limit=10&after=2"))
	if len(page2.Documents) != 3 || page2.Next != nil {
		t.Fatalf("page 2: %+v", page2)
	}
	if d := page2.Documents[2]; d.Seq != 5 || d.Status != batch.StatusSkipped {
		t.Errorf("doc 5: %+v", d)
	}
}

func TestListDocumentsRejectsBadParams(t *testing.T) {
	_, h := setup(t)
	for _, q := range []string{"limit=0", "limit=201", "limit=ten", "after=-1"} {
		if rec := get(t, h, "/api/batches/"+batchID1+"/documents?"+q); rec.Code != http.StatusBadRequest {
			t.Errorf("%s: status %d, want 400", q, rec.Code)
		}
	}
}

func TestDocumentFile(t *testing.T) {
	_, h := setup(t)
	base := "/api/batches/" + batchID1 + "/documents/"

	rec := get(t, h, base+"1/file")
	if rec.Code != http.StatusFound || rec.Header().Get("Location") != "https://files.example/documents/b/000001-f.pdf?signature=x" {
		t.Errorf("status %d, Location %q", rec.Code, rec.Header().Get("Location"))
	}
	if rec.Header().Get("Cache-Control") != "no-store" {
		t.Error("the redirect must not be cached: each request signs a fresh URL")
	}

	tests := map[string]int{
		"5/file":  http.StatusNotFound,   // skipped: no file stored
		"99/file": http.StatusNotFound,   // no such document
		"0/file":  http.StatusBadRequest, // seq starts at 1
		"x/file":  http.StatusBadRequest,
	}
	for path, want := range tests {
		if rec := get(t, h, base+path); rec.Code != want {
			t.Errorf("%s: status %d, want %d", path, rec.Code, want)
		}
	}

	// Without a DocumentFiles, previews are off rather than broken.
	mux := http.NewServeMux()
	store, _ := setup(t)
	New(store, nil).Register(mux)
	if rec := get(t, mux, base+"1/file"); rec.Code != http.StatusNotFound {
		t.Errorf("previews disabled: status %d, want 404", rec.Code)
	}
}

func TestListIncludesContentType(t *testing.T) {
	_, h := setup(t)
	page := decode[listResponse](t, get(t, h, "/api/batches/"+batchID1+"/documents?limit=1"))
	if page.Documents[0].ContentType != "application/pdf" {
		t.Errorf("contentType %q", page.Documents[0].ContentType)
	}
}
