package upload

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"slices"
	"sync"
	"testing"

	"github.com/daniel-mutuku/textract-bulk-onboarding/internal/storage"
)

const testID = "6f1c2b9e-8d3a-4c5e-9f70-1a2b3c4d5e6f"

// fakeStore keeps multipart uploads in memory and behaves like S3 for the
// cases the handler cares about.
type fakeStore struct {
	mu        sync.Mutex
	next      int
	uploads   map[string]*fakeUpload // by upload ID
	completed map[string][]storage.Part
}

type fakeUpload struct {
	key, contentType string
	meta             map[string]string
}

func newFakeStore() *fakeStore {
	return &fakeStore{uploads: map[string]*fakeUpload{}, completed: map[string][]storage.Part{}}
}

func (f *fakeStore) Create(_ context.Context, key, contentType string, meta map[string]string) (string, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.next++
	id := fmt.Sprintf("upload-%d", f.next)
	f.uploads[id] = &fakeUpload{key: key, contentType: contentType, meta: meta}
	return id, nil
}

func (f *fakeStore) lookup(key, uploadID string) error {
	u, ok := f.uploads[uploadID]
	if !ok || u.key != key {
		return storage.ErrNoSuchUpload
	}
	return nil
}

func (f *fakeStore) PresignPart(_ context.Context, key, uploadID string, partNumber int32) (string, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if err := f.lookup(key, uploadID); err != nil {
		return "", err
	}
	return fmt.Sprintf("https://s3.example/%s?partNumber=%d&uploadId=%s", key, partNumber, uploadID), nil
}

func (f *fakeStore) Complete(_ context.Context, key, uploadID string, parts []storage.Part) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if err := f.lookup(key, uploadID); err != nil {
		return err
	}
	delete(f.uploads, uploadID)
	f.completed[key] = parts
	return nil
}

func (f *fakeStore) Abort(_ context.Context, key, uploadID string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if err := f.lookup(key, uploadID); err != nil {
		return err
	}
	delete(f.uploads, uploadID)
	return nil
}

func newTestServer(t *testing.T) (*fakeStore, *http.ServeMux) {
	t.Helper()
	store := newFakeStore()
	h := New(store, Options{PartSize: 10 << 20, MaxSize: 50 << 30})
	h.newID = func() string { return testID }
	mux := http.NewServeMux()
	h.Register(mux)
	return store, mux
}

func post(t *testing.T, mux http.Handler, url string, body any) *httptest.ResponseRecorder {
	t.Helper()
	b, err := json.Marshal(body)
	if err != nil {
		t.Fatal(err)
	}
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, httptest.NewRequest(http.MethodPost, url, bytes.NewReader(b)))
	return rec
}

func decodeBody[T any](t *testing.T, rec *httptest.ResponseRecorder) T {
	t.Helper()
	var v T
	if err := json.Unmarshal(rec.Body.Bytes(), &v); err != nil {
		t.Fatalf("decode %q: %v", rec.Body.String(), err)
	}
	return v
}

func startUpload(t *testing.T, mux http.Handler) startResponse {
	t.Helper()
	rec := post(t, mux, "/api/uploads", startRequest{Filename: "batch.zip", Size: 25 << 20, ContentType: "application/zip"})
	if rec.Code != http.StatusCreated {
		t.Fatalf("start: status %d, body %s", rec.Code, rec.Body)
	}
	return decodeBody[startResponse](t, rec)
}

func TestStart(t *testing.T) {
	store, mux := newTestServer(t)
	res := startUpload(t, mux)

	wantKey := "uploads/" + testID + "/upload.zip"
	if res.ID != testID || res.Key != wantKey || res.UploadID == "" {
		t.Errorf("got %+v", res)
	}
	// 25 MiB in 10 MiB parts: 10 + 10 + 5.
	if res.PartSize != 10<<20 || res.PartCount != 3 {
		t.Errorf("partSize %d partCount %d, want %d and 3", res.PartSize, res.PartCount, 10<<20)
	}
	u := store.uploads[res.UploadID]
	if u.key != wantKey || u.contentType != "application/zip" || u.meta["original-filename"] != "batch.zip" {
		t.Errorf("store got %+v", u)
	}
}

func TestStartRejects(t *testing.T) {
	tests := []struct {
		name string
		req  startRequest
		want int
	}{
		{"not a zip extension", startRequest{"report.pdf", 1024, "application/pdf"}, http.StatusBadRequest},
		{"zip name, wrong type", startRequest{"sneaky.zip", 1024, "text/html"}, http.StatusBadRequest},
		{"empty file", startRequest{"a.zip", 0, "application/zip"}, http.StatusBadRequest},
		{"negative size", startRequest{"a.zip", -5, "application/zip"}, http.StatusBadRequest},
		{"over max size", startRequest{"a.zip", 51 << 30, "application/zip"}, http.StatusRequestEntityTooLarge},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			store, mux := newTestServer(t)
			rec := post(t, mux, "/api/uploads", tt.req)
			if rec.Code != tt.want {
				t.Fatalf("status %d, want %d; body %s", rec.Code, tt.want, rec.Body)
			}
			if decodeBody[map[string]string](t, rec)["error"] == "" {
				t.Error("missing error message")
			}
			if len(store.uploads) != 0 {
				t.Error("rejected request still created an upload")
			}
		})
	}
}

func TestStartRejectsTooManyParts(t *testing.T) {
	store := newFakeStore()
	h := New(store, Options{PartSize: 5 << 20, MaxSize: 1 << 40})
	mux := http.NewServeMux()
	h.Register(mux)
	// 10,001 parts of 5 MiB.
	rec := post(t, mux, "/api/uploads", startRequest{"big.zip", 10_001 * (5 << 20), "application/zip"})
	if rec.Code != http.StatusRequestEntityTooLarge {
		t.Fatalf("status %d, body %s", rec.Code, rec.Body)
	}
}

func TestStartAcceptsBrowserZipTypes(t *testing.T) {
	for _, ct := range []string{"application/zip", "application/x-zip-compressed", "application/octet-stream", "", "Application/ZIP"} {
		_, mux := newTestServer(t)
		rec := post(t, mux, "/api/uploads", startRequest{"A.ZIP", 1024, ct})
		if rec.Code != http.StatusCreated {
			t.Errorf("content type %q: status %d, body %s", ct, rec.Code, rec.Body)
		}
	}
}

func TestPartURL(t *testing.T) {
	_, mux := newTestServer(t)
	up := startUpload(t, mux)
	base := "/api/uploads/" + up.ID + "/part-url"

	rec := post(t, mux, base, partURLRequest{UploadID: up.UploadID, PartNumber: 2})
	if rec.Code != http.StatusOK {
		t.Fatalf("status %d, body %s", rec.Code, rec.Body)
	}
	want := fmt.Sprintf("https://s3.example/%s?partNumber=2&uploadId=%s", up.Key, up.UploadID)
	if got := decodeBody[partURLResponse](t, rec).URL; got != want {
		t.Errorf("url %q, want %q", got, want)
	}

	tests := []struct {
		name string
		url  string
		req  partURLRequest
		want int
	}{
		{"part 0", base, partURLRequest{up.UploadID, 0}, http.StatusBadRequest},
		{"part 10001", base, partURLRequest{up.UploadID, 10_001}, http.StatusBadRequest},
		{"missing uploadId", base, partURLRequest{"", 1}, http.StatusBadRequest},
		{"unknown uploadId", base, partURLRequest{"nope", 1}, http.StatusNotFound},
		{"id is not a uuid", "/api/uploads/..%2Fsecret/part-url", partURLRequest{up.UploadID, 1}, http.StatusBadRequest},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if rec := post(t, mux, tt.url, tt.req); rec.Code != tt.want {
				t.Errorf("status %d, want %d; body %s", rec.Code, tt.want, rec.Body)
			}
		})
	}
}

func TestComplete(t *testing.T) {
	store, mux := newTestServer(t)
	up := startUpload(t, mux)

	// Parts finish out of order in the browser; the handler must sort them.
	rec := post(t, mux, "/api/uploads/"+up.ID+"/complete", completeRequest{
		UploadID: up.UploadID,
		Parts:    []storage.Part{part(3, `"c"`), part(1, `"a"`), part(2, `"b"`)},
	})
	if rec.Code != http.StatusOK {
		t.Fatalf("status %d, body %s", rec.Code, rec.Body)
	}
	if got := decodeBody[completeResponse](t, rec).Key; got != up.Key {
		t.Errorf("key %q, want %q", got, up.Key)
	}
	want := []storage.Part{part(1, `"a"`), part(2, `"b"`), part(3, `"c"`)}
	if got := store.completed[up.Key]; !slices.Equal(got, want) {
		t.Errorf("store got parts %v, want %v", got, want)
	}

	// Completing again finds no upload.
	rec = post(t, mux, "/api/uploads/"+up.ID+"/complete", completeRequest{UploadID: up.UploadID, Parts: want})
	if rec.Code != http.StatusNotFound {
		t.Errorf("second complete: status %d, want 404", rec.Code)
	}
}

func TestCompleteRejects(t *testing.T) {
	tests := []struct {
		name  string
		parts []storage.Part
	}{
		{"no parts", nil},
		{"duplicate part", []storage.Part{part(1, `"a"`), part(1, `"a"`)}},
		{"missing etag", []storage.Part{part(1, "")}},
		{"part out of range", []storage.Part{part(0, `"a"`)}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			store, mux := newTestServer(t)
			up := startUpload(t, mux)
			rec := post(t, mux, "/api/uploads/"+up.ID+"/complete", completeRequest{UploadID: up.UploadID, Parts: tt.parts})
			if rec.Code != http.StatusBadRequest {
				t.Errorf("status %d, want 400; body %s", rec.Code, rec.Body)
			}
			if _, open := store.uploads[up.UploadID]; !open {
				t.Error("rejected complete should leave the upload open")
			}
		})
	}
}

func TestAbort(t *testing.T) {
	store, mux := newTestServer(t)
	up := startUpload(t, mux)
	url := "/api/uploads/" + up.ID + "/abort"

	if rec := post(t, mux, url, abortRequest{up.UploadID}); rec.Code != http.StatusNoContent {
		t.Fatalf("status %d, body %s", rec.Code, rec.Body)
	}
	if _, open := store.uploads[up.UploadID]; open {
		t.Error("upload still open after abort")
	}
	// Aborting twice is not an error.
	if rec := post(t, mux, url, abortRequest{up.UploadID}); rec.Code != http.StatusNoContent {
		t.Errorf("second abort: status %d, want 204", rec.Code)
	}
}

func TestSanitizeFilename(t *testing.T) {
	tests := map[string]string{
		"batch.zip":             "batch.zip",
		"Q3 report (final).zip": "Q3_report__final_.zip",
		`C:\Users\me\files.zip`: "files.zip",
		"../../etc/passwd.zip":  "passwd.zip",
		"données.zip":           "donn_es.zip",
	}
	for in, want := range tests {
		if got := sanitizeFilename(in); got != want {
			t.Errorf("sanitizeFilename(%q) = %q, want %q", in, got, want)
		}
	}
}

func part(n int32, etag string) storage.Part {
	return storage.Part{PartNumber: n, ETag: etag}
}
