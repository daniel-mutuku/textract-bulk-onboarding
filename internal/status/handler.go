// Package status serves the read-only batch endpoints the upload page polls.
package status

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"strconv"
	"time"

	"github.com/google/uuid"

	"github.com/daniel-mutuku/textract-bulk-onboarding/internal/batch"
	"github.com/daniel-mutuku/textract-bulk-onboarding/internal/httpx"
)

const (
	defaultLimit = 50
	maxLimit     = 200
)

// DocumentFiles gives the browser temporary access to a stored document. In
// production it is a presigned S3 GET URL, so the bucket stays private.
type DocumentFiles interface {
	URL(ctx context.Context, key string) (string, error)
}

type Handler struct {
	store batch.Store
	files DocumentFiles // nil disables document previews
}

func New(store batch.Store, files DocumentFiles) *Handler {
	return &Handler{store: store, files: files}
}

// Register adds the batch routes to mux.
func (h *Handler) Register(mux *http.ServeMux) {
	mux.HandleFunc("GET /api/batches/{id}", h.getBatch)
	mux.HandleFunc("GET /api/batches/{id}/documents", h.listDocuments)
	mux.HandleFunc("GET /api/batches/{id}/documents/{seq}/file", h.documentFile)
}

type chunksResponse struct {
	Done  int `json:"done"`
	Total int `json:"total"`
}

type batchResponse struct {
	ID               string         `json:"id"`
	State            batch.State    `json:"state"`
	OriginalFilename string         `json:"originalFilename"`
	Total            int            `json:"total"`
	Processed        int            `json:"processed"`
	Failed           int            `json:"failed"`
	Skipped          int            `json:"skipped"`
	Pending          int            `json:"pending"`
	Chunks           chunksResponse `json:"chunks"`
	Error            string         `json:"error,omitempty"`
	CreatedAt        time.Time      `json:"createdAt"`
	UpdatedAt        time.Time      `json:"updatedAt"`
}

type fieldResponse struct {
	Value      string  `json:"value"`
	Confidence float64 `json:"confidence"`
}

type documentResponse struct {
	Seq         int                      `json:"seq"`
	Filename    string                   `json:"filename"`
	Status      batch.Status             `json:"status"`
	Reason      string                   `json:"reason,omitempty"`
	Key         string                   `json:"key,omitempty"`
	ContentType string                   `json:"contentType,omitempty"`
	Size        int64                    `json:"size"`
	Fields      map[string]fieldResponse `json:"fields,omitempty"`
	ExtractedAt *time.Time               `json:"extractedAt,omitempty"`
}

type listResponse struct {
	Documents []documentResponse `json:"documents"`
	Next      *int               `json:"next"` // null when there are no more pages
}

func (h *Handler) getBatch(w http.ResponseWriter, r *http.Request) {
	id, ok := batchID(w, r)
	if !ok {
		return
	}
	b, err := h.store.GetBatch(r.Context(), id)
	if errors.Is(err, batch.ErrNotFound) {
		// Normal for a few seconds after upload, before the planner has run.
		httpx.Error(w, http.StatusNotFound, "batch not found; processing may not have started yet")
		return
	}
	if err != nil {
		slog.Error("get batch", "id", id, "err", err)
		httpx.Error(w, http.StatusInternalServerError, "could not load batch")
		return
	}
	noStore(w)
	httpx.JSON(w, http.StatusOK, batchResponse{
		ID:               b.ID,
		State:            b.State(),
		OriginalFilename: b.OriginalFilename,
		Total:            b.Total,
		Processed:        b.Processed,
		Failed:           b.Failed,
		Skipped:          b.Skipped,
		Pending:          b.Pending(),
		Chunks:           chunksResponse{Done: b.ChunksDone, Total: b.ChunkCount},
		Error:            b.Error,
		CreatedAt:        b.CreatedAt,
		UpdatedAt:        b.UpdatedAt,
	})
}

func (h *Handler) listDocuments(w http.ResponseWriter, r *http.Request) {
	id, ok := batchID(w, r)
	if !ok {
		return
	}
	after, ok := intParam(w, r, "after", 0, 0, 1<<31-1)
	if !ok {
		return
	}
	limit, ok := intParam(w, r, "limit", defaultLimit, 1, maxLimit)
	if !ok {
		return
	}
	docs, next, err := h.store.ListDocuments(r.Context(), id, after, limit)
	if err != nil {
		slog.Error("list documents", "id", id, "err", err)
		httpx.Error(w, http.StatusInternalServerError, "could not list documents")
		return
	}
	resp := listResponse{Documents: make([]documentResponse, len(docs))}
	for i, d := range docs {
		resp.Documents[i] = toDocumentResponse(d)
	}
	if next != 0 {
		resp.Next = &next
	}
	noStore(w)
	httpx.JSON(w, http.StatusOK, resp)
}

// documentFile redirects to a short-lived URL for the document's file, so an
// <img> or <iframe> can show it straight from S3 without the bucket being
// public or the bytes passing through this server. Each request signs a fresh
// URL, so a page left open never holds an expired link.
func (h *Handler) documentFile(w http.ResponseWriter, r *http.Request) {
	id, ok := batchID(w, r)
	if !ok {
		return
	}
	seq, err := strconv.Atoi(r.PathValue("seq"))
	if err != nil || seq < 1 {
		httpx.Error(w, http.StatusBadRequest, "invalid document number")
		return
	}
	if h.files == nil {
		httpx.Error(w, http.StatusNotFound, "document previews are not configured (set DOCUMENTS_BUCKET)")
		return
	}
	d, err := h.store.GetDocument(r.Context(), id, seq)
	if errors.Is(err, batch.ErrNotFound) {
		httpx.Error(w, http.StatusNotFound, "document not found")
		return
	}
	if err != nil {
		slog.Error("get document", "id", id, "seq", seq, "err", err)
		httpx.Error(w, http.StatusInternalServerError, "could not load document")
		return
	}
	if d.Key == "" {
		httpx.Error(w, http.StatusNotFound, "no file is stored for a "+string(d.Status)+" document")
		return
	}
	url, err := h.files.URL(r.Context(), d.Key)
	if err != nil {
		slog.Error("sign document URL", "key", d.Key, "err", err)
		httpx.Error(w, http.StatusInternalServerError, "could not create a link to the document")
		return
	}
	noStore(w)
	http.Redirect(w, r, url, http.StatusFound)
}

func toDocumentResponse(d batch.Document) documentResponse {
	out := documentResponse{
		Seq:         d.Seq,
		Filename:    d.Filename,
		Status:      d.Status,
		Reason:      d.Reason,
		Key:         d.Key,
		ContentType: d.ContentType,
		Size:        d.Size,
	}
	if len(d.Fields) > 0 {
		out.Fields = make(map[string]fieldResponse, len(d.Fields))
		for name, f := range d.Fields {
			out.Fields[name] = fieldResponse{Value: f.Value, Confidence: f.Confidence}
		}
	}
	if !d.ExtractedAt.IsZero() {
		out.ExtractedAt = &d.ExtractedAt
	}
	return out
}

func batchID(w http.ResponseWriter, r *http.Request) (string, bool) {
	id := r.PathValue("id")
	if err := uuid.Validate(id); err != nil {
		httpx.Error(w, http.StatusBadRequest, "invalid batch id")
		return "", false
	}
	return id, true
}

func intParam(w http.ResponseWriter, r *http.Request, name string, def, lo, hi int) (int, bool) {
	v := r.URL.Query().Get(name)
	if v == "" {
		return def, true
	}
	n, err := strconv.Atoi(v)
	if err != nil || n < lo || n > hi {
		httpx.Error(w, http.StatusBadRequest, name+" must be a whole number from "+strconv.Itoa(lo)+" to "+strconv.Itoa(hi))
		return 0, false
	}
	return n, true
}

// Polled responses must always be fresh.
func noStore(w http.ResponseWriter) {
	w.Header().Set("Cache-Control", "no-store")
}
