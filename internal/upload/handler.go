// Package upload serves the HTTP API that coordinates browser-to-S3 multipart uploads.
package upload

import (
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"path"
	"slices"
	"strings"

	"github.com/google/uuid"

	"github.com/daniel-mutuku/textract-bulk-onboarding/internal/httpx"
	"github.com/daniel-mutuku/textract-bulk-onboarding/internal/storage"
)

// Options are the limits the handler enforces.
type Options struct {
	PartSize int64 // bytes per part; the last part may be smaller
	MaxSize  int64 // largest accepted upload in bytes
}

type Handler struct {
	store storage.MultipartStore
	opts  Options
	newID func() string
}

func New(store storage.MultipartStore, opts Options) *Handler {
	return &Handler{store: store, opts: opts, newID: uuid.NewString}
}

// Register adds the upload routes to mux.
func (h *Handler) Register(mux *http.ServeMux) {
	mux.HandleFunc("POST /api/uploads", h.start)
	mux.HandleFunc("POST /api/uploads/{id}/part-url", h.partURL)
	mux.HandleFunc("POST /api/uploads/{id}/complete", h.complete)
	mux.HandleFunc("POST /api/uploads/{id}/abort", h.abort)
}

// objectKey derives the S3 key from the upload id. The client never supplies a
// key, so it cannot point a request at any other object in the bucket.
func objectKey(id string) string {
	return "uploads/" + id + "/upload.zip"
}

// Browsers disagree on the MIME type of a zip, and some send none at all.
var zipContentTypes = []string{
	"application/zip",
	"application/x-zip-compressed",
	"application/octet-stream",
	"",
}

type startRequest struct {
	Filename    string `json:"filename"`
	Size        int64  `json:"size"`
	ContentType string `json:"contentType"`
}

type startResponse struct {
	ID        string `json:"id"`
	UploadID  string `json:"uploadId"`
	Key       string `json:"key"`
	PartSize  int64  `json:"partSize"`
	PartCount int64  `json:"partCount"`
}

func (h *Handler) start(w http.ResponseWriter, r *http.Request) {
	var req startRequest
	if !httpx.Decode(w, r, &req) {
		return
	}
	if !strings.EqualFold(path.Ext(req.Filename), ".zip") {
		httpx.Error(w, http.StatusBadRequest, "only .zip files can be uploaded")
		return
	}
	if !slices.Contains(zipContentTypes, strings.ToLower(req.ContentType)) {
		httpx.Error(w, http.StatusBadRequest, fmt.Sprintf("content type %q is not a zip", req.ContentType))
		return
	}
	if req.Size <= 0 {
		httpx.Error(w, http.StatusBadRequest, "file is empty")
		return
	}
	if req.Size > h.opts.MaxSize {
		httpx.Error(w, http.StatusRequestEntityTooLarge,
			fmt.Sprintf("file is %d bytes; the limit is %d bytes", req.Size, h.opts.MaxSize))
		return
	}
	partCount := (req.Size + h.opts.PartSize - 1) / h.opts.PartSize
	if partCount > storage.MaxParts {
		httpx.Error(w, http.StatusRequestEntityTooLarge,
			fmt.Sprintf("file needs %d parts; S3 allows at most %d", partCount, storage.MaxParts))
		return
	}

	id := h.newID()
	key := objectKey(id)
	meta := map[string]string{"original-filename": sanitizeFilename(req.Filename)}
	uploadID, err := h.store.Create(r.Context(), key, "application/zip", meta)
	if err != nil {
		storeError(w, "create upload", err)
		return
	}
	httpx.JSON(w, http.StatusCreated, startResponse{
		ID:        id,
		UploadID:  uploadID,
		Key:       key,
		PartSize:  h.opts.PartSize,
		PartCount: partCount,
	})
}

type partURLRequest struct {
	UploadID   string `json:"uploadId"`
	PartNumber int32  `json:"partNumber"`
}

type partURLResponse struct {
	URL string `json:"url"`
}

func (h *Handler) partURL(w http.ResponseWriter, r *http.Request) {
	key, ok := keyFromPath(w, r)
	if !ok {
		return
	}
	var req partURLRequest
	if !httpx.Decode(w, r, &req) || !requireUploadID(w, req.UploadID) {
		return
	}
	if req.PartNumber < 1 || req.PartNumber > storage.MaxParts {
		httpx.Error(w, http.StatusBadRequest, fmt.Sprintf("partNumber must be between 1 and %d", storage.MaxParts))
		return
	}
	url, err := h.store.PresignPart(r.Context(), key, req.UploadID, req.PartNumber)
	if err != nil {
		storeError(w, "presign part", err)
		return
	}
	httpx.JSON(w, http.StatusOK, partURLResponse{URL: url})
}

type completeRequest struct {
	UploadID string         `json:"uploadId"`
	Parts    []storage.Part `json:"parts"`
}

type completeResponse struct {
	Key string `json:"key"`
}

func (h *Handler) complete(w http.ResponseWriter, r *http.Request) {
	key, ok := keyFromPath(w, r)
	if !ok {
		return
	}
	var req completeRequest
	if !httpx.Decode(w, r, &req) || !requireUploadID(w, req.UploadID) {
		return
	}
	if len(req.Parts) == 0 {
		httpx.Error(w, http.StatusBadRequest, "parts must not be empty")
		return
	}
	// S3 requires ascending part numbers; the browser finishes parts in any order.
	parts := slices.Clone(req.Parts)
	slices.SortFunc(parts, func(a, b storage.Part) int { return int(a.PartNumber - b.PartNumber) })
	for i, p := range parts {
		if p.PartNumber < 1 || p.PartNumber > storage.MaxParts {
			httpx.Error(w, http.StatusBadRequest, fmt.Sprintf("part number %d is out of range", p.PartNumber))
			return
		}
		if p.ETag == "" {
			httpx.Error(w, http.StatusBadRequest, fmt.Sprintf("part %d has no etag", p.PartNumber))
			return
		}
		if i > 0 && parts[i-1].PartNumber == p.PartNumber {
			httpx.Error(w, http.StatusBadRequest, fmt.Sprintf("part %d is listed twice", p.PartNumber))
			return
		}
	}
	if err := h.store.Complete(r.Context(), key, req.UploadID, parts); err != nil {
		storeError(w, "complete upload", err)
		return
	}
	httpx.JSON(w, http.StatusOK, completeResponse{Key: key})
}

type abortRequest struct {
	UploadID string `json:"uploadId"`
}

func (h *Handler) abort(w http.ResponseWriter, r *http.Request) {
	key, ok := keyFromPath(w, r)
	if !ok {
		return
	}
	var req abortRequest
	if !httpx.Decode(w, r, &req) || !requireUploadID(w, req.UploadID) {
		return
	}
	err := h.store.Abort(r.Context(), key, req.UploadID)
	// Already gone is the outcome the caller wanted, so aborting twice is fine.
	if err != nil && !errors.Is(err, storage.ErrNoSuchUpload) {
		storeError(w, "abort upload", err)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func keyFromPath(w http.ResponseWriter, r *http.Request) (string, bool) {
	id := r.PathValue("id")
	if err := uuid.Validate(id); err != nil {
		httpx.Error(w, http.StatusBadRequest, "invalid upload id")
		return "", false
	}
	return objectKey(id), true
}

func requireUploadID(w http.ResponseWriter, uploadID string) bool {
	if uploadID == "" {
		httpx.Error(w, http.StatusBadRequest, "uploadId is required")
		return false
	}
	return true
}

// sanitizeFilename keeps the name readable but safe for an S3 metadata header,
// which only carries ASCII.
func sanitizeFilename(name string) string {
	name = path.Base(strings.ReplaceAll(name, `\`, "/"))
	var b strings.Builder
	for _, c := range name {
		switch {
		case c >= 'a' && c <= 'z', c >= 'A' && c <= 'Z', c >= '0' && c <= '9', c == '.', c == '-', c == '_':
			b.WriteRune(c)
		default:
			b.WriteByte('_')
		}
	}
	out := b.String()
	if len(out) > 200 {
		out = out[len(out)-200:]
	}
	return out
}

func storeError(w http.ResponseWriter, op string, err error) {
	switch {
	case errors.Is(err, storage.ErrNoSuchUpload):
		httpx.Error(w, http.StatusNotFound, "upload not found; it may have been completed or aborted")
	case errors.Is(err, storage.ErrInvalidPart):
		httpx.Error(w, http.StatusBadRequest, "a part number or etag does not match what was uploaded")
	default:
		slog.Error(op, "err", err)
		httpx.Error(w, http.StatusBadGateway, op+" failed")
	}
}
