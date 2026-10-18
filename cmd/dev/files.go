package main

import (
	"context"
	"net/http"

	"github.com/daniel-mutuku/textract-bulk-onboarding/internal/blob/memblob"
)

// devFiles stands in for presigned S3 URLs: it links documents to a local
// route that serves them out of the in-memory blob store.
type devFiles struct {
	blobs *memblob.Store
}

func (f devFiles) URL(_ context.Context, key string) (string, error) {
	return "/dev/files/" + key, nil
}

func (f devFiles) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	data, contentType, ok := f.blobs.Get(documentsBucket, r.PathValue("key"))
	if !ok {
		http.NotFound(w, r)
		return
	}
	w.Header().Set("Content-Type", contentType)
	w.Write(data)
}
