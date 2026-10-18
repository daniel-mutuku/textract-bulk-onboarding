package unzip

import (
	"archive/zip"
	"bytes"
	"fmt"
	"path"
	"strings"
)

// Formats synchronous Textract AnalyzeDocument accepts, with the bytes each
// file type must start with. Checking content as well as the extension
// catches, say, a .txt renamed to .pdf before we pay Textract to reject it.
var formats = map[string]struct {
	contentType string
	magic       [][]byte
}{
	".pdf":  {"application/pdf", [][]byte{[]byte("%PDF-")}},
	".png":  {"image/png", [][]byte{[]byte("\x89PNG\r\n\x1a\n")}},
	".jpg":  {"image/jpeg", [][]byte{{0xFF, 0xD8, 0xFF}}},
	".jpeg": {"image/jpeg", [][]byte{{0xFF, 0xD8, 0xFF}}},
	".tif":  {"image/tiff", [][]byte{[]byte("II*\x00"), []byte("MM\x00*")}},
	".tiff": {"image/tiff", [][]byte{[]byte("II*\x00"), []byte("MM\x00*")}},
}

// documentEntries lists a zip's entries minus directories. A document's seq
// is its index here plus one. The planner and every chunk compute this the
// same way from the same central directory, so they agree on numbering.
func documentEntries(zr *zip.Reader) []*zip.File {
	var files []*zip.File
	for _, f := range zr.File {
		if !f.FileInfo().IsDir() {
			files = append(files, f)
		}
	}
	return files
}

// skipReason decides from the central directory alone, without reading any
// bytes, whether an entry can't be a document. "" means it is a candidate.
func skipReason(f *zip.File, maxBytes int64) string {
	name := f.Name
	base := path.Base(name)
	ext := strings.ToLower(path.Ext(base))
	switch {
	case strings.HasPrefix(name, "__MACOSX/") || strings.HasPrefix(base, "._"):
		return "macOS metadata file"
	case strings.HasPrefix(base, "."):
		return "hidden system file"
	case f.Flags&0x1 != 0:
		return "entry is password-protected"
	case ext == ".zip":
		return "nested zip archives are not unpacked"
	case ext == "":
		return "no file extension"
	}
	if _, ok := formats[ext]; !ok {
		return fmt.Sprintf("unsupported file type %s (expected PDF, PNG, JPEG or TIFF)", ext)
	}
	switch size := f.UncompressedSize64; {
	case size == 0:
		return "empty file"
	case size > uint64(maxBytes):
		return fmt.Sprintf("%.1f MB is over the %d MB limit for synchronous Textract",
			float64(size)/(1<<20), maxBytes>>20)
	}
	return ""
}

// sniff checks a candidate's bytes match its extension and returns its content type.
func sniff(name string, data []byte) (string, bool) {
	f := formats[strings.ToLower(path.Ext(name))]
	for _, m := range f.magic {
		if bytes.HasPrefix(data, m) {
			return f.contentType, true
		}
	}
	return "", false
}

// documentKey is where a document is stored: documents/{batchId}/{seq}-{name}.
// The seq prefix keeps names unique (zips can hold two "scan.pdf" in different
// folders) and the zero padding makes S3 list them in zip order.
func documentKey(batchID string, seq int, name string) string {
	return fmt.Sprintf("documents/%s/%06d-%s", batchID, seq, safeName(path.Base(name)))
}

// safeName keeps a filename readable but limited to characters that need no
// escaping in an S3 key or URL. The extension is kept when shortening.
func safeName(name string) string {
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
	if len(out) > 100 {
		ext := path.Ext(out)
		if len(ext) > 10 {
			ext = ""
		}
		out = out[:100-len(ext)] + ext
	}
	return out
}
