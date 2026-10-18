// Package faketextract is a stand-in for Textract in tests and local runs.
package faketextract

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"

	"github.com/daniel-mutuku/textract-bulk-onboarding/internal/extract"
)

// Analyzer answers with whatever Respond returns and counts calls per key.
type Analyzer struct {
	Respond func(bucket, key string, call int) ([]byte, error)

	mu    sync.Mutex
	calls map[string]int
}

var _ extract.Analyzer = (*Analyzer)(nil)

func (a *Analyzer) Analyze(_ context.Context, bucket, key string, _ []extract.Query) ([]byte, error) {
	a.mu.Lock()
	if a.calls == nil {
		a.calls = map[string]int{}
	}
	a.calls[key]++
	call := a.calls[key]
	a.mu.Unlock()
	return a.Respond(bucket, key, call)
}

// Calls is how many times key was analysed.
func (a *Analyzer) Calls(key string) int {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.calls[key]
}

// Answer is one query's answer in a synthetic response.
type Answer struct {
	Text       string
	Confidence float64
}

// Response builds a minimal AnalyzeDocument response in Textract's real shape:
// one QUERY block per alias, linked by an ANSWER relationship to a
// QUERY_RESULT block. An alias with no answers gets a QUERY with no ANSWER,
// which is how Textract reports "not found".
func Response(answers map[string][]Answer) []byte {
	type rel struct {
		Type string   `json:"Type"`
		Ids  []string `json:"Ids"`
	}
	type query struct {
		Text  string `json:"Text"`
		Alias string `json:"Alias"`
	}
	type block struct {
		BlockType     string  `json:"BlockType"`
		Id            string  `json:"Id"`
		Text          string  `json:"Text,omitempty"`
		Confidence    float64 `json:"Confidence,omitempty"`
		Query         *query  `json:"Query,omitempty"`
		Relationships []rel   `json:"Relationships,omitempty"`
	}
	blocks := []block{{BlockType: "PAGE", Id: "page-1"}}
	n := 0
	for alias, as := range answers {
		n++
		q := block{BlockType: "QUERY", Id: fmt.Sprintf("q-%d", n), Query: &query{Text: "What is the " + alias + "?", Alias: alias}}
		var ids []string
		for i, a := range as {
			id := fmt.Sprintf("r-%d-%d", n, i)
			ids = append(ids, id)
			blocks = append(blocks, block{BlockType: "QUERY_RESULT", Id: id, Text: a.Text, Confidence: a.Confidence})
		}
		if len(ids) > 0 {
			q.Relationships = []rel{{Type: "ANSWER", Ids: ids}}
		}
		blocks = append(blocks, q)
	}
	b, _ := json.Marshal(map[string]any{"DocumentMetadata": map[string]int{"Pages": 1}, "Blocks": blocks})
	return b
}
