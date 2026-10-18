package extract

import (
	"encoding/json"
	"fmt"
	"strings"

	"github.com/daniel-mutuku/textract-bulk-onboarding/internal/batch"
)

// A Textract response is a flat list of Blocks that point at each other by ID:
//
//	PAGE ──CHILD──▶ LINE ──CHILD──▶ WORD            (the text on the page)
//	QUERY ──ANSWER──▶ QUERY_RESULT                  (one per question asked)
//
// A QUERY block carries the question and our Alias. Its ANSWER relationship
// lists the IDs of QUERY_RESULT blocks, each with the answer Text and a
// Confidence from 0 to 100. A QUERY with no ANSWER means Textract found nothing.
//
// Only the fields used here are decoded. The names match both the API's wire
// format and the Go SDK's structs marshalled with encoding/json, so the same
// code reads a live response and one saved earlier.
type response struct {
	Blocks []block `json:"Blocks"`
}

type block struct {
	ID            string         `json:"Id"`
	BlockType     string         `json:"BlockType"`
	Text          string         `json:"Text"`
	Confidence    float64        `json:"Confidence"`
	Query         *queryInfo     `json:"Query"`
	Relationships []relationship `json:"Relationships"`
}

type queryInfo struct {
	Text  string `json:"Text"`
	Alias string `json:"Alias"`
}

type relationship struct {
	Type string   `json:"Type"`
	IDs  []string `json:"Ids"`
}

// MapFields turns a raw AnalyzeDocument response into fields keyed by alias.
// A field Textract found no answer for is left out, so "missing" and "empty"
// stay distinguishable for the review step.
func MapFields(raw []byte) (map[string]batch.Field, error) {
	var resp response
	if err := json.Unmarshal(raw, &resp); err != nil {
		return nil, fmt.Errorf("decode Textract response: %w", err)
	}

	byID := make(map[string]*block, len(resp.Blocks))
	for i := range resp.Blocks {
		byID[resp.Blocks[i].ID] = &resp.Blocks[i]
	}

	fields := map[string]batch.Field{}
	for _, b := range resp.Blocks {
		if b.BlockType != "QUERY" || b.Query == nil || b.Query.Alias == "" {
			continue
		}
		// A query can have several answers; keep the one Textract is surest of.
		var best *block
		for _, rel := range b.Relationships {
			if rel.Type != "ANSWER" {
				continue
			}
			for _, id := range rel.IDs {
				if ans := byID[id]; ans != nil && ans.BlockType == "QUERY_RESULT" &&
					(best == nil || ans.Confidence > best.Confidence) {
					best = ans
				}
			}
		}
		if best == nil {
			continue
		}
		fields[b.Query.Alias] = batch.Field{
			Value:      strings.TrimSpace(best.Text),
			Confidence: best.Confidence,
		}
	}
	return fields, nil
}
