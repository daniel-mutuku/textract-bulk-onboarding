// Package dynamostore is the DynamoDB implementation of batch.Store.
//
// Item layout (single table, see infra/dynamodb.tf):
//
//	pk           sk             item
//	BATCH#<id>   BATCH          batch counters and metadata
//	BATCH#<id>   CHUNK#0003     marker: unzip chunk 3 finished
//	BATCH#<id>   DOC#00000017   document 17
//
// Two DynamoDB features give the Store its guarantees:
//
//   - Condition expressions: a write states what must be true first
//     ("this item doesn't exist yet", "status is still pending"). If it isn't,
//     DynamoDB rejects the write and nothing changes. This is optimistic
//     concurrency: no locks, the database checks at write time.
//   - TransactWriteItems: up to 100 writes, across items, that all succeed or
//     all fail. Changing a document's status and the batch's counter is one
//     transaction, so the counter can never move without the status, or twice.
package dynamostore

import (
	"context"
	"errors"
	"fmt"
	"math/rand/v2"
	"strconv"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/feature/dynamodb/attributevalue"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"

	"github.com/daniel-mutuku/textract-bulk-onboarding/internal/batch"
)

type Store struct {
	client *dynamodb.Client
	table  string
	now    func() time.Time
}

func New(client *dynamodb.Client, table string) *Store {
	return &Store{client: client, table: table, now: time.Now}
}

var _ batch.Store = (*Store)(nil)

// ---------- Item shapes ----------

type batchItem struct {
	PK               string    `dynamodbav:"pk"`
	SK               string    `dynamodbav:"sk"`
	SourceKey        string    `dynamodbav:"sourceKey"`
	OriginalFilename string    `dynamodbav:"originalFilename"`
	Total            int       `dynamodbav:"total"`
	ChunkCount       int       `dynamodbav:"chunkCount"`
	ChunksDone       int       `dynamodbav:"chunksDone"`
	Processed        int       `dynamodbav:"processed"`
	Failed           int       `dynamodbav:"failed"`
	Skipped          int       `dynamodbav:"skipped"`
	Error            string    `dynamodbav:"error,omitempty"`
	CreatedAt        time.Time `dynamodbav:"createdAt"`
	UpdatedAt        time.Time `dynamodbav:"updatedAt"`
}

type docItem struct {
	PK          string               `dynamodbav:"pk"`
	SK          string               `dynamodbav:"sk"`
	Seq         int                  `dynamodbav:"seq"`
	Filename    string               `dynamodbav:"filename"`
	Key         string               `dynamodbav:"key,omitempty"`
	Size        int64                `dynamodbav:"size"`
	ContentType string               `dynamodbav:"contentType,omitempty"`
	Status      string               `dynamodbav:"status"`
	Reason      string               `dynamodbav:"reason,omitempty"`
	Fields      map[string]fieldItem `dynamodbav:"fields,omitempty"`
	RawKey      string               `dynamodbav:"rawKey,omitempty"`
	ExtractedAt *time.Time           `dynamodbav:"extractedAt,omitempty"`
}

type fieldItem struct {
	Value      string  `dynamodbav:"value"`
	Confidence float64 `dynamodbav:"confidence"`
}

func batchPK(id string) string { return "BATCH#" + id }

const batchSK = "BATCH"

// Zero-padded so that the string sort key orders numerically: DOC#00000002 < DOC#00000010.
func docSK(seq int) string { return fmt.Sprintf("DOC#%08d", seq) }
func chunkSK(n int) string { return fmt.Sprintf("CHUNK#%04d", n) }
func itemKey(pk, sk string) map[string]types.AttributeValue {
	return map[string]types.AttributeValue{
		"pk": &types.AttributeValueMemberS{Value: pk},
		"sk": &types.AttributeValueMemberS{Value: sk},
	}
}

// ---------- Batches ----------

func (s *Store) CreateBatch(ctx context.Context, b batch.Batch) error {
	now := s.now().UTC()
	item, err := attributevalue.MarshalMap(batchItem{
		PK: batchPK(b.ID), SK: batchSK,
		SourceKey: b.SourceKey, OriginalFilename: b.OriginalFilename,
		Total: b.Total, ChunkCount: b.ChunkCount, Error: b.Error,
		CreatedAt: now, UpdatedAt: now,
	})
	if err != nil {
		return err
	}
	_, err = s.client.PutItem(ctx, &dynamodb.PutItemInput{
		TableName: &s.table,
		Item:      item,
		// Without this, PutItem silently overwrites, resetting the counters.
		ConditionExpression: aws.String("attribute_not_exists(pk)"),
	})
	var ccf *types.ConditionalCheckFailedException
	if errors.As(err, &ccf) {
		return batch.ErrExists
	}
	return err
}

func (s *Store) GetBatch(ctx context.Context, id string) (batch.Batch, error) {
	var it batchItem
	if err := s.get(ctx, batchPK(id), batchSK, &it); err != nil {
		return batch.Batch{}, err
	}
	return batch.Batch{
		ID: id, SourceKey: it.SourceKey, OriginalFilename: it.OriginalFilename,
		Total: it.Total, ChunkCount: it.ChunkCount, ChunksDone: it.ChunksDone,
		Processed: it.Processed, Failed: it.Failed, Skipped: it.Skipped, Error: it.Error,
		CreatedAt: it.CreatedAt, UpdatedAt: it.UpdatedAt,
	}, nil
}

func (s *Store) FinishChunk(ctx context.Context, batchID string, chunk int) error {
	now := s.now().UTC()
	reasons, err := s.transact(ctx,
		// 0: the marker can only be created once...
		types.TransactWriteItem{Put: &types.Put{
			TableName:           &s.table,
			Item:                withAttr(itemKey(batchPK(batchID), chunkSK(chunk)), "finishedAt", timeValue(now)),
			ConditionExpression: aws.String("attribute_not_exists(pk)"),
		}},
		// 1: ...so this increment can only happen once per chunk.
		s.incrementBatch(batchID, now, map[string]int{"chunksDone": 1}),
	)
	switch {
	case err != nil:
		return err
	case failedCondition(reasons, 1):
		return batch.ErrNotFound
	case failedCondition(reasons, 0):
		return nil // a retried chunk; already counted
	}
	return nil
}

// ---------- Documents ----------

func (s *Store) CreateDocument(ctx context.Context, d batch.Document) error {
	var counters map[string]int
	switch d.Status {
	case batch.StatusPending:
	case batch.StatusSkipped:
		counters = map[string]int{"skipped": 1}
	case batch.StatusFailed:
		counters = map[string]int{"failed": 1}
	default:
		return fmt.Errorf("a new document cannot start as %q", d.Status)
	}
	item, err := attributevalue.MarshalMap(toDocItem(d))
	if err != nil {
		return err
	}
	now := s.now().UTC()

	// The batch must exist. With a counter to bump, the conditional update
	// checks that; without one, a ConditionCheck does: a transaction step that
	// asserts a condition on an item without writing it.
	batchOp := types.TransactWriteItem{ConditionCheck: &types.ConditionCheck{
		TableName:           &s.table,
		Key:                 itemKey(batchPK(d.BatchID), batchSK),
		ConditionExpression: aws.String("attribute_exists(pk)"),
	}}
	if counters != nil {
		batchOp = s.incrementBatch(d.BatchID, now, counters)
	}

	reasons, err := s.transact(ctx,
		types.TransactWriteItem{Put: &types.Put{
			TableName:           &s.table,
			Item:                item,
			ConditionExpression: aws.String("attribute_not_exists(pk)"),
		}},
		batchOp,
	)
	switch {
	case err != nil:
		return err
	case failedCondition(reasons, 1):
		return batch.ErrNotFound
	case failedCondition(reasons, 0):
		return batch.ErrExists
	}
	return nil
}

func (s *Store) GetDocument(ctx context.Context, batchID string, seq int) (batch.Document, error) {
	var it docItem
	if err := s.get(ctx, batchPK(batchID), docSK(seq), &it); err != nil {
		return batch.Document{}, err
	}
	return fromDocItem(batchID, it), nil
}

func (s *Store) ListDocuments(ctx context.Context, batchID string, afterSeq, limit int) ([]batch.Document, int, error) {
	in := &dynamodb.QueryInput{
		TableName:              &s.table,
		KeyConditionExpression: aws.String("pk = :pk AND begins_with(sk, :doc)"),
		ExpressionAttributeValues: map[string]types.AttributeValue{
			":pk":  &types.AttributeValueMemberS{Value: batchPK(batchID)},
			":doc": &types.AttributeValueMemberS{Value: "DOC#"},
		},
		Limit:          aws.Int32(int32(limit)),
		ConsistentRead: aws.Bool(true),
	}
	// The cursor is just a seq: "start after DOC#<afterSeq>". DynamoDB doesn't
	// need that item to exist, so the API can stay stateless.
	if afterSeq > 0 {
		in.ExclusiveStartKey = itemKey(batchPK(batchID), docSK(afterSeq))
	}
	out, err := s.client.Query(ctx, in)
	if err != nil {
		return nil, 0, err
	}
	docs := make([]batch.Document, 0, len(out.Items))
	for _, raw := range out.Items {
		var it docItem
		if err := attributevalue.UnmarshalMap(raw, &it); err != nil {
			return nil, 0, err
		}
		docs = append(docs, fromDocItem(batchID, it))
	}
	// LastEvaluatedKey means "there may be more". It can also be set when the
	// page ended exactly at the last item, which the contract allows.
	next := 0
	if out.LastEvaluatedKey != nil && len(docs) > 0 {
		next = docs[len(docs)-1].Seq
	}
	return docs, next, nil
}

func (s *Store) CompleteDocument(ctx context.Context, batchID string, seq int, from batch.Status, e batch.Extraction) error {
	if from != batch.StatusPending && from != batch.StatusFailed {
		return fmt.Errorf("cannot complete a document from %q", from)
	}
	fields, err := attributevalue.Marshal(toFieldItems(e.Fields))
	if err != nil {
		return err
	}
	now := s.now().UTC()
	counters := map[string]int{"processed": 1}
	if from == batch.StatusFailed {
		counters["failed"] = -1 // a dead-letter replay moves between counters
	}
	return s.changeStatus(ctx, batchID, seq, from,
		"SET #status = :to, #fields = :fields, #rawKey = :rawKey, #extractedAt = :at REMOVE #reason",
		map[string]string{"#reason": "reason", "#fields": "fields", "#rawKey": "rawKey", "#extractedAt": "extractedAt"},
		map[string]types.AttributeValue{
			":to":     &types.AttributeValueMemberS{Value: string(batch.StatusExtracted)},
			":fields": fields,
			":rawKey": &types.AttributeValueMemberS{Value: e.RawKey},
			":at":     timeValue(e.At.UTC()),
		},
		s.incrementBatch(batchID, now, counters),
	)
}

func (s *Store) FailDocument(ctx context.Context, batchID string, seq int, reason string) error {
	return s.changeStatus(ctx, batchID, seq, batch.StatusPending,
		"SET #status = :to, #reason = :reason",
		map[string]string{"#reason": "reason"},
		map[string]types.AttributeValue{
			":to":     &types.AttributeValueMemberS{Value: string(batch.StatusFailed)},
			":reason": &types.AttributeValueMemberS{Value: reason},
		},
		s.incrementBatch(batchID, s.now().UTC(), map[string]int{"failed": 1}),
	)
}

// changeStatus updates a document only if its status is still `from`, and
// applies batchOp in the same transaction.
//
// "status", "reason" and friends are DynamoDB reserved words, so expressions
// refer to attributes through #placeholders in names. DynamoDB rejects a
// request that declares a placeholder it doesn't use, so each caller passes
// exactly the ones its update needs.
func (s *Store) changeStatus(ctx context.Context, batchID string, seq int, from batch.Status,
	update string, names map[string]string, values map[string]types.AttributeValue, batchOp types.TransactWriteItem) error {

	names["#status"] = "status"
	values[":from"] = &types.AttributeValueMemberS{Value: string(from)}
	reasons, err := s.transact(ctx,
		types.TransactWriteItem{Update: &types.Update{
			TableName:                 &s.table,
			Key:                       itemKey(batchPK(batchID), docSK(seq)),
			UpdateExpression:          &update,
			ConditionExpression:       aws.String("attribute_exists(pk) AND #status = :from"),
			ExpressionAttributeNames:  names,
			ExpressionAttributeValues: values,
			// On failure, return the item as it was, so a missing document can
			// be told apart from one in another status.
			ReturnValuesOnConditionCheckFailure: types.ReturnValuesOnConditionCheckFailureAllOld,
		}},
		batchOp,
	)
	if err != nil {
		return err
	}
	if failedCondition(reasons, 0) {
		if reasons[0].Item == nil {
			return batch.ErrNotFound
		}
		return batch.ErrConflict
	}
	if failedCondition(reasons, 1) {
		return batch.ErrNotFound
	}
	return nil
}

// incrementBatch atomically adds to batch counters. ADD on a number is
// DynamoDB's atomic counter: the server applies the delta, so concurrent
// increments never overwrite each other the way read-modify-write would.
func (s *Store) incrementBatch(batchID string, now time.Time, deltas map[string]int) types.TransactWriteItem {
	names := map[string]string{}
	values := map[string]types.AttributeValue{":now": timeValue(now)}
	var adds []string
	i := 0
	for attr, delta := range deltas {
		i++
		names["#c"+strconv.Itoa(i)] = attr
		values[":d"+strconv.Itoa(i)] = &types.AttributeValueMemberN{Value: strconv.Itoa(delta)}
		adds = append(adds, fmt.Sprintf("#c%d :d%d", i, i))
	}
	names["#updatedAt"] = "updatedAt"
	return types.TransactWriteItem{Update: &types.Update{
		TableName:                 &s.table,
		Key:                       itemKey(batchPK(batchID), batchSK),
		UpdateExpression:          aws.String("ADD " + strings.Join(adds, ", ") + " SET #updatedAt = :now"),
		ConditionExpression:       aws.String("attribute_exists(pk)"), // ADD would otherwise create a stray batch
		ExpressionAttributeNames:  names,
		ExpressionAttributeValues: values,
	}}
}

// ---------- Helpers ----------

// get reads one item with a strongly consistent read. The default (eventually
// consistent) read can miss a write from a moment ago, which would make the
// worker's "already extracted?" check, and the progress counts, lag.
func (s *Store) get(ctx context.Context, pk, sk string, out any) error {
	res, err := s.client.GetItem(ctx, &dynamodb.GetItemInput{
		TableName:      &s.table,
		Key:            itemKey(pk, sk),
		ConsistentRead: aws.Bool(true),
	})
	if err != nil {
		return err
	}
	if res.Item == nil {
		return batch.ErrNotFound
	}
	return attributevalue.UnmarshalMap(res.Item, out)
}

const maxTransactAttempts = 5

// transact runs a transaction. When DynamoDB cancels it because a condition
// failed, transact returns the per-item cancellation reasons and a nil error;
// the caller decides what each failure means.
//
// A cancellation with reason TransactionConflict is different: another
// transaction was touching the same item at that moment. That is contention,
// not a business outcome, so transact retries it after a short random pause.
// (Ten workers completing documents of one batch all increment the same
// batch item, so this happens under load.)
func (s *Store) transact(ctx context.Context, items ...types.TransactWriteItem) ([]types.CancellationReason, error) {
	for attempt := 1; ; attempt++ {
		_, err := s.client.TransactWriteItems(ctx, &dynamodb.TransactWriteItemsInput{TransactItems: items})
		var tce *types.TransactionCanceledException
		if !errors.As(err, &tce) {
			return nil, err // success, or a non-cancellation error
		}
		conflict := false
		for _, r := range tce.CancellationReasons {
			if aws.ToString(r.Code) == "TransactionConflict" {
				conflict = true
			}
		}
		if !conflict {
			return tce.CancellationReasons, nil
		}
		if attempt == maxTransactAttempts {
			return nil, fmt.Errorf("transaction conflict persisted after %d attempts: %w", attempt, err)
		}
		pause := time.Duration(rand.N(25<<attempt)) * time.Millisecond
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(pause):
		}
	}
}

func failedCondition(reasons []types.CancellationReason, i int) bool {
	return i < len(reasons) && aws.ToString(reasons[i].Code) == "ConditionalCheckFailed"
}

func timeValue(t time.Time) types.AttributeValue {
	return &types.AttributeValueMemberS{Value: t.Format(time.RFC3339Nano)}
}

func withAttr(item map[string]types.AttributeValue, name string, v types.AttributeValue) map[string]types.AttributeValue {
	item[name] = v
	return item
}

func toDocItem(d batch.Document) docItem {
	it := docItem{
		PK: batchPK(d.BatchID), SK: docSK(d.Seq), Seq: d.Seq,
		Filename: d.Filename, Key: d.Key, Size: d.Size, ContentType: d.ContentType,
		Status: string(d.Status), Reason: d.Reason, Fields: toFieldItems(d.Fields), RawKey: d.RawKey,
	}
	if !d.ExtractedAt.IsZero() {
		at := d.ExtractedAt.UTC()
		it.ExtractedAt = &at
	}
	return it
}

func fromDocItem(batchID string, it docItem) batch.Document {
	d := batch.Document{
		BatchID: batchID, Seq: it.Seq, Filename: it.Filename, Key: it.Key, Size: it.Size,
		ContentType: it.ContentType, Status: batch.Status(it.Status), Reason: it.Reason, RawKey: it.RawKey,
	}
	if len(it.Fields) > 0 {
		d.Fields = make(map[string]batch.Field, len(it.Fields))
		for k, f := range it.Fields {
			d.Fields[k] = batch.Field{Value: f.Value, Confidence: f.Confidence}
		}
	}
	if it.ExtractedAt != nil {
		d.ExtractedAt = *it.ExtractedAt
	}
	return d
}

func toFieldItems(fields map[string]batch.Field) map[string]fieldItem {
	if len(fields) == 0 {
		return nil
	}
	out := make(map[string]fieldItem, len(fields))
	for k, f := range fields {
		out[k] = fieldItem{Value: f.Value, Confidence: f.Confidence}
	}
	return out
}
