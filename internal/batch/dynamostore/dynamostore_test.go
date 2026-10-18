package dynamostore

import (
	"context"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/google/uuid"

	"github.com/daniel-mutuku/textract-bulk-onboarding/internal/awstest"
	"github.com/daniel-mutuku/textract-bulk-onboarding/internal/batch"
	"github.com/daniel-mutuku/textract-bulk-onboarding/internal/batch/storetest"
)

// TestContract runs the same suite as the in-memory fake, against real
// DynamoDB. It creates a temporary on-demand table and deletes it afterwards,
// so the pipeline table is never touched. Opt in with:
//
//	AWS_TEST=1 AWS_PROFILE=textract-onboarding go test ./internal/batch/dynamostore -v
func TestContract(t *testing.T) {
	ctx := context.Background()
	client := dynamodb.NewFromConfig(awstest.Config(t))

	table := "textract-onboarding-test-" + uuid.NewString()[:8]
	_, err := client.CreateTable(ctx, &dynamodb.CreateTableInput{
		TableName:   &table,
		BillingMode: types.BillingModePayPerRequest,
		AttributeDefinitions: []types.AttributeDefinition{
			{AttributeName: aws.String("pk"), AttributeType: types.ScalarAttributeTypeS},
			{AttributeName: aws.String("sk"), AttributeType: types.ScalarAttributeTypeS},
		},
		KeySchema: []types.KeySchemaElement{
			{AttributeName: aws.String("pk"), KeyType: types.KeyTypeHash},
			{AttributeName: aws.String("sk"), KeyType: types.KeyTypeRange},
		},
		Tags: []types.Tag{{Key: aws.String("Project"), Value: aws.String("textract-onboarding")}},
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if _, err := client.DeleteTable(context.Background(), &dynamodb.DeleteTableInput{TableName: &table}); err != nil {
			t.Errorf("delete test table %s: %v", table, err)
		}
	})
	t.Logf("created table %s, waiting for ACTIVE", table)
	if err := dynamodb.NewTableExistsWaiter(client).Wait(ctx, &dynamodb.DescribeTableInput{TableName: &table}, 2*time.Minute); err != nil {
		t.Fatal(err)
	}

	storetest.Run(t, func(*testing.T) batch.Store { return New(client, table) })
}
