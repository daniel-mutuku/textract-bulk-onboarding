package extract_test

import (
	"os"
	"testing"

	"github.com/daniel-mutuku/textract-bulk-onboarding/internal/batch"
	"github.com/daniel-mutuku/textract-bulk-onboarding/internal/extract"
	"github.com/daniel-mutuku/textract-bulk-onboarding/internal/extract/faketextract"
)

// What Textract returned for the sample forms in testdata/forms. The odd
// values are real Textract output, kept as-is (form 2's stray ")" and
// truncated ".co.k", form 5's signature caught in the occupation): flagging
// those is the review step's job, not the mapping's.
var wantForms = map[string]map[string]string{
	"1": {
		"member_number": "2023-A-001", "full_name": "John Doe", "date_of_birth": "15/05/1988",
		"id_passport_number": "33445566", "kra_pin": "A012345678Q", "mobile_number": "+254 712 345 678",
		"email": "johndoe@sample.co.ke", "postal_address": "P.O. Box 900", "town": "Nairobi",
		"occupation": "Accountant", "account_name": "John Doe", "account_number": "0101010101",
		"bank_name": "Equity Bank", "bank_branch": "Kenyatta Avenue",
		"total_amount_invested": "Sixty Thousand Kenyan Shillings Only",
	},
	"2": {
		"member_number": "KE/2026/IND/115)", "full_name": "Mercy Atieno Ochieng", "date_of_birth": "19/08/1992",
		"id_passport_number": "22334455", "kra_pin": "A098765432R", "mobile_number": "(0733554433)",
		"email": "atieno.mercy@outlook.co.k", "postal_address": "P.O. Box 4120", "town": "Kisumu",
		"occupation": "High School Teacher", "account_name": "Mercy A. Ochieng", "account_number": "2233445566",
		"bank_name": "Co-operative Bank", "bank_branch": "Kenyatta Avenue Branch, Kisumu",
		"total_amount_invested": "Fifty thousand Kenyan Shillings only",
	},
	"3": {
		"member_number": "KE/2026/IND/042", "full_name": "James Mwangi Kamau", "date_of_birth": "12/05/1988",
		"id_passport_number": "33445566", "kra_pin": "A012345678Q", "mobile_number": "0722113344",
		"email": "james.k@gmail.com", "postal_address": "P.O. Box 7890-00100", "town": "Nairobi",
		"occupation": "Software Developer", "account_name": "James Mwangi Kamau", "account_number": "110022334455",
		"bank_name": "KCB Bank", "bank_branch": "Moi Avenue",
		"total_amount_invested": "Sixty thousand Kenyan Shillings only",
	},
	"4": {
		"member_number": "KE/2026/IND/200)", "full_name": "John Otieno Kamau", "date_of_birth": "05/11/1975",
		"id_passport_number": "12345678", "kra_pin": "P012345678Q", "mobile_number": "+254711111111",
		"email": "j.kamau@corporate.co.ke", "postal_address": "P.O. Box 1234", "town": "Nairobi",
		"occupation": "Financial Analyst", "account_name": "John Otieno Kamau", "account_number": "123456789012",
		"bank_name": "Standard Chartered Bank", "bank_branch": "Moi Avenue Branch",
		"total_amount_invested": "200,000",
	},
	"5": {
		"member_number": "KE/2026/IND/201", "full_name": "Mary Wambui Karanja", "date_of_birth": "12/07/1988",
		"id_passport_number": "87654321", "kra_pin": "P02345678R", "mobile_number": "+254722222222",
		"email": "m.karanja@corporate.co.ke", "postal_address": "P.O. Box 4567", "town": "Nairobi",
		"occupation": "Human Resource Manager M.W. Karanja", "account_name": "Mary Wambui Karanja",
		"account_number": "987654321098", "bank_name": "Barclays Bank of Kenya", "bank_branch": "Westlands Branch",
		"total_amount_invested": "500,000",
	},
}

func TestMapFieldsRealResponses(t *testing.T) {
	for form, want := range wantForms {
		raw, err := os.ReadFile("../../testdata/textract/" + form + ".json")
		if err != nil {
			t.Fatal(err)
		}
		fields, err := extract.MapFields(raw)
		if err != nil {
			t.Fatalf("form %s: %v", form, err)
		}
		if len(fields) != len(extract.Queries) {
			t.Errorf("form %s: %d fields, want %d", form, len(fields), len(extract.Queries))
		}
		for alias, value := range want {
			f := fields[alias]
			if f.Value != value {
				t.Errorf("form %s %s = %q, want %q", form, alias, f.Value, value)
			}
			if f.Confidence <= 0 || f.Confidence > 100 {
				t.Errorf("form %s %s: confidence %v out of range", form, alias, f.Confidence)
			}
		}
	}
}

// Every alias in Queries must be one the fixtures were captured with, so a
// renamed alias can't silently stop matching.
func TestQueriesMatchFixtures(t *testing.T) {
	if len(extract.Queries) > 15 {
		t.Errorf("%d queries; synchronous AnalyzeDocument allows 15", len(extract.Queries))
	}
	for _, q := range extract.Queries {
		if _, ok := wantForms["1"][q.Alias]; !ok {
			t.Errorf("query alias %q has no expected value in the fixtures", q.Alias)
		}
	}
}

func TestMapFieldsPicksBestAnswerAndSkipsMissing(t *testing.T) {
	raw := faketextract.Response(map[string][]faketextract.Answer{
		"full_name":     {{Text: "J0hn Doe", Confidence: 41}, {Text: " John Doe ", Confidence: 93.5}},
		"kra_pin":       {},
		"mobile_number": {{Text: "0712 345 678", Confidence: 88}},
	})
	fields, err := extract.MapFields(raw)
	if err != nil {
		t.Fatal(err)
	}
	want := map[string]batch.Field{
		"full_name":     {Value: "John Doe", Confidence: 93.5},
		"mobile_number": {Value: "0712 345 678", Confidence: 88},
	}
	if len(fields) != len(want) {
		t.Errorf("got %v, want %v (kra_pin had no answer and must be absent)", fields, want)
	}
	for k, v := range want {
		if fields[k] != v {
			t.Errorf("%s = %+v, want %+v", k, fields[k], v)
		}
	}
}

func TestMapFieldsRejectsGarbage(t *testing.T) {
	if _, err := extract.MapFields([]byte("<html>")); err == nil {
		t.Error("expected an error")
	}
}
