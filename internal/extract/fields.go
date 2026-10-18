// Package extract is the extraction stage: it takes one document job off the
// queue, runs Amazon Textract on it, maps the response to named fields, and
// records the result exactly once.
package extract

// Query is one field to extract and the question Textract's Queries feature
// answers for it. Textract returns each answer tagged with the Alias.
type Query struct {
	Alias string
	Text  string
}

// Queries are the fields read from the CIC investment application form.
// Synchronous AnalyzeDocument allows at most 15 queries per page.
//
// The wording was tuned against real responses (testdata/textract). The rule
// that matters: point at the label beside the answer, not a section heading.
// "account number in the customer's bank details" returned CIC's own printed
// account number on every form; "Account No. next to the Account Name"
// returns the applicant's.
var Queries = []Query{
	{"member_number", "What is the M No?"},
	{"full_name", "What is the applicant's name?"},
	{"date_of_birth", "What is the date of birth?"},
	{"id_passport_number", "What is the ID/Passport No?"},
	{"kra_pin", "What is the PIN No?"},
	{"mobile_number", "What is the mobile number?"},
	{"email", "What is the email address?"},
	{"postal_address", "What is the postal address?"},
	{"town", "What is the town?"},
	{"occupation", "What is the nature of occupation or business?"},
	{"account_name", "What is the account name in the customer's bank details?"},
	{"account_number", "What is the Account No. next to the Account Name?"},
	{"bank_name", "What is the bank name in the customer's bank details?"},
	{"bank_branch", "What is the branch name in the customer's bank details?"},
	{"total_amount_invested", "What is the total amount invested?"},
}
