resource "aws_dynamodb_table" "pipeline" {
  name = "${var.project}-pipeline"

  # On-demand: pay per request, no capacity to plan, and nothing when idle.
  billing_mode = "PAY_PER_REQUEST"

  hash_key  = "pk" # partition key: which partition stores the item
  range_key = "sk" # sort key: order within the partition

  # Only key attributes are declared. DynamoDB is schemaless for everything else.
  attribute {
    name = "pk"
    type = "S"
  }
  attribute {
    name = "sk"
    type = "S"
  }

  # Continuous backups: restore to any second in the last 35 days.
  point_in_time_recovery {
    enabled = true
  }

  # Encrypted at rest with an AWS-owned key by default. A customer-managed KMS
  # key would add audit trails and key policies, at a monthly cost.
  server_side_encryption {
    enabled = false # false = the default AWS-owned key, which is still encrypted
  }

  # Left off so `terraform destroy` can clean up this demo. Turn on for real data.
  deletion_protection_enabled = false
}
