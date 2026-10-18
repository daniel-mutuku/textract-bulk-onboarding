output "uploads_bucket" {
  value = aws_s3_bucket.uploads.bucket
}

output "documents_bucket" {
  value = aws_s3_bucket.documents.bucket
}
output "pipeline_table" {
  value = aws_dynamodb_table.pipeline.name
}

output "queue_urls" {
  value = { for name, q in aws_sqs_queue.main : name => q.url }
}

output "dlq_urls" {
  value = { for name, q in aws_sqs_queue.dlq : name => q.url }
}

output "lambda_functions" {
  value = { for name, f in aws_lambda_function.fn : name => f.function_name }
}
