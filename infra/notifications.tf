data "aws_caller_identity" "current" {}

# A queue policy is a RESOURCE-based policy: it is attached to the queue and
# says who may act on it. Here, the S3 service may send messages, but only on
# behalf of our uploads bucket in our account.
#
# The two conditions prevent the "confused deputy" problem: without them, any
# S3 bucket in any account could be configured to send events into this queue.
resource "aws_sqs_queue_policy" "zip_events" {
  queue_url = aws_sqs_queue.main["zip-events"].id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Sid       = "AllowUploadsBucketEvents"
      Effect    = "Allow"
      Principal = { Service = "s3.amazonaws.com" }
      Action    = "sqs:SendMessage"
      Resource  = aws_sqs_queue.main["zip-events"].arn
      Condition = {
        ArnEquals    = { "aws:SourceArn" = aws_s3_bucket.uploads.arn }
        StringEquals = { "aws:SourceAccount" = data.aws_caller_identity.current.account_id }
      }
    }]
  })
}

# When an upload completes, S3 sends an event to the zip-events queue.
# A bucket has exactly one notification configuration, so all of the
# bucket's notifications must live in this one resource.
resource "aws_s3_bucket_notification" "uploads" {
  bucket = aws_s3_bucket.uploads.id

  queue {
    queue_arn = aws_sqs_queue.main["zip-events"].arn
    # Covers PUT, POST, COPY and CompleteMultipartUpload, which is what the
    # browser's multipart upload produces.
    events        = ["s3:ObjectCreated:*"]
    filter_prefix = "uploads/"
    filter_suffix = ".zip"
  }

  # S3 checks it can deliver to the queue when the notification is saved
  # (that is the s3:TestEvent), so the policy must exist first.
  depends_on = [aws_sqs_queue_policy.zip_events]
}
