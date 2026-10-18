locals {
  # Lambda timeouts in seconds. The queues' visibility timeouts derive from
  # them, and the Lambda functions (added later) will use the same values.
  lambda_timeout = {
    unzip_plan  = 120 # reads only the zip's central directory
    unzip_chunk = 600 # up to 250 documents / 1 GiB per chunk
    extract     = 60  # one synchronous Textract call takes a few seconds
  }

  # One entry per pipeline stage: the queue in front of each Lambda.
  queues = {
    zip-events = {
      consumer     = "unzip_plan"
      max_receives = 3
    }
    unzip-chunks = {
      consumer     = "unzip_chunk"
      max_receives = 3
    }
    documents = {
      consumer     = "extract"
      max_receives = 5 # must equal the extract worker's MaxAttempts
    }
  }
}

# Dead-letter queues: where a message goes after max_receives failed attempts,
# so one bad message can't be retried forever or block the rest.
resource "aws_sqs_queue" "dlq" {
  for_each = local.queues

  name = "${var.project}-${each.key}-dlq"

  # 14 days, the maximum. A standard queue keeps a message's ORIGINAL enqueue
  # time when it moves to the DLQ, so the DLQ must retain longer than the
  # source queue or messages could expire the moment they arrive.
  message_retention_seconds = 1209600

  sqs_managed_sse_enabled = true
}

resource "aws_sqs_queue" "main" {
  for_each = local.queues

  name = "${var.project}-${each.key}"

  # While a consumer works on a message, SQS hides it from other consumers for
  # this long. Too short and a slow Lambda's message reappears and is processed
  # twice. AWS's guidance for Lambda consumers is 6x the function timeout,
  # which leaves room for the Lambda service's own retries.
  visibility_timeout_seconds = 6 * local.lambda_timeout[each.value.consumer]

  message_retention_seconds = 345600 # 4 days

  # Long polling: a receive waits up to 20s for a message instead of
  # returning empty at once. Fewer empty receives, lower cost.
  receive_wait_time_seconds = 20

  sqs_managed_sse_enabled = true

  # After max_receives deliveries without being deleted, SQS moves the
  # message to this queue's DLQ.
  redrive_policy = jsonencode({
    deadLetterTargetArn = aws_sqs_queue.dlq[each.key].arn
    maxReceiveCount     = each.value.max_receives
  })
}

# Only each stage's own queue may use its DLQ as a dead-letter target.
resource "aws_sqs_queue_redrive_allow_policy" "dlq" {
  for_each = local.queues

  queue_url = aws_sqs_queue.dlq[each.key].id
  redrive_allow_policy = jsonencode({
    redrivePermission = "byQueue"
    sourceQueueArns   = [aws_sqs_queue.main[each.key].arn]
  })
}
