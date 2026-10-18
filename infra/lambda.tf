locals {
  functions = {
    unzip-plan = {
      queue   = "zip-events"
      timeout = local.lambda_timeout.unzip_plan
      memory  = 256
      # The lowest maximum_concurrency allowed. Planning is quick and rare.
      concurrency = 2
      env = {
        CHUNKS_QUEUE_URL = aws_sqs_queue.main["unzip-chunks"].url
      }
    }
    unzip-chunk = {
      queue   = "unzip-chunks"
      timeout = local.lambda_timeout.unzip_chunk
      # Lambda gives CPU and network bandwidth in proportion to memory, and
      # this function streams up to 1 GiB from S3 and decompresses it.
      memory      = 2048
      concurrency = 10
      env = {
        DOCUMENTS_QUEUE_URL = aws_sqs_queue.main["documents"].url
        DOCUMENTS_BUCKET    = aws_s3_bucket.documents.bucket
      }
    }
    extract = {
      queue   = "documents"
      timeout = local.lambda_timeout.extract
      memory  = 256 # it mostly waits on Textract
      # The Textract rate limiter: at most this many documents in flight.
      concurrency = var.extract_concurrency
      env = {
        MAX_ATTEMPTS = tostring(local.queues["documents"].max_receives)
      }
    }
  }
}

# Created here, rather than by Lambda on first run, so we control retention.
# Lambda's auto-created groups keep logs forever, and storage adds up.
resource "aws_cloudwatch_log_group" "lambda" {
  for_each = local.functions

  name              = "/aws/lambda/${var.project}-${each.key}"
  retention_in_days = 14
}

resource "aws_lambda_function" "fn" {
  for_each = local.functions

  function_name = "${var.project}-${each.key}"
  role          = aws_iam_role.lambda[each.key].arn

  # Go on Lambda: the OS-only runtime runs our compiled "bootstrap" binary.
  runtime       = "provided.al2023"
  handler       = "bootstrap"
  architectures = ["arm64"] # Graviton: ~20% cheaper per GB-second than x86

  filename = data.archive_file.lambda[each.key].output_path
  # Hash of the binary, not the zip: the zip embeds file timestamps, so
  # rezipping identical code would otherwise redeploy every function.
  source_code_hash = filebase64sha256("${path.module}/../build/${each.key}/bootstrap")

  memory_size = each.value.memory
  timeout     = each.value.timeout

  environment {
    variables = merge({ PIPELINE_TABLE = aws_dynamodb_table.pipeline.name }, each.value.env)
  }

  logging_config {
    log_format = "Text" # our Go code already writes one JSON object per line
    log_group  = aws_cloudwatch_log_group.lambda[each.key].name
  }

  # The role's permissions must exist before Lambda starts polling with them.
  depends_on = [aws_iam_role_policy.lambda]
}

data "archive_file" "lambda" {
  for_each = local.functions

  type        = "zip"
  source_file = "${path.module}/../build/${each.key}/bootstrap"
  output_path = "${path.module}/../build/${each.key}.zip"
}

# An event source mapping makes the Lambda service poll the queue (long
# polling, so idle time is free) and invoke the function with what it gets.
resource "aws_lambda_event_source_mapping" "sqs" {
  for_each = local.functions

  event_source_arn = aws_sqs_queue.main[each.value.queue].arn
  function_name    = aws_lambda_function.fn[each.key].arn

  # One message per invocation keeps "concurrent invocations" equal to
  # "documents in flight", so maximum_concurrency is an exact rate limit.
  # Invocations cost $0.20 per million, so this is not where money goes.
  batch_size = 1

  # Without this, Lambda ignores the batchItemFailures the handler returns
  # and deletes every message, failed ones included.
  function_response_types = ["ReportBatchItemFailures"]

  scaling_config {
    maximum_concurrency = each.value.concurrency
  }
}
