# One execution role per Lambda. A role has two policies:
#   - a TRUST policy: who may assume the role (here, the Lambda service)
#   - a PERMISSIONS policy: what the role may do once assumed
# Lambda assumes the role on each cold start and hands the function temporary
# credentials, which the Go SDK picks up from the environment.

data "aws_iam_policy_document" "lambda_trust" {
  statement {
    actions = ["sts:AssumeRole"]
    principals {
      type        = "Service"
      identifiers = ["lambda.amazonaws.com"]
    }
    # Only Lambda functions in this account may assume it (confused-deputy guard).
    condition {
      test     = "StringEquals"
      variable = "aws:SourceAccount"
      values   = [data.aws_caller_identity.current.account_id]
    }
  }
}

resource "aws_iam_role" "lambda" {
  for_each = local.functions

  name               = "${var.project}-${each.key}"
  assume_role_policy = data.aws_iam_policy_document.lambda_trust.json
}

resource "aws_iam_role_policy" "lambda" {
  for_each = local.functions

  name   = "pipeline"
  role   = aws_iam_role.lambda[each.key].id
  policy = data.aws_iam_policy_document.lambda[each.key].json
}

locals {
  table_arn     = aws_dynamodb_table.pipeline.arn
  uploads_arn   = aws_s3_bucket.uploads.arn
  documents_arn = aws_s3_bucket.documents.arn

  # What each function does beyond the shared statements below.
  # DynamoDB: a TransactWriteItems call is authorised per item, by the action
  # each item performs (Put -> PutItem, Update -> UpdateItem, ConditionCheck ->
  # ConditionCheckItem). There is no separate "transact" permission to grant.
  extra_statements = {
    unzip-plan = [
      { sid = "ReadUploadedZips", actions = ["s3:GetObject"], resources = ["${local.uploads_arn}/uploads/*"] },
      { sid = "CreateBatch", actions = ["dynamodb:PutItem"], resources = [local.table_arn] },
      { sid = "EnqueueChunks", actions = ["sqs:SendMessage"], resources = [aws_sqs_queue.main["unzip-chunks"].arn] },
    ]
    unzip-chunk = [
      { sid = "ReadUploadedZips", actions = ["s3:GetObject"], resources = ["${local.uploads_arn}/uploads/*"] },
      { sid = "WriteDocuments", actions = ["s3:PutObject"], resources = ["${local.documents_arn}/documents/*"] },
      {
        sid       = "RecordDocuments"
        actions   = ["dynamodb:GetItem", "dynamodb:PutItem", "dynamodb:UpdateItem", "dynamodb:ConditionCheckItem"]
        resources = [local.table_arn]
      },
      { sid = "EnqueueDocuments", actions = ["sqs:SendMessage"], resources = [aws_sqs_queue.main["documents"].arn] },
    ]
    extract = [
      # Textract fetches the document from S3 with the CALLER's permissions,
      # so this role must be able to read it even though the code never does.
      { sid = "ReadDocumentsAndRawResponses", actions = ["s3:GetObject"], resources = ["${local.documents_arn}/documents/*", "${local.documents_arn}/textract/*"] },
      { sid = "SaveRawResponses", actions = ["s3:PutObject"], resources = ["${local.documents_arn}/textract/*"] },
      # Textract actions don't support resource-level permissions, hence "*".
      { sid = "AnalyzeDocuments", actions = ["textract:AnalyzeDocument"], resources = ["*"] },
      { sid = "RecordResults", actions = ["dynamodb:GetItem", "dynamodb:UpdateItem"], resources = [local.table_arn] },
    ]
  }
}

data "aws_iam_policy_document" "lambda" {
  for_each = local.functions

  # Shared 1: write to its own log group (created in lambda.tf, so no
  # logs:CreateLogGroup needed).
  statement {
    sid       = "WriteLogs"
    actions   = ["logs:CreateLogStream", "logs:PutLogEvents"]
    resources = ["${aws_cloudwatch_log_group.lambda[each.key].arn}:*"]
  }

  # Shared 2: consume its queue. The event source mapping polls using the
  # FUNCTION's role, so the role needs receive/delete/attributes.
  # ChangeMessageVisibility is for the per-message retry delay.
  statement {
    sid = "ConsumeOwnQueue"
    actions = [
      "sqs:ReceiveMessage", "sqs:DeleteMessage", "sqs:GetQueueAttributes", "sqs:ChangeMessageVisibility",
    ]
    resources = [aws_sqs_queue.main[each.value.queue].arn]
  }

  dynamic "statement" {
    for_each = local.extra_statements[each.key]
    content {
      sid       = statement.value.sid
      actions   = statement.value.actions
      resources = statement.value.resources
    }
  }

  # Without s3:ListBucket, S3 answers a request for a MISSING object with 403
  # Access Denied instead of 404 Not Found (it won't reveal what doesn't
  # exist). The extract worker checks whether a raw response already exists,
  # so it needs a real 404. An s3:prefix condition can't narrow this: a
  # HEAD/GET on an object carries no prefix for the condition to match.
  dynamic "statement" {
    for_each = each.key == "extract" ? [1] : []
    content {
      sid       = "SeeMissingRawResponses"
      actions   = ["s3:ListBucket"]
      resources = [local.documents_arn]
    }
  }
}
