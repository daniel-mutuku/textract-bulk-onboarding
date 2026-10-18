# Any message in a dead-letter queue means work that failed every retry and
# needs a human. Alarm as soon as one appears.
resource "aws_sns_topic" "alarms" {
  name = "${var.project}-alarms"
}

# Email subscriptions must be confirmed from the inbox before they deliver.
resource "aws_sns_topic_subscription" "alarm_email" {
  count = var.alarm_email == "" ? 0 : 1

  topic_arn = aws_sns_topic.alarms.arn
  protocol  = "email"
  endpoint  = var.alarm_email
}

resource "aws_cloudwatch_metric_alarm" "dlq_not_empty" {
  for_each = local.queues

  alarm_name        = "${var.project}-${each.key}-dlq-not-empty"
  alarm_description = "Messages in ${aws_sqs_queue.dlq[each.key].name} failed every retry. Inspect them, fix the cause, then redrive."

  namespace   = "AWS/SQS"
  metric_name = "ApproximateNumberOfMessagesVisible"
  dimensions  = { QueueName = aws_sqs_queue.dlq[each.key].name }
  statistic   = "Maximum"
  period      = 300 # SQS publishes queue metrics every 5 minutes
  # One datapoint above zero is enough.
  evaluation_periods  = 1
  comparison_operator = "GreaterThanThreshold"
  threshold           = 0
  # An idle queue can report no data; that is fine, not alarming.
  treat_missing_data = "notBreaching"

  alarm_actions = [aws_sns_topic.alarms.arn]
  ok_actions    = [aws_sns_topic.alarms.arn]
}
