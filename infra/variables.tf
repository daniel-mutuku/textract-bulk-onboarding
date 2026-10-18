variable "region" {
  type    = string
  default = "eu-west-1"
}

variable "profile" {
  type        = string
  description = "AWS CLI profile to use"
}

variable "project" {
  type    = string
  default = "textract-onboarding"
}

# Buckets are globally unique, so a random suffix avoids name clashes.
variable "bucket_suffix" {
  type        = string
  description = "A short unique suffix, e.g. your initials + a number"
}

# The web origin allowed to upload directly (your frontend).
variable "allowed_origin" {
  type = string
}

# How many documents the extract Lambda processes at once: the Textract rate
# limiter. Check your AnalyzeDocument quota (Service Quotas console) before raising.
variable "extract_concurrency" {
  type    = number
  default = 5
}

# Email for DLQ alarms. Empty means alarms are created without notifications.
variable "alarm_email" {
  type    = string
  default = ""
}
