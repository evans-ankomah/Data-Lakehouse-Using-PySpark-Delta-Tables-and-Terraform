locals {
  job_names = ["products-etl", "orders-etl", "order-items-etl"]
}

# ── CloudWatch Log Groups ─────────────────────────────────────────────────────

resource "aws_cloudwatch_log_group" "glue_jobs" {
  for_each = toset(local.job_names)

  name              = "/aws/glue/jobs/lakehouse-${var.environment}-${each.key}"
  retention_in_days = 30
  kms_key_id        = var.kms_key_arn
}

resource "aws_cloudwatch_log_group" "step_functions" {
  name              = "/aws/states/lakehouse-pipeline-${var.environment}"
  retention_in_days = 30
  kms_key_id        = var.kms_key_arn
}

# ── CloudWatch Metric Filters ─────────────────────────────────────────────────

resource "aws_cloudwatch_log_metric_filter" "glue_errors" {
  for_each = toset(local.job_names)

  name           = "lakehouse-${var.environment}-${each.key}-errors"
  log_group_name = aws_cloudwatch_log_group.glue_jobs[each.key].name
  pattern        = "{ $.level = \"ERROR\" }"

  metric_transformation {
    name          = "LakehouseGlueErrors"
    namespace     = "Lakehouse/Pipeline"
    value         = "1"
    default_value = "0"
    dimensions = {
      JobName     = each.key
      Environment = var.environment
    }
  }
}

resource "aws_cloudwatch_log_metric_filter" "quarantine_records" {
  for_each = toset(local.job_names)

  name           = "lakehouse-${var.environment}-${each.key}-quarantine"
  log_group_name = aws_cloudwatch_log_group.glue_jobs[each.key].name
  pattern        = "{ $.records_rejected > 0 }"

  metric_transformation {
    name          = "LakehouseQuarantineRecords"
    namespace     = "Lakehouse/Pipeline"
    value         = "$.records_rejected"
    default_value = "0"
    dimensions = {
      JobName     = each.key
      Environment = var.environment
    }
  }
}

resource "aws_cloudwatch_log_metric_filter" "job_completions" {
  for_each = toset(local.job_names)

  name           = "lakehouse-${var.environment}-${each.key}-complete"
  log_group_name = aws_cloudwatch_log_group.glue_jobs[each.key].name
  pattern        = "{ $.event = \"JOB_COMPLETE\" }"

  metric_transformation {
    name          = "LakehouseJobCompletions"
    namespace     = "Lakehouse/Pipeline"
    value         = "1"
    default_value = "0"
    dimensions = {
      JobName     = each.key
      Environment = var.environment
    }
  }
}

# ── CloudWatch Alarms ─────────────────────────────────────────────────────────

resource "aws_cloudwatch_metric_alarm" "glue_error_alarm" {
  alarm_name          = "lakehouse-${var.environment}-glue-errors"
  alarm_description   = "Fires when any Glue job emits an ERROR log."
  comparison_operator = "GreaterThanOrEqualToThreshold"
  evaluation_periods  = 1
  metric_name         = "LakehouseGlueErrors"
  namespace           = "Lakehouse/Pipeline"
  period              = 300
  statistic           = "Sum"
  threshold           = 1
  treat_missing_data  = "notBreaching"
  alarm_actions       = [var.sns_alert_topic_arn]
  ok_actions          = [var.sns_alert_topic_arn]
}

resource "aws_cloudwatch_metric_alarm" "quarantine_spike_alarm" {
  alarm_name          = "lakehouse-${var.environment}-quarantine-spike"
  alarm_description   = "Fires when quarantine record count exceeds 100 in 5 minutes."
  comparison_operator = "GreaterThanThreshold"
  evaluation_periods  = 1
  metric_name         = "LakehouseQuarantineRecords"
  namespace           = "Lakehouse/Pipeline"
  period              = 300
  statistic           = "Sum"
  threshold           = 100
  treat_missing_data  = "notBreaching"
  alarm_actions       = [var.sns_alert_topic_arn]
}

# ── CloudWatch Dashboard ──────────────────────────────────────────────────────

resource "aws_cloudwatch_dashboard" "lakehouse" {
  dashboard_name = "lakehouse-pipeline-${var.environment}"

  dashboard_body = jsonencode({
    widgets = [
      {
        type = "metric"
        properties = {
          title   = "Glue Errors"
          metrics = [["Lakehouse/Pipeline", "LakehouseGlueErrors", "Environment", var.environment]]
          period  = 300
          stat    = "Sum"
          view    = "timeSeries"
        }
      },
      {
        type = "metric"
        properties = {
          title   = "Quarantine Records"
          metrics = [["Lakehouse/Pipeline", "LakehouseQuarantineRecords", "Environment", var.environment]]
          period  = 300
          stat    = "Sum"
          view    = "timeSeries"
        }
      },
      {
        type = "metric"
        properties = {
          title   = "Job Completions"
          metrics = [["Lakehouse/Pipeline", "LakehouseJobCompletions", "Environment", var.environment]]
          period  = 300
          stat    = "Sum"
          view    = "timeSeries"
        }
      },
    ]
  })
}
