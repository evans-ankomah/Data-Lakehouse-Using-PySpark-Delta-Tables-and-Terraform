output "step_functions_log_group_name" {
  value = aws_cloudwatch_log_group.step_functions.name
}

output "glue_log_group_names" {
  value = { for k, v in aws_cloudwatch_log_group.glue_jobs : k => v.name }
}

output "dashboard_name" {
  value = aws_cloudwatch_dashboard.lakehouse.dashboard_name
}
