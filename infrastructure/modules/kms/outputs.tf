output "raw_key_arn" {
  value = aws_kms_key.keys["raw"].arn
}

output "processed_key_arn" {
  value = aws_kms_key.keys["processed"].arn
}

output "archive_key_arn" {
  value = aws_kms_key.keys["archive"].arn
}

output "logs_key_arn" {
  value = aws_kms_key.keys["logs"].arn
}

output "artifacts_key_arn" {
  value = aws_kms_key.keys["artifacts"].arn
}

output "glue_key_arn" {
  value = aws_kms_key.keys["glue"].arn
}

output "sns_key_arn" {
  value = aws_kms_key.keys["sns"].arn
}
