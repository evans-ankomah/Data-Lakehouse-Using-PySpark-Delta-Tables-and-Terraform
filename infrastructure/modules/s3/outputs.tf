output "raw_bucket_name" {
  value = aws_s3_bucket.buckets["raw"].bucket
}

output "raw_bucket_arn" {
  value = aws_s3_bucket.buckets["raw"].arn
}

output "processed_bucket_name" {
  value = aws_s3_bucket.buckets["processed"].bucket
}

output "processed_bucket_arn" {
  value = aws_s3_bucket.buckets["processed"].arn
}

output "archive_bucket_name" {
  value = aws_s3_bucket.buckets["archive"].bucket
}

output "archive_bucket_arn" {
  value = aws_s3_bucket.buckets["archive"].arn
}

output "quarantine_bucket_name" {
  value = aws_s3_bucket.buckets["quarantine"].bucket
}

output "quarantine_bucket_arn" {
  value = aws_s3_bucket.buckets["quarantine"].arn
}

output "logs_bucket_name" {
  value = aws_s3_bucket.buckets["logs"].bucket
}

output "logs_bucket_arn" {
  value = aws_s3_bucket.buckets["logs"].arn
}

output "artifacts_bucket_name" {
  value = aws_s3_bucket.buckets["artifacts"].bucket
}

output "artifacts_bucket_arn" {
  value = aws_s3_bucket.buckets["artifacts"].arn
}
