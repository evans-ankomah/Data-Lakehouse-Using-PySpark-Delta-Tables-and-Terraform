locals {
  keys = {
    raw       = "S3 raw landing zone encryption"
    processed = "S3 processed Delta Lake encryption"
    archive   = "S3 archive zone encryption"
    logs      = "S3 logs bucket and CloudWatch log encryption"
    artifacts = "S3 artifacts bucket encryption"
    glue      = "AWS Glue job and security configuration encryption"
    sns       = "SNS topic encryption"
  }
}

resource "aws_kms_key" "keys" {
  for_each = local.keys

  description             = "lakehouse-${var.environment}-${each.key}: ${each.value}"
  deletion_window_in_days = 30
  enable_key_rotation     = true

  tags = {
    Name    = "lakehouse-${var.environment}-${each.key}"
    Purpose = each.value
  }
}

resource "aws_kms_alias" "aliases" {
  for_each = local.keys

  name          = "alias/lakehouse-${var.environment}-${each.key}"
  target_key_id = aws_kms_key.keys[each.key].key_id
}
