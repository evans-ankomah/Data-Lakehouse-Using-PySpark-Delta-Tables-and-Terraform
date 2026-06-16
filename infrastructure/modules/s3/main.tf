locals {
  env = var.environment
  acct = var.account_id

  buckets = {
    raw        = "lakehouse-raw-${local.env}-${local.acct}"
    processed  = "lakehouse-processed-${local.env}-${local.acct}"
    archive    = "lakehouse-archive-${local.env}-${local.acct}"
    quarantine = "lakehouse-quarantine-${local.env}-${local.acct}"
    logs       = "lakehouse-logs-${local.env}-${local.acct}"
    artifacts  = "lakehouse-artifacts-${local.env}-${local.acct}"
  }

  # Which buckets get versioning enabled
  versioned_buckets = toset(["raw", "processed", "archive", "artifacts"])

  # KMS key per bucket (logs and quarantine share the logs key; artifacts shares too)
  kms_for_bucket = {
    raw        = var.kms_key_arns.raw
    processed  = var.kms_key_arns.processed
    archive    = var.kms_key_arns.archive
    quarantine = var.kms_key_arns.logs
    logs       = var.kms_key_arns.logs
    artifacts  = var.kms_key_arns.artifacts
  }
}

# ── Bucket resources ──────────────────────────────────────────────────────────

resource "aws_s3_bucket" "buckets" {
  for_each = local.buckets

  bucket        = each.value
  force_destroy = local.env == "dev" ? true : false

  tags = { Name = each.value, Purpose = each.key }
}

# ── Public access block (all four flags true on every bucket) ─────────────────

resource "aws_s3_bucket_public_access_block" "block" {
  for_each = local.buckets

  bucket                  = aws_s3_bucket.buckets[each.key].id
  block_public_acls       = true
  ignore_public_acls      = true
  block_public_policy     = true
  restrict_public_buckets = true
}

# ── Versioning ────────────────────────────────────────────────────────────────

resource "aws_s3_bucket_versioning" "versioning" {
  for_each = local.buckets

  bucket = aws_s3_bucket.buckets[each.key].id

  versioning_configuration {
    status = contains(local.versioned_buckets, each.key) ? "Enabled" : "Suspended"
  }
}

# ── SSE-KMS encryption ────────────────────────────────────────────────────────

resource "aws_s3_bucket_server_side_encryption_configuration" "sse" {
  for_each = local.buckets

  bucket = aws_s3_bucket.buckets[each.key].id

  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm     = "aws:kms"
      kms_master_key_id = local.kms_for_bucket[each.key]
    }
    bucket_key_enabled = true
  }
}

# ── Server access logging (all buckets log to the logs bucket) ────────────────

resource "aws_s3_bucket_logging" "logging" {
  for_each = { for k, v in local.buckets : k => v if k != "logs" }

  bucket        = aws_s3_bucket.buckets[each.key].id
  target_bucket = aws_s3_bucket.buckets["logs"].id
  target_prefix = "s3-access-logs/${each.key}/"
}

# ── Lifecycle rules ───────────────────────────────────────────────────────────

resource "aws_s3_bucket_lifecycle_configuration" "raw_lifecycle" {
  bucket = aws_s3_bucket.buckets["raw"].id

  rule {
    id     = "raw-expire"
    status = "Enabled"
    filter { prefix = "" }

    transition {
      days          = 30
      storage_class = "STANDARD_IA"
    }
    expiration {
      days = 90
    }
    noncurrent_version_expiration {
      noncurrent_days = 7
    }
  }
}

resource "aws_s3_bucket_lifecycle_configuration" "archive_lifecycle" {
  bucket = aws_s3_bucket.buckets["archive"].id

  rule {
    id     = "archive-tiering"
    status = "Enabled"
    filter { prefix = "" }

    transition {
      days          = 90
      storage_class = "GLACIER"
    }
    transition {
      days          = 365
      storage_class = "DEEP_ARCHIVE"
    }
  }
}

resource "aws_s3_bucket_lifecycle_configuration" "quarantine_lifecycle" {
  bucket = aws_s3_bucket.buckets["quarantine"].id

  rule {
    id     = "quarantine-expire"
    status = "Enabled"
    filter { prefix = "" }

    expiration {
      days = 180
    }
  }
}

resource "aws_s3_bucket_lifecycle_configuration" "logs_lifecycle" {
  bucket = aws_s3_bucket.buckets["logs"].id

  rule {
    id     = "logs-expire"
    status = "Enabled"
    filter { prefix = "" }

    expiration {
      days = 365
    }
  }
}

# ── EventBridge notification on raw bucket (routes S3 events to EventBridge) ──

resource "aws_s3_bucket_notification" "raw_eventbridge" {
  bucket      = aws_s3_bucket.buckets["raw"].id
  eventbridge = true
}
