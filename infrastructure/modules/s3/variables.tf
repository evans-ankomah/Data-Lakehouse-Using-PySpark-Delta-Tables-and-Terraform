variable "environment" {
  type = string
}

variable "account_id" {
  type = string
}

variable "kms_key_arns" {
  description = "Map of KMS key ARNs keyed by bucket purpose."
  type = object({
    raw       = string
    processed = string
    archive   = string
    logs      = string
    artifacts = string
  })
}
