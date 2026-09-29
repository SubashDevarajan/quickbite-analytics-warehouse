variable "project_id" {
  description = "GCP project that hosts the warehouse"
  type        = string
}

variable "region" {
  description = "Region for the landing bucket and BigQuery datasets"
  type        = string
  default     = "asia-south1" # Mumbai
}

variable "environment" {
  description = "dev lets `terraform destroy` delete non-empty buckets and datasets; prod does not"
  type        = string
  default     = "dev"

  validation {
    condition     = contains(["dev", "prod"], var.environment)
    error_message = "environment must be dev or prod"
  }
}

variable "landing_retention_days" {
  description = "Raw landing files are deleted after this many days (bronze data lives in BigQuery raw)"
  type        = number
  default     = 30
}
