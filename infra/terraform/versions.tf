terraform {
  required_version = ">= 1.5"

  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "~> 6.0"
    }
  }

  # For a team, keep state in a GCS bucket instead of on a laptop:
  # backend "gcs" {
  #   bucket = "my-terraform-state"
  #   prefix = "quickbite"
  # }
}

provider "google" {
  project = var.project_id
  region  = var.region
}
