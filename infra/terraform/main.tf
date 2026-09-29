locals {
  layers = ["raw", "staging", "snapshots", "marts"]
  labels = {
    project     = "quickbite"
    environment = var.environment
    managed_by  = "terraform"
  }
  is_dev = var.environment == "dev"
}

resource "google_project_service" "apis" {
  for_each           = toset(["bigquery.googleapis.com", "storage.googleapis.com", "iam.googleapis.com"])
  service            = each.value
  disable_on_destroy = false
}

# ------------------------------------------------------------------ landing zone
resource "google_storage_bucket" "landing" {
  name                        = "${var.project_id}-quickbite-landing"
  location                    = var.region
  uniform_bucket_level_access = true
  public_access_prevention    = "enforced"
  force_destroy               = local.is_dev
  labels                      = local.labels

  lifecycle_rule {
    condition {
      age = var.landing_retention_days
    }
    action {
      type = "Delete"
    }
  }

  depends_on = [google_project_service.apis]
}

# ------------------------------------------------------------------ warehouse
resource "google_bigquery_dataset" "layer" {
  for_each                   = toset(local.layers)
  dataset_id                 = each.value
  location                   = var.region
  description                = "QuickBite ${each.value} layer"
  delete_contents_on_destroy = local.is_dev
  labels                     = local.labels

  depends_on = [google_project_service.apis]
}

# ------------------------------------------------------------------ pipeline identity
# Least privilege: the pipeline can read/write its own bucket and datasets and
# run query jobs, nothing else in the project.
resource "google_service_account" "pipeline" {
  account_id   = "quickbite-pipeline"
  display_name = "QuickBite Airflow + dbt pipeline"
}

resource "google_storage_bucket_iam_member" "pipeline_landing" {
  bucket = google_storage_bucket.landing.name
  role   = "roles/storage.objectAdmin"
  member = "serviceAccount:${google_service_account.pipeline.email}"
}

resource "google_bigquery_dataset_iam_member" "pipeline_editor" {
  for_each   = google_bigquery_dataset.layer
  dataset_id = each.value.dataset_id
  role       = "roles/bigquery.dataEditor"
  member     = "serviceAccount:${google_service_account.pipeline.email}"
}

resource "google_project_iam_member" "pipeline_job_user" {
  project = var.project_id
  role    = "roles/bigquery.jobUser"
  member  = "serviceAccount:${google_service_account.pipeline.email}"
}
