output "landing_bucket" {
  value = google_storage_bucket.landing.name
}

output "datasets" {
  value = [for d in google_bigquery_dataset.layer : d.dataset_id]
}

output "pipeline_service_account" {
  value = google_service_account.pipeline.email
}
