# Infrastructure (GCP)

Only needed for `WAREHOUSE=bigquery`. The local DuckDB setup needs none of this.

Creates: a landing bucket (30-day lifecycle), four BigQuery datasets
(`raw`, `staging`, `snapshots`, `marts`), and a least-privilege service
account for the pipeline.

```bash
# one-time: install terraform + gcloud, then
gcloud auth application-default login
cp terraform.tfvars.example terraform.tfvars   # set your project id
terraform init
terraform plan
terraform apply

# key for the pipeline containers (kept out of git by .gitignore)
gcloud iam service-accounts keys create ../../secrets/gcp-key.json \
  --iam-account "$(terraform output -raw pipeline_service_account)"
```

Then in the repo's `.env`:

```
WAREHOUSE=bigquery
GCP_PROJECT=<your project>
GCS_BUCKET=<terraform output landing_bucket>
GOOGLE_APPLICATION_CREDENTIALS=/opt/quickbite/secrets/gcp-key.json
```

and `make up`. Tear down with `terraform destroy`.

Cost: at this data volume (about 1,500 orders a day) usage stays within the
BigQuery and Cloud Storage free tiers; check current GCP pricing for your region.
