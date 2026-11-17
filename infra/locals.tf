locals {
  labels = {
    app  = "runbook-assistant"
    data = "synthetic"
  }

  # Emails are built from known inputs instead of module outputs so the IAM
  # modules can use them as for_each keys during the first plan.
  pipeline_sa_id   = "incident-pipeline"
  reader_sa_id     = "incident-assistant-reader"
  pipeline_sa      = "${local.pipeline_sa_id}@${var.project_id}.iam.gserviceaccount.com"
  reader_sa        = "${local.reader_sa_id}@${var.project_id}.iam.gserviceaccount.com"
  dataset_owner    = { role = "roles/bigquery.dataOwner", special_group = "projectOwners" }
  pipeline_writer  = { role = "roles/bigquery.dataEditor", user_by_email = local.pipeline_sa }
  assistant_reader = { role = "roles/bigquery.dataViewer", user_by_email = local.reader_sa }
}
