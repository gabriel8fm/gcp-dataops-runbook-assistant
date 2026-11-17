locals {
  labels = {
    app  = "runbook-assistant"
    data = "synthetic"
  }

  datasets = {
    incident_raw        = "Raw synthetic incident events; one partition per delivery date."
    incident_analytics  = "Dataform outputs: stg_incident_events view and incidents_validated table."
    incident_assertions = "Dataform assertion views; rows in a view are quality failures."
  }

  pipeline_dataset_roles = { for name in keys(local.datasets) : name => "roles/bigquery.dataEditor" }
}

resource "google_project_service" "apis" {
  for_each = toset(["bigquery.googleapis.com", "iam.googleapis.com"])

  service            = each.value
  disable_on_destroy = false
}

resource "google_bigquery_dataset" "datasets" {
  for_each = local.datasets

  dataset_id                 = each.key
  description                = each.value
  location                   = var.bigquery_location
  max_time_travel_hours      = "48"
  delete_contents_on_destroy = !var.deletion_protection

  depends_on = [google_project_service.apis]
}

resource "google_bigquery_table" "incident_events" {
  dataset_id          = google_bigquery_dataset.datasets["incident_raw"].dataset_id
  table_id            = "incident_events"
  description         = "Synthetic incident lifecycle events. Loads replace one batch_date partition (WRITE_TRUNCATE on table$YYYYMMDD)."
  schema              = file("${path.module}/../schema/incident_events.json")
  clustering          = ["service"]
  deletion_protection = var.deletion_protection

  time_partitioning {
    type  = "DAY"
    field = "batch_date"
  }
}

resource "google_service_account" "pipeline" {
  account_id   = "incident-pipeline"
  display_name = "Incident pipeline (loader and Dataform runner)"
  depends_on   = [google_project_service.apis]
}

resource "google_service_account" "assistant_reader" {
  account_id   = "incident-assistant-reader"
  display_name = "Runbook assistant (read-only on incident_analytics)"
  depends_on   = [google_project_service.apis]
}

# Running query and load jobs is a project-level permission.
resource "google_project_iam_member" "job_user" {
  for_each = {
    pipeline         = google_service_account.pipeline.member
    assistant_reader = google_service_account.assistant_reader.member
  }

  project = var.project_id
  role    = "roles/bigquery.jobUser"
  member  = each.value
}

# Data access is granted per dataset, never project-wide.
resource "google_bigquery_dataset_iam_member" "pipeline_editor" {
  for_each = local.pipeline_dataset_roles

  dataset_id = google_bigquery_dataset.datasets[each.key].dataset_id
  role       = each.value
  member     = google_service_account.pipeline.member
}

resource "google_bigquery_dataset_iam_member" "assistant_viewer" {
  dataset_id = google_bigquery_dataset.datasets["incident_analytics"].dataset_id
  role       = "roles/bigquery.dataViewer"
  member     = google_service_account.assistant_reader.member
}

resource "google_service_account_iam_member" "operator_impersonation" {
  for_each = {
    for pair in setproduct(["pipeline", "assistant_reader"], var.operator_members) :
    "${pair[0]}:${pair[1]}" => { account = pair[0], member = pair[1] }
  }

  service_account_id = each.value.account == "pipeline" ? google_service_account.pipeline.name : google_service_account.assistant_reader.name
  role               = "roles/iam.serviceAccountTokenCreator"
  member             = each.value.member
}
