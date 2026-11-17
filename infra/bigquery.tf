# Datasets and the raw table, one terraform-google-modules/bigquery call per
# dataset.
#
# Dataset access is set through each module's `access` list, not through
# google_bigquery_dataset_iam_member. The module always writes an authoritative
# `access` block (its default keeps projectOwners as dataOwner), and additive
# dataset IAM members would be removed by that block on the next apply. Each
# list therefore repeats the owner entry and adds only the service accounts that
# need the dataset.

module "incident_raw" {
  source  = "terraform-google-modules/bigquery/google"
  version = "~> 10.2"

  project_id                 = var.project_id
  dataset_id                 = "incident_raw"
  dataset_name               = "incident_raw"
  description                = "Raw synthetic incident events; one partition per delivery date."
  location                   = var.bigquery_location
  max_time_travel_hours      = 48
  delete_contents_on_destroy = !var.deletion_protection
  deletion_protection        = var.deletion_protection
  dataset_labels             = local.labels

  access = [local.dataset_owner, local.pipeline_writer]

  tables = [{
    table_id    = "incident_events"
    description = "Synthetic incident lifecycle events. Loads replace one batch_date partition (WRITE_TRUNCATE on table$YYYYMMDD)."
    schema      = file("${path.module}/../schema/incident_events.json")
    clustering  = ["service"]
    time_partitioning = {
      type          = "DAY"
      field         = "batch_date"
      expiration_ms = null
    }
    labels = local.labels
  }]

  depends_on = [module.project_services, module.service_accounts]
}

module "incident_analytics" {
  source  = "terraform-google-modules/bigquery/google"
  version = "~> 10.2"

  project_id                 = var.project_id
  dataset_id                 = "incident_analytics"
  dataset_name               = "incident_analytics"
  description                = "Dataform outputs: stg_incident_events view and incidents_validated table."
  location                   = var.bigquery_location
  max_time_travel_hours      = 48
  delete_contents_on_destroy = !var.deletion_protection
  dataset_labels             = local.labels

  access = [local.dataset_owner, local.pipeline_writer, local.assistant_reader]

  depends_on = [module.project_services, module.service_accounts]
}

module "incident_assertions" {
  source  = "terraform-google-modules/bigquery/google"
  version = "~> 10.2"

  project_id                 = var.project_id
  dataset_id                 = "incident_assertions"
  dataset_name               = "incident_assertions"
  description                = "Dataform assertion views; rows in a view are quality failures."
  location                   = var.bigquery_location
  max_time_travel_hours      = 48
  delete_contents_on_destroy = !var.deletion_protection
  dataset_labels             = local.labels

  access = [local.dataset_owner, local.pipeline_writer]

  depends_on = [module.project_services, module.service_accounts]
}
