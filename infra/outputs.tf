output "raw_table_id" {
  description = "Load target for pipeline.py --backend bigquery."
  value       = "${var.project_id}.${module.incident_raw.bigquery_dataset.dataset_id}.${module.incident_raw.table_names[0]}"
}

output "validated_table_id" {
  description = "Table that Dataform builds and the assistant reads."
  value       = "${var.project_id}.${module.incident_analytics.bigquery_dataset.dataset_id}.incidents_validated"
}

output "pipeline_service_account" {
  value = module.service_accounts.emails[local.pipeline_sa_id]
}

output "assistant_reader_service_account" {
  value = module.service_accounts.emails[local.reader_sa_id]
}
