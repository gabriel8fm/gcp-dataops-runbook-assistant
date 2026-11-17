output "raw_table_id" {
  description = "Load target for pipeline.py --backend bigquery."
  value       = "${var.project_id}.${google_bigquery_table.incident_events.dataset_id}.${google_bigquery_table.incident_events.table_id}"
}

output "validated_table_id" {
  description = "Table that Dataform builds and the assistant reads."
  value       = "${var.project_id}.incident_analytics.incidents_validated"
}

output "pipeline_service_account" {
  value = google_service_account.pipeline.email
}

output "assistant_reader_service_account" {
  value = google_service_account.assistant_reader.email
}
