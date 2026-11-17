variable "project_id" {
  description = "Existing GCP project that hosts the demo. Terraform does not create projects."
  type        = string
}

variable "region" {
  description = "Default provider region."
  type        = string
  default     = "us-central1"
}

variable "bigquery_location" {
  description = "BigQuery location for all datasets. Must match dataform/workflow_settings.yaml."
  type        = string
  default     = "US"
}

variable "operator_members" {
  description = "Principals allowed to impersonate the service accounts, e.g. [\"user:you@example.com\"]. No keys are created."
  type        = list(string)
  default     = []
}

variable "deletion_protection" {
  description = "Protect the raw table from terraform destroy. Off by default so the demo can be removed."
  type        = bool
  default     = false
}
