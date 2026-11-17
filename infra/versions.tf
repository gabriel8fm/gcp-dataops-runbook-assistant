terraform {
  required_version = ">= 1.6"

  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "~> 6.0"
    }
  }

  # Local state by default (gitignored). For shared use, configure a GCS backend:
  # backend "gcs" { bucket = "<state-bucket>" prefix = "runbook-assistant" }
}

provider "google" {
  project = var.project_id
  region  = var.region

  default_labels = local.labels
}
