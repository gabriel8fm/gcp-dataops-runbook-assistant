terraform {
  required_version = ">= 1.6"

  # Pinned below 8.0 because terraform-google-modules/bigquery 10.x supports
  # google < 8.
  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "~> 7.46"
    }
    google-beta = {
      source  = "hashicorp/google-beta"
      version = "~> 7.46"
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

provider "google-beta" {
  project = var.project_id
  region  = var.region

  default_labels = local.labels
}
