# Enables the APIs this stack needs. Services stay enabled on destroy so removing
# the demo never disables an API that other workloads in the project use.
module "project_services" {
  source  = "terraform-google-modules/project-factory/google//modules/project_services"
  version = "~> 18.4"

  project_id                  = var.project_id
  disable_services_on_destroy = false
  activate_apis = [
    "bigquery.googleapis.com",
    "iam.googleapis.com",
  ]
}
