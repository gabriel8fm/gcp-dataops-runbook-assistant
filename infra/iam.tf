# Keyless service accounts plus the grants that are not dataset access
# (dataset access lives in bigquery.tf). Every grant is additive.

# Both accounts need roles/bigquery.jobUser to run load and query jobs; data
# access is granted per dataset only.
module "service_accounts" {
  source  = "terraform-google-modules/service-accounts/google"
  version = "~> 5.1"

  project_id   = var.project_id
  names        = [local.pipeline_sa_id, local.reader_sa_id]
  display_name = "Runbook assistant demo"
  descriptions = [
    "Incident pipeline: loads raw batches and runs Dataform",
    "Runbook assistant: read-only on incident_analytics",
  ]
  project_roles = ["${var.project_id}=>roles/bigquery.jobUser"]
  generate_keys = false

  depends_on = [module.project_services]
}

# Operators impersonate the accounts instead of downloading keys.
module "operator_impersonation" {
  source  = "terraform-google-modules/iam/google//modules/service_accounts_iam"
  version = "~> 8.3"

  count = length(var.operator_members) > 0 ? 1 : 0

  project          = var.project_id
  service_accounts = [local.pipeline_sa, local.reader_sa]
  mode             = "additive"

  bindings = {
    "roles/iam.serviceAccountTokenCreator" = var.operator_members
  }

  depends_on = [module.service_accounts]
}
