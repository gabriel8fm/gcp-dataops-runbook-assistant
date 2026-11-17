// Raw landing table created by Terraform (infra/) and filled by pipeline.py load.
// Partitioned by batch_date; each load job replaces exactly one partition.
declare({
  schema: dataform.projectConfig.vars.raw_dataset,
  name: "incident_events",
  description: "Synthetic incident lifecycle events, one partition per delivery date.",
});
