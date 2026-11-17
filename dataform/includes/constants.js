// Shared helpers for SQL generated from workflow_settings.yaml vars.
function quotedList(csv) {
  return csv
    .split(",")
    .map((value) => `'${value.trim()}'`)
    .join(", ");
}

module.exports = { quotedList };
