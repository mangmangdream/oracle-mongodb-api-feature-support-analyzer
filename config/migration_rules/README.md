# Migration Rules

This directory stores rule-table inputs for MongoDB migration complexity assessment.

The baseline is intentionally scoped to application compatibility when migrating
from MongoDB to Oracle Database API for MongoDB:

- application-facing APIs and application-managed objects stay in scope
- operational, diagnostic, security-management, and cluster-management commands
  are usually marked out of scope unless the customer explicitly wants to keep
  the same operational workflow
- if the selected Oracle target combination supports an application-facing
  feature, the effective migration complexity is reduced to `Low` because the
  default assumption is that application code can use it directly without
  material changes

Files:

- `rules_manifest.json`: version metadata for the built-in ruleset
- `command_rules.csv`: command-level baseline rules
- `stage_rules.csv`: aggregation stage baseline rules
- `operator_rules.csv`: query/update operator baseline rules
- `expression_rules.csv`: aggregation expression baseline rules
- `customer_overrides.csv`: customer-specific overrides without modifying built-in rules

Override behavior:

- built-in baseline rules are loaded first
- Oracle support status then raises minimum complexity floors
- customer overrides are applied last

Expected override columns:

- `feature_type`
- `feature_name`
- `override_scope`
- `override_complexity`
- `override_reason`
- `override_action`
- `enabled`
