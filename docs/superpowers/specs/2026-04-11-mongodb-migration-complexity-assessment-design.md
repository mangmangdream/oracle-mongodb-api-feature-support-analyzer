# MongoDB Migration Complexity Assessment Design

## Status

This design is implemented. The document below reflects the current rule engine, UI, and output artifacts rather than the original proposed shape.

## Goal

The migration assessment layer classifies observed MongoDB APIs into migration scope, migration necessity, complexity, priority, recommended action, hotspot, and excluded-command views so the app can answer a practical question:

> For the APIs this application actually uses, what is the migration workload to Oracle Database API for MongoDB?

## Current Inputs

The implemented assessment consumes four inputs:

### 1. Observed MongoDB usage

From the usage-analysis pipeline:

- `feature_type`
- `feature_name`
- `command_name`
- `usage_count`
- `max_duration_ms`
- timestamps
- sample path and sample value
- evidence attribution (`database_level` or `instance_level`)

### 2. Oracle support mapping

From the synced Oracle `Feature Support` detail dataset:

- `oracle_support_status`
- `oracle_support_since`
- `oracle_category`
- `oracle_feature`

### 3. Built-in rules

Loaded from:

- `config/migration_rules/rules_manifest.json`
- `config/migration_rules/command_rules.csv`
- `config/migration_rules/stage_rules.csv`
- `config/migration_rules/operator_rules.csv`
- `config/migration_rules/expression_rules.csv`

### 4. Customer overrides

Loaded from:

- `config/migration_rules/customer_overrides.csv`

## Rule And Override Schema

### Baseline rule columns

The current validator expects:

- `feature_name`
- `feature_type`
- `migration_scope`
- `scope_confidence`
- `needs_review`
- `default_complexity`
- `complexity_reason`
- `recommended_action`
- `review_priority`
- `notes`
- `enabled`

### Override columns

The current override loader supports:

- `feature_type`
- `feature_name`
- `override_scope`
- `override_complexity`
- `override_reason`
- `override_action`
- `enabled`

The UI currently saves `override_complexity` and `override_reason` from the baseline table. `override_scope` and `override_action` remain supported by the backend schema but are not exposed in the current editor.

## Controlled Vocabulary

### Scope

- `application_api`
- `likely_admin`
- `ignore`

### Complexity

- `Ignore`
- `Low`
- `Medium`
- `High`
- `Blocker`

### Recommended action

- `direct_use`
- `verify_semantics`
- `minor_query_rewrite`
- `minor_update_rewrite`
- `rewrite_aggregation`
- `replace_operator`
- `review_index_management`
- `exclude_from_scope`
- `manual_assessment`
- `migration_blocker_review`

## Implemented Assessment Pipeline

### 1. Match baseline rules

Rules are matched by:

- `feature_type`
- `feature_name`

If a rule is found, the row inherits default scope, default complexity, reason, action, and rule metadata.

### 2. Apply fallback classification

If no baseline rule matches, the current implementation falls back to:

- `application_api`
- `Medium`
- manual review messaging

This keeps unmatched APIs visible and contributes to `unclassified_feature_count`.

### 3. Apply support-based adjustment

The implemented logic uses Oracle support as a complexity adjuster:

- `Supported`
  - lowers application-scope complexity to at most `Low`
- `Partially Supported`
  - raises complexity to at least `Medium`
- `Not Supported`
  - raises complexity to at least `High`
- `Unknown`
  - raises complexity to at least `Medium`

For non-application scope rows, the current implementation forces effective complexity to `Ignore`.

### 4. Promote blocker candidates

The current code contains a narrow blocker candidate list. At the moment, `("stage", "$graphLookup")` is explicitly treated as a blocker candidate when Oracle marks it `Not Supported`.

### 5. Apply administrative heuristics

In addition to CSV rules, the current implementation includes built-in heuristics for:

- large sets of admin or operational command names
- admin-style command prefixes
- object-management commands that are conditionally in scope
- non-core compatibility commands

These heuristics prevent operational commands from distorting application migration totals.

### 6. Apply customer overrides

If a customer override matches:

- scope can be replaced
- complexity can be replaced
- reason can be replaced
- action can be replaced

The result also tracks whether the override was applied.

### 7. Derive effective outputs

The resulting usage detail dataset includes:

- `default_scope`
- `effective_scope`
- `default_complexity`
- `effective_complexity`
- `complexity_reason`
- `complexity_adjustment_reason`
- `recommended_action`
- `migration_priority`
- `priority_reason`
- `override_applied`
- `rule_source`
- `rules_version`

## Current UI Surfaces

Migration assessment is rendered inside `MongoDB Usage 分析` and is split into two tabs:

- `API 基准`
- `实际使用 API`

### `API 基准`

This tab shows the Oracle catalog baseline enriched with migration classification. It currently supports:

- total API counts and observed-in-profile counts
- effective migration necessity
- effective complexity
- observed usage counts and command contexts
- inline editing for override complexity and override reason
- saving overrides back to `customer_overrides.csv`

### `实际使用 API`

This tab focuses on observed workload APIs and currently exposes:

- actual used API counts
- high-complexity counts
- hotspot counts
- migration necessity
- Oracle support status
- effective complexity
- migration priority
- evidence samples for the selected row

## Output Artifacts

The current implementation writes these migration-related artifacts under `outputs/mongodb_usage_<timestamp>/`:

- `mongodb_migration_complexity_detail.csv`
- `mongodb_migration_summary.csv`
- `mongodb_migration_hotspots.csv`
- `mongodb_migration_excluded_commands.csv`

At the wrapper level, the same run also exports:

- `mongodb_usage_report.html`
- `mongodb_usage_analysis.xlsx`

Compared with the original design, there is no standalone Markdown migration report in the current app.

## Summary Metrics

The current run metadata and summary views expose:

- `rules_version`
- `override_count`
- `rules_coverage_rate`
- `unclassified_feature_count`
- `requested_strategy`
- `resolved_strategy`
- `effective_source`
- `fallback_chain`
- `database_attribution`
- workload counts by complexity
- hotspot counts
- excluded operational items

When the workload source resolves to `serverStatus.metrics`, the assessment layer treats the result as instance-level evidence. These rows remain assessable for support and complexity, but they are not treated as precise database-level workload attribution.

## Important Deltas From The Original Plan

- migration assessment is fully integrated into the usage-analysis page rather than a separate report surface
- the primary UI is `API 基准` plus `实际使用 API`, not separate `Hotspots`, `Excluded`, and `Evidence` tabs
- HTML and Excel are the current exported formats
- backend override schema is broader than the current UI editor
- support status is recomputed dynamically against selected Oracle target version and deployment mode
- workload collection now uses short-circuit source selection (`system.profile -> global log -> serverStatus.metrics`) instead of combining multiple workload sources in one run

## Risks And Constraints

- baseline rules will not cover every MongoDB API, so fallback classification remains necessary
- some APIs have ambiguous ownership between application logic and operational tooling
- support-based lowering for `Supported` features is intentionally optimistic and should still be validated against customer semantics
- hotspot ranking depends on observed profile samples, so poor sampling will skew priorities
- assessment confidence should depend on evidence source quality, not only on observed counts
- `LOG_ONLY` and `METRICS_ONLY` style collection should be treated as lower-fidelity evidence than `PROFILE_ONLY`
- when collector-lite introduces multi-strategy collection, the assessment output must preserve `requested_strategy`, `effective_source`, and source-derived confidence
