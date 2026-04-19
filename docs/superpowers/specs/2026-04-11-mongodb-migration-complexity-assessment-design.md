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

The app now also builds a separate `MongoDB API baseline + Oracle compatibility mapping` for the `API 基准` page. That baseline is a catalog and reference surface. The migration assessment engine still consumes Oracle-compatible support rows from the usage-analysis path and does not switch to the baseline as its primary scoring input.

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

Migration assessment is rendered primarily inside `MongoDB Usage 分析`, while the `API 基准` page exposes the full MongoDB baseline and its Oracle compatibility mapping.

Within `MongoDB Usage 分析`, the current right-side panel is organized as:

- `采集概览`
- `实际使用 API`

### `API 基准` page

The separate `API 基准` page now shows:

- a full MongoDB API baseline
- Oracle compatibility mapping attached to each MongoDB API
- Oracle `Feature Support` main-table browsing under `Feature Support 明细与覆盖规则`
- inline editing for override complexity and override reason
- saving overrides back to `customer_overrides.csv`

Current limitation:

- `override_scope` and `override_action` are supported by the backend schema and rule engine, but the current editor does not expose them yet

### `MongoDB Usage 分析`

The usage page still focuses on observed workload APIs. It currently supports:

- actual used API counts
- related baseline comparison against the Oracle-compatible catalog rows
- high-complexity counts
- hotspot counts
- migration necessity
- Oracle support status
- effective complexity
- migration priority
- evidence samples for the selected row

Current limitations:

- hotspots and excluded operational items are not yet rendered as first-class tabs or tables
- explanation fields such as `priority_reason`, `complexity_adjustment_reason`, `scope_confidence`, and `needs_review` are not fully surfaced in the main table workflow

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

## Remaining Gaps

The assessment engine is ahead of the UI in several places:

- backend support already exists for `override_scope` and `override_action`, but the editor saves only complexity and reason
- `rules_coverage_rate`, fallback classifications, hotspots, and excluded items are generated, but not all of them have dedicated UI surfaces
- the engine can explain why a row was classified a certain way, but the main workflow still emphasizes outcome over rationale

## New Direction

The migration assessment design now follows the same two-track direction as the usage-analysis layer:

- `usage-hardening`
  - remains the default path
  - keeps migration assessment centered on observed profile-derived APIs
  - improves trust by exposing fallback classifications, confidence signals, and richer rationale in the current workflow
- `collector-lite`
  - remains an experiment path
  - extends assessment inputs with lightweight environment, topology, and object metadata
  - is intended to test whether richer evidence changes migration necessity, complexity, or priority outcomes in a meaningful way

Assessment logic should continue to optimize for interpretability on the default path. Any new evidence source from `collector-lite` must justify itself by improving explanation quality, confidence, or prioritization accuracy.

## Delivery Roadmap

### P0

`usage-hardening`:

- expose full override editing in the baseline table
- add dedicated views for hotspots, excluded items, and fallback-classified APIs
- derive migration work packages from `recommended_action`, scope, and complexity

`collector-lite`:

- incorporate preflight and structural metadata into the assessment context
- test whether administrative-looking APIs can be classified more accurately when deployment and object metadata are available
- verify whether additional evidence improves hotspot explanation rather than only adding more fields

### P1

`usage-hardening`:

- add confidence and sampling sufficiency scoring
- surface `priority_reason`, `complexity_adjustment_reason`, `scope_confidence`, and `needs_review`
- support richer evidence inspection, including multiple samples for the same API

`collector-lite`:

- evaluate sharding and topology evidence as migration-priority modifiers
- distinguish directly observed behavior from metadata-derived inference in the assessment output
- test whether profiler gaps should trigger explicit low-confidence or alternate-evidence handling

### P2

Make the assessment useful across the whole migration program and converge the tracks:

- compare saved runs over time
- compare environments
- compare Oracle target scenarios
- show how rules and customer overrides changed the final migration conclusions
- decide whether `collector-lite` evidence should remain optional, merge into the default path, or be dropped

## Risks And Constraints

- baseline rules will not cover every MongoDB API, so fallback classification remains necessary
- some APIs have ambiguous ownership between application logic and operational tooling
- support-based lowering for `Supported` features is intentionally optimistic and should still be validated against customer semantics
- hotspot ranking depends on observed profile samples, so poor sampling will skew priorities
- assessment confidence should depend on evidence source quality, not only on observed counts
- `LOG_ONLY` and `METRICS_ONLY` style collection should be treated as lower-fidelity evidence than `PROFILE_ONLY`
- when collector-lite introduces multi-strategy collection, the assessment output must preserve `requested_strategy`, `effective_source`, and source-derived confidence
