# MongoDB Profile Usage Analysis Design

## Status

This design is now implemented and has been updated to describe the current app behavior instead of the original plan.

## Goal

The `MongoDB Usage 分析` page connects to MongoDB, reads `system.profile`, extracts actual API usage, maps observed APIs to Oracle `Feature Support` detail rows, and feeds the result into the migration assessment layer.

This means the usage analysis flow is no longer a standalone report-only feature. In the current app it is the upstream data source for migration necessity, complexity, hotspot, and evidence views.

## Current User Flow

1. User first syncs Oracle official documentation in the `文档同步` tab.
2. User opens `MongoDB Usage 分析`.
3. User enters a MongoDB URI that already includes the target database in the path.
4. User optionally fills a start time, end time, and sample limit.
5. User can click `测试连接` to confirm the database is reachable and whether `system.profile` exists.
6. User clicks `分析 system.profile`.
7. The app reads profile records, normalizes them, extracts features, maps them to Oracle support data, applies migration rules, and renders the result.
8. User can switch Oracle target version and deployment mode to recompute effective Oracle support status and migration complexity without rereading MongoDB.
9. User can export HTML and Excel artifacts or load a previous cached run.

## Architecture

The implemented flow is split across these modules:

- [src/oracle_feature_support/mongodb_profile_reader.py](/Users/qizou/aiworkspace/mongodbapi-feature-update/src/oracle_feature_support/mongodb_profile_reader.py)
  - MongoDB connection test and `system.profile` reads
- [src/oracle_feature_support/profile_parser.py](/Users/qizou/aiworkspace/mongodbapi-feature-update/src/oracle_feature_support/profile_parser.py)
  - profile record normalization and feature extraction
- [src/oracle_feature_support/feature_mapper.py](/Users/qizou/aiworkspace/mongodbapi-feature-update/src/oracle_feature_support/feature_mapper.py)
  - exact-name mapping from observed MongoDB features to Oracle detail rows
- [src/oracle_feature_support/migration_assessment.py](/Users/qizou/aiworkspace/mongodbapi-feature-update/src/oracle_feature_support/migration_assessment.py)
  - migration necessity, complexity, priority, hotspot, and excluded-item assessment
- [src/oracle_feature_support/usage_report.py](/Users/qizou/aiworkspace/mongodbapi-feature-update/src/oracle_feature_support/usage_report.py)
  - summary building and CSV artifact output
- [app.py](/Users/qizou/aiworkspace/mongodbapi-feature-update/app.py)
  - Streamlit orchestration, cache restore, export, filtering, and evidence rendering

## Input Model

The current UI accepts:

- `MongoDB URI`
- `开始时间（可选）`
- `结束时间（可选）`
- `最大采样条数`
- `显示日志`

The database name is derived from the URI path and is not entered separately anymore. If the URI path does not include a database, the UI rejects the request.

## `system.profile` Read Rules

The current implementation:

- connects directly to the target MongoDB URI
- derives the target database from the URI path
- reads only from `<database>.system.profile`
- supports optional `ts` range filtering
- sorts by `ts` descending
- fetches `limit + 1` rows to detect truncation
- projects only the required profile fields:
  - `ts`
  - `ns`
  - `op`
  - `command`
  - `query`
  - `updateobj`
  - `millis`
  - `nreturned`
  - `docsExamined`
  - `keysExamined`
  - `errCode`
  - `errMsg`

If `system.profile` does not exist, the app returns a user-visible error or warning. If the query hits the sampling limit, the UI warns that the result may be truncated.

## Data Model

### ProfileEvent

Normalized profile records carry:

- `ts`
- `db_name`
- `collection_name`
- `op`
- `command_name`
- `command_doc`
- `duration_ms`
- `docs_examined`
- `keys_examined`
- `nreturned`
- `err_code`
- `err_msg`
- `raw`

### FeatureUsage

Raw extracted features carry:

- `feature_type`
- `feature_name`
- `command_name`
- `op_type`
- `database`
- `collection`
- `event_ts`
- `duration_ms`
- `sample_path`
- `sample_value`

The aggregated usage dataset keeps:

- `feature_type`
- `feature_name`
- `command_name`
- `op_type`
- `database`
- `collection`
- `usage_count`
- `first_seen`
- `last_seen`
- `max_duration_ms`
- `sample_path`
- `sample_value`
- Oracle support columns after mapping
- migration-assessment columns after rule evaluation

## Implemented Feature Extraction

The current parser extracts four feature classes:

- `command`
- `stage`
- `operator`
- `expression`

### Explicitly recognized commands

- `find`
- `aggregate`
- `insert`
- `update`
- `delete`
- `findAndModify`
- `distinct`
- `count`
- `createIndexes`
- `dropIndexes`
- `listIndexes`

When the command is not in the explicit list, the parser now falls back to the first non-metadata top-level key in the command document. This keeps unknown or newly introduced commands visible instead of collapsing them into silent misses.

### Recognized stages

- `$match`
- `$project`
- `$group`
- `$sort`
- `$limit`
- `$skip`
- `$unwind`
- `$lookup`
- `$facet`
- `$count`
- `$addFields`
- `$set`
- `$unset`
- `$merge`
- `$out`
- `$bucketAuto`
- `$graphLookup`

### Recognized operators

- `$expr`
- `$in`
- `$nin`
- `$eq`
- `$ne`
- `$gt`
- `$gte`
- `$lt`
- `$lte`
- `$exists`
- `$regex`
- `$and`
- `$or`
- `$not`
- `$nor`
- `$elemMatch`
- `$size`
- `$all`
- `$set`
- `$unset`
- `$inc`
- `$push`
- `$pull`
- `$addToSet`
- `$rename`
- `$min`
- `$max`

### Recognized expressions

- `$sum`
- `$avg`
- `$min`
- `$max`
- `$cond`
- `$ifNull`
- `$map`
- `$filter`
- `$reduce`
- `$concat`
- `$substr`
- `$toString`
- `$dateToString`
- `$year`
- `$month`
- `$dayOfMonth`
- `$regexMatch`
- `$setField`

### Traversal rules

The implemented extractor walks:

- `find.filter`
- `find.projection`
- `update.updates[].q`
- `update.updates[].u`
- `delete.deletes[].q`
- `findAndModify.query`
- `findAndModify.update`
- `aggregate.pipeline[*]`
- nested `$lookup.pipeline`
- nested `$facet` pipelines

When the parser encounters unknown `$` stage, operator, or expression keys in those traversal paths, it now keeps them in the extracted usage dataset. They may still remain `Unknown` at the Oracle mapping stage, but they are no longer silently discarded.

For older or alternate profile shapes, the parser also normalizes:

- `query` records into `find`-style command docs
- `update` records into `update.updates[].{q,u}`
- `remove` records into `delete.deletes[].q`

This improves coverage for profile samples where `command` is absent but `query` / `updateobj` still carry the useful evidence.

## Oracle Mapping Rules

The mapping layer uses exact normalized-name matching against the Oracle detail dataset:

- `command` -> Oracle `Command`
- `stage` -> Oracle `Stage`
- `operator` and `expression` -> Oracle `Operator`

The mapped output uses:

- `Supported`
- `Partially Supported`
- `Not Supported`
- `Unknown`

If no Oracle row matches, the feature stays `Unknown`. The current metadata also records:

- `unknown_command_event_count`
- `unmapped_feature_count`

## Current UI Surfaces

After a successful run, the right-side panel renders:

- analysis metadata and truncation warning
- Oracle target version and deployment controls
- usage summary cards
- filter controls
- `API 基准` tab
- `实际使用 API` tab
- evidence samples for the selected workload row
- HTML and Excel export actions
- cache load and cache clear actions

## Output Artifacts

The usage analysis flow writes to `outputs/mongodb_usage_<timestamp>/`:

- `mongodb_usage_feature_detail.csv`
- `mongodb_usage_feature_summary.csv`
- `mongodb_usage_metadata.json`
- `mongodb_migration_complexity_detail.csv`
- `mongodb_migration_summary.csv`
- `mongodb_migration_hotspots.csv`
- `mongodb_migration_excluded_commands.csv`
- `mongodb_usage_report.html`
- `mongodb_usage_analysis.xlsx`

Compared with the original design, Markdown export is no longer part of the current implementation.

## Important Deltas From The Original Plan

- migration complexity is now part of the same end-to-end flow instead of a later phase
- database selection now comes from the MongoDB URI, not a separate input field
- export format is HTML and Excel, not Markdown
- Oracle target version and deployment mode can be adjusted after a run
- usage analysis includes cache reload, offline report generation, and evidence drill-down

## Current Gaps

The main remaining gaps are not in the read or parse pipeline. They are in workflow completeness for delivery use:

- the override editor in the UI still exposes only `override_complexity` and `override_reason`
- hotspots and excluded items are exported, but not yet promoted to first-class UI views
- key explanation fields such as confidence, review flags, and priority reasons are computed but not fully surfaced in the main workflow
- the app still treats each analysis as a snapshot and does not compare runs over time

## Delivery-Focused Roadmap

### P0

Turn usage analysis into execution-ready migration input:

- complete the override editor with `override_scope` and `override_action`
- add explicit hotspots, excluded-items, and fallback-rule views
- group observed APIs into delivery work packages such as query rewrite, aggregation rewrite, semantic validation, index review, and blocker investigation

### P1

Improve trustworthiness of the assessment:

- add sampling coverage scoring
- add confidence labels and low-confidence warnings
- preserve and render more than one evidence sample per API when needed
- surface `complexity_adjustment_reason`, `priority_reason`, `scope_confidence`, and `needs_review` in the UI

### P2

Support ongoing migration programs rather than one-off analysis:

- compare two saved runs and highlight new, removed, or risk-increased APIs
- compare different environments such as test, UAT, and production
- compare Oracle target versions and deployment modes side by side

## Risks And Constraints

- results only reflect the sampled profile window
- missing observations do not prove missing application usage
- fast operations may be absent if profiling settings were too weak before sampling
- some MongoDB semantics cannot be reconstructed from profile data alone
- Oracle support mapping depends on the latest synced Oracle detail dataset
