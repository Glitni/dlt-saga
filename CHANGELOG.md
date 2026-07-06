## 0.12.0 — 2026-07-06

### Fixed
- Use explicit column lists in historize INSERTs to prevent positional misalignment (#199)
- Make SQL string-literal escaping dialect-aware (BigQuery \' vs DuckDB '') (#197)
- Prune native_load dedup set only when the file scan is restricted (#195)
- Re-enable failed native_load files via latest-event-wins dedup (#193)
- Keep load_id aligned with its row past the first chunk in native_load bulk insert (#191)
- Stamp one tz-aware run timestamp for _dlt_ingested_at on the Arrow path (#189)
- Draw incremental historize baseline from target open rows (replace+historize re-versioning) (#187)

## 0.11.0 — 2026-07-02

### Added
- Reconcile column/table description & classification drift (+ DuckDB, historized tables) (#184)
- Column & table descriptions and data classification in pipeline config (#181)


### Fixed
- Read config YAML as UTF-8 to preserve non-ASCII values (#183)
- Escape control characters in hand-built SQL string literals (#182)

## 0.10.0 — 2026-07-01

### Breaking Changes
- Consolidate env-var secret handling (remove ${VAR}, rename env:: → env_secret::) (#175)


### Fixed
- Recognize conventional "!" breaking-change marker in changelog generation (#174)

## 0.9.0 — 2026-07-01

### Added
- Add Entra ID certificate authentication for SharePoint (Azure ACS retired) (#169)


### Fixed
- Reconcile partition/cluster DDL so Databricks records local runs (#170)

## 0.8.0 — 2026-06-30

### Added
- Introspect destination configs for the profile schema with per-type validation (#164)
- Record local run outcomes so failures appear in saga report (#162)
- Improve saga report design, usability, and navigation (#158)


### Fixed
- Attribute failed runs to the correct phase in saga report (#163)
- Store the real pipeline error so saga report shows the cause, not "failed" (#160)

## 0.7.2 — 2026-06-27

### Added
- Add dev: config override block with dynamic templating (#154)

## 0.7.1 — 2026-06-27

### Fixed
- Stream date-window records and warn on lossy overlap config (#151)

## 0.7.0 — 2026-06-26

### Added
- Add date-window incremental pipeline for REST API sources (#149)

## 0.6.3 — 2026-06-26

### Fixed
- Skip api incremental filter for fetch_data-overriding subclasses (#146)

## 0.6.2 — 2026-06-26

### Fixed
- Implement incremental loading for the api source (#144)

## 0.6.1 — 2026-06-25

### Added
- Auto-link project-level files in generate-schemas (#140)


### Fixed
- Add missing storage_root field to profile schema (#141)

## 0.6.0 — 2026-06-24

### Added
- Add `saga lint` to flag adapter & config convention anti-patterns (#135)
- Add `saga new config` to scaffold pipeline configs for existing adapters (#133)
- Add `saga new adapter` scaffolder to guide adapter authoring (#129)


### Fixed
- Rename SharePoint auth_secret config field to token_request_body (#137)

## 0.5.0 — 2026-06-23

### Added
- Use sandboxed Jinja2 for config interpolation (profiles, saga_project, configs) (#126)
- Auto-link pipeline config files to their schema by adapter (#124)


### Changed
- Dedup saga_project schema config fields via $defs (#122)

## 0.4.3 — 2026-06-22

### Fixed
- Inject _dlt_ingested_at for replace so replace+historize works (#118)
- Validate shared adapter config keys in saga_project.yml (#116)

## 0.4.2 — 2026-06-19

### Fixed
- Pre-create BigLake Iceberg tables with all NOT NULL columns, not just primary_key (#112)

## 0.4.1 — 2026-06-18

### Fixed
- Record actual task_count in _saga_executions row (#110)
- Pass data project_id to dlt's BigQuery destination, not billing project (#108)

## 0.4.0 — 2026-06-17

### Added
- Layer-aware naming hooks, BigLake URI routing, historize_schema_access overlay, dataset_access rename (#103)


### Changed
- Collapse build_historize_create_table_sql signature into MaterializationHints dataclass (#104)

## 0.3.7 — 2026-06-15

### Fixed
- Cli survives third-party logging.dictConfig that disables saga loggers (#99)

## 0.3.6 — 2026-06-15

### Fixed
- Surface per-pipeline errors at CLI exit boundary (#95)

## 0.3.5 — 2026-06-14

### Fixed
- Normalize schema name when cleaning _dlt_version on Databricks full-refresh (#92)

## 0.3.4 — 2026-06-11

### Fixed
- Native_load chunks by load_batch_size, not by parent directory (#89)

## 0.3.3 — 2026-06-11

### Fixed
- Interleave task groups by schema alongside singletons (#86)

## 0.3.2 — 2026-06-11

### Fixed
- Cap and propagate worker concurrency in orchestrated mode (#83)

## 0.3.1 — 2026-06-10

### Added
- Honor initial_value in native_load for first-run backfill (#78)


### Fixed
- Collapse write_disposition to a single source of truth (#80)

## 0.3.0 — 2026-06-10

### Added
- Add merge_key to historize for scoped deletion detection (#69)
- Configurable SCD2 column names in historize (#53)
- Sync partition_expiration_days on existing BigQuery tables (#68)
- Add partition_expiration_days to BigQuery destination (#67)
- Add CSV quoted-newlines and parity options to native_load (#64)


### Fixed
- Surface partition_expiration_days in generated JSON schemas (#74)
- Avoid correlated subquery in window clause (Databricks historize) (#55)
- Skip adapter validation for historize-only pipelines (#54)

## 0.2.9 — 2026-06-03

### Added
- Accept explicit execution_id in saga plan command (#51)

## 0.2.8 — 2026-06-03

### Added
- --dry-run, grant/revoke labels, change-count summary for update-access (#49)
- Add saga plan --compact for single-line JSON output (#44)
- Add orchestration.dataset_access for the orchestration schema (#43)


### Changed
- Clearer logs for saga update-access (#47)


### Fixed
- Validate dataset_access OWNER entries to prevent lockout (#48)
- Correct misleading native_load warning during saga update-access (#46)
- Correct Session profile resolution to use the standard env/config chain (#45)

## 0.2.7 — 2026-05-20

### Added
- Declarative row filters (ingest + historize) (#41)
- Declarative row filters (ingest + historize)
- Insert-only merge strategy (#40)
- Databricks Zerobus insert_api support (#39)

## 0.2.6 — 2026-05-19

### Added
- Dbt-style debug log files in logs/ (#36)

## 0.2.5 — 2026-05-19

### Added
- Per-weekday hour bindings in hourly tags (#34)


### Fixed
- Thread log_prefix through ingest and historize pipeline-internal logs (#33)

## 0.2.4 — 2026-05-13

### Fixed
- Release script regenerates and commits uv.lock alongside the version bump (#30)

## 0.2.3 — 2026-05-13

### Fixed
- Native_load completion summary shows "Unknown → Unknown: 0 rows" (#27)

# Changelog

All notable changes to dlt-saga are documented here.

## 0.2.2 — 2026-05-08

### Added
- Configurable table_format and placement for historize layer (#19)
- Add track_columns/ignore_columns for historize change detection (#15)
- Native cloud-storage load adapter (dlt_saga.native_load) (#13)
- Add saga ai-setup command for AI-assisted development (#4)


### Fixed
- Make gh CLI optional in release script (#24)
- Harden historize state log and native_load error paths (#21)
- Propagate --start-value-override / --end-value-override / --force to orchestrated workers (#18)
- Override timestamp_n_days_ago in DuckDB destination (#14)
- Support hierarchical tag syntax in JSON schema (#9)
- Separate codecov workflow for main branch coverage baseline (#3)
- Master→main in release scripts and remove stale references (#2)


### Security
- Pin CI action SHAs and add safe dependency upgrade script (#12)
