## Unreleased

### Added
- Configurable SCD2 column names for historize via `valid_from_column`, `valid_to_column`, and `is_deleted_column` (default to `_dlt_valid_from` / `_dlt_valid_to` / `_dlt_is_deleted`). `partition_column` now defaults to `valid_from_column`. The names are part of the historize config fingerprint, so renaming a column on an existing table triggers the standard "config changed → run `saga historize --full-refresh`" guard.

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
