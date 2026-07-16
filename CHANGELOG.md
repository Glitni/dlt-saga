## 0.26.0 — 2026-07-16

### Breaking Changes
- Unify placement config vocabulary across ingest and historize (#404)


### Added
- Saga doctor advises on deprecated config-key aliases (#406)
- Fail runs when multiple pipelines resolve to the same destination table (#398)
- Compact internal logs and reconcile stale tasks in saga maintenance (#396)
- Reconcile internal-log-table clustering via saga maintenance (#394)


### Fixed
- Compose schema_name per environment (stop the dev-schema leak) (#403)
- Make target-collision guard project-wide and environment-aware (#401)
- Cluster historize state log on pipeline_name (#391)
- Cluster native_load state log on its read predicates (#389)
- Shrink native_load flat-mode incremental dedup memory footprint (#387)
- Dedupe and clarify historize failure reporting (#385)
- Make incremental historize re-entrant across crash/retry (#382)

## 0.25.0 — 2026-07-13

### Added
- Add saga destroy to remove a pipeline's warehouse footprint (#375)


### Changed
- Extract shared confirmation-banner helper for CLI destructive prompts (#377)


### Fixed
- Record consistent qualified table ids in _saga_historize_log failure entries (#379)

## 0.24.0 — 2026-07-13

### Breaking Changes
- Make -v mean --verbose consistently; move version to -V (#372)


### Changed
- Use schema (not BigQuery 'dataset') for the generic native_load target schema (#368)


### Fixed
- Native_load no longer creates an unused staging schema on Databricks (#370)
- Surface transitive import failures and historize output_table-rename with clear errors (#366)
- Native_load companion view fails silently on BigQuery; promote spec._source_uri to a real field (#364)
- Comma+space selector combos silently match nothing (#362)

## 0.23.0 — 2026-07-11

### Fixed
- Saga generate-schemas requires a profile/dev schema just to link configs (#359)
- Generated per-adapter config schema rejects +key: merge syntax (#357)
- List/validate/plan use only the first config path in multi-path projects (#355)
- +key: list merge crashes on dict entries (unhashable dedup) (#353)
- Route packages.yml parsing through load_yaml chokepoint (#351)
- Restore YAML merge key (<<: *anchor) support in load_yaml (#349)

## 0.22.0 — 2026-07-10

### Added
- Record backfill window in _saga_executions and saga report (#338)
- Stamp _dlt_ingested_at on plain merge pipelines (exclude only scd2) (#336)


### Fixed
- Hard-error on unresolved dev schema instead of silent shared dlt_dev (#346)
- Treat orchestration.schema as prod-only so dev execution-plan tables stay per-developer (#345)
- Reconcile timing breakdown to total with a setup bucket (#342)
- Suppress spurious 'did not match' warning from the disabled-config probe (#340)
- Register hand-wrapped SecretStr values for log/metadata redaction (#334)

## 0.21.0 — 2026-07-09

### Breaking Changes
- Normalize the group segment in schema and table names (#327)


### Added
- Redact resolved secrets from logs and table descriptions (#319)


### Fixed
- Surface silently-swallowed failures across adapters, auth, report, and historize (#331)
- Native_load first run in partition mode skips all history (#329)
- CLI/report polish & config-loading robustness (#325)
- Destination hygiene (DuckDB record mutation + search_path leak, BigQuery staging suffix, Databricks skip_leading_rows) (#323)
- Harden source-adapter parsing (headers, BOM, end_date, API body/cap, database probe) (#321)
- Correct destination/table-format schema and harden config validation UX (#312)
- Report Duration column unsortable and Executions tab missing sort/column controls (#314)

## 0.20.0 — 2026-07-08

### Added
- Support S3 native load via BigQuery Omni (#309)

## 0.19.0 — 2026-07-08

### Fixed
- CLI/report/config-loading polish (--yes prompts, orchestrate flags, report NULL sort, impersonation scopes, config-loading robustness) (#306)
- Native_load cursor correctness (max-cursor + monotonic format) and orphan-sweep race (#304)
- Loaded_tables leaks dlt system tables into access grants / option sync / doc reconcile (#302)
- Quote_identifier escaping + partition/cluster DDL quoting; de-BigQuery-ify base dialect defaults (#300)
- Update-access reports success despite failed dataset sync / table grants (#298)
- BigQuery ensure_schema_exists race — any error read as missing + no exists_ok (#296)

## 0.18.0 — 2026-07-08

### Added
- Report active build and resolved schema in saga doctor (#292)


### Fixed
- Resolve saga run dev schema from profile instead of dlt_dev fallback (#293)
- Record extraction-start as started_at to close change-detection race (+ correct run duration) (#289)
- Database incremental WHERE quoting/injection + custom-query first-run placeholder (#287)
- Api offset pagination truncation on capped page size + retry connection errorr (#285)
- Honor source_database for historize reads (cross-project external delivery) (#283)
- Resolve historize max snapshot before reprocess to avoid losing mid-run snapshots (#281)
- Validate historize column-name fields and handle empty historize: block (#279)
- Apply historize.filters on partial refresh and use EXISTS for composite-PK rollback (#277)

## 0.17.0 — 2026-07-07

### Fixed
- Databricks apply_hints passes wrong kwargs to databricks_adapter (description + clustering silently dropped) (#272)


### Performance
- Cache google_sheets API services per run (#274)
- Cache DefaultAzureCredential for Databricks azure_default auth (#270)
- Batch BigQuery load-info inserts into one multi-row DML (#268)
- Eliminate per-iteration overhead in hot paths (per-row imports; full-listing slice) (#266)
- Stream filesystem CSV metadata reads instead of loading the whole file (#264)
- Pool BigQuery clients instead of constructing one per operation (#262)
- Cache config discovery in FilePipelineConfig (#260)

## 0.16.0 — 2026-07-07

### Fixed
- Swallowed exceptions hide real failures in historize log-clear and report queries (#257)
- Incremental state reads swallow infra errors → silent full re-extract / duplicate loads (#255)
- Worker crash leaves task 'running'; worker mode ignores typed subcommand (#253)
- BigQuery execute_sql hard 120s client-side timeout kills long historize MERGEs (#251)
- Empty-source historize poisons state; next run crashes (#249)

## 0.15.0 — 2026-07-06

### Changed
- Hoist access-manager orchestration into an AccessManager base template (#246)
- Rename generic-layer dataset_name to schema_name (#244)
- Drop the dead dataset_name config-key alias; schema_name is the sole override (#242)


### Fixed
- Declare schema_name as a config field; deprecate dead dataset_name override (#240)
- Don't record config/validation errors as run failures (also fixes post-run hang) (#238)

## 0.14.0 — 2026-07-06

### Fixed
- Databricks apply_hints warns loudly instead of silently discarding hints (#235)
- Databricks native-load replace loads 0 rows (COPY INTO skips already-loaded files) (#233)
- SCD2 change-detection hash collisions on Databricks and DuckDB (#231)
- Split execute_sql on ';' outside string literals and comments (Databricks + DuckDB) (#229)
- Databricks access manager — honor --dry-run, accurate counters, table-scoped diff (#227)
- Qualify Databricks access-management table names (was granting against 'default' schema) (#225)
- Suppress spurious 'I/O operation on closed file' thrift ERROR after Databricks runs (#223)
- Databricks GRANT uses invalid Unity Catalog syntax — grants fail silently (#221)

## 0.13.0 — 2026-07-06

### Breaking Changes
- Require sheet_name so a google_sheets config maps to one table (#208)


### Fixed
- Apply merge_key when merge_strategy is unset so merge doesn't degrade to append (#218)
- Database connection_string pipelines crash on SecretStr after full fetch (#216)
- Deterministic impersonation source without global GOOGLE_APPLICATION_CREDENTIALS mutation (#214)
- Remove no-op filesystem delete_after_load (unsafe to implement) (#212)
- Read the full sheet by default so google_sheets doesn't truncate at column Z (#210)
- Apply dict-valued project/group config defaults (folder detection by real dirs) (#206)
- Remove no-op --force from historize command (#202)

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
