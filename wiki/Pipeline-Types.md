# Pipeline Types

Each pipeline type is configured with a YAML file in `configs/<source_type>/`. The framework auto-discovers configs and resolves the correct implementation.

---

## API

REST API sources with pagination and incremental cursor support. Create a config in `configs/api/<api_name>/`.

The framework uses polymorphic pipeline loading — implementations resolve from most specific to least specific:

1. `pipelines/api/<api_name>/<endpoint>/pipeline.py` (config-specific)
2. `pipelines/api/<api_name>/pipeline.py` (API-specific)
3. `pipelines/api/pipeline.py` (base API pipeline)

### Basic

```yaml
# configs/api/myservice/users.yml
base_url: "https://api.example.com"
endpoint: "/users"
auth_type: "bearer"
auth_token: "googlesecretmanager::project::api-token"
response_path: "data"           # JSON path to the records array

write_disposition: "append"
tags: ["daily"]
```

### With pagination

```yaml
# configs/api/myservice/events.yml
base_url: "https://api.example.com"
endpoint: "/events"
auth_type: "bearer"
auth_token: "googlesecretmanager::project::api-token"
response_path: "data"

pagination:
  type: cursor                  # offset, page, cursor, next_url
  cursor_path: "meta.next_cursor"
  cursor_param: "cursor"
  limit: 100
  limit_param: "per_page"

page_delay: 0.2                 # seconds between requests (rate limiting)

write_disposition: "append"
tags: ["daily"]
```

### Incremental loading

Enable `incremental` and name the column whose high-water mark seeds the cursor.
The resolved cursor is the max already-loaded value of `incremental_column`
(falling back to `initial_value` on the first run), substituted into
`query_params` wherever the `{incremental_value}` placeholder appears — so the
API only returns new rows. This is idempotent and self-healing: a missed run is
caught up on the next run.

```yaml
incremental: true
incremental_column: "created_at"   # column read from the destination for the watermark
initial_value: "2024-01-01"        # cursor for the first run

query_params:
  updated_since: "{incremental_value}"   # cursor is substituted here before the request
```

`{incremental_value}` is required somewhere in `query_params` when `incremental`
is enabled — without it the cursor cannot filter the request. `{incremental_column}`
is also available if the API expects the column name as a parameter.

Backfill from a chosen point with `--start-value-override` (this wins over the
tracked watermark for the run):

```bash
saga ingest --select "api__myservice__events" --start-value-override "2024-03-01"
```

> The `{incremental_value}` placeholder is for a single cursor on one filter
> param. If your API takes a **date range** (`from`/`to`, `startDate`/`endDate`),
> use the date-window adapter below instead — it handles the range, overlap and
> backfill for you.

### Date-window incremental (`dlt_saga.api.date_window`)

Most incremental APIs are queried by a date range: "give me everything between
`from` and `to`." The `dlt_saga.api.date_window` adapter does the bookkeeping —
resume from the warehouse high-water mark, re-fetch a small `overlap` to catch
late-arriving or corrected rows, and load a `[start, end]` window — so you never
hardcode "yesterday" (which can't recover from a missed run). For the common case
it needs **no Python**:

```yaml
# configs/api/myservice/events.yml
adapter: dlt_saga.api.date_window
tags: [daily]
write_disposition: "merge"          # delete-insert on the date column → idempotent re-runs
merge_key: "event_date"

base_url: "https://api.example.com"
endpoint: "/events"
response_path: "data"

incremental_column: "event_date"    # the warehouse column whose MAX seeds the cursor
initial_value: "2024-01-01"         # first-run start (or set on_first_run)
overlap: 2                          # re-fetch the last 2 loaded days each run

start_param: "from"                 # query params that receive the window bounds
end_param: "to"
date_format: "%Y-%m-%d"
# window_end: today | yesterday      (default: today)
# per_period_requests: true          (one request per day, for single-date APIs)
# pagination: { ... }                (paginates the window when set)
```

Backfill a specific range with the standard override (wins over the watermark):

```bash
saga ingest --select "api__myservice__events" --start-value-override 2024-03-01 --end-value-override 2024-03-31
```

When the API doesn't take the window as plain query params — a report body, a
custom cursor, per-row enrichment — subclass `DateWindowApiPipeline` and override
**`_fetch_window(start, end)`**. You keep the window resolution, `overlap`,
backfill and watermark logic; you only describe how to fetch one window:

```python
from dlt_saga.pipelines.api.date_window import DateWindowApiPipeline

class MyReportPipeline(DateWindowApiPipeline):
    def _fetch_window(self, start, end):
        report = self.client.run_report(start, end)   # your request shape
        return self._rows_from(report)
```

Other override points for less common needs: `resolve_window()` (e.g. ISO-week
periods instead of days), `iter_days()`, `_render_date()`.

---

## Database

SQL databases via [ConnectorX](https://github.com/sfu-db/connector-x) with Apache Arrow for high-performance extraction.

**Supported databases:** PostgreSQL, MySQL, MariaDB, SQL Server, Oracle, SQLite, Redshift, ClickHouse, BigQuery, Trino

### Connection string

```yaml
# configs/database/mydb/customers.yml
connection_string: "postgresql://user:pass@host:5432/mydb"

source_table: "customers"

write_disposition: "merge"
primary_key: "customer_id"
tags: ["daily"]
```

### Individual components

```yaml
# configs/database/mydb/customers.yml
database_type: "postgres"  # postgres, mysql, mssql, oracle, etc.
host: "db.example.com"
port: 5432
source_database: "mydb"
username: "googlesecretmanager::project::db-user"
password: "googlesecretmanager::project::db-password"

source_table: "customers"
# OR: query: "SELECT * FROM customers WHERE active = true"

# Incremental loading (optional)
incremental: true
incremental_column: "updated_at"
initial_value: "2025-01-01"

write_disposition: "merge"
primary_key: "customer_id"
tags: ["daily"]
```

---

## Filesystem (GCS, SFTP, local)

Extracts files from cloud storage or local paths. Supports CSV, JSON, JSONL, and Parquet.

```yaml
# configs/filesystem/mybucket/events.yml
filesystem_type: "gs"  # gs (GCS), sftp, file (local)
bucket_name: "my-bucket"
file_glob: "data/events/*.parquet"
file_type: "parquet"   # csv, json, jsonl, parquet

# CSV-specific
csv_separator: ","

# Incremental loading
incremental: true
incremental_column: "modification_date"
initial_value: "2024-01-01"

write_disposition: "append"
tags: ["hourly"]
```

### Snapshot date extraction

For snapshot-style pipelines where file paths encode the snapshot date (e.g. `snapshots/2024-03-15.parquet`):

```yaml
snapshot_date_regex: "\\d{4}-\\d{2}-\\d{2}"
snapshot_date_format: "%Y-%m-%d"
```

### GCS authentication

When `filesystem_type: gs` and no explicit `credentials:` are configured, the
filesystem client clears `GOOGLE_APPLICATION_CREDENTIALS` from the process
environment and falls through to **Application Default Credentials (ADC)** —
gcloud user credentials, the GCE/Cloud Run metadata server, or the active
profile's `run_as` impersonation. The active profile (and any `--target` you
selected) is therefore the single source of truth for identity. Run with
`--verbose` to see a debug log when GAC is cleared.

**Why this happens only for filesystem.** Other GCS-touching code paths
(BigQuery destination, `native_load`, the Cloud Run trigger, the Secret
Manager resolver) all route through `google.auth.default()`, which dlt-saga
monkey-patches inside `ImpersonationManager._activate_impersonation()` so
that GAC and `run_as` impersonation compose correctly. fsspec / gcsfs read
GAC directly at module load time and bypass that hook, so the env var is
cleared up-front instead.

**To use a specific service-account key file**, pass it explicitly through
the pipeline config:

```yaml
filesystem_type: "gs"
credentials:
  type: "service_account"
  project_id: "my-project"
  private_key: "googlesecretmanager::my-project::gcs-reader-key"
  client_email: "gcs-reader@my-project.iam.gserviceaccount.com"
```

This bypasses the GAC clearing and gives the resource the exact credentials
you specify.

---

## Google Sheets

Extracts data from Google Sheets using the Drive API, with change detection to avoid redundant loads.

Grant Viewer access to the service account configured in `providers.google_secrets` in `saga_project.yml`.

```yaml
# configs/google_sheets/my_sheet.yml
spreadsheet_id: "YOUR_SPREADSHEET_ID"  # From the URL
sheet_name: "Sheet1"
range: "A:Z"

write_disposition: "replace"
tags: ["daily"]
```

---

## SharePoint

Downloads a file from SharePoint using the app-only OAuth 2.0 flow.

**Requires:** `pip install "dlt-saga[azure]"`

```yaml
# configs/sharepoint/my_report.yml
adapter: dlt_saga.sharepoint

# Authentication (SharePoint app-only OAuth2 form body stored in a secrets provider)
token_request_body: "azurekeyvault::https://my-vault.vault.azure.net::MY-SP-TOKEN-BODY"
tenant_id: "<azure-ad-tenant-id>"

# File location
site_url: "https://contoso.sharepoint.com/sites/MyTeam"
file_path: "/sites/MyTeam/Shared Documents/reports/weekly.xlsx"
file_type: xlsx      # xlsx, csv, json, jsonl

# Excel-specific (optional)
sheet_name: "Data"
header_row: 1

write_disposition: "replace"
tags: ["daily"]
```

The `token_request_body` must resolve to a URL-encoded OAuth2 form body:

```
grant_type=client_credentials&client_id=<app-id>@<tenant-id>&client_secret=<secret>&resource=00000003-0000-0ff1ce00-000000000000/<host>@<tenant-id>
```

> The key was previously named `auth_secret`. That name still works but is
> deprecated (it logs a warning) — rename it to `token_request_body`.

---

## Native cloud-storage loader

Bulk-loads Parquet, CSV, or JSONL files from cloud storage directly into the warehouse using destination-native mechanics (BigQuery external tables, Databricks `COPY INTO`). Designed for sources with >1 000 files or >1 GB per run where dlt's extract/normalize overhead would be prohibitive.

See the **[Native Cloud-Storage Loader](Native-Load)** page for full documentation.

```yaml
# configs/native_load/my_bucket/orders.yml
adapter: dlt_saga.native_load
tags: [daily]

source_uri: gs://my-bucket/exports/orders/
file_type: parquet                     # parquet | csv | jsonl

write_disposition: append+historize
primary_key: [order_id]

historize:
  partition_column: _dlt_valid_from
  cluster_columns: [order_id]
  track_deletions: true
```

Supported destinations: **BigQuery** (`gs://`) and **Databricks** (`gs://`, `abfss://`).

---

## Custom pipeline implementations

For sources not covered by built-in types, write a custom `BasePipeline` subclass and register it via `packages.yml`. See [Plugin Development](Plugin-Development).

```yaml
# configs/api/myservice/data.yml
adapter: myorg.api.myservice   # References your registered plugin namespace
tags: [daily]
write_disposition: append
```

See [Configuration](Configuration) for the full list of common config fields.
