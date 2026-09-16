"""Framework-injected column names shared across layers.

Single definition of the columns dlt and dlt-saga add to ingested tables.
Both the destination-agnostic historize SQL builder and the destination-side
historize DDL paths (BigQuery Iceberg emits explicit column definitions rather
than a CTAS) decide what belongs in a historized target, and they must agree:
two copies of this set drift into a target whose shape depends on which code
path built it.
"""

# Columns that are never carried into a historized target table: dlt's own
# bookkeeping, saga's ingest-time metadata, and the SCD2 output columns under
# their default names (configured names are excluded separately, at the call site).
HISTORIZE_SYSTEM_COLUMNS = frozenset(
    {
        "_dlt_id",
        "_dlt_load_id",
        "_dlt_valid_from",
        "_dlt_valid_to",
        "_dlt_is_deleted",
        "_dlt_source_file_name",
        "_dlt_source_modification_date",
        "_dlt_ingested_at",
    }
)
