from typing import Any, Dict

from dlt_saga.pipelines.target.config import MergeStrategy, TargetConfig


class TargetWriter:
    def __init__(self, config: TargetConfig):
        self.config = config

    def apply_hints(self, data: Any) -> Any:
        """Apply hints to the data based on the target configuration"""
        # Strip "+historize" for dlt disposition checks — historization is handled separately
        base_disposition = self.config.write_disposition.replace("+historize", "")
        if base_disposition == "merge":
            # An explicit strategy (scd2, upsert, insert-only, delete-insert) is
            # passed to dlt as a dict config; a bare `write_disposition: merge`
            # falls back to dlt's default strategy (delete-insert). Either way the
            # merge_key/primary_key must be applied — dropping them silently
            # degrades merge to keyless append and duplicates rows.
            if self.config.merge_strategy:
                data.apply_hints(write_disposition=self._build_merge_config())
            elif not self.config.deduplicate:
                # Strategy left to the destination's default, as for "merge".
                data.apply_hints(
                    write_disposition={"disposition": "merge", "deduplicated": True}
                )
            else:
                data.apply_hints(write_disposition="merge")
            if self.config.merge_key:
                data.apply_hints(merge_key=self.config.merge_key)
            if self.config.primary_key:
                data.apply_hints(primary_key=self.config.primary_key)
        elif base_disposition == "replace":
            # replace_strategy is destination config in dlt, not a table hint —
            # BasePipeline sets it on the pipeline's dlt destination.
            data.apply_hints(write_disposition="replace")
        else:
            # For append disposition (including full_refresh mode)
            data.apply_hints(write_disposition=base_disposition)
            # Still apply primary_key for deduplication within the incoming batch
            if self.config.primary_key:
                data.apply_hints(primary_key=self.config.primary_key)

        # Apply column-specific hints if any (filtering out custom fields like 'default')
        dlt_columns = self.config.get_dlt_column_hints()
        if dlt_columns:
            for column_name, hints in dlt_columns.items():
                data.apply_hints(columns={column_name: hints})

        return data

    def _build_merge_config(self) -> Dict[str, Any]:
        """Build merge configuration based on target configuration"""
        merge_config: Dict[str, Any] = {
            "disposition": "merge",
            "strategy": self.config.merge_strategy.value,
        }
        if not self.config.deduplicate:
            # dlt's flag is inverted: deduplicated=True declares the incoming
            # data already unique, so dlt skips its own deduplication. Config
            # validation limits this to delete-insert.
            merge_config["deduplicated"] = True

        if self.config.merge_strategy == MergeStrategy.SCD2:
            merge_config.update(
                {
                    "validity_column_names": [
                        self.config.valid_from_column,
                        self.config.valid_to_column,
                    ]
                }
            )
            if self.config.row_version_column_name:
                merge_config["row_version_column_name"] = (
                    self.config.row_version_column_name
                )
            if self.config.active_record_timestamp:
                merge_config["active_record_timestamp"] = (
                    self.config.active_record_timestamp
                )
            if self.config.boundary_timestamp:
                merge_config["boundary_timestamp"] = self.config.boundary_timestamp
        elif (
            self.config.merge_strategy == MergeStrategy.DELETE_INSERT
            and self.config.primary_key
        ):
            merge_config["primary_key"] = self.config.primary_key
        elif (
            self.config.merge_strategy == MergeStrategy.UPSERT
            and self.config.primary_key
        ):
            merge_config["primary_key"] = self.config.primary_key
        elif self.config.merge_strategy == MergeStrategy.INSERT_ONLY:
            # primary_key is required by validation; merge_key is rejected upstream.
            merge_config["primary_key"] = self.config.primary_key

        return merge_config
