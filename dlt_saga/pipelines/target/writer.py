from typing import Any, Dict

from dlt_saga.pipelines.target.config import MergeStrategy, TargetConfig


class TargetWriter:
    def __init__(self, config: TargetConfig):
        self.config = config

    def apply_hints(self, data: Any) -> Any:
        """Apply hints to the data based on the target configuration"""
        # Strip "+historize" for dlt disposition checks — historization is handled separately
        base_disposition = self.config.write_disposition.replace("+historize", "")
        if base_disposition == "merge" and self.config.merge_strategy:
            merge_config = self._build_merge_config()
            data.apply_hints(write_disposition=merge_config)
            if self.config.merge_key:
                data.apply_hints(merge_key=self.config.merge_key)
            if self.config.primary_key:
                data.apply_hints(primary_key=self.config.primary_key)
        elif base_disposition == "replace":
            replace_config = self._build_replace_config()
            data.apply_hints(write_disposition=replace_config)
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

        return merge_config

    def _build_replace_config(self) -> Dict[str, Any]:
        """Build replace configuration based on target configuration"""
        replace_config = {"disposition": "replace"}
        if self.config.replace_strategy:
            replace_config["strategy"] = self.config.replace_strategy.value
        return replace_config
