"""Unit tests for config-key alias normalization."""

import logging

import pytest

from dlt_saga.pipeline_config.compat import find_legacy_keys, normalize_config_aliases


@pytest.mark.unit
class TestNormalizeConfigAliases:
    def test_renames_dataset_access_at_top_level(self):
        data = {
            "dataset_access": [
                "OWNER:serviceAccount:sa@p.iam.gserviceaccount.com",
            ],
        }
        normalize_config_aliases(data)
        assert "schema_access" in data
        assert "dataset_access" not in data

    def test_renames_plus_prefixed_form(self):
        data = {"+dataset_access": ["READER:group:analysts@example.com"]}
        normalize_config_aliases(data)
        assert "+schema_access" in data
        assert "+dataset_access" not in data

    def test_renames_nested(self):
        data = {
            "pipelines": {
                "dataset_access": ["OWNER:serviceAccount:sa@p.iam.gserviceaccount.com"],
                "filesystem": {
                    "+dataset_access": [
                        "AUTHORIZED_DATASET:proj.dlt_filesystem",
                    ],
                },
            },
        }
        normalize_config_aliases(data)
        assert "schema_access" in data["pipelines"]
        assert "dataset_access" not in data["pipelines"]
        assert "+schema_access" in data["pipelines"]["filesystem"]
        assert "+dataset_access" not in data["pipelines"]["filesystem"]

    def test_canonical_wins_when_both_set(self, caplog):
        data = {
            "dataset_access": ["OWNER:legacy"],
            "schema_access": ["OWNER:canonical"],
        }
        with caplog.at_level(logging.WARNING):
            normalize_config_aliases(data)
        # Canonical preserved; legacy dropped.
        assert data == {"schema_access": ["OWNER:canonical"]}
        # A warning surfaces the conflict so it's not silent.
        assert any("takes precedence" in r.message for r in caplog.records)

    def test_silent_on_pure_alias_rewrite(self, caplog):
        """A clean legacy → canonical rewrite emits no log lines at WARNING+."""
        data = {"dataset_access": ["OWNER:serviceAccount:sa@p.iam.gserviceaccount.com"]}
        with caplog.at_level(logging.WARNING):
            normalize_config_aliases(data)
        assert caplog.records == []

    def test_no_mutation_when_no_legacy_keys(self):
        data = {
            "schema_access": ["OWNER:canonical"],
            "pipelines": {"other_key": "value"},
        }
        normalize_config_aliases(data)
        assert data == {
            "schema_access": ["OWNER:canonical"],
            "pipelines": {"other_key": "value"},
        }

    def test_renames_historize_placement_keys(self):
        # The historize placement vocabulary was unified with the ingest layer's
        # schema_name / table_name; the output_ prefix is dropped.
        data = {
            "historize": {
                "output_schema": "archive",
                "output_table": "orders",
                "output_table_suffix": "_scd2",
            },
        }
        normalize_config_aliases(data)
        assert data["historize"] == {
            "schema_name": "archive",
            "table_name": "orders",
            "table_suffix": "_scd2",
        }

    def test_renames_plus_prefixed_historize_keys(self):
        data = {"historize": {"+output_schema": "archive"}}
        normalize_config_aliases(data)
        assert data["historize"] == {"+schema_name": "archive"}

    def test_handles_lists_of_dicts(self):
        data = {
            "items": [
                {"dataset_access": ["OWNER:in-list"]},
                {"other": "value"},
            ],
        }
        normalize_config_aliases(data)
        assert data["items"][0] == {"schema_access": ["OWNER:in-list"]}
        assert data["items"][1] == {"other": "value"}

    def test_non_dict_non_list_input_returns_unchanged(self):
        assert normalize_config_aliases("string") == "string"
        assert normalize_config_aliases(42) == 42
        assert normalize_config_aliases(None) is None


@pytest.mark.unit
class TestFindLegacyKeys:
    def test_none_when_only_canonical_keys(self):
        assert (
            find_legacy_keys({"schema_name": "x", "historize": {"table_name": "y"}})
            == []
        )

    def test_finds_top_level_and_nested(self):
        raw = {
            "dataset_access": ["OWNER:x"],
            "historize": {"output_schema": "archive", "output_table": "orders"},
        }
        assert find_legacy_keys(raw) == [
            ("dataset_access", "schema_access"),
            ("output_schema", "schema_name"),
            ("output_table", "table_name"),
        ]

    def test_reports_plus_prefixed_form(self):
        assert find_legacy_keys({"pipelines": {"+dataset_access": ["x"]}}) == [
            ("+dataset_access", "+schema_access")
        ]

    def test_deduplicates_repeated_keys(self):
        raw = {
            "google_sheets": {"historize": {"output_table": "a"}},
            "filesystem": {"historize": {"output_table": "b"}},
        }
        assert find_legacy_keys(raw) == [("output_table", "table_name")]

    def test_walks_lists(self):
        raw = {"items": [{"output_schema": "a"}, {"ok": 1}]}
        assert find_legacy_keys(raw) == [("output_schema", "schema_name")]
