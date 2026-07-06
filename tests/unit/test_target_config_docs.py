"""Unit tests for TargetConfig description/classification and persist_docs."""

import pytest

from dlt_saga.pipelines.target.config import PersistDocs, TargetConfig

GENERATED = "Data from https://services.api.no/api/config/sites"


@pytest.mark.unit
class TestSchemaNameField:
    """`schema_name` is the sole, declared per-pipeline schema override key."""

    def test_schema_name_accepted(self):
        assert TargetConfig(schema_name="my_schema").schema_name == "my_schema"

    def test_schema_name_bad_pattern_raises(self):
        with pytest.raises(ValueError, match="schema_name"):
            TargetConfig(schema_name="bad-name!")

    def test_schema_name_is_a_declared_schema_property(self):
        # Must be a real field so `saga generate-schemas` emits it and editors
        # stop rejecting `schema_name:` as an unknown key.
        from dlt_saga.utility.generate_schemas import get_target_fields_from_dataclass

        props = get_target_fields_from_dataclass()
        assert "schema_name" in props

    def test_dataset_name_is_not_a_config_key(self):
        # The dead `dataset_name` config override was removed — only `schema_name`.
        from dlt_saga.utility.generate_schemas import get_target_fields_from_dataclass

        assert "dataset_name" not in get_target_fields_from_dataclass()


@pytest.mark.unit
class TestPersistDocsFromValue:
    def test_default_table_on_columns_off(self):
        pd = PersistDocs.from_value(None)
        assert pd.table is True and pd.columns is False

    def test_bool_shorthand(self):
        assert PersistDocs.from_value(False) == PersistDocs(table=False, columns=False)

    def test_dict_partial_defaults_remaining_on(self):
        assert PersistDocs.from_value({"columns": False}) == PersistDocs(
            table=True, columns=False
        )

    def test_instance_passthrough(self):
        pd = PersistDocs(table=False, columns=True)
        assert PersistDocs.from_value(pd) is pd

    def test_unknown_key_rejected(self):
        with pytest.raises(ValueError, match="unknown key"):
            PersistDocs.from_value({"relation": True})

    def test_bad_type_rejected(self):
        with pytest.raises(ValueError, match="must be a bool or a mapping"):
            PersistDocs.from_value(["table"])

    def test_target_config_normalizes_dict(self):
        cfg = TargetConfig(persist_docs={"table": False})
        assert isinstance(cfg.persist_docs, PersistDocs)
        assert cfg.persist_docs.table is False
        assert cfg.persist_docs.columns is False  # default when key omitted


@pytest.mark.unit
class TestColumnHintComposition:
    # Column docs are opt-in (persist_docs.columns defaults off), so these
    # enable columns explicitly.
    def test_classification_folded_into_description_sorted(self):
        cfg = TargetConfig(
            persist_docs=True,
            columns={
                "email": {
                    "description": "Contact email",
                    "classification": ["pii", "contact"],
                }
            },
        )
        hints = cfg.get_dlt_column_hints()
        assert (
            hints["email"]["description"]
            == "Contact email [saga:classification=contact,pii]"
        )

    def test_classification_never_leaks_to_dlt(self):
        cfg = TargetConfig(
            persist_docs=True, columns={"email": {"classification": ["pii"]}}
        )
        hints = cfg.get_dlt_column_hints()
        assert "classification" not in hints["email"]
        assert hints["email"]["description"] == "[saga:classification=pii]"

    def test_type_hints_still_flow_alongside_docs(self):
        cfg = TargetConfig(
            persist_docs=True,
            columns={"amount": {"data_type": "decimal", "description": "Total"}},
        )
        hints = cfg.get_dlt_column_hints()
        assert hints["amount"]["data_type"] == "decimal"
        assert hints["amount"]["description"] == "Total"

    def test_persist_docs_columns_off_drops_description_keeps_type(self):
        cfg = TargetConfig(
            persist_docs={"columns": False},
            columns={
                "amount": {
                    "data_type": "decimal",
                    "description": "Total",
                    "classification": ["pii"],
                }
            },
        )
        hints = cfg.get_dlt_column_hints()
        assert hints["amount"] == {"data_type": "decimal"}

    def test_docs_only_column_dropped_when_persist_off(self):
        """A column carrying only docs disappears from hints when docs are off."""
        cfg = TargetConfig(
            persist_docs={"columns": False},
            columns={"email": {"description": "Contact", "classification": ["pii"]}},
        )
        assert cfg.get_dlt_column_hints() == {}

    def test_columns_default_off_drops_description(self):
        """persist_docs.columns defaults off — column docs are opt-in."""
        cfg = TargetConfig(
            columns={"email": {"description": "Contact", "classification": ["pii"]}}
        )
        assert cfg.get_dlt_column_hints() == {}

    def test_default_field_is_not_passed_to_dlt(self):
        cfg = TargetConfig(columns={"x": {"default": 1, "data_type": "bigint"}})
        hints = cfg.get_dlt_column_hints()
        assert hints["x"] == {"data_type": "bigint"}


@pytest.mark.unit
class TestResolveTableDescription:
    def test_manual_description_overrides_generated(self):
        cfg = TargetConfig(description="Curated site config")
        assert cfg.resolve_table_description(GENERATED) == "Curated site config"

    def test_generated_used_when_no_config_description(self):
        cfg = TargetConfig()
        assert cfg.resolve_table_description(GENERATED) == GENERATED

    def test_table_classification_encoded_into_description(self):
        cfg = TargetConfig(
            description="Site config", classification=["confidential", "pii"]
        )
        assert (
            cfg.resolve_table_description(GENERATED)
            == "Site config [saga:classification=confidential,pii]"
        )

    def test_classification_encoded_onto_generated_description(self):
        cfg = TargetConfig(classification=["pii"])
        assert (
            cfg.resolve_table_description(GENERATED)
            == f"{GENERATED} [saga:classification=pii]"
        )

    def test_resolved_table_description_is_single_line(self):
        cfg = TargetConfig(description="Site config", classification=["pii"])
        assert "\n" not in cfg.resolve_table_description(GENERATED)

    def test_persist_docs_table_off_returns_none(self):
        cfg = TargetConfig(description="Curated", persist_docs={"table": False})
        assert cfg.resolve_table_description(GENERATED) is None

    def test_empty_generated_and_no_config_returns_none(self):
        cfg = TargetConfig()
        assert cfg.resolve_table_description("") is None


@pytest.mark.unit
class TestClassificationValidation:
    def test_column_classification_must_be_list(self):
        with pytest.raises(ValueError, match="must be a list of strings"):
            TargetConfig(columns={"email": {"classification": "pii"}})

    def test_column_classification_must_be_strings(self):
        with pytest.raises(ValueError, match="must be a list of strings"):
            TargetConfig(columns={"email": {"classification": [1, 2]}})

    def test_column_classification_delimiter_rejected(self):
        with pytest.raises(ValueError, match="invalid characters"):
            TargetConfig(columns={"email": {"classification": ["pi,i"]}})

    def test_table_classification_delimiter_rejected(self):
        with pytest.raises(ValueError, match="classification entry"):
            TargetConfig(classification=["a;b"])

    def test_valid_classification_characters_accepted(self):
        cfg = TargetConfig(
            persist_docs=True,
            columns={"email": {"classification": ["pii", "team:sales", "gdpr-art9"]}},
        )
        assert (
            "[saga:classification=gdpr-art9,pii,team:sales]"
            in cfg.get_dlt_column_hints()["email"]["description"]
        )
