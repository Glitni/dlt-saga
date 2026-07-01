"""Unit tests for generate_schemas pure-function helpers."""

import json
from dataclasses import dataclass, field
from enum import Enum
from typing import List, Optional

import pytest


@pytest.mark.unit
class TestExtractEnumValues:
    def test_string_enum_returns_values(self):
        from dlt_saga.utility.generate_schemas import extract_enum_values

        class Color(Enum):
            RED = "red"
            GREEN = "green"
            BLUE = "blue"

        assert sorted(extract_enum_values(Color)) == ["blue", "green", "red"]

    def test_non_enum_class_returns_empty(self):
        from dlt_saga.utility.generate_schemas import extract_enum_values

        assert extract_enum_values(str) == []
        assert extract_enum_values(int) == []

    def test_non_type_returns_empty(self):
        from dlt_saga.utility.generate_schemas import extract_enum_values

        assert extract_enum_values("not a type") == []  # type: ignore[arg-type]


@pytest.mark.unit
class TestDataclassToJsonSchema:
    def test_primitive_fields(self):
        from dlt_saga.utility.generate_schemas import dataclass_to_json_schema

        @dataclass
        class Primitive:
            name: str = ""
            count: int = 0
            active: bool = True
            ratio: float = 0.0

        schema = dataclass_to_json_schema(Primitive)
        assert schema["type"] == "object"
        props = schema["properties"]
        assert props["name"]["type"] == "string"
        assert props["count"]["type"] == "integer"
        assert props["active"]["type"] == "boolean"
        assert props["ratio"]["type"] == "number"

    def test_required_fields_captured(self):
        from dlt_saga.utility.generate_schemas import dataclass_to_json_schema

        @dataclass
        class HasRequired:
            required_name: str  # no default → required
            optional_count: int = 0

        schema = dataclass_to_json_schema(HasRequired)
        assert "required" in schema
        assert "required_name" in schema["required"]
        assert "optional_count" not in schema.get("required", [])

    def test_optional_field_unwrapped(self):
        from dlt_saga.utility.generate_schemas import dataclass_to_json_schema

        @dataclass
        class WithOptional:
            maybe: Optional[str] = None

        schema = dataclass_to_json_schema(WithOptional)
        assert schema["properties"]["maybe"]["type"] == "string"

    def test_list_of_strings(self):
        from dlt_saga.utility.generate_schemas import dataclass_to_json_schema

        @dataclass
        class WithList:
            tags: List[str] = field(default_factory=list)

        schema = dataclass_to_json_schema(WithList)
        tags_schema = schema["properties"]["tags"]
        assert tags_schema["type"] == "array"
        assert tags_schema["items"]["type"] == "string"

    def test_private_fields_excluded(self):
        from dlt_saga.utility.generate_schemas import dataclass_to_json_schema

        @dataclass
        class WithPrivate:
            public_name: str = ""
            _private: str = ""

        schema = dataclass_to_json_schema(WithPrivate)
        assert "public_name" in schema["properties"]
        assert "_private" not in schema["properties"]

    def test_non_dataclass_returns_empty(self):
        from dlt_saga.utility.generate_schemas import dataclass_to_json_schema

        assert dataclass_to_json_schema(str) == {}
        assert dataclass_to_json_schema(dict) == {}

    def test_default_value_included(self):
        from dlt_saga.utility.generate_schemas import dataclass_to_json_schema

        @dataclass
        class WithDefault:
            mode: str = "append"

        schema = dataclass_to_json_schema(WithDefault)
        assert schema["properties"]["mode"].get("default") == "append"


@pytest.mark.unit
class TestCollectConfigFieldUnion:
    def test_merges_disjoint_fields(self):
        from dlt_saga.utility.generate_schemas import _collect_config_field_union

        schemas = {
            "a": {"properties": {"foo": {"type": "string"}}},
            "b": {"properties": {"bar": {"type": "integer"}}},
        }
        union = _collect_config_field_union(schemas)
        assert union == {"foo": {"type": "string"}, "bar": {"type": "integer"}}

    def test_identical_fields_kept(self):
        from dlt_saga.utility.generate_schemas import _collect_config_field_union

        schemas = {
            "a": {"properties": {"foo": {"type": "string"}}},
            "b": {"properties": {"foo": {"type": "string"}}},
        }
        union = _collect_config_field_union(schemas)
        assert union["foo"] == {"type": "string"}

    def test_conflicting_fields_widened_to_any(self):
        from dlt_saga.utility.generate_schemas import _collect_config_field_union

        schemas = {
            "a": {"properties": {"foo": {"type": "string"}}},
            "b": {"properties": {"foo": {"type": "integer"}}},
        }
        union = _collect_config_field_union(schemas)
        assert union["foo"] == {}

    def test_ignores_schemas_without_properties(self):
        from dlt_saga.utility.generate_schemas import _collect_config_field_union

        schemas = {"defs_only": {"$defs": {"x": {}}}}
        assert _collect_config_field_union(schemas) == {}


@pytest.mark.unit
class TestWithInheritAliases:
    def test_adds_plus_prefixed_alias(self):
        from dlt_saga.utility.generate_schemas import _with_inherit_aliases

        out = _with_inherit_aliases({"tags": {"type": "array"}})
        assert out["tags"] == {"type": "array"}
        assert out["+tags"] == {"type": "array"}

    def test_empty_input(self):
        from dlt_saga.utility.generate_schemas import _with_inherit_aliases

        assert _with_inherit_aliases({}) == {}


@pytest.mark.unit
class TestSharedPropsViaDefs:
    def test_field_defined_once_and_referenced(self):
        from dlt_saga.utility.generate_schemas import _shared_props_via_defs

        field_defs, shared = _shared_props_via_defs({"foo": {"type": "string"}})

        # The full schema lives once in $defs ...
        assert field_defs == {"config_field_foo": {"type": "string"}}
        # ... and both the plain and +merge forms are thin refs to it.
        ref = {"$ref": "#/$defs/config_field_foo"}
        assert shared["foo"] == ref
        assert shared["+foo"] == ref

    def test_none_input(self):
        from dlt_saga.utility.generate_schemas import _shared_props_via_defs

        assert _shared_props_via_defs(None) == ({}, {})


@pytest.mark.unit
class TestSchemaFilenameForConfigClass:
    def test_camel_case_to_snake(self):
        from dlt_saga.utility.generate_schemas import schema_filename_for_config_class

        assert (
            schema_filename_for_config_class("NativeLoadConfig")
            == "native_load_config.json"
        )
        assert schema_filename_for_config_class("ApiConfig") == "api_config.json"


@pytest.mark.unit
class TestSchemaFilenameForAdapter:
    """Resolves a config file's adapter to its generated schema filename —
    must match what generation actually writes for each built-in adapter."""

    @pytest.mark.parametrize(
        "adapter,expected",
        [
            ("dlt_saga.native_load", "native_load_config.json"),
            ("dlt_saga.filesystem", "filesystem_config.json"),
            ("dlt_saga.database", "database_config.json"),
            ("dlt_saga.google_sheets", "g_sheets_config.json"),
            ("dlt_saga.api", "api_config.json"),
            ("dlt_saga.sharepoint", "share_point_config.json"),
        ],
    )
    def test_builtin_adapters_resolve(self, adapter, expected):
        from dlt_saga.utility.generate_schemas import schema_filename_for_adapter

        assert schema_filename_for_adapter(adapter) == expected

    def test_unresolvable_adapter_returns_none(self):
        from dlt_saga.utility.generate_schemas import schema_filename_for_adapter

        assert schema_filename_for_adapter("bogus.does_not_exist") is None

    def test_resolved_filename_actually_generated(self, tmp_path):
        """The filename the linker would reference must be a file generation
        actually produces — guards against drift between the two code paths."""
        from dlt_saga.utility.generate_schemas import (
            generate_schemas,
            schema_filename_for_adapter,
        )

        generate_schemas(tmp_path)
        for adapter in ("dlt_saga.filesystem", "dlt_saga.native_load", "dlt_saga.api"):
            filename = schema_filename_for_adapter(adapter)
            assert filename and (tmp_path / filename).exists(), (
                f"{adapter} -> {filename} was not generated"
            )


@pytest.mark.unit
class TestGenerateSchemasOutput:
    def test_creates_json_files(self, tmp_path):
        from dlt_saga.utility.generate_schemas import generate_schemas

        exit_code = generate_schemas(tmp_path)
        json_files = list(tmp_path.glob("*.json"))
        assert len(json_files) > 0, "Expected at least one JSON schema file"
        # 0 = all OK, 1 = partial errors are acceptable during dev
        assert exit_code in (0, 1)

    def test_output_files_are_valid_json(self, tmp_path):
        from dlt_saga.utility.generate_schemas import generate_schemas

        generate_schemas(tmp_path)
        for json_file in tmp_path.glob("*.json"):
            content = json_file.read_text(encoding="utf-8")
            parsed = json.loads(content)
            assert isinstance(parsed, dict), f"{json_file.name} is not a JSON object"
            assert "$schema" in parsed, f"{json_file.name} missing $schema key"

    def test_project_level_schemas_generated(self, tmp_path):
        from dlt_saga.utility.generate_schemas import generate_schemas

        generate_schemas(tmp_path)
        file_names = {f.name for f in tmp_path.glob("*.json")}
        assert "profiles_config.json" in file_names
        assert "saga_project_config.json" in file_names
        assert "packages_config.json" in file_names

    def test_partition_expiration_days_in_pipeline_schemas(self, tmp_path):
        """Pipeline-level override surfaces on every per-pipeline schema (mirrors
        partition_column / cluster_columns, which also live on TargetConfig)."""
        from dlt_saga.utility.generate_schemas import generate_schemas

        generate_schemas(tmp_path)
        for schema_path in tmp_path.glob("*_config.json"):
            # Skip the static base and the project-level schemas; only check
            # source-pipeline schemas (which include TargetConfig fields).
            if schema_path.name in {
                "dlt_common.json",
                "profiles_config.json",
                "saga_project_config.json",
                "packages_config.json",
            }:
                continue
            data = json.loads(schema_path.read_text(encoding="utf-8"))
            props = data.get("properties", {})
            assert "partition_expiration_days" in props, (
                f"{schema_path.name} missing partition_expiration_days "
                f"(should be inherited from TargetConfig)"
            )
            field = props["partition_expiration_days"]
            assert field.get("type") == "integer"
            assert field.get("minimum") == 1
            assert "BigQuery" in field.get("description", "")

    def test_dev_override_block_in_pipeline_schemas(self, tmp_path):
        """Every per-pipeline schema allows a `dev:` override block that mirrors
        the config keys, requires nothing, and rejects typos."""
        from dlt_saga.utility.generate_schemas import generate_schemas

        generate_schemas(tmp_path)
        project_level = {
            "dlt_common.json",
            "profiles_config.json",
            "saga_project_config.json",
            "packages_config.json",
        }
        checked = 0
        for schema_path in tmp_path.glob("*_config.json"):
            if schema_path.name in project_level:
                continue
            data = json.loads(schema_path.read_text(encoding="utf-8"))
            props = data.get("properties", {})
            assert "dev" in props, f"{schema_path.name} missing dev override block"
            dev = props["dev"]
            assert dev["type"] == "object"
            # Mirrors config keys (so values validate/autocomplete)...
            assert "initial_value" in dev["properties"]
            # ...but does not nest, requires nothing, and still catches typos.
            assert "dev" not in dev["properties"]
            assert "required" not in dev
            assert dev["additionalProperties"] is False
            checked += 1
        assert checked > 0

    def test_shared_adapter_keys_valid_in_pipelines_section(self, tmp_path):
        """Adapter config keys shared across a group via saga_project.yml must
        validate as typed properties (both the plain and `+merge` forms),
        rather than falling through to the nested-entry schema."""
        from dlt_saga.utility.generate_schemas import generate_schemas

        generate_schemas(tmp_path)
        data = json.loads(
            (tmp_path / "saga_project_config.json").read_text(encoding="utf-8")
        )
        group = data["properties"]["pipelines"]["additionalProperties"]
        group_props = group["properties"]
        entry_props = group["additionalProperties"]["properties"]

        # A TargetConfig field shared at group level
        assert "partition_column" in group_props
        assert "+partition_column" in group_props
        # Available at the individual-pipeline level too
        assert "write_disposition" in entry_props

        # Explicit keys still win over the injected union
        assert (
            group_props["schema_access"]["$ref"]
            == "dlt_common.json#/$defs/schema_access_list"
        )
        assert "examples" in group_props["adapter"]

    def test_shared_fields_are_deduplicated_via_defs(self, tmp_path):
        """The config-field union is defined once under $defs and referenced,
        not inlined six times (3 levels x plain/+merge). Guards against the
        project schema ballooning as more adapters contribute fields."""
        from dlt_saga.utility.generate_schemas import generate_schemas

        generate_schemas(tmp_path)
        raw = (tmp_path / "saga_project_config.json").read_text(encoding="utf-8")
        data = json.loads(raw)

        defs = data.get("$defs", {})
        assert defs, "expected a $defs block with field definitions"

        group = data["properties"]["pipelines"]["additionalProperties"]
        group_props = group["properties"]

        # A union field resolves to a $ref into $defs, and both forms point
        # to the same definition.
        ref = group_props["partition_column"]["$ref"]
        assert ref.startswith("#/$defs/")
        assert group_props["+partition_column"]["$ref"] == ref
        assert ref.split("/")[-1] in defs

        # Dedup proof: a field's (non-trivial) description text appears exactly
        # once in the whole file — in its definition, not at each ref site.
        desc = defs[ref.split("/")[-1]].get("description")
        assert desc and raw.count(desc) == 1

    def test_project_schema_size_is_bounded(self, tmp_path):
        """With $defs dedup the project schema stays modest even with every
        built-in adapter's fields. A generous ceiling catches a regression that
        re-inlines the union (which previously tripled the file)."""
        from dlt_saga.utility.generate_schemas import generate_schemas

        generate_schemas(tmp_path)
        size = (tmp_path / "saga_project_config.json").stat().st_size
        assert size < 160_000, (
            f"saga_project_config.json is {size} bytes — expected < 160KB. "
            "A jump likely means the config-field union is being inlined "
            "instead of referenced via $defs."
        )

    def test_partition_expiration_days_in_profile_target(self, tmp_path):
        """Profile-level default surfaces under each target's properties."""
        from dlt_saga.utility.generate_schemas import generate_schemas

        generate_schemas(tmp_path)
        data = json.loads(
            (tmp_path / "profiles_config.json").read_text(encoding="utf-8")
        )
        target_props = data["additionalProperties"]["properties"]["outputs"][
            "additionalProperties"
        ]["properties"]
        assert "partition_expiration_days" in target_props
        field = target_props["partition_expiration_days"]
        assert field["type"] == "integer"
        assert field["minimum"] == 1
        assert "BigQuery" in field["description"]

    def test_storage_root_in_profile_target(self, tmp_path):
        """Databricks storage_root surfaces under each target's properties."""
        from dlt_saga.utility.generate_schemas import generate_schemas

        generate_schemas(tmp_path)
        data = json.loads(
            (tmp_path / "profiles_config.json").read_text(encoding="utf-8")
        )
        target_props = data["additionalProperties"]["properties"]["outputs"][
            "additionalProperties"
        ]["properties"]
        assert "storage_root" in target_props
        field = target_props["storage_root"]
        assert field["type"] == "string"
        assert "Databricks" in field["description"]

    def _profile_target(self, tmp_path):
        from dlt_saga.utility.generate_schemas import generate_schemas

        generate_schemas(tmp_path)
        data = json.loads(
            (tmp_path / "profiles_config.json").read_text(encoding="utf-8")
        )
        return data["additionalProperties"]["properties"]["outputs"][
            "additionalProperties"
        ]

    def test_destination_keys_use_yaml_names_not_dataclass_fields(self, tmp_path):
        """Introspection maps field aliases to YAML keys (billing_project_id → billing_project)."""
        props = self._profile_target(tmp_path)["properties"]
        assert "billing_project" in props
        # Raw dataclass field names must not leak into the profile schema.
        assert "billing_project_id" not in props
        assert "dataset_name" not in props

    def test_generic_keys_allowed_on_every_destination(self, tmp_path):
        """Canonical keys + the cross-destination `destination_type` synonym apply
        on every destination, so they validate on any target."""
        generic = ("database", "schema", "type", "destination_type")
        target = self._profile_target(tmp_path)
        by_type = {
            b["if"]["properties"]["type"]["const"]: b["then"]["properties"]
            for b in target["allOf"]
        }
        for dtype, then_props in by_type.items():
            for alias in generic:
                assert alias in then_props, f"{alias} not allowed on {dtype} target"

    def test_idiomatic_aliases_are_per_destination(self, tmp_path):
        """Destination-idiomatic aliases are scoped to their destination and
        flagged on the wrong target: project_id/project/dataset on BigQuery,
        catalog on Databricks."""
        by_type = {
            b["if"]["properties"]["type"]["const"]: b["then"]["properties"]
            for b in self._profile_target(tmp_path)["allOf"]
        }
        # BigQuery: database aliases + dataset (BigQuery's word for schema).
        assert {"project_id", "project", "dataset"}.issubset(by_type["bigquery"])
        assert "catalog" not in by_type["bigquery"]
        # Databricks: catalog only; not BigQuery's aliases.
        assert "catalog" in by_type["databricks"]
        assert "project_id" not in by_type["databricks"]
        assert "dataset" not in by_type["databricks"]
        # DuckDB: only the canonical keys.
        assert "database" in by_type["duckdb"]
        assert "dataset" not in by_type["duckdb"]
        assert "catalog" not in by_type["duckdb"]

    def test_profile_target_has_conditional_blocks(self, tmp_path):
        """Each registered destination gets an if/then block with closed props."""
        target = self._profile_target(tmp_path)
        blocks = target["allOf"]
        types = {b["if"]["properties"]["type"]["const"] for b in blocks}
        assert {"bigquery", "databricks", "duckdb"}.issubset(types)
        for b in blocks:
            assert b["then"]["additionalProperties"] is False

    def test_conditional_blocks_partition_keys_by_destination(self, tmp_path):
        """A destination's keys appear only in its own block, plus generic keys."""
        target = self._profile_target(tmp_path)
        by_type = {
            b["if"]["properties"]["type"]["const"]: b["then"]["properties"]
            for b in target["allOf"]
        }
        # BigQuery has its own key, not Databricks'.
        assert "billing_project" in by_type["bigquery"]
        assert "staging_volume_name" not in by_type["bigquery"]
        # Databricks has its own key, not BigQuery's.
        assert "staging_volume_name" in by_type["databricks"]
        assert "billing_project" not in by_type["databricks"]
        assert "partition_expiration_days" not in by_type["databricks"]
        # DuckDB only its own.
        assert "database_path" in by_type["duckdb"]
        assert "billing_project" not in by_type["duckdb"]
        # Generic keys are allowed in every block (so they aren't rejected).
        for props in by_type.values():
            assert "database" in props
            assert "schema" in props
            assert "historize" in props


@pytest.mark.unit
class TestSecretSupportNote:
    """Fields that resolve secret references are documented uniformly."""

    def _schema(self):
        from dlt_saga.utility.generate_schemas import dataclass_to_json_schema
        from dlt_saga.utility.secrets.secret_str import SecretStr

        @dataclass
        class WithSecrets:
            token: Optional[SecretStr] = field(
                default=None, metadata={"description": "API token"}
            )
            client_id: str = field(
                default="",
                metadata={"description": "Client ID", "secret": True},
            )
            region: str = field(default="", metadata={"description": "Region"})

        return dataclass_to_json_schema(WithSecrets)

    def test_secretstr_field_gets_note(self):
        desc = self._schema()["properties"]["token"]["description"]
        assert desc.startswith("API token")
        assert "secret URI" in desc
        assert "azurekeyvault::" in desc

    def test_note_separated_from_description_by_period(self):
        # A description without terminal punctuation gets a period before the note.
        desc = self._schema()["properties"]["token"]["description"]
        assert "API token. Accepts a secret URI" in desc

    def test_existing_terminal_punctuation_not_doubled(self):
        from dlt_saga.utility.generate_schemas import dataclass_to_json_schema
        from dlt_saga.utility.secrets.secret_str import SecretStr

        @dataclass
        class Ends:
            token: Optional[SecretStr] = field(
                default=None, metadata={"description": "Token value."}
            )

        desc = dataclass_to_json_schema(Ends)["properties"]["token"]["description"]
        assert "Token value. Accepts" in desc
        assert "value.. Accepts" not in desc

    def test_marked_plain_field_gets_note(self):
        desc = self._schema()["properties"]["client_id"]["description"]
        assert desc.startswith("Client ID")
        assert "secret URI" in desc

    def test_note_omits_env_var_templating(self):
        # {{ env_var() }} is universal Jinja, not a per-field capability.
        desc = self._schema()["properties"]["token"]["description"]
        assert "${ENV_VAR}" not in desc
        assert "env_var" not in desc

    def test_non_secret_field_has_no_note(self):
        desc = self._schema()["properties"]["region"]["description"]
        assert desc == "Region"

    def test_secret_marker_not_emitted_as_schema_key(self):
        # The marker drives the note; it must not leak into the JSON schema.
        assert "secret" not in self._schema()["properties"]["client_id"]

    def test_sharepoint_config_documents_resolvable_identifiers(self):
        from dlt_saga.pipelines.sharepoint.config import SharePointConfig
        from dlt_saga.utility.generate_schemas import dataclass_to_json_schema

        props = dataclass_to_json_schema(SharePointConfig)["properties"]
        # Plain-str identifier that is resolved at runtime.
        assert "secret URI" in props["client_id"]["description"]
        # SecretStr credential.
        assert "secret URI" in props["certificate"]["description"]
        # Non-secret field untouched.
        assert "secret URI" not in props["file_type"]["description"]
