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
