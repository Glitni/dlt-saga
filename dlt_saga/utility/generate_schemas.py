"""Generate JSON schemas from pipeline configuration dataclasses.

Introspects pipeline config dataclasses registered via the namespace registry
and generates JSON Schema files for IDE autocomplete and validation.

Public API:
    generate_schemas(output_dir) -> int
"""

import importlib
import json
import re
import shutil
from dataclasses import MISSING, Field, fields, is_dataclass
from enum import Enum
from pathlib import Path
from typing import Any, Dict, List, Optional, Union, get_args, get_origin

# ---------------------------------------------------------------------------
# Static schema location
# ---------------------------------------------------------------------------

_STATIC_SCHEMAS_DIR = Path(__file__).parent.parent / "schemas"

# ---------------------------------------------------------------------------
# Type conversion helpers (pure functions)
# ---------------------------------------------------------------------------


def extract_enum_values(enum_class: type) -> List[str]:
    """Extract string values from an Enum class."""
    if not isinstance(enum_class, type) or not issubclass(enum_class, Enum):
        return []
    return [e.value for e in enum_class]


def _unwrap_optional(field_type):
    """Unwrap Optional[T] to T."""
    origin = get_origin(field_type)
    if origin is Union:
        args = get_args(field_type)
        non_none_args = [arg for arg in args if arg is not type(None)]
        if len(non_none_args) == 1:
            return non_none_args[0], get_origin(non_none_args[0])
    return field_type, origin


def _handle_list_type(field_type, metadata):
    """Handle List[T] types."""
    schema = {"type": "array"}
    args = get_args(field_type)
    if args:
        item_type = args[0]
        type_map = {
            str: "string",
            int: "integer",
            float: "number",
            bool: "boolean",
        }
        if item_type in type_map:
            schema["items"] = {"type": type_map[item_type]}
        elif is_dataclass(item_type):
            schema["items"] = dataclass_to_json_schema(item_type)
        else:
            schema["items"] = {"type": "object"}
    return schema


def _handle_dict_type(field_type, metadata):
    """Handle Dict[K, V] types."""
    schema = {"type": "object"}
    args = get_args(field_type)
    if len(args) == 2:
        value_type = args[1]
        if metadata and "$ref" in metadata:
            schema["additionalProperties"] = {"$ref": metadata["$ref"]}
        elif value_type is str:
            schema["additionalProperties"] = {"type": "string"}
        elif value_type is int:
            schema["additionalProperties"] = {"type": "integer"}
        elif is_dataclass(value_type):
            schema["additionalProperties"] = dataclass_to_json_schema(value_type)
        else:
            schema["additionalProperties"] = True
    else:
        schema["additionalProperties"] = True
    return schema


def _handle_union_type(field_type):
    """Handle Union types (not Optional)."""
    schema = {"oneOf": []}
    args = get_args(field_type)
    _primitive_map = {str: "string", int: "integer", float: "number", bool: "boolean"}
    for arg in args:
        if arg is type(None):
            continue
        if arg in _primitive_map:
            schema["oneOf"].append({"type": _primitive_map[arg]})
        elif get_origin(arg) is list:
            item_args = get_args(arg)
            if item_args and item_args[0] in _primitive_map:
                schema["oneOf"].append(
                    {"type": "array", "items": {"type": _primitive_map[item_args[0]]}}
                )
            else:
                schema["oneOf"].append({"type": "array"})
        elif arg is dict or get_origin(arg) is dict:
            schema["oneOf"].append({"type": "object"})
        else:
            schema["oneOf"].append({"type": "object"})
    return schema


def _handle_primitive_or_dataclass(field_type, metadata):
    """Handle primitive types and dataclasses."""
    schema = {}
    type_map = {
        str: "string",
        "str": "string",
        int: "integer",
        "int": "integer",
        float: "number",
        "float": "number",
        bool: "boolean",
        "bool": "boolean",
    }

    if field_type in type_map:
        schema["type"] = type_map[field_type]
    elif is_dataclass(field_type):
        schema.update(dataclass_to_json_schema(field_type))
    elif metadata and "type" in metadata:
        schema["type"] = metadata["type"]
    else:
        schema["type"] = "string"
    return schema


def _add_default_value(schema, field):
    """Add default value to schema if present."""
    if field.default is not MISSING:
        default_value = field.default
        if isinstance(default_value, Enum):
            schema["default"] = default_value.value
        elif default_value is not None:
            schema["default"] = default_value


def _add_metadata_fields(schema, metadata):
    """Add metadata fields to schema."""
    if not metadata:
        return schema

    metadata_fields = [
        "description",
        "pattern",
        "format",
        "enum",
        "maxItems",
        "oneOf",
        "items",
    ]
    for field in metadata_fields:
        if field in metadata:
            schema[field] = metadata[field]

    if "$ref" in metadata and "additionalProperties" not in schema:
        return {"$ref": metadata["$ref"]}

    return schema


def python_type_to_json_schema(
    field: Field, field_metadata: Optional[Dict[str, Any]] = None
) -> Dict[str, Any]:
    """Convert a Python type annotation to JSON Schema type definition.

    Args:
        field: Dataclass field to convert
        field_metadata: Optional metadata (description, pattern, format, etc.)

    Returns:
        JSON Schema type definition
    """
    metadata = field.metadata if field.metadata else (field_metadata or {})
    field_type, origin = _unwrap_optional(field.type)

    if origin is list:
        schema = _handle_list_type(field_type, metadata)
    elif origin is dict:
        schema = _handle_dict_type(field_type, metadata)
    elif origin is Union:
        schema = _handle_union_type(field_type)
    else:
        schema = _handle_primitive_or_dataclass(field_type, metadata)

    _add_default_value(schema, field)
    return _add_metadata_fields(schema, metadata)


def dataclass_to_json_schema(
    dataclass_type: type, field_metadata_map: Optional[Dict[str, Dict[str, Any]]] = None
) -> Dict[str, Any]:
    """Convert a dataclass to a JSON Schema properties object.

    Args:
        dataclass_type: Dataclass type to convert
        field_metadata_map: Optional mapping of field names to metadata

    Returns:
        JSON Schema with properties and required fields
    """
    if not is_dataclass(dataclass_type):
        return {}

    properties = {}
    required_fields = []
    field_metadata_map = field_metadata_map or {}

    for field in fields(dataclass_type):
        if field.name.startswith("_"):
            continue

        field_metadata = field_metadata_map.get(field.name)
        field_schema = python_type_to_json_schema(field, field_metadata)
        properties[field.name] = field_schema

        if field.default is MISSING and field.default_factory is MISSING:
            required_fields.append(field.name)

    result = {"type": "object", "properties": properties}
    if required_fields:
        result["required"] = sorted(required_fields)
    return result


def get_target_fields_from_dataclass() -> Dict[str, Dict[str, Any]]:
    """Extract target configuration fields from TargetConfig dataclass.

    Returns:
        Mapping of field names to JSON Schema definitions
    """
    from dlt_saga.pipelines.target.config import TargetConfig

    target_schema = dataclass_to_json_schema(TargetConfig)
    return target_schema.get("properties", {})


def generate_schema_for_pipeline(
    pipeline_name: str,
    dataclass_type: type,
) -> Dict[str, Any]:
    """Generate complete JSON Schema for a pipeline configuration.

    Walks the full inheritance chain from the dataclass and merges all fields,
    then appends TargetConfig fields.

    Args:
        pipeline_name: Human-readable name (e.g., "Google Sheets")
        dataclass_type: Source config dataclass

    Returns:
        Complete JSON Schema
    """
    schema = {
        "$schema": "http://json-schema.org/draft-07/schema#",
        "title": f"DLT {pipeline_name} Pipeline Configuration",
        "description": f"Configuration schema for {pipeline_name} data pipelines",
        "type": "object",
        "properties": {},
        "additionalProperties": False,
    }

    all_properties = {}
    all_required = []

    # Walk inheritance chain from most general to most specific
    inheritance_chain = []
    current_class = dataclass_type
    while current_class:
        if is_dataclass(current_class):
            inheritance_chain.append(current_class)
        bases = [
            b for b in current_class.__bases__ if b is not object and is_dataclass(b)
        ]
        current_class = bases[0] if bases else None

    for parent_class in reversed(inheritance_chain):
        parent_schema = dataclass_to_json_schema(parent_class)
        all_properties.update(parent_schema.get("properties", {}))
        all_required.extend(parent_schema.get("required", []))

    target_fields = get_target_fields_from_dataclass()
    all_properties.update(target_fields)

    schema["properties"] = all_properties
    if all_required:
        schema["required"] = sorted(list(set(all_required)))

    return schema


# ---------------------------------------------------------------------------
# Project-level schema helpers
# ---------------------------------------------------------------------------


def _build_project_schema(
    dataclass_type: type,
    title: str,
    description: str,
    additional_properties: bool = True,
) -> Dict[str, Any]:
    """Build a complete JSON Schema from a project-level dataclass."""
    inner = dataclass_to_json_schema(dataclass_type)
    schema = {
        "$schema": "http://json-schema.org/draft-07/schema#",
        "title": title,
        "description": description,
        "type": "object",
        "properties": inner.get("properties", {}),
    }
    if inner.get("required"):
        schema["required"] = inner["required"]
    if not additional_properties:
        schema["additionalProperties"] = False
    return schema


def _patch_pipelines_section(
    schema: Dict[str, Any], adapter_examples: List[str]
) -> None:
    """Patch the pipelines property with rich nested structure."""
    pipelines_prop = schema.get("properties", {}).get("pipelines")
    if not pipelines_prop:
        return

    adapter_prop: Dict[str, Any] = {
        "type": "string",
        "description": (
            "Default pipeline adapter for this group (e.g., 'dlt_saga.api.genesyscloud')"
        ),
    }
    if adapter_examples:
        adapter_prop["examples"] = adapter_examples

    pipelines_prop.update(
        {
            "type": "object",
            "properties": {
                "dataset_access": {
                    "$ref": "dlt_common.json#/$defs/dataset_access_list",
                    "description": (
                        "Project-wide dataset access control "
                        "(applied to all pipeline groups). "
                        "Formats: ROLE:entity_type:email, "
                        "AUTHORIZED_DATASET:project.dataset, "
                        "AUTHORIZED_VIEW:project.dataset.view"
                    ),
                },
            },
            "additionalProperties": {
                "type": "object",
                "description": (
                    "Pipeline group configuration "
                    "(e.g., google_sheets, filesystem, api)"
                ),
                "properties": {
                    "dataset_access": {
                        "$ref": "dlt_common.json#/$defs/dataset_access_list",
                        "description": (
                            "Dataset access control (replaces parent). "
                            "Formats: ROLE:entity_type:email, "
                            "AUTHORIZED_DATASET:project.dataset, "
                            "AUTHORIZED_VIEW:project.dataset.view"
                        ),
                    },
                    "+dataset_access": {
                        "$ref": "dlt_common.json#/$defs/dataset_access_list",
                        "description": (
                            "Dataset access control (merges with parent). "
                            "Formats: ROLE:entity_type:email, "
                            "AUTHORIZED_DATASET:project.dataset, "
                            "AUTHORIZED_VIEW:project.dataset.view"
                        ),
                    },
                    "task_group": {
                        "type": "string",
                        "description": (
                            "Group name for pipelines that should run together "
                            "in the same Cloud Run task"
                        ),
                    },
                    "adapter": adapter_prop,
                },
                "additionalProperties": {
                    "type": "object",
                    "description": (
                        "Pipeline-specific configuration or nested group settings. "
                        "Pipeline entries can contain adapter, access, "
                        "secrets (via googlesecretmanager:: prefix), "
                        "and other pipeline-specific fields."
                    ),
                    "properties": {
                        "adapter": adapter_prop,
                    },
                    "additionalProperties": True,
                },
            },
        }
    )


def _inject_adapter_examples(
    schemas: Dict[str, Dict[str, Any]], examples: List[str]
) -> None:
    """Inject discovered adapter values as examples into all schemas with an 'adapter' property."""
    if not examples:
        return
    for schema in schemas.values():
        props = schema.get("properties", {})
        if "adapter" in props:
            props["adapter"]["examples"] = examples


def _build_profiles_schema(
    profile_dataclass: type,
    target_dataclass: type,
) -> Dict[str, Any]:
    """Build the profiles.yml schema from Profile and ProfileTarget dataclasses."""
    target_schema = dataclass_to_json_schema(target_dataclass)
    target_props = target_schema.get("properties", {})

    yaml_target_props = {}
    for key, value in target_props.items():
        if key == "destination_type":
            yaml_target_props["type"] = value
        elif key in ("name", "destination_config"):
            continue
        else:
            yaml_target_props[key] = value

    # ---- generic fields ----
    yaml_target_props["database"] = {
        "type": "string",
        "description": (
            "Top-level database identifier. "
            "BigQuery: GCP project ID. "
            "Databricks: alias for 'catalog'. "
            "Accepted aliases: catalog, project_id, project."
        ),
    }
    yaml_target_props["schema"] = {
        "type": "string",
        "description": (
            "Target schema / dataset name. "
            "Supports env_var() interpolation, e.g. "
            "\"{{ env_var('SAGA_SCHEMA_NAME', 'dlt_dev') }}\""
        ),
    }
    yaml_target_props["location"] = {
        "type": "string",
        "description": "Data location/region (e.g., 'EU', 'US')",
    }
    yaml_target_props["auth_provider"] = {
        "type": "string",
        "description": (
            "Authentication provider. "
            "'gcp' for BigQuery, 'databricks' for Databricks. "
            "Inferred from 'type' when omitted."
        ),
    }
    yaml_target_props["run_as"] = {
        "type": "string",
        "description": "Identity to impersonate (e.g., service account email for GCP)",
    }
    yaml_target_props["billing_project"] = {
        "type": "string",
        "description": "GCP project used for job execution / billing (BigQuery-specific, defaults to 'database')",
    }
    yaml_target_props["database_path"] = {
        "type": "string",
        "description": "Path to local DuckDB file (DuckDB-specific, use ':memory:' for in-memory)",
    }
    yaml_target_props["storage_path"] = {
        "type": "string",
        "description": "Cloud storage path for Iceberg tables (e.g., 'gs://bucket/path')",
    }

    # ---- Databricks-specific fields ----
    yaml_target_props["catalog"] = {
        "type": "string",
        "description": "Unity Catalog name (Databricks alias for 'database')",
    }
    yaml_target_props["server_hostname"] = {
        "type": "string",
        "description": (
            "Databricks workspace hostname "
            "(e.g., adb-1234567890.12.azuredatabricks.net). "
            "Find it in Settings → Developer → SQL Warehouse → Connection Details."
        ),
    }
    yaml_target_props["http_path"] = {
        "type": "string",
        "description": (
            "Databricks SQL Warehouse HTTP path "
            "(e.g., /sql/1.0/warehouses/abc123). "
            "Find it in the warehouse's Connection Details tab."
        ),
    }
    yaml_target_props["auth_mode"] = {
        "type": "string",
        "enum": ["u2m", "m2m", "pat"],
        "description": (
            "Databricks authentication mode. "
            "'u2m': browser OAuth (default). "
            "'m2m': service principal OAuth. "
            "'pat': personal access token."
        ),
    }
    yaml_target_props["access_token"] = {
        "type": "string",
        "description": (
            "Databricks personal access token (auth_mode: pat). "
            "Prefer a secret URI: "
            "'azurekeyvault::https://my-vault.vault.azure.net::secret-name'"
        ),
    }
    yaml_target_props["client_id"] = {
        "type": "string",
        "description": "Databricks service principal client ID (auth_mode: m2m)",
    }
    yaml_target_props["client_secret"] = {
        "type": "string",
        "description": (
            "Databricks service principal client secret (auth_mode: m2m). "
            "Prefer a secret URI: "
            "'azurekeyvault::https://my-vault.vault.azure.net::secret-name'"
        ),
    }
    yaml_target_props["staging_volume_name"] = {
        "type": "string",
        "description": (
            "Fully-qualified Unity Catalog volume used to stage Parquet files "
            "before COPY INTO (e.g., 'my_catalog.my_schema.ingest_volume'). "
            "Recommended: point to a shared team volume. "
            "When omitted, dlt auto-creates '_dlt_staging_load_volume' in the target schema."
        ),
    }
    yaml_target_props["staging_credentials_name"] = {
        "type": "string",
        "description": "Named Unity Catalog storage credential used in COPY INTO (optional)",
    }

    # Patch historize sub-section with explicit key schemas (overrides the Dict[str, Any] default)
    yaml_target_props["historize"] = {
        "type": "object",
        "description": (
            "Historize-layer overrides for this target. "
            "Keys override the top-level target values for historized tables only."
        ),
        "properties": {
            "table_format": {
                "type": "string",
                "enum": ["native", "iceberg", "delta", "delta_uniform"],
                "description": (
                    "Table format for historized tables. "
                    "BigQuery: 'native' or 'iceberg' (BigLake). "
                    "Databricks: 'native' (=delta), 'iceberg', 'delta', 'delta_uniform'."
                ),
            },
            "storage_path": {
                "type": "string",
                "description": (
                    "Cloud storage path for historized Iceberg tables "
                    "(e.g., 'gs://bucket/historized/'). "
                    "Overrides the top-level storage_path for historized tables only."
                ),
            },
        },
        "additionalProperties": False,
    }

    target_obj = {
        "type": "object",
        "description": (
            "A target defines connection and execution parameters "
            "for a specific environment"
        ),
        "properties": yaml_target_props,
        "additionalProperties": {
            "description": "Additional destination-specific configuration fields",
        },
    }

    profile_schema = dataclass_to_json_schema(profile_dataclass)
    profile_props = profile_schema.get("properties", {})

    yaml_profile_props = {}
    for key, value in profile_props.items():
        if key == "default_target":
            yaml_profile_props["target"] = value
        elif key == "targets":
            yaml_profile_props["outputs"] = {
                "type": "object",
                "description": value.get("description", "Named output targets"),
                "additionalProperties": target_obj,
                "minProperties": 1,
            }
        elif key == "name":
            continue
        else:
            yaml_profile_props[key] = value

    profile_obj = {
        "type": "object",
        "description": "A profile with a default target and multiple output targets",
        "properties": yaml_profile_props,
        "required": ["outputs"],
        "additionalProperties": False,
    }

    return {
        "$schema": "http://json-schema.org/draft-07/schema#",
        "title": "Saga Profiles Configuration",
        "description": (
            "Configuration schema for profiles.yml — dbt-style environment "
            "profiles with multiple targets. Supports "
            "{{ env_var('VAR', 'default') }} interpolation in string values."
        ),
        "type": "object",
        "additionalProperties": profile_obj,
        "propertyNames": {
            "description": (
                "Profile names. Entries starting with _ are treated "
                "as YAML anchors and ignored."
            ),
        },
    }


def generate_project_level_schemas(
    adapter_examples: Optional[List[str]] = None,
) -> Dict[str, Dict[str, Any]]:
    """Generate JSON schemas for project-level config files.

    Returns:
        Dict mapping schema filenames to schema dicts
    """
    from dlt_saga.packages import PackagesConfig
    from dlt_saga.project_config import SagaProjectConfig
    from dlt_saga.utility.cli.profiles import Profile, ProfileTarget

    schemas = {}

    schemas["packages_config.json"] = _build_project_schema(
        PackagesConfig,
        title="Saga Pipeline Packages",
        description=(
            "Configuration schema for packages.yml — "
            "external pipeline implementation packages"
        ),
        additional_properties=False,
    )
    print("  [OK] Generated packages_config.json (PackagesConfig)")

    schema = _build_project_schema(
        SagaProjectConfig,
        title="Saga Project Configuration",
        description=(
            "Configuration schema for saga_project.yml — "
            "project-level settings for config discovery, providers, "
            "and pipeline defaults"
        ),
        additional_properties=False,
    )
    _patch_pipelines_section(schema, adapter_examples or [])
    schemas["saga_project_config.json"] = schema
    print("  [OK] Generated saga_project_config.json (SagaProjectConfig)")

    schemas["profiles_config.json"] = _build_profiles_schema(Profile, ProfileTarget)
    print("  [OK] Generated profiles_config.json (Profile, ProfileTarget)")

    return schemas


# ---------------------------------------------------------------------------
# Discovery helpers
# ---------------------------------------------------------------------------

# Modules to skip during discovery (internal / not user-facing configs)
_SKIP_MODULE_SUFFIXES = {
    "target.config",
}


def _process_config_module(modname: str, base_module: str) -> Dict[str, Dict[str, Any]]:
    """Import *modname* and generate schemas for all Config dataclasses in it.

    Args:
        modname: Full dotted module name (e.g., 'dlt_saga.pipelines.google_sheets.config')
        base_module: The namespace base (e.g., 'dlt_saga.pipelines') for relative naming

    Returns:
        Dict mapping schema filenames to schema dicts
    """
    schemas: dict[str, dict] = {}
    config_module = importlib.import_module(modname)

    config_classes = [
        getattr(config_module, attr_name)
        for attr_name in dir(config_module)
        if attr_name.endswith("Config")
        and is_dataclass(getattr(config_module, attr_name))
        and getattr(config_module, attr_name).__module__ == modname
    ]

    if not config_classes:
        return schemas

    # Derive human-readable name from the relative module path.
    # e.g., 'dlt_saga.pipelines.api.genesyscloud.config' with base 'dlt_saga.pipelines'
    # → relative = 'api.genesyscloud' → parts = ['api', 'genesyscloud']
    relative = modname.removeprefix(f"{base_module}.").removesuffix(".config")
    parts = relative.split(".")
    if len(parts) == 1:
        human_name = parts[0].replace("_", " ").title()
    else:
        human_name = f"{parts[-1].replace('_', ' ').title()} {parts[0].upper()}"

    for config_class in config_classes:
        config_class_name = config_class.__name__

        schema_filename = config_class_name.replace("Config", "")
        schema_filename = re.sub(r"(?<!^)(?=[A-Z])", "_", schema_filename).lower()
        schema_filename = f"{schema_filename}_config.json"

        schema = generate_schema_for_pipeline(human_name, config_class)
        schemas[schema_filename] = schema
        print(f"  [OK] Generated {schema_filename} ({config_class_name})")

    return schemas


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------


def _discover_namespace_schemas(
    namespace_registry: Dict[str, str],
) -> tuple[Dict[str, Dict[str, Any]], bool]:
    """Discover and generate schemas for all config modules in registered namespaces.

    Returns:
        Tuple of (schemas dict, had_error flag).
    """
    import pkgutil

    schemas: Dict[str, Dict[str, Any]] = {}
    had_error = False

    for _namespace, base_module in namespace_registry.items():
        try:
            base = importlib.import_module(base_module)
        except ImportError:
            continue

        if not hasattr(base, "__path__"):
            continue

        for _importer, modname, _ispkg in pkgutil.walk_packages(
            base.__path__, prefix=f"{base_module}."
        ):
            if not modname.endswith(".config"):
                continue

            relative_suffix = modname.removeprefix(f"{base_module}.")
            if relative_suffix in _SKIP_MODULE_SUFFIXES:
                continue

            try:
                schemas.update(_process_config_module(modname, base_module))
            except Exception as e:
                print(f"  [ERROR] {modname}: {e}")
                had_error = True

    return schemas, had_error


def _write_schemas(schemas: Dict[str, Dict[str, Any]], output_dir: Path) -> None:
    """Write schema dicts to JSON files in output_dir."""
    for filename, schema in schemas.items():
        output_path = output_dir / filename
        with open(output_path, "w", encoding="utf-8") as f:
            json.dump(schema, f, indent=2, ensure_ascii=False)
            f.write("\n")


def generate_schemas(output_dir: Path) -> int:
    """Generate JSON schemas from all registered pipeline namespaces.

    Copies static schemas from ``dlt_saga/schemas/``, then generates schemas
    for all pipeline config classes discovered via the namespace registry.
    Also generates project-level schemas (saga_project_config, profiles_config,
    packages_config).

    Args:
        output_dir: Directory to write generated schemas to.

    Returns:
        Exit code: 0 on full success, 1 if any schema failed.
    """
    from dlt_saga.pipelines.registry import (
        _NAMESPACE_REGISTRY,
        _ensure_packages_loaded,
        discover_implementations,
    )

    _ensure_packages_loaded()
    output_dir.mkdir(parents=True, exist_ok=True)

    adapter_examples = [impl["adapter"] for impl in discover_implementations()]

    if _STATIC_SCHEMAS_DIR.is_dir():
        for src in sorted(_STATIC_SCHEMAS_DIR.glob("*.json")):
            shutil.copy2(src, output_dir / src.name)
            print(f"  [OK] Copied {src.name}")

    all_schemas, had_error = _discover_namespace_schemas(_NAMESPACE_REGISTRY)

    print("Generating project-level schemas...")
    try:
        all_schemas.update(generate_project_level_schemas(adapter_examples))
    except Exception as e:
        print(f"  [ERROR] Project-level schemas: {e}")
        had_error = True

    _inject_adapter_examples(all_schemas, adapter_examples)

    _write_schemas(all_schemas, output_dir)

    return 1 if had_error else 0
