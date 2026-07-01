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
from typing import Any, Dict, List, Mapping, Optional, Union, get_args, get_origin

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
        "minimum",
        "maximum",
        "exclusiveMinimum",
        "exclusiveMaximum",
        "oneOf",
        "items",
    ]
    for field in metadata_fields:
        if field in metadata:
            schema[field] = metadata[field]

    if "$ref" in metadata and "additionalProperties" not in schema:
        return {"$ref": metadata["$ref"]}

    return schema


# Standard note appended to the description of any field that accepts a secret
# reference. Keeps secret-capability documentation uniform and in one place
# rather than hand-written (and drifting) across each field's description.
# Only the secret-URI form is per-field; ``{{ env_var('VAR') }}`` templating
# works in every field (Jinja, applied at load time) so it is not mentioned here.
_SECRET_SUPPORT_NOTE = (
    "Accepts a secret URI resolved at runtime: "
    "'azurekeyvault::<vault-url>::<name>', "
    "'googlesecretmanager::<project>::<name>', or 'env::<VAR>'."
)


def _accepts_secret(field: Field, metadata: Mapping[str, Any]) -> bool:
    """True if the field resolves secret references at runtime.

    Detected from an explicit ``metadata={"secret": True}`` marker (for fields
    resolved but not typed as ``SecretStr``, e.g. identifiers) or from a
    ``SecretStr`` type annotation. Name-based type detection keeps this robust
    to ``from __future__ import annotations`` (string annotations).
    """
    if metadata.get("secret"):
        return True
    field_type, _ = _unwrap_optional(field.type)
    if isinstance(field_type, str):
        return "SecretStr" in field_type
    return getattr(field_type, "__name__", "") == "SecretStr"


def _append_secret_note(schema: Dict[str, Any]) -> None:
    """Append the standard secret-support note to a schema's description."""
    existing = schema.get("description", "").strip()
    if not existing:
        schema["description"] = _SECRET_SUPPORT_NOTE
        return
    if existing[-1] not in ".!?":
        existing += "."
    schema["description"] = f"{existing} {_SECRET_SUPPORT_NOTE}"


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
    schema = _add_metadata_fields(schema, metadata)
    if (
        isinstance(schema, dict)
        and "$ref" not in schema
        and _accepts_secret(field, metadata)
    ):
        _append_secret_note(schema)
    return schema


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


def _destination_profile_props(config_class: type) -> Dict[str, Any]:
    """Build profile-schema props for one destination's specific YAML keys.

    Introspects the destination config dataclass: fields flagged with
    ``metadata["profile_field"]`` become profile keys. The YAML key defaults to
    the field name unless ``metadata["profile_key"]`` overrides it (e.g.
    ``billing_project_id`` → ``billing_project``). Type comes from the
    annotation; ``description``/``enum``/``minimum`` come from the field
    metadata. The config dataclass is thus the single source of truth — adding a
    field there surfaces it in the profile schema with no hand-list to drift.
    """
    import typing

    props: Dict[str, Any] = {}
    if not is_dataclass(config_class):
        return props

    # Resolve annotations to real types. The config modules use
    # ``from __future__ import annotations``, so ``field.type`` is a string
    # (e.g. "Optional[int]") that wouldn't type-map correctly.
    try:
        hints = typing.get_type_hints(config_class)
    except Exception:
        hints = {}
    type_map = {str: "string", int: "integer", float: "number", bool: "boolean"}

    for f in fields(config_class):
        if not (f.metadata and f.metadata.get("profile_field")):
            continue
        key = f.metadata.get("profile_key", f.name)
        annotation = hints.get(f.name, str)
        if typing.get_origin(annotation) is typing.Union:
            non_none = [a for a in typing.get_args(annotation) if a is not type(None)]
            annotation = non_none[0] if non_none else str
        prop: Dict[str, Any] = {"type": type_map.get(annotation, "string")}
        for meta_key in (
            "description",
            "enum",
            "minimum",
            "maximum",
            "pattern",
            "format",
        ):
            if meta_key in f.metadata:
                prop[meta_key] = f.metadata[meta_key]
        props[key] = prop

    # Destination-idiomatic aliases for generic keys (e.g. BigQuery's project_id
    # for `database`), surfaced only on this destination's target.
    for canonical, alias_names in getattr(
        config_class, "PROFILE_KEY_ALIASES", {}
    ).items():
        for alias in alias_names:
            props[alias] = {
                "type": "string",
                "description": f"Alias for '{canonical}' on this destination.",
            }
    return props


def _build_destination_conditionals(
    generic_props: Dict[str, Any],
) -> "tuple[Dict[str, Any], List[Dict[str, Any]]]":
    """Introspect registered destinations into profile-schema pieces.

    Returns ``(union_props, conditional_blocks)`` where ``union_props`` is every
    destination's specific keys merged (for the base ``properties``, so editors
    autocomplete them all) and ``conditional_blocks`` is one ``if/then`` per
    destination type: when ``type`` is that destination, only the generic keys
    plus that destination's keys are allowed (``additionalProperties: false``),
    so a sibling destination's key on the wrong target is flagged.
    """
    from dlt_saga.destinations.factory import DestinationFactory

    per_type_props = {
        dtype: _destination_profile_props(config_class)
        for dtype, config_class in DestinationFactory._config_registry.items()
    }

    union_props: Dict[str, Any] = {}
    for type_props in per_type_props.values():
        union_props.update(type_props)

    conditional_blocks = [
        {
            "if": {"properties": {"type": {"const": dtype}}, "required": ["type"]},
            "then": {
                "properties": {**generic_props, **type_props},
                "additionalProperties": False,
            },
        }
        for dtype, type_props in per_type_props.items()
    ]
    return union_props, conditional_blocks


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

    # `dev:` is a load-time override block, not a dataclass field. Mirror every
    # config key so values inside it validate and autocomplete, but require
    # nothing — a dev block overrides only a subset.
    all_properties["dev"] = _dev_override_property(all_properties)

    schema["properties"] = all_properties
    if all_required:
        schema["required"] = sorted(list(set(all_required)))

    return schema


def _dev_override_property(properties: Dict[str, Any]) -> Dict[str, Any]:
    """Schema for the ``dev:`` override block: the config's keys, none required.

    Applied only in the ``dev`` environment (and stripped elsewhere), the block
    may override any config key — so it mirrors the surrounding property set with
    no ``required`` constraint. ``dev`` itself is excluded to avoid nesting.
    """
    return {
        "type": "object",
        "description": (
            "Overrides applied only when the active environment is 'dev' "
            "(stripped in all environments). Any config key may be overridden; "
            "values support Jinja templating (e.g. a rolling initial_value)."
        ),
        "properties": {k: v for k, v in properties.items() if k != "dev"},
        "additionalProperties": False,
    }


# ---------------------------------------------------------------------------
# Project-level schema helpers
# ---------------------------------------------------------------------------


def _collect_config_field_union(
    pipeline_schemas: Dict[str, Dict[str, Any]],
) -> Dict[str, Any]:
    """Merge the ``properties`` of all pipeline config schemas into one set.

    The project-level schema (``saga_project.yml``) lets a group of pipelines
    share config keys by declaring them once under the pipeline group. Those
    keys are adapter-specific, so the project schema cannot know them up front.
    This collects every field declared by every discovered adapter config (plus
    the inherited base/target fields) so shared keys validate as typed
    properties instead of being reported as invalid.

    First occurrence of a field name wins. If the same name appears with a
    differing schema across adapters, it is widened to an unconstrained schema
    (``{}``, i.e. "any") rather than picking one adapter's definition.
    """
    union: Dict[str, Any] = {}
    for schema in pipeline_schemas.values():
        props = schema.get("properties")
        if not isinstance(props, dict):
            continue
        for name, field_schema in props.items():
            if name not in union:
                union[name] = field_schema
            elif union[name] != {} and union[name] != field_schema:
                union[name] = {}
    return union


def _with_inherit_aliases(props: Dict[str, Any]) -> Dict[str, Any]:
    """Return *props* plus a ``+name`` alias for every key.

    Mirrors the runtime merge syntax: ``key:`` overrides the parent value while
    ``+key:`` merges with it. Both forms are valid wherever a config key may be
    shared (project/group/folder levels), so the schema must accept either.
    """
    out: Dict[str, Any] = {}
    for name, schema in props.items():
        out[name] = schema
        out[f"+{name}"] = schema
    return out


_FIELD_DEF_PREFIX = "config_field_"


def _shared_props_via_defs(
    config_field_props: Optional[Dict[str, Any]],
) -> tuple[Dict[str, Any], Dict[str, Any]]:
    """Split the config-field union into ``$defs`` plus a thin ref map.

    The union is injected at three levels of the project schema (project-wide,
    group, pipeline) and each field carries a ``+merge`` alias — so inlining the
    full field schemas would duplicate every field (with its description) six
    times. Instead we define each field once under ``$defs`` and reference it,
    keeping the generated schema small and flat regardless of how many adapters
    contribute fields.

    Returns:
        (field_defs, shared_props) where *field_defs* maps a ``$defs`` key to
        the field schema, and *shared_props* maps each field name (and its
        ``+name`` alias) to a ``{"$ref": ...}`` pointing at that def.
    """
    union = config_field_props or {}
    field_defs = {
        f"{_FIELD_DEF_PREFIX}{name}": schema for name, schema in union.items()
    }
    ref_props = {name: {"$ref": f"#/$defs/{_FIELD_DEF_PREFIX}{name}"} for name in union}
    return field_defs, _with_inherit_aliases(ref_props)


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
    schema: Dict[str, Any],
    adapter_examples: List[str],
    config_field_props: Optional[Dict[str, Any]] = None,
) -> None:
    """Patch the pipelines property with rich nested structure.

    *config_field_props* is the union of all adapter config fields (see
    ``_collect_config_field_union``). Each field is defined once under the
    schema's ``$defs`` and referenced — with ``+name`` merge aliases — at every
    level where a config key may be shared (project-wide, pipeline group, and
    individual pipeline), so shared adapter keys validate as typed properties
    rather than falling through to the nested-entry schema and being reported
    as invalid. Explicit keys defined below always take precedence over the
    union (e.g. the richer ``schema_access`` $ref).
    """
    pipelines_prop = schema.get("properties", {}).get("pipelines")
    if not pipelines_prop:
        return

    field_defs, shared_props = _shared_props_via_defs(config_field_props)
    if field_defs:
        schema.setdefault("$defs", {}).update(field_defs)

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
                **shared_props,
                "schema_access": {
                    "$ref": "dlt_common.json#/$defs/schema_access_list",
                    "description": (
                        "Project-wide schema access control "
                        "(applied to all pipeline groups). "
                        "Formats: ROLE:entity_type:email, "
                        "AUTHORIZED_DATASET:project.dataset, "
                        "AUTHORIZED_VIEW:project.dataset.view"
                    ),
                },
                "historize_schema_access": {
                    "$ref": "dlt_common.json#/$defs/schema_access_list",
                    "description": (
                        "Overlay added on top of schema_access when granting "
                        "access on the historize-layer schema. Only meaningful "
                        "when the historize schema is distinct from the ingest "
                        "schema (placement: schema_suffix, or a custom "
                        "naming_module that returns a different name for "
                        "layer='historize')."
                    ),
                },
                "dataset_access": {
                    "$ref": "dlt_common.json#/$defs/schema_access_list",
                    "description": (
                        "Deprecated alias for schema_access — kept as a "
                        "read-time alias for backwards compatibility."
                    ),
                    "deprecated": True,
                },
            },
            "additionalProperties": {
                "type": "object",
                "description": (
                    "Pipeline group configuration "
                    "(e.g., google_sheets, filesystem, api)"
                ),
                "properties": {
                    **shared_props,
                    "schema_access": {
                        "$ref": "dlt_common.json#/$defs/schema_access_list",
                        "description": (
                            "Schema access control (replaces parent). "
                            "Formats: ROLE:entity_type:email, "
                            "AUTHORIZED_DATASET:project.dataset, "
                            "AUTHORIZED_VIEW:project.dataset.view"
                        ),
                    },
                    "+schema_access": {
                        "$ref": "dlt_common.json#/$defs/schema_access_list",
                        "description": (
                            "Schema access control (merges with parent). "
                            "Formats: ROLE:entity_type:email, "
                            "AUTHORIZED_DATASET:project.dataset, "
                            "AUTHORIZED_VIEW:project.dataset.view"
                        ),
                    },
                    "historize_schema_access": {
                        "$ref": "dlt_common.json#/$defs/schema_access_list",
                        "description": (
                            "Overlay added on top of schema_access for the "
                            "historize schema (replaces parent)."
                        ),
                    },
                    "+historize_schema_access": {
                        "$ref": "dlt_common.json#/$defs/schema_access_list",
                        "description": (
                            "Overlay added on top of schema_access for the "
                            "historize schema (merges with parent)."
                        ),
                    },
                    "dataset_access": {
                        "$ref": "dlt_common.json#/$defs/schema_access_list",
                        "description": (
                            "Deprecated alias for schema_access — accepted "
                            "as a read-time alias."
                        ),
                        "deprecated": True,
                    },
                    "+dataset_access": {
                        "$ref": "dlt_common.json#/$defs/schema_access_list",
                        "description": (
                            "Deprecated alias for +schema_access — accepted "
                            "as a read-time alias."
                        ),
                        "deprecated": True,
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
                        **shared_props,
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
    # `destination_type` is a cross-destination synonym for `type` (the field
    # name), so it's generic. Destination-idiomatic aliases (catalog, project_id,
    # dataset, …) are declared per-destination via PROFILE_KEY_ALIASES instead.
    yaml_target_props["destination_type"] = {
        "type": "string",
        "description": "Alias for 'type'.",
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

    # Everything accumulated so far is destination-agnostic (generic). Fold in
    # the introspected per-destination keys and build the conditional blocks.
    union_props, conditional_blocks = _build_destination_conditionals(
        dict(yaml_target_props)
    )
    # Base properties = generic + the union of every destination's keys, so
    # editors autocomplete all keys and the schema knows each key's type.
    for key, prop in union_props.items():
        yaml_target_props.setdefault(key, prop)

    target_obj = {
        "type": "object",
        "description": (
            "A target defines connection and execution parameters "
            "for a specific environment"
        ),
        "properties": yaml_target_props,
        "allOf": conditional_blocks,
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
    config_field_props: Optional[Dict[str, Any]] = None,
) -> Dict[str, Dict[str, Any]]:
    """Generate JSON schemas for project-level config files.

    Args:
        adapter_examples: Discovered adapter values, surfaced as ``examples``.
        config_field_props: Union of all adapter config fields, injected into
            the ``pipelines`` section so shared keys validate (see
            ``_collect_config_field_union``).

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
    _patch_pipelines_section(schema, adapter_examples or [], config_field_props)
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
        schema_filename = schema_filename_for_config_class(config_class_name)

        schema = generate_schema_for_pipeline(human_name, config_class)
        schemas[schema_filename] = schema
        print(f"  [OK] Generated {schema_filename} ({config_class_name})")

    return schemas


def schema_filename_for_config_class(config_class_name: str) -> str:
    """Derive the generated schema filename for a config dataclass name.

    e.g. ``NativeLoadConfig`` -> ``native_load_config.json``.
    """
    stem = config_class_name.replace("Config", "")
    stem = re.sub(r"(?<!^)(?=[A-Z])", "_", stem).lower()
    return f"{stem}_config.json"


def _defined_config_class(module: Any, modname: str) -> Optional[type]:
    """Return the ``*Config`` dataclass *defined* in *module* (not imported)."""
    for attr_name in dir(module):
        if not attr_name.endswith("Config"):
            continue
        obj = getattr(module, attr_name)
        if isinstance(obj, type) and is_dataclass(obj) and obj.__module__ == modname:
            return obj
    return None


def config_class_for_adapter(
    adapter: Optional[str],
    pipeline_group: str = "",
    config_path: Optional[str] = None,
) -> Optional[type]:
    """Resolve the ``*Config`` dataclass an adapter's configs are validated against.

    Resolves the pipeline class (via the same registry logic used at runtime),
    then walks its package ancestry for the nearest ``config.py`` that defines a
    ``*Config`` dataclass — mirroring how schema generation discovers configs.
    Returns the dataclass, or ``None`` if none can be resolved (e.g. an
    unresolvable adapter).
    """
    from dlt_saga.pipelines.registry import _NAMESPACE_REGISTRY, get_pipeline_class

    try:
        pipeline_cls = get_pipeline_class(pipeline_group, config_path, adapter)
    except Exception:
        return None

    pkg = pipeline_cls.__module__.rsplit(".", 1)[0]
    bases = tuple(_NAMESPACE_REGISTRY.values())
    base = next((b for b in bases if pkg == b or pkg.startswith(f"{b}.")), None)

    parts = pkg.split(".")
    while parts:
        candidate_pkg = ".".join(parts)
        # Never walk above the namespace base (avoids matching unrelated configs).
        if base and not (candidate_pkg == base or candidate_pkg.startswith(f"{base}.")):
            break
        modname = f"{candidate_pkg}.config"
        try:
            module = importlib.import_module(modname)
        except ImportError:
            module = None
        if module is not None:
            cls = _defined_config_class(module, modname)
            if cls is not None:
                return cls
        if base is None:
            break  # unknown namespace: only try the leaf package
        parts = parts[:-1]
    return None


def schema_filename_for_adapter(
    adapter: Optional[str],
    pipeline_group: str = "",
    config_path: Optional[str] = None,
) -> Optional[str]:
    """Resolve the generated schema filename a config file should reference.

    Returns the matching ``*_config.json`` filename, or ``None`` if no config
    class can be resolved (e.g. an unresolvable adapter).
    """
    cls = config_class_for_adapter(adapter, pipeline_group, config_path)
    return schema_filename_for_config_class(cls.__name__) if cls is not None else None


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

    # Teach the project-level schema about adapter-specific config keys so that
    # keys shared across a pipeline group via saga_project.yml validate.
    config_field_props = _collect_config_field_union(all_schemas)

    print("Generating project-level schemas...")
    try:
        all_schemas.update(
            generate_project_level_schemas(adapter_examples, config_field_props)
        )
    except Exception as e:
        print(f"  [ERROR] Project-level schemas: {e}")
        had_error = True

    _inject_adapter_examples(all_schemas, adapter_examples)

    _write_schemas(all_schemas, output_dir)

    return 1 if had_error else 0
