"""Base class for BigQuery-based destinations.

Provides shared functionality for destinations that use BigQuery datasets
(BigQuery and BigLake Iceberg).
"""

import json
import logging
from typing import TYPE_CHECKING, Any, List, Optional

from dlt_saga.destinations.base import Destination

if TYPE_CHECKING:
    from dlt_saga.destinations.bigquery.access import BigQueryAccessManager
    from dlt_saga.destinations.bigquery.config import BigQueryDestinationConfig

logger = logging.getLogger(__name__)


def _normalize_entity_type(entity_type: Any) -> str:
    """Normalize entity_type to a plain string.

    Handles enum vs string differences: locally we may use EntityTypes.DATASET
    (enum) while the API returns the plain string "dataset".
    """
    return getattr(entity_type, "value", str(entity_type))


def _stable_entry_key(entry: Any) -> tuple:
    """Create a stable, comparable key for a BigQuery AccessEntry.

    Normalizes entity_type (enum vs string) and uses json.dumps with sort_keys
    for dict entity_ids to avoid false mismatches between locally constructed
    entries and API-returned entries.
    """
    entity_type_str = _normalize_entity_type(entry.entity_type)

    entity_id = entry.entity_id
    if isinstance(entity_id, dict):
        entity_id_str = json.dumps(entity_id, sort_keys=True)
    else:
        entity_id_str = str(entity_id)

    return (entry.role, entity_type_str, entity_id_str)


# Entity types produced by our config parsers (_parse_role_based_access,
# _parse_authorized_view). Authorized dataset entries are identified
# separately by role=None + dict entity_id.
_MANAGED_ENTITY_TYPES = frozenset({"userByEmail", "groupByEmail", "view"})


def _is_managed_entry(entry: Any) -> bool:
    """Check if an access entry is one we manage via config.

    Managed entries are those created by our access config parsers:
    - Role-based: userByEmail, groupByEmail (serviceAccount/user/group configs)
    - Authorized views: entity_type="view"
    - Authorized datasets: role=None with dict entity_id

    Unmanaged entries are BigQuery defaults (e.g. specialGroup: projectOwners,
    projectWriters, projectReaders) which we preserve as-is.
    """
    if _normalize_entity_type(entry.entity_type) in _MANAGED_ENTITY_TYPES:
        return True
    # Authorized dataset entries: role=None with dict entity_id
    if entry.role is None and isinstance(entry.entity_id, dict):
        return True
    return False


class BigQueryBaseDestination(Destination):
    """Base class for destinations that use BigQuery datasets.

    Provides common functionality for:
    - Dataset management
    - Access control management
    - BigQuery IAM access manager
    - Support capability flags
    """

    config: "BigQueryDestinationConfig"
    _access_manager: Optional["BigQueryAccessManager"]
    _synced_datasets: set = set()  # Class-level cache of (project_id, dataset_name)

    def __init__(self, config: "BigQueryDestinationConfig"):
        """Initialize BigQuery base destination.

        Args:
            config: BigQuery-specific configuration
        """
        super().__init__(config)
        self._access_manager = None

    def _get_bigquery_destination_kwargs(self) -> dict:
        """Get additional kwargs for BigQuery destination creation.

        Subclasses can override to add destination-specific parameters.

        Returns:
            Dictionary of additional kwargs
        """
        return {}

    def create_dlt_destination(self) -> Any:
        """Create dlt BigQuery destination instance.

        Returns:
            dlt BigQuery destination configured with project and location
        """
        import os

        from dlt.destinations import bigquery

        # When using impersonation, we rely on google.auth.default() picking up
        # the impersonated credentials from our custom setup
        # We don't pass credentials explicitly to avoid dlt validation issues
        service_account = os.getenv("GOOGLE_IMPERSONATE_SERVICE_ACCOUNT")
        if service_account:
            logger.debug(
                f"Creating dlt BigQuery destination (will use impersonation for {service_account})"
            )
        else:
            logger.debug(
                f"Creating BigQuery destination: project={self.config.project_id}, "
                f"location={self.config.location}"
            )

        # Build kwargs with base parameters
        # Use job_project_id for dlt's BigQuery client (job execution/billing),
        # NOT project_id (which is the data project for table references)
        kwargs: dict[str, Any] = {
            "destination_name": "bigquery",
            "project_id": self.config.job_project_id,
            "location": self.config.location,
        }

        # Add destination-specific kwargs
        kwargs.update(self._get_bigquery_destination_kwargs())

        return bigquery(**kwargs)

    def get_access_manager(self) -> Any:
        """Get BigQuery access manager for IAM-based access control.

        Returns:
            BigQueryAccessManager instance
        """
        if self._access_manager is None:
            from dlt_saga.destinations.bigquery.access import BigQueryAccessManager

            self._access_manager = BigQueryAccessManager(
                project_id=self.config.project_id,
                dataset_id=None,  # Will be set per-pipeline
            )
            logger.debug("Initialized BigQuery access manager")

        return self._access_manager

    def supports_access_management(self) -> bool:
        """BigQuery-based destinations support IAM-based access control.

        Returns:
            True
        """
        return True

    def supports_partitioning(self) -> bool:
        """BigQuery-based destinations support table partitioning.

        Returns:
            True
        """
        return True

    def supports_clustering(self) -> bool:
        """BigQuery-based destinations support table clustering.

        Returns:
            True
        """
        return True

    @staticmethod
    def _parse_authorized_view(entry: str, view_ref: str) -> Optional[Any]:
        """Parse AUTHORIZED_VIEW entry."""
        from google.cloud import bigquery

        if view_ref.count(".") != 2:
            logger.warning(
                f"Invalid authorized view format: '{entry}'. "
                "Expected format: 'AUTHORIZED_VIEW:project-id.dataset-id.view-id'"
            )
            return None

        project_id, dataset_id, view_id = view_ref.split(".", 2)
        logger.debug(f"Added authorized view: {project_id}.{dataset_id}.{view_id}")
        return bigquery.AccessEntry(
            role=None,
            entity_type="view",
            entity_id={
                "projectId": project_id,
                "datasetId": dataset_id,
                "tableId": view_id,
            },
        )

    @staticmethod
    def _parse_authorized_dataset(entry: str, dataset_ref: str) -> Optional[Any]:
        """Parse AUTHORIZED_DATASET entry."""
        from google.cloud import bigquery
        from google.cloud.bigquery.enums import EntityTypes

        if dataset_ref.count(".") != 1:
            logger.warning(
                f"Invalid authorized dataset format: '{entry}'. "
                "Expected format: 'AUTHORIZED_DATASET:project-id.dataset-id'"
            )
            return None

        project_id, dataset_id = dataset_ref.split(".", 1)
        logger.debug(f"Added authorized dataset: {project_id}.{dataset_id}")
        return bigquery.AccessEntry(
            role=None,
            entity_type=EntityTypes.DATASET,
            entity_id={
                "dataset": {
                    "projectId": project_id,
                    "datasetId": dataset_id,
                },
                "targetTypes": ["VIEWS"],
            },
        )

    @staticmethod
    def _parse_role_based_access(
        entry: str, role: str, entity_type: str, email: str
    ) -> Optional[Any]:
        """Parse role-based access entry."""
        from google.cloud import bigquery

        role = role.strip().upper()
        entity_type = entity_type.strip().lower()
        email = email.strip()

        # Map entity_type to BigQuery's expected format
        entity_type_map = {
            "serviceaccount": "userByEmail",
            "user": "userByEmail",
            "group": "groupByEmail",
        }

        entity_type_param = entity_type_map.get(entity_type)
        if not entity_type_param:
            logger.warning(
                f"Unknown entity type '{entity_type}' in entry: '{entry}'. "
                "Supported types: serviceAccount, user, group, AUTHORIZED_DATASET, AUTHORIZED_VIEW"
            )
            return None

        if role not in ["OWNER", "WRITER", "READER"]:
            logger.warning(
                f"Unknown role '{role}' in entry: '{entry}'. "
                "Supported roles: OWNER, WRITER, READER"
            )
            return None

        return bigquery.AccessEntry(
            role=role, entity_type=entity_type_param, entity_id=email
        )

    @staticmethod
    def _parse_dataset_access_static(access_config: List[str]) -> List[Any]:
        """Parse dataset access configuration into BigQuery AccessEntry objects.

        Args:
            access_config: List of access strings in supported formats

        Returns:
            List of BigQuery AccessEntry objects

        Supported formats:
            - OWNER:serviceAccount:email@project.iam.gserviceaccount.com
            - WRITER:user:user@example.com
            - READER:group:group@example.com
            - AUTHORIZED_DATASET:project-id.dataset-id
            - AUTHORIZED_VIEW:project-id.dataset-id.view-id
        """
        access_entries = []

        for entry in access_config:
            parts = entry.split(":", 2)
            if len(parts) not in [2, 3]:
                logger.warning(
                    f"Invalid dataset access entry format: '{entry}'. "
                    "Expected format: 'role:entity_type:email', 'AUTHORIZED_DATASET:project.dataset', "
                    "or 'AUTHORIZED_VIEW:project.dataset.view'"
                )
                continue

            # Parse based on entry type
            parsed_entry = None
            if len(parts) == 2:
                entry_type = parts[0].strip().upper()
                if entry_type == "AUTHORIZED_VIEW":
                    parsed_entry = BigQueryBaseDestination._parse_authorized_view(
                        entry, parts[1].strip()
                    )
                elif entry_type == "AUTHORIZED_DATASET":
                    parsed_entry = BigQueryBaseDestination._parse_authorized_dataset(
                        entry, parts[1].strip()
                    )
                else:
                    logger.warning(f"Invalid dataset access entry format: '{entry}'.")
            elif len(parts) == 3:
                parsed_entry = BigQueryBaseDestination._parse_role_based_access(
                    entry, *parts
                )

            if parsed_entry:
                access_entries.append(parsed_entry)

        return access_entries

    @staticmethod
    def _filter_staging_access_entries(
        access_entries: List[Any], dataset_name: str
    ) -> List[Any]:
        """Filter access entries for staging datasets to only OWNER/WRITER.

        Staging datasets are temporary processing datasets and should not have
        READER access or authorized datasets/views.

        Args:
            access_entries: List of AccessEntry objects
            dataset_name: Name of the dataset

        Returns:
            Filtered list (only OWNER/WRITER for staging), or original list for non-staging
        """
        if "_staging" in dataset_name:
            return [
                entry for entry in access_entries if entry.role in ("OWNER", "WRITER")
            ]
        return access_entries

    def sync_dataset_and_access(self, dataset_name: str) -> None:
        """Sync BigQuery dataset existence and access controls with configuration.

        Creates dataset if needed and synchronizes access controls with config (in prod).
        Skips if this (project, dataset) was already synced in this process.

        Args:
            dataset_name: Name of the dataset to ensure exists
        """
        cache_key = (self.config.project_id, dataset_name)
        if cache_key in BigQueryBaseDestination._synced_datasets:
            logger.debug(f"Dataset {dataset_name} already synced, skipping")
            return

        self._sync_dataset_and_access_static(
            project_id=self.config.project_id,
            location=self.config.location,
            dataset_name=dataset_name,
            dataset_access=self.config.dataset_access,
        )
        BigQueryBaseDestination._synced_datasets.add(cache_key)

    @staticmethod
    def _sync_dataset_and_access_static(
        project_id: str,
        location: str,
        dataset_name: str,
        dataset_access: Optional[List[str]] = None,
        client: Any = None,
    ) -> None:
        """Sync BigQuery dataset existence and access controls (static implementation).

        Creates dataset if needed and synchronizes access controls with config (in prod).
        This prevents race conditions when running pipelines in parallel.

        Args:
            project_id: GCP project ID
            location: BigQuery location (e.g., "EU", "US")
            dataset_name: Name of the dataset to ensure exists
            dataset_access: Optional dataset-level access control list
            client: Optional BigQuery client to reuse (avoids creating a new one)
        """
        import os

        from google.api_core.exceptions import Conflict
        from google.cloud import bigquery
        from google.cloud.exceptions import NotFound

        from dlt_saga.utility.naming import get_environment

        environment = get_environment()

        try:
            if client is None:
                service_account = os.getenv("GOOGLE_IMPERSONATE_SERVICE_ACCOUNT")
                if service_account:
                    logger.debug(
                        f"Using impersonated credentials for {service_account}"
                    )
                client = bigquery.Client(project=project_id, location=location)

            # Parse access entries once (if configured and in prod)
            access_entries = None
            if dataset_access and environment == "prod":
                access_entries = BigQueryBaseDestination._parse_dataset_access_static(
                    dataset_access
                )
                access_entries = BigQueryBaseDestination._filter_staging_access_entries(
                    access_entries, dataset_name
                )

            # Step 1: Ensure dataset exists
            try:
                existing_dataset = client.get_dataset(dataset_name)
                logger.debug(f"BigQuery dataset {dataset_name} exists")
            except NotFound:
                dataset = bigquery.Dataset(f"{project_id}.{dataset_name}")
                dataset.location = location

                if access_entries:
                    dataset.access_entries = access_entries
                    logger.debug(
                        f"Setting {len(access_entries)} access entries for {dataset_name}"
                    )

                try:
                    client.create_dataset(dataset, exists_ok=True)
                    logger.info(f"Created BigQuery dataset {dataset_name}")
                except Conflict:
                    logger.debug(f"Dataset {dataset_name} created by another process")

                # Dataset just created with correct access, no update needed
                return

            # Step 2: Sync access controls on existing dataset (prod only)
            if access_entries:
                BigQueryBaseDestination._update_access_if_needed(
                    client, existing_dataset, dataset_name, access_entries
                )

        except NotFound:
            logger.debug(
                f"Could not find dataset {dataset_name}, "
                f"DLT will handle dataset creation."
            )
        except Exception as e:
            logger.warning(f"Failed to sync dataset access for {dataset_name}: {e}")

    @staticmethod
    def _update_access_if_needed(
        client: Any,
        existing_dataset: Any,
        dataset_name: str,
        access_entries: List[Any],
    ) -> None:
        """Declaratively sync access controls on an existing dataset.

        Separates existing entries into managed (types we control via config)
        and unmanaged (BigQuery defaults like projectOwners). Compares only
        managed entries against desired config. Rebuilds the full list as
        unmanaged + desired, so both additions and removals are applied.

        Args:
            client: BigQuery client
            existing_dataset: Dataset object from client.get_dataset()
            dataset_name: Name of the dataset (for logging)
            access_entries: Desired access entries from config
        """
        existing_managed_keys = {
            _stable_entry_key(e)
            for e in existing_dataset.access_entries
            if _is_managed_entry(e)
        }
        desired_keys = {_stable_entry_key(e) for e in access_entries}

        if existing_managed_keys == desired_keys:
            return

        # Preserve unmanaged entries (BigQuery defaults), replace managed with desired
        unmanaged = [
            e for e in existing_dataset.access_entries if not _is_managed_entry(e)
        ]
        final_entries = unmanaged + access_entries

        added = desired_keys - existing_managed_keys
        removed = existing_managed_keys - desired_keys

        if added:
            logger.debug(f"Access entries to add for {dataset_name}: {added}")
        if removed:
            logger.debug(f"Access entries to remove for {dataset_name}: {removed}")

        existing_dataset.access_entries = final_entries
        client.update_dataset(existing_dataset, ["access_entries"])

        parts = []
        if added:
            parts.append(f"added {len(added)}")
        if removed:
            parts.append(f"removed {len(removed)}")
        logger.info(
            f"Updated access controls for dataset {dataset_name} "
            f"({', '.join(parts)}, total: {len(final_entries)})"
        )

    @classmethod
    def prepare_for_execution(cls, pipeline_configs: list[Any]) -> None:
        """Pre-create all unique datasets needed by the pipelines.

        This prevents race conditions and duplicate work when running pipelines in parallel.
        Each dataset is created only once, even if multiple pipelines use it.

        Args:
            pipeline_configs: List of PipelineConfig objects that will be executed
        """
        from dlt_saga.utility.cli.context import get_execution_context

        context = get_execution_context()

        # Collect all unique (project, location, dataset_name, dataset_access) combinations
        datasets_to_create = set()

        for config in pipeline_configs:
            # Get values using same logic as BasePipeline
            project = context.get_database() or config.config_dict.get("gcp_project_id")
            location = context.get_location() or config.config_dict.get(
                "location", "EU"
            )
            dataset_name = config.schema_name
            dataset_access = config.config_dict.get("dataset_access")

            if project and dataset_name:
                datasets_to_create.add(
                    (
                        project,
                        location,
                        dataset_name,
                        tuple(dataset_access) if dataset_access else None,
                    )
                )

        # Create all unique datasets
        if datasets_to_create:
            from google.cloud import bigquery

            # Reuse a single client for all datasets to avoid repeated credential setup
            first = next(iter(datasets_to_create))
            client = bigquery.Client(project=first[0], location=first[1])

            for (
                project,
                location,
                dataset_name,
                dataset_access_tuple,
            ) in datasets_to_create:
                dataset_access = (
                    list(dataset_access_tuple) if dataset_access_tuple else None
                )

                # Use static method to sync dataset and access
                cls._sync_dataset_and_access_static(
                    project_id=project,
                    location=location,
                    dataset_name=dataset_name,
                    dataset_access=dataset_access,
                    client=client,
                )

    def _get_dlt_table_id(self, table_name: str) -> str:
        """Get fully qualified table ID for dlt metadata table.

        Args:
            table_name: Name of dlt table (e.g., '_dlt_pipeline_state', '_saga_load_info')

        Returns:
            Fully qualified table ID
        """
        return f"{self.config.project_id}.{self.config.dataset_name}.{table_name}"

    def _drop_table(self, table_id: str, description: str = "") -> None:
        """Drop a BigQuery table.

        Args:
            table_id: Fully qualified table ID
            description: Description for logging
        """
        from google.cloud import bigquery

        client = bigquery.Client(project=self.config.project_id)
        client.delete_table(table_id, not_found_ok=True)
        logger.debug(f"Dropped {description or 'table'}: {table_id}")

    def _delete_from_table(
        self, table_id: str, where_clause: str, parameters: list
    ) -> None:
        """Delete rows from a BigQuery table.

        Args:
            table_id: Fully qualified table ID
            where_clause: WHERE clause (without 'WHERE' keyword)
            parameters: List of query parameters
        """
        from google.cloud import bigquery

        client = bigquery.Client(project=self.config.project_id)
        delete_query = f"DELETE FROM `{table_id}` WHERE {where_clause}"

        job_config = bigquery.QueryJobConfig(query_parameters=parameters)
        client.query(delete_query, job_config=job_config).result()
        logger.debug(f"Deleted rows from {table_id}")

    def _cleanup_load_info_table(self, pipeline_name: str, table_name: str) -> None:
        """Delete load-info rows for a specific pipeline and table.

        Args:
            pipeline_name: Pipeline name
            table_name: Table name being reset
        """
        from google.cloud import bigquery

        from dlt_saga.project_config import get_load_info_table_name

        try:
            table_id = self._get_dlt_table_id(get_load_info_table_name())
            self._delete_from_table(
                table_id,
                "pipeline_name = @pipeline_name AND table_name = @table_name",
                [
                    bigquery.ScalarQueryParameter(
                        "pipeline_name", "STRING", pipeline_name
                    ),
                    bigquery.ScalarQueryParameter("table_name", "STRING", table_name),
                ],
            )
            logger.debug(f"Cleaned up load info for {pipeline_name}/{table_name}")
        except Exception as e:
            logger.debug(f"Could not clean load info: {e}")
