"""BigQuery table access control manager for automated access management."""

import logging
from typing import TYPE_CHECKING, Dict, List, Optional, Set

if TYPE_CHECKING:
    from google.iam.v1 import policy_pb2

from dlt_saga.destinations.base import AccessManager
from dlt_saga.utility.gcp.client_pool import bigquery_pool

logger = logging.getLogger(__name__)


class BigQueryAccessManager(AccessManager):
    """Manages BigQuery table access control based on configuration.

    Implements IAM-based access control for BigQuery tables, granting
    roles/bigquery.dataViewer to specified users, groups, and service accounts.
    """

    def __init__(
        self, project_id: Optional[str] = None, dataset_id: Optional[str] = None
    ):
        """Initialize the access manager.

        Args:
            project_id: GCP project ID containing the dataset
            dataset_id: BigQuery dataset ID
        """
        self.project_id = project_id
        self.dataset_id = dataset_id

    @property
    def client(self):
        """Get BigQuery client from connection pool (lazy property)."""
        return bigquery_pool.get_client(self.project_id)

    def parse_access_list(self, access_list: List[str]) -> Dict[str, Set[str]]:
        """Parse access list from config into structured format.

        Expected formats:
            - user:email@example.com
            - group:group@example.com
            - serviceAccount:sa@project.iam.gserviceaccount.com

        Args:
            access_list: List of access strings from config

        Returns:
            Dict with keys 'users', 'groups', 'serviceAccounts' containing sets of IAM member strings
        """
        parsed: Dict[str, Set[str]] = {
            "users": set(),
            "groups": set(),
            "serviceAccounts": set(),
        }

        for access_entry in access_list:
            if ":" not in access_entry:
                logger.warning(
                    f"Invalid access entry format: '{access_entry}'. "
                    "Expected format: 'type:email' (e.g., 'user:email@example.com')"
                )
                continue

            entity_type, entity_email = access_entry.split(":", 1)
            entity_type = entity_type.strip().lower()
            entity_email = entity_email.strip()

            # Convert to IAM member format
            if entity_type == "user":
                parsed["users"].add(f"user:{entity_email}")
            elif entity_type == "group":
                parsed["groups"].add(f"group:{entity_email}")
            elif entity_type in ["serviceaccount", "serviceAccount"]:
                parsed["serviceAccounts"].add(f"serviceAccount:{entity_email}")
            else:
                logger.warning(
                    f"Unknown entity type '{entity_type}' in access entry: '{access_entry}'. "
                    "Supported types: user, group, serviceAccount"
                )

        return parsed

    def get_current_table_iam_policy(
        self, table_id: str
    ) -> Optional["policy_pb2.Policy"]:
        """Get current IAM policy for a table.

        Args:
            table_id: Table ID (name only, not fully qualified)

        Returns:
            Current IAM policy or None if fetch fails
        """
        # BigQuery client expects standard SQL format: project.dataset.table
        table_ref = f"{self.project_id}.{self.dataset_id}.{table_id}"

        try:
            policy = self.client.get_iam_policy(table_ref)
            return policy
        except Exception as e:
            logger.warning(f"Could not fetch IAM policy for table {table_id}: {str(e)}")
            return None

    def _get_table_policy(self, table_ref: str, table_id: str):
        """Get IAM policy for a table."""
        try:
            return self.client.get_iam_policy(table_ref)
        except Exception as e:
            logger.error(f"Failed to fetch IAM policy for table {table_id}: {str(e)}")
            return None

    def _get_or_create_viewer_binding(self, policy, viewer_role: str):
        """Find or create the viewer role binding in policy."""
        for binding in policy.bindings:
            if binding["role"] == viewer_role:
                binding["members"] = set(binding["members"])
                return binding

        # Create new binding
        viewer_binding = {"role": viewer_role, "members": set()}
        policy.bindings.append(viewer_binding)
        return viewer_binding

    def _grant_missing_access(
        self, viewer_binding, missing_members: Set[str], viewer_role: str, table_id: str
    ):
        """Grant access to members missing from current policy."""
        if not missing_members:
            return

        for member in missing_members:
            logger.info(f"Granting {viewer_role} to {member} on table {table_id}")
            viewer_binding["members"].add(member)

    def _revoke_extra_access(
        self,
        viewer_binding,
        current_members: Set[str],
        all_desired_members: Set[str],
        viewer_role: str,
        table_id: str,
    ) -> Set[str]:
        """Revoke access from members not in desired list. Returns set of revoked members."""
        revoked_members = set()
        extra_members = current_members - all_desired_members

        logger.debug(
            f"Revocation check for {table_id}: current={current_members}, "
            f"desired={all_desired_members}, extra={extra_members}"
        )

        for member in extra_members:
            # Only revoke user/group/serviceAccount members, not special entries
            if member.startswith(("user:", "group:", "serviceAccount:")):
                logger.info(f"Revoking {viewer_role} from {member} on table {table_id}")
                viewer_binding["members"].discard(member)
                revoked_members.add(member)
            else:
                logger.debug(f"Skipping revocation of special member: {member}")

        return revoked_members

    def _apply_policy_update(
        self,
        table_ref: str,
        policy,
        missing_members: Set[str],
        revoked_members: Set[str],
        table_id: str,
    ):
        """Apply updated IAM policy if there were changes."""
        if not (missing_members or revoked_members):
            return

        logger.debug(
            f"Policy update decision for {table_id}: missing={len(missing_members)}, revoked={len(revoked_members)}"
        )

        try:
            self.client.set_iam_policy(table_ref, policy)
            changes = []
            if missing_members:
                changes.append(f"granted {len(missing_members)}")
            if revoked_members:
                changes.append(f"revoked {len(revoked_members)}")
            logger.info(
                f"Updated IAM policy for table {table_id}: {', '.join(changes)} access entries"
            )
        except Exception as e:
            logger.error(f"Failed to update IAM policy for table {table_id}: {str(e)}")

    def apply_table_access(
        self,
        table_id: str,
        desired_access: Dict[str, Set[str]],
        revoke_extra: bool = False,
    ) -> None:
        """Apply access control to a BigQuery table using table-level IAM.

        Args:
            table_id: Table ID to manage access for
            desired_access: Desired access state with IAM member strings (e.g., "user:email@domain.com")
            revoke_extra: If True, revoke access not in desired_access list
        """
        table_ref = f"{self.project_id}.{self.dataset_id}.{table_id}"
        viewer_role = "roles/bigquery.dataViewer"

        # Get current policy
        policy = self._get_table_policy(table_ref, table_id)
        if policy is None:
            return

        # Flatten desired access
        all_desired_members = (
            desired_access["users"]
            | desired_access["groups"]
            | desired_access["serviceAccounts"]
        )

        # Get or create viewer binding
        viewer_binding = self._get_or_create_viewer_binding(policy, viewer_role)
        current_members = viewer_binding["members"].copy()

        # Calculate changes
        missing_members = all_desired_members - current_members
        revoked_members = set()

        # Apply changes
        self._grant_missing_access(
            viewer_binding, missing_members, viewer_role, table_id
        )
        if revoke_extra:
            revoked_members = self._revoke_extra_access(
                viewer_binding,
                current_members,
                all_desired_members,
                viewer_role,
                table_id,
            )

        # Convert set back to list for API
        viewer_binding["members"] = list(viewer_binding["members"])

        # Apply policy update
        self._apply_policy_update(
            table_ref, policy, missing_members, revoked_members, table_id
        )

    def manage_access_for_tables(
        self,
        table_ids: List[str],
        access_config: Optional[List[str]],
        revoke_extra: bool = True,
    ) -> None:
        """Manage access for multiple tables based on config.

        Args:
            table_ids: List of table IDs to manage
            access_config: Access list from config file
                - None: Skip access management entirely
                - []: Empty list means revoke all managed access
                - [...]: List of members to grant access
            revoke_extra: If True, revoke access not in config (default: True)
        """
        # None means "don't manage access" - skip entirely
        if access_config is None:
            logger.debug("No access configuration provided, skipping access management")
            return

        if not isinstance(access_config, list):
            logger.warning(f"Access config must be a list, got {type(access_config)}")
            return

        # Empty list [] means "revoke all access" - this is intentional
        # Parse desired access (will be empty if access_config is [])
        desired_access = self.parse_access_list(access_config)

        # Apply access to each table
        # Even if desired access is empty, we still process to handle revocations
        for table_id in table_ids:
            try:
                self.apply_table_access(table_id, desired_access, revoke_extra)
            except Exception as e:
                logger.error(f"Failed to manage access for table {table_id}: {str(e)}")
