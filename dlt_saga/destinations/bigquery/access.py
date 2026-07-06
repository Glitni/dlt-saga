"""BigQuery table access control manager for automated access management."""

import logging
from typing import Dict, List, Optional, Set

from dlt_saga.destinations.base import AccessManager
from dlt_saga.utility.gcp.client_pool import bigquery_pool

logger = logging.getLogger(__name__)


class BigQueryAccessManager(AccessManager):
    """Manages BigQuery table access control based on configuration.

    Implements IAM-based access control for BigQuery tables, granting
    roles/bigquery.dataViewer to specified users, groups, and service accounts.
    """

    VIEWER_ROLE = "roles/bigquery.dataViewer"
    # Only members with these IAM prefixes are managed; special entries
    # (owners, domains, project-level roles) are never revoked.
    _MANAGED_PREFIXES = ("user:", "group:", "serviceAccount:")

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
        # current_principals() fetches the table's IAM policy; apply_access_changes()
        # reuses the same policy object so a table needs one read + one write.
        self._policy_cache: Dict[str, tuple] = {}

    @property
    def client(self):
        """Get BigQuery client from connection pool (lazy property)."""
        return bigquery_pool.get_client(self.project_id)

    def _table_ref(self, table_id: str) -> str:
        """Standard-SQL table reference expected by the IAM policy API."""
        return f"{self.project_id}.{self.dataset_id}.{table_id}"

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

    def current_principals(self, table_id: str) -> Set[str]:
        """Return the IAM members holding the viewer role on the table.

        Fetches the table's IAM policy and caches it (plus the viewer binding)
        so :meth:`apply_access_changes` can mutate and write the same object —
        one read + one write per changed table. Lets fetch errors propagate so
        the base's per-table handler skips the table cleanly.
        """
        table_ref = self._table_ref(table_id)
        policy = self.client.get_iam_policy(table_ref)
        viewer_binding = self._get_or_create_viewer_binding(policy, self.VIEWER_ROLE)
        self._policy_cache[table_id] = (table_ref, policy, viewer_binding)
        return set(viewer_binding["members"])

    def _revocable(self, current: Set[str]) -> Set[str]:
        """Only user/group/serviceAccount members are managed and revocable.

        Special IAM entries (dataset owners, domains, project-level roles) are
        left untouched — table-level access management must never remove them.
        """
        return {m for m in current if m.startswith(self._MANAGED_PREFIXES)}

    def apply_access_changes(
        self, table_id: str, to_grant: Set[str], to_revoke: Set[str]
    ) -> None:
        """Apply grants and revokes in a single ``set_iam_policy`` write.

        Reuses the policy object read by :meth:`current_principals`, so both
        directions land in one atomic update.
        """
        entry = self._policy_cache.get(table_id)
        if entry is None:
            # current_principals wasn't called (or failed) — nothing to write.
            return
        table_ref, policy, viewer_binding = entry
        members = set(viewer_binding["members"])
        members |= to_grant
        members -= to_revoke
        viewer_binding["members"] = list(members)
        self.client.set_iam_policy(table_ref, policy)
