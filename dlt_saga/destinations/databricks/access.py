"""Databricks Unity Catalog access manager.

Issues ``GRANT`` / ``REVOKE`` SQL statements on Unity Catalog objects to
manage table-level read access for users, groups, and service principals.

Access list format in ``saga_project.yml`` (same shape as BigQuery):
    ``- "user:analyst@company.com:SELECT"``
    ``- "group:data-readers:SELECT"``
    ``- "service_principal:my-sp-id:SELECT"``
"""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING, Dict, List, Optional, Set

from dlt_saga.destinations.base import AccessManager

if TYPE_CHECKING:
    from dlt_saga.destinations.databricks.destination import DatabricksDestination

logger = logging.getLogger(__name__)

_UNITY_PRIVILEGE = "SELECT"


class DatabricksAccessManager(AccessManager):
    """Manages Unity Catalog table access via ``GRANT`` / ``REVOKE`` SQL.

    Principal types:
        ``user``                — ``user:email@company.com``
        ``group``               — ``group:data-readers``
        ``service_principal``   — ``service_principal:app-id-or-name``
    """

    def __init__(self, destination: "DatabricksDestination") -> None:
        self._destination = destination

    # ------------------------------------------------------------------
    # AccessManager interface
    # ------------------------------------------------------------------

    def parse_access_list(self, access_list: List[str]) -> Dict[str, Set[str]]:
        """Parse config access list into ``{type_key: {principal, ...}}``.

        Expected format: ``type:principal`` — the optional ``:PRIVILEGE``
        suffix (e.g. ``:SELECT``) is accepted but the privilege is always
        normalised to ``SELECT``.

        Args:
            access_list: List of access strings from config.

        Returns:
            Dict with keys ``users``, ``groups``, ``service_principals``.
        """
        parsed: Dict[str, Set[str]] = {
            "users": set(),
            "groups": set(),
            "service_principals": set(),
        }

        for entry in access_list:
            parts = entry.split(":")
            if len(parts) < 2:
                logger.warning(
                    "Invalid Databricks access entry '%s'. "
                    "Expected: type:principal[:PRIVILEGE]",
                    entry,
                )
                continue

            principal_type = parts[0].strip().lower()
            principal = parts[1].strip()

            if principal_type == "user":
                parsed["users"].add(principal)
            elif principal_type == "group":
                parsed["groups"].add(principal)
            elif principal_type in ("service_principal", "serviceprincipal"):
                parsed["service_principals"].add(principal)
            else:
                logger.warning(
                    "Unknown principal type '%s' in access entry '%s'. "
                    "Supported: user, group, service_principal",
                    principal_type,
                    entry,
                )

        return parsed

    def manage_access_for_tables(
        self,
        table_ids: List[str],
        access_config: Optional[List[str]],
        revoke_extra: bool = True,
    ) -> None:
        """Manage Unity Catalog ``SELECT`` access for multiple tables.

        Args:
            table_ids: Fully-qualified table IDs (3-part: catalog.schema.table).
            access_config: List of access entries, or ``None`` to skip.
            revoke_extra: If ``True``, revoke any existing grants not in
                ``access_config``.  Requires querying
                ``system.information_schema.table_privileges``.
        """
        if access_config is None:
            logger.debug("No access_config — skipping Databricks access management")
            return

        desired = self.parse_access_list(access_config)

        all_principals: Dict[str, str] = {}  # principal → principal_type SQL keyword
        for principal in desired["users"]:
            all_principals[principal] = "user"
        for principal in desired["groups"]:
            all_principals[principal] = "group"
        for principal in desired["service_principals"]:
            all_principals[principal] = "service_principal"

        for table_id in table_ids:
            try:
                self._apply_table_access(
                    table_id, all_principals, revoke_extra=revoke_extra
                )
            except Exception as e:
                logger.error(
                    "Failed to manage access for Databricks table %s: %s", table_id, e
                )

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _apply_table_access(
        self,
        table_id: str,
        desired_principals: Dict[str, str],
        revoke_extra: bool,
    ) -> None:
        """Grant / revoke ``SELECT`` on a single table.

        Args:
            table_id: Fully-qualified table ID.
            desired_principals: ``{principal: type_keyword}`` map.
            revoke_extra: Whether to revoke principals not in desired set.
        """
        if revoke_extra:
            existing = self._get_existing_grants(table_id)
            to_revoke = existing - set(desired_principals)
            for principal in to_revoke:
                self._revoke(table_id, principal)

        for principal, ptype in desired_principals.items():
            self._grant(table_id, principal, ptype)

    def _grant(self, table_id: str, principal: str, ptype: str) -> None:
        sql = f"GRANT {_UNITY_PRIVILEGE} ON TABLE {table_id} TO {ptype} `{principal}`"
        logger.info(
            "Granting %s on %s to %s:%s", _UNITY_PRIVILEGE, table_id, ptype, principal
        )
        try:
            self._destination.execute_sql(sql)
        except Exception as e:
            logger.error(
                "Failed to grant %s on %s to %s: %s",
                _UNITY_PRIVILEGE,
                table_id,
                principal,
                e,
            )

    def _revoke(self, table_id: str, principal: str) -> None:
        sql = f"REVOKE {_UNITY_PRIVILEGE} ON TABLE {table_id} FROM `{principal}`"
        logger.info("Revoking %s on %s from %s", _UNITY_PRIVILEGE, table_id, principal)
        try:
            self._destination.execute_sql(sql)
        except Exception as e:
            logger.error(
                "Failed to revoke %s on %s from %s: %s",
                _UNITY_PRIVILEGE,
                table_id,
                principal,
                e,
            )

    def _get_existing_grants(self, table_id: str) -> Set[str]:
        """Query existing SELECT grants on a table via SHOW GRANTS."""
        try:
            rows = self._destination.execute_sql(f"SHOW GRANTS ON TABLE {table_id}")
            principals: Set[str] = set()
            for row in rows:
                # SHOW GRANTS columns: principal_type, principal, action_type, object_type, object_key
                try:
                    privilege = row[2] if len(row) > 2 else ""
                    principal = row[1] if len(row) > 1 else ""
                    if str(privilege).upper() == _UNITY_PRIVILEGE and principal:
                        principals.add(str(principal))
                except Exception:
                    continue
            return principals
        except Exception as e:
            logger.debug("Could not retrieve existing grants for %s: %s", table_id, e)
            return set()
