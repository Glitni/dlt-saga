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
        # Set by the pipeline (mirrors the BigQuery access manager). Access
        # management passes bare table names; the manager qualifies them to a
        # 3-part Unity Catalog name using this dataset (schema).
        self.dataset_id: Optional[str] = None

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
            table_ids: Bare table names (same contract as the BigQuery access
                manager). Each is qualified to a 3-part Unity Catalog name via
                ``dataset_id`` and the destination catalog before use.
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

    def _full_table_id(self, table: str) -> str:
        """Qualify a bare table name to ``catalog.schema.table``.

        Unity Catalog GRANT/REVOKE/SHOW GRANTS must target a fully-qualified
        name; a bare name resolves against the connection's default schema
        (saga's SQL connection sets none), which points GRANTs at the wrong —
        usually non-existent — table. Falls back to the input unchanged when no
        ``dataset_id`` was provided.
        """
        if self.dataset_id:
            return self._destination.get_full_table_id(self.dataset_id, table)
        return table

    def _apply_table_access(
        self,
        table_id: str,
        desired_principals: Dict[str, str],
        revoke_extra: bool,
    ) -> None:
        """Grant / revoke ``SELECT`` on a single table.

        Args:
            table_id: Bare table name; qualified to a 3-part name internally.
            desired_principals: ``{principal: type_keyword}`` map.
            revoke_extra: Whether to revoke principals not in desired set.
        """
        full_id = self._full_table_id(table_id)

        # Diff against current grants so the counters reflect real changes (a
        # re-grant of an already-granted principal is not a change) — mirrors the
        # BigQuery access manager, which grants/revokes only the delta.
        existing = self._get_existing_grants(self.dataset_id, table_id)

        for principal in sorted(set(desired_principals) - existing):
            self._grant(full_id, principal, desired_principals[principal])

        if revoke_extra:
            for principal in sorted(existing - set(desired_principals)):
                self._revoke(full_id, principal)

    def _grant(self, table_id: str, principal: str, ptype: str) -> None:
        # Unity Catalog GRANT resolves the principal by name; it takes no
        # principal-type keyword (unlike some other SQL dialects). Emitting
        # `TO user \`p\`` is a parse error — UC wants `TO \`p\`` (matching REVOKE).
        from dlt_saga.utility.cli.context import get_execution_context

        context = get_execution_context()
        sql = f"GRANT {_UNITY_PRIVILEGE} ON TABLE {table_id} TO `{principal}`"
        logger.info(
            "Granting %s on %s to %s:%s", _UNITY_PRIVILEGE, table_id, ptype, principal
        )
        try:
            # Honour --dry-run: skip the write, but still bump the counter so the
            # run summary reports what *would* be applied (matches BigQuery).
            if not context.dry_run:
                self._destination.execute_sql(sql)
            context.access_grants_applied += 1
        except Exception as e:
            logger.error(
                "Failed to grant %s on %s to %s: %s",
                _UNITY_PRIVILEGE,
                table_id,
                principal,
                e,
            )

    def _revoke(self, table_id: str, principal: str) -> None:
        from dlt_saga.utility.cli.context import get_execution_context

        context = get_execution_context()
        sql = f"REVOKE {_UNITY_PRIVILEGE} ON TABLE {table_id} FROM `{principal}`"
        logger.info("Revoking %s on %s from %s", _UNITY_PRIVILEGE, table_id, principal)
        try:
            if not context.dry_run:
                self._destination.execute_sql(sql)
            context.access_revokes_applied += 1
        except Exception as e:
            logger.error(
                "Failed to revoke %s on %s from %s: %s",
                _UNITY_PRIVILEGE,
                table_id,
                principal,
                e,
            )

    def _get_existing_grants(self, dataset: Optional[str], table: str) -> Set[str]:
        """Return principals with a *direct* table-level SELECT grant.

        Reads ``<catalog>.information_schema.table_privileges`` filtered to
        ``inherited_from = 'NONE'`` — the explicit signal for a grant made
        directly on the table. Grants inherited from the parent catalog or
        schema (``inherited_from`` CATALOG / SCHEMA) are excluded: table-level
        access management manages only direct table grants, otherwise
        ``revoke_extra`` would try to revoke an inherited grant on every run.

        Returns an empty set when the schema is unknown (no ``dataset_id``),
        which makes the caller grant the desired set and revoke nothing.
        """
        if not dataset:
            return set()
        try:
            catalog = self._destination.quote_identifier(
                self._destination.config.catalog
            )
            safe_schema = self._destination.escape_string_literal(dataset)
            safe_table = self._destination.escape_string_literal(table)
            rows = self._destination.execute_sql(
                f"SELECT grantee FROM {catalog}.information_schema.table_privileges "
                f"WHERE table_schema = '{safe_schema}' "
                f"AND table_name = '{safe_table}' "
                f"AND privilege_type = '{_UNITY_PRIVILEGE}' "
                f"AND inherited_from = 'NONE'"
            )
            return {str(row[0]) for row in rows if len(row) > 0 and row[0]}
        except Exception as e:
            logger.debug(
                "Could not retrieve existing grants for %s.%s: %s", dataset, table, e
            )
            return set()
