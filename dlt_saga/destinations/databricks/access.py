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
        # Populated by _prime_current_grants: {table_name: {grantee, ...}} for
        # every table in the schema, read in a single information_schema scan.
        self._grant_cache: Optional[Dict[str, Set[str]]] = None

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

    def _prime_current_grants(self, table_ids: List[str]) -> None:
        """Batch-read every table's direct SELECT grants in one query.

        Reads ``<catalog>.information_schema.table_privileges`` scoped to the
        whole schema (``table_schema = dataset_id``, ``inherited_from = 'NONE'``)
        and caches ``{table_name: {grantee, ...}}``. A batch ``update-access``
        over many pipelines in the same schema then does one metadata query per
        schema instead of one per table.

        On any failure — or when the schema is unknown — the cache is left
        empty, so every table reads as "no current grants" (grant the desired
        set, revoke nothing).
        """
        self._grant_cache = {}
        if not self.dataset_id:
            return
        try:
            catalog = self._destination.quote_identifier(
                self._destination.config.catalog
            )
            safe_schema = self._destination.escape_string_literal(self.dataset_id)
            rows = self._destination.execute_sql(
                f"SELECT table_name, grantee FROM {catalog}.information_schema.table_privileges "
                f"WHERE table_schema = '{safe_schema}' "
                f"AND privilege_type = '{_UNITY_PRIVILEGE}' "
                f"AND inherited_from = 'NONE'"
            )
            for row in rows:
                if len(row) < 2 or not row[0] or not row[1]:
                    continue
                self._grant_cache.setdefault(str(row[0]), set()).add(str(row[1]))
        except Exception as e:
            logger.debug(
                "Could not batch-read grants for schema %s: %s", self.dataset_id, e
            )
            self._grant_cache = {}

    def current_principals(self, table_id: str) -> Set[str]:
        """Return principals with a direct table-level SELECT grant.

        Reads from the per-schema cache primed by :meth:`_prime_current_grants`.
        """
        cache = self._grant_cache or {}
        return set(cache.get(table_id, set()))

    def apply_access_changes(
        self, table_id: str, to_grant: Set[str], to_revoke: Set[str]
    ) -> None:
        """Issue per-principal ``GRANT`` / ``REVOKE`` SQL for the delta."""
        full_id = self._full_table_id(table_id)
        for principal in sorted(to_grant):
            self._grant(full_id, principal)
        for principal in sorted(to_revoke):
            self._revoke(full_id, principal)

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

    def _grant(self, table_id: str, principal: str) -> None:
        # Unity Catalog GRANT resolves the principal by name; it takes no
        # principal-type keyword (unlike some other SQL dialects). Emitting
        # `TO user \`p\`` is a parse error — UC wants `TO \`p\`` (matching REVOKE).
        sql = f"GRANT {_UNITY_PRIVILEGE} ON TABLE {table_id} TO `{principal}`"
        self._destination.execute_sql(sql)

    def _revoke(self, table_id: str, principal: str) -> None:
        sql = f"REVOKE {_UNITY_PRIVILEGE} ON TABLE {table_id} FROM `{principal}`"
        self._destination.execute_sql(sql)
