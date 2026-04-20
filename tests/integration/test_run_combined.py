"""Integration tests for combined ingest+historize flow."""

from tests.integration.conftest import (
    SNAPSHOT_1,
    SNAPSHOT_2,
    SNAPSHOT_3,
    query_historized,
    run_historize,
)


class TestHistorizeOnly:
    """Test historize-only flow (write_disposition: 'historize')."""

    def test_historize_only_pipeline(self, duckdb_destination):
        """Pipeline with write_disposition='historize' reads from source table."""
        result = run_historize(
            duckdb_destination,
            [SNAPSHOT_1, SNAPSHOT_2, SNAPSHOT_3],
            write_disposition="historize",
        )

        assert result["status"] == "completed"
        assert len(query_historized(duckdb_destination)) == 6
