"""Unit tests for the labels on datasets saga creates
(utility/gcp/dataset_labels.py) and the three places a dataset is created."""

from unittest.mock import MagicMock

import pytest
from google.cloud.exceptions import NotFound

from dlt_saga.destinations.bigquery.base import BigQueryBaseDestination
from dlt_saga.destinations.bigquery.destination import BigQueryDestination
from dlt_saga.utility.cli.context import execution_context_scope
from dlt_saga.utility.cli.profiles import ProfileTarget
from dlt_saga.utility.gcp.dataset_labels import (
    BUILT_IN_LABEL_COUNT,
    MAX_LABELS_PER_DATASET,
    dataset_labels,
    validate_dataset_labels,
)


def _target(**destination_config):
    return ProfileTarget(
        name="prod",
        environment="prod",
        destination_type="bigquery",
        destination_config={"database": "proj", **destination_config},
    )


@pytest.mark.unit
class TestValidateDatasetLabels:
    def test_none_is_empty(self):
        assert validate_dataset_labels(None, "here") == {}

    def test_cleans_values_and_drops_empty(self):
        labels = validate_dataset_labels({"team": "Data Platform", "unset": ""}, "here")
        assert labels == {"team": "data-platform"}

    def test_saga_prefix_is_reserved(self):
        with pytest.raises(ValueError, match="dataset label key .* is reserved"):
            validate_dataset_labels({"saga-pipeline": "v"}, "here")

    def test_app_may_be_declared(self):
        assert validate_dataset_labels({"app": "Billing"}, "here") == {"app": "billing"}

    def test_invalid_key_rejected(self):
        with pytest.raises(ValueError, match="is invalid"):
            validate_dataset_labels({"Bad": "v"}, "here")

    def test_non_mapping_names_the_key(self):
        with pytest.raises(ValueError, match="dataset_labels must be a mapping"):
            validate_dataset_labels(["a=b"], "here")

    def test_too_many_rejected(self):
        fits = MAX_LABELS_PER_DATASET - BUILT_IN_LABEL_COUNT
        validate_dataset_labels({f"k{i}": "v" for i in range(fits)}, "here")
        with pytest.raises(ValueError, match="per dataset"):
            validate_dataset_labels({f"k{i}": "v" for i in range(fits + 1)}, "here")


@pytest.mark.unit
class TestDatasetLabels:
    def test_app_label_without_a_target(self):
        assert dataset_labels() == {"app": "dlt-saga"}

    def test_app_label_without_a_declaration(self):
        with execution_context_scope(_target()):
            assert dataset_labels() == {"app": "dlt-saga"}

    def test_declared_labels_beside_app(self):
        with execution_context_scope(_target(dataset_labels={"team": "Data"})):
            assert dataset_labels() == {"team": "data", "app": "dlt-saga"}

    def test_job_labels_are_not_dataset_labels(self):
        with execution_context_scope(_target(job_labels={"team": "data"})):
            assert dataset_labels() == {"app": "dlt-saga"}

    def test_declared_app_replaces_the_default(self):
        with execution_context_scope(_target(dataset_labels={"app": "billing"})):
            assert dataset_labels() == {"app": "billing"}

    def test_unusable_declaration_is_left_out_not_raised(self, caplog):
        with execution_context_scope(_target(dataset_labels={"saga-x": "other"})):
            with caplog.at_level("WARNING"):
                assert dataset_labels() == {"app": "dlt-saga"}
        assert "reserved" in caplog.text


@pytest.mark.unit
class TestCreatedDatasetsAreLabelled:
    def test_sync_creates_a_missing_dataset_with_labels(self):
        client = MagicMock()
        client.get_dataset.side_effect = NotFound("missing")
        with execution_context_scope(_target(dataset_labels={"team": "data"})):
            BigQueryBaseDestination._sync_dataset_and_access_static(
                project_id="proj", location="EU", dataset_name="ds", client=client
            )
        created = client.create_dataset.call_args[0][0]
        assert created.labels == {"team": "data", "app": "dlt-saga"}

    def test_sync_leaves_an_existing_dataset_alone(self):
        client = MagicMock()
        with execution_context_scope(_target(dataset_labels={"team": "data"})):
            BigQueryBaseDestination._sync_dataset_and_access_static(
                project_id="proj", location="EU", dataset_name="ds", client=client
            )
        client.create_dataset.assert_not_called()
        client.update_dataset.assert_not_called()

    def _destination(self, client):
        dest = MagicMock()
        dest.config.project_id = "proj"
        dest.config.location = "EU"
        dest._client.return_value = client
        return dest

    def test_ensure_schema_exists_labels_what_it_creates(self):
        client = MagicMock()
        client.get_dataset.side_effect = NotFound("missing")
        with execution_context_scope(_target()):
            BigQueryDestination.ensure_schema_exists(self._destination(client), "ds")
        created = client.create_dataset.call_args[0][0]
        assert created.labels == {"app": "dlt-saga"}

    def test_ensure_schema_exists_leaves_an_existing_dataset_alone(self):
        client = MagicMock()
        with execution_context_scope(_target()):
            BigQueryDestination.ensure_schema_exists(self._destination(client), "ds")
        client.create_dataset.assert_not_called()
        client.update_dataset.assert_not_called()

    def test_omni_staging_dataset_is_labelled(self):
        client = MagicMock()
        with execution_context_scope(_target()):
            BigQueryDestination._ensure_dataset(
                self._destination(client), "ds_omni_staging", "aws-eu-west-1"
            )
        created = client.create_dataset.call_args[0][0]
        assert created.labels == {"app": "dlt-saga"}
