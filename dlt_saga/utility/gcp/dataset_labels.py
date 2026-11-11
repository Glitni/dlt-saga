"""BigQuery labels on the datasets saga creates.

Storage is billed per dataset, and the billing export breaks it down by the
dataset's labels — the storage-side counterpart of the job labels in
:mod:`dlt_saga.utility.gcp.job_labels`. A dataset saga creates carries:

* ``app``: ``dlt-saga``, unless the target declares its own
* the profile target's ``dataset_labels``, as declared

Labels are set when saga creates a dataset and never afterwards: a dataset
that already exists keeps the labels it has.
"""

from __future__ import annotations

import logging
from typing import Any, Dict

from dlt_saga.utility.gcp.job_labels import validate_labels
from dlt_saga.utility.job_labels import APP_NAME

logger = logging.getLogger(__name__)

# BigQuery allows 64 labels per dataset; saga's default `app` takes one.
MAX_LABELS_PER_DATASET = 64
BUILT_IN_LABEL_COUNT = 1


def validate_dataset_labels(labels: Any, where: str) -> Dict[str, str]:
    """Check a user-declared ``dataset_labels`` mapping and return it cleaned.

    Same key and value rules as ``job_labels``
    (:func:`dlt_saga.utility.gcp.job_labels.validate_job_labels`), except that
    ``app`` may be declared: on a dataset it is a default, not a fact about
    who issued something.

    Args:
        labels: The raw ``dataset_labels`` value (``None`` for none).
        where: Where it was declared, for the error message.

    Raises:
        ValueError: On a non-mapping value, an invalid or reserved key, a
            non-scalar value, or more labels than a dataset can carry.
    """
    return validate_labels(
        labels,
        where,
        scope="dataset",
        limit=MAX_LABELS_PER_DATASET,
        built_in=BUILT_IN_LABEL_COUNT,
        reserved_keys=frozenset(),
    )


def dataset_labels() -> Dict[str, str]:
    """The labels for a dataset saga is about to create.

    ``app`` with the active profile target's ``dataset_labels`` over it, so a
    project with its own meaning for ``app`` can declare it. A declaration
    BigQuery can't take is left out with a warning rather than raised: the
    destination config and ``saga validate`` are where it is an error, and a
    dataset must still be created.
    """
    from dlt_saga.utility.cli.context import get_execution_context

    labels: Dict[str, str] = {"app": APP_NAME}
    target = get_execution_context().profile_target
    declared = target.destination_config.get("dataset_labels") if target else None
    if declared:
        try:
            labels.update(
                validate_dataset_labels(
                    declared, f"profile target '{target.name}': dataset_labels"
                )
            )
        except ValueError as exc:
            logger.warning("Creating the dataset without its declared labels: %s", exc)
    return labels
