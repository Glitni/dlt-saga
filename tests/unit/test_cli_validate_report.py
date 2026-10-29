"""Unit tests for how ``saga validate`` groups findings in its output."""

import logging

import pytest

from dlt_saga.cli import _report_result_details
from dlt_saga.validate import ValidationResult

_SHARED = "shared warning"


def _records(caplog, results):
    with caplog.at_level(logging.WARNING, logger="dlt_saga.cli"):
        _report_result_details(results)
    return [(r.levelno, r.msg, r.args) for r in caplog.records]


@pytest.mark.unit
class TestReportResultDetails:
    def test_shared_warning_is_printed_once_with_its_configs(self, caplog):
        records = _records(
            caplog,
            [
                ValidationResult("a", warnings=[_SHARED]),
                ValidationResult("b", warnings=[_SHARED]),
            ],
        )
        assert records == [
            (logging.WARNING, "%s (%d pipelines):", (_SHARED, 2)),
            (logging.WARNING, "  - %s", ("a",)),
            (logging.WARNING, "  - %s", ("b",)),
        ]

    def test_single_finding_is_one_line(self, caplog):
        records = _records(caplog, [ValidationResult("a", warnings=["only"])])
        assert records == [(logging.WARNING, "%s: %s", ("a", "only"))]

    def test_unique_warnings_stay_with_their_config(self, caplog):
        records = _records(
            caplog,
            [
                ValidationResult("a", errors=["broken"], warnings=[_SHARED, "own"]),
                ValidationResult("b", warnings=[_SHARED]),
            ],
        )
        assert records[:3] == [
            (logging.ERROR, "%s:", ("a",)),
            (logging.ERROR, "  - %s", ("broken",)),
            (logging.WARNING, "  - %s", ("own",)),
        ]
        assert records[3] == (logging.WARNING, "%s (%d pipelines):", (_SHARED, 2))

    def test_project_results_are_not_grouped(self, caplog):
        records = _records(
            caplog,
            [
                ValidationResult(
                    "saga_project.yml", warnings=[_SHARED], kind="project"
                ),
                ValidationResult("a", warnings=[_SHARED]),
            ],
        )
        assert records == [
            (logging.WARNING, "%s: %s", ("saga_project.yml", _SHARED)),
            (logging.WARNING, "%s: %s", ("a", _SHARED)),
        ]
