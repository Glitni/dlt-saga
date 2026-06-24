"""Unit tests for dlt_saga.lint."""

from dataclasses import dataclass, field

import pytest

from dlt_saga.lint import (
    AdapterTarget,
    check_field_name_near_miss,
    check_hardcoded_window,
    check_secret_field_names,
    is_project_owned,
    run_lint,
    standard_field_names,
)

pytestmark = pytest.mark.unit


@dataclass
class _BadNamingConfig:
    api_secret: str = ""  # named by secrecy
    auth_plaintext: str = ""  # named by secrecy
    primary_keys: list = field(default_factory=list)  # near-miss of primary_key
    base_url: str = ""  # legitimately new field
    partition_on: str = ""  # legit (shares prefix with partition_column)


@dataclass
class _CleanConfig:
    base_url: str = ""
    api_token: str = ""
    partition_on: str = ""
    partition_num: int = 0


def _target(config_class=None, pipeline_file=None) -> AdapterTarget:
    return AdapterTarget(
        adapter="local.api.test",
        source="local",
        config_class=config_class,
        pipeline_file=pipeline_file,
    )


# ---------------------------------------------------------------------------
# Standard vocabulary
# ---------------------------------------------------------------------------


def test_standard_field_names_includes_known_fields():
    names = standard_field_names()
    assert {
        "primary_key",
        "write_disposition",
        "partition_column",
        "incremental",
    } <= names


# ---------------------------------------------------------------------------
# secret-naming
# ---------------------------------------------------------------------------


class TestSecretNaming:
    def test_flags_secret_suffixes(self):
        findings = list(check_secret_field_names(_target(_BadNamingConfig)))
        flagged = {f.location for f in findings}
        assert flagged == {"api_secret", "auth_plaintext"}
        assert all(f.code == "secret-naming" for f in findings)

    def test_clean_config_has_no_secret_findings(self):
        assert list(check_secret_field_names(_target(_CleanConfig))) == []

    def test_no_config_class_is_safe(self):
        assert list(check_secret_field_names(_target(None))) == []


# ---------------------------------------------------------------------------
# field-name near miss
# ---------------------------------------------------------------------------


class TestFieldNameNearMiss:
    def test_flags_near_miss_of_standard_field(self):
        findings = list(check_field_name_near_miss(_target(_BadNamingConfig)))
        flagged = {f.location for f in findings}
        assert "primary_keys" in flagged
        # genuinely-new and prefix-sharing fields are NOT flagged
        assert "base_url" not in flagged
        assert "partition_on" not in flagged

    def test_prefix_sharing_fields_not_flagged(self):
        # Regression guard for the 0.88 cutoff: these are legit, distinct fields.
        findings = list(check_field_name_near_miss(_target(_CleanConfig)))
        assert findings == []


# ---------------------------------------------------------------------------
# hardcoded-window (AST)
# ---------------------------------------------------------------------------

_NON_IDEMPOTENT = """
from datetime import datetime, timedelta

class P:
    def extract_data(self):
        start = datetime.now() - timedelta(days=1)
        return self.client.fetch(start)
"""

_IDEMPOTENT = """
from datetime import datetime, timedelta

class P:
    def extract_data(self):
        start = self.destination.get_max_column_value(t, c) or self.config.initial_value
        fallback = datetime.now() - timedelta(days=1)
        return self.client.fetch(start)
"""


class TestHardcodedWindow:
    def test_flags_now_minus_timedelta_without_watermark(self, tmp_path):
        pf = tmp_path / "pipeline.py"
        pf.write_text(_NON_IDEMPOTENT, encoding="utf-8")
        findings = list(check_hardcoded_window(_target(pipeline_file=pf)))
        assert len(findings) == 1
        assert findings[0].code == "hardcoded-window"
        assert str(pf) in findings[0].location

    def test_not_flagged_when_watermark_present(self, tmp_path):
        pf = tmp_path / "pipeline.py"
        pf.write_text(_IDEMPOTENT, encoding="utf-8")
        assert list(check_hardcoded_window(_target(pipeline_file=pf))) == []

    def test_missing_file_is_safe(self, tmp_path):
        pf = tmp_path / "does_not_exist.py"
        assert list(check_hardcoded_window(_target(pipeline_file=pf))) == []


# ---------------------------------------------------------------------------
# run_lint aggregation
# ---------------------------------------------------------------------------


def test_run_lint_aggregates_over_targets(tmp_path):
    pf = tmp_path / "pipeline.py"
    pf.write_text(_NON_IDEMPOTENT, encoding="utf-8")
    targets = [_target(_BadNamingConfig, pf), _target(_CleanConfig)]
    findings = run_lint(targets=targets)
    codes = {f.code for f in findings}
    assert {"secret-naming", "field-name", "hardcoded-window"} <= codes


def test_run_lint_clean_target_is_empty():
    assert run_lint(targets=[_target(_CleanConfig)]) == []


# ---------------------------------------------------------------------------
# is_project_owned (ownership by location, not registration)
# ---------------------------------------------------------------------------


class TestIsProjectOwned:
    def test_file_inside_project_is_owned(self, tmp_path):
        pf = tmp_path / "pipelines" / "api" / "foo" / "pipeline.py"
        pf.parent.mkdir(parents=True)
        pf.write_text("", encoding="utf-8")
        assert is_project_owned(pf, tmp_path) is True

    def test_file_in_site_packages_not_owned(self, tmp_path):
        # Even under the project root, a venv's site-packages is installed code.
        pf = tmp_path / ".venv" / "lib" / "site-packages" / "thirdparty" / "pipeline.py"
        pf.parent.mkdir(parents=True)
        pf.write_text("", encoding="utf-8")
        assert is_project_owned(pf, tmp_path) is False

    def test_file_outside_project_not_owned(self, tmp_path):
        outside = tmp_path / "elsewhere" / "pipeline.py"
        outside.parent.mkdir(parents=True)
        outside.write_text("", encoding="utf-8")
        project_root = tmp_path / "project"
        project_root.mkdir()
        assert is_project_owned(outside, project_root) is False

    def test_none_is_not_owned(self, tmp_path):
        assert is_project_owned(None, tmp_path) is False
