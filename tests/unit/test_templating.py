"""Unit tests for the shared Jinja2 config templating helper."""

import pytest

from dlt_saga.utility.templating import render_template_str, render_templates


@pytest.mark.unit
class TestRenderTemplateStr:
    def test_env_var(self, monkeypatch):
        monkeypatch.setenv("MY_VAR", "hello")
        assert render_template_str("{{ env_var('MY_VAR') }}") == "hello"

    def test_env_var_with_default_used_when_set(self, monkeypatch):
        monkeypatch.setenv("MY_VAR", "set")
        assert render_template_str("{{ env_var('MY_VAR', 'fallback') }}") == "set"

    def test_env_var_default_when_unset(self, monkeypatch):
        monkeypatch.delenv("MISSING_VAR", raising=False)
        assert (
            render_template_str("{{ env_var('MISSING_VAR', 'fallback') }}")
            == "fallback"
        )

    def test_missing_var_no_default_is_empty(self, monkeypatch):
        monkeypatch.delenv("MISSING_VAR", raising=False)
        assert render_template_str("{{ env_var('MISSING_VAR') }}") == ""

    def test_filter_replace(self, monkeypatch):
        monkeypatch.setenv("GCP_DATASET", "my-proj-dev")
        result = render_template_str("{{ env_var('GCP_DATASET') | replace('-', '_') }}")
        assert result == "my_proj_dev"

    def test_filter_chain(self, monkeypatch):
        monkeypatch.setenv("NAME", "My-Dataset")
        result = render_template_str(
            "{{ env_var('NAME') | replace('-', '_') | lower }}"
        )
        assert result == "my_dataset"

    def test_multiple_env_vars_in_one_string(self, monkeypatch):
        monkeypatch.setenv("A", "foo")
        monkeypatch.setenv("B", "bar")
        assert render_template_str("{{ env_var('A') }}-{{ env_var('B') }}") == "foo-bar"

    def test_non_string_passthrough(self):
        assert render_template_str(42) == 42  # type: ignore[arg-type]
        assert render_template_str(None) is None  # type: ignore[arg-type]
        assert render_template_str(True) is True  # type: ignore[arg-type]

    def test_plain_string_unchanged(self):
        assert render_template_str("just a value") == "just a value"
        # Secret URIs are plain strings — must pass through untouched.
        assert (
            render_template_str("googlesecretmanager::proj::secret")
            == "googlesecretmanager::proj::secret"
        )

    def test_raw_block_escapes_templating(self):
        assert (
            render_template_str("{% raw %}{{ literal }}{% endraw %}") == "{{ literal }}"
        )

    def test_malformed_template_raises_valueerror(self):
        with pytest.raises(ValueError, match="Failed to render template"):
            render_template_str("{{ env_var('X' }}")  # missing close paren


@pytest.mark.unit
class TestDateTimeGlobals:
    """Stdlib datetime/timedelta/timezone exposed for rolling, dynamic values."""

    def test_relative_date_expression(self):
        from datetime import datetime, timedelta, timezone

        expected = (datetime.now(timezone.utc) - timedelta(days=7)).strftime("%Y-%m-%d")
        result = render_template_str(
            "{{ (datetime.now(timezone.utc) - timedelta(days=7)).strftime('%Y-%m-%d') }}"
        )
        assert result == expected

    def test_today_iso(self):
        from datetime import datetime, timezone

        expected = datetime.now(timezone.utc).strftime("%Y-%m-%d")
        assert (
            render_template_str("{{ datetime.now(timezone.utc).strftime('%Y-%m-%d') }}")
            == expected
        )

    def test_custom_format(self):
        from datetime import datetime, timezone

        expected = datetime.now(timezone.utc).strftime("%Y%m%d")
        assert (
            render_template_str("{{ datetime.now(timezone.utc).strftime('%Y%m%d') }}")
            == expected
        )


@pytest.mark.unit
class TestRenderTemplates:
    def test_recurses_dicts_and_lists(self, monkeypatch):
        monkeypatch.setenv("DS", "data")
        obj = {
            "dataset": "{{ env_var('DS') }}",
            "nested": {"tags": ["{{ env_var('DS') }}", "static"]},
            "count": 3,
        }
        result = render_templates(obj)
        assert result == {
            "dataset": "data",
            "nested": {"tags": ["data", "static"]},
            "count": 3,
        }

    def test_keys_are_not_rendered(self, monkeypatch):
        # Merge markers like "+key" must survive untouched (keys not rendered).
        monkeypatch.setenv("V", "x")
        result = render_templates({"+tags": "{{ env_var('V') }}"})
        assert result == {"+tags": "x"}

    def test_non_container_passthrough(self):
        assert render_templates(5) == 5
        assert render_templates(None) is None
