"""Unit tests for dlt_saga.ai_setup_command."""

from unittest.mock import patch

from dlt_saga.ai_setup_command import (
    AI_CONTEXT_FILENAME,
    _get_generated_hash,
    _render_template,
    _template_hash,
    check_staleness,
    run_ai_setup,
)

# ---------------------------------------------------------------------------
# _render_template
# ---------------------------------------------------------------------------


class TestRenderTemplate:
    def test_replaces_version_and_date(self):
        template = "# dlt-saga AI Context (v{version})\n<!-- Generated on {date} -->"
        result = _render_template(template, "1.2.3")
        assert "v1.2.3" in result
        assert "{version}" not in result
        assert "{date}" not in result

    def test_preserves_other_content(self):
        template = "## Architecture\nSome content here.\nVersion: {version}"
        result = _render_template(template, "0.1.0")
        assert "## Architecture" in result
        assert "Some content here." in result

    def test_embeds_template_hash(self):
        template = "Some template with {version} and {date}"
        result = _render_template(template, "0.1.0")
        assert "<!-- template-hash:" in result
        # Hash is of the raw template, not the rendered content
        expected_hash = _template_hash(template)
        assert expected_hash in result


# ---------------------------------------------------------------------------
# _get_generated_hash
# ---------------------------------------------------------------------------


class TestGetGeneratedHash:
    def test_returns_none_when_file_missing(self, tmp_path):
        assert _get_generated_hash(tmp_path) is None

    def test_extracts_hash(self, tmp_path):
        context_file = tmp_path / AI_CONTEXT_FILENAME
        context_file.write_text(
            "# Header\nContent\n<!-- template-hash: abc123def456 -->\n",
            encoding="utf-8",
        )
        assert _get_generated_hash(tmp_path) == "abc123def456"

    def test_returns_none_for_no_hash(self, tmp_path):
        context_file = tmp_path / AI_CONTEXT_FILENAME
        context_file.write_text("# Just content, no hash\n", encoding="utf-8")
        assert _get_generated_hash(tmp_path) is None

    def test_returns_none_for_empty_file(self, tmp_path):
        context_file = tmp_path / AI_CONTEXT_FILENAME
        context_file.write_text("", encoding="utf-8")
        assert _get_generated_hash(tmp_path) is None


# ---------------------------------------------------------------------------
# _template_hash
# ---------------------------------------------------------------------------


class TestTemplateHash:
    def test_deterministic(self):
        template = "some content"
        assert _template_hash(template) == _template_hash(template)

    def test_different_content_different_hash(self):
        assert _template_hash("content A") != _template_hash("content B")

    def test_returns_12_char_hex(self):
        result = _template_hash("anything")
        assert len(result) == 12
        assert all(c in "0123456789abcdef" for c in result)


# ---------------------------------------------------------------------------
# check_staleness
# ---------------------------------------------------------------------------


class TestCheckStaleness:
    def test_returns_none_when_no_file(self, tmp_path):
        assert check_staleness(tmp_path) is None

    def test_returns_none_when_hash_matches(self, tmp_path):
        """No warning when the template hasn't changed, even across versions."""
        # Generate with the current template
        run_ai_setup(project_dir=tmp_path)
        assert check_staleness(tmp_path) is None

    def test_returns_warning_when_template_changed(self, tmp_path):
        """Warns when the shipped template differs from the generated file."""
        context_file = tmp_path / AI_CONTEXT_FILENAME
        context_file.write_text(
            "# Old content\n<!-- template-hash: 000000000000 -->\n",
            encoding="utf-8",
        )
        warning = check_staleness(tmp_path)
        assert warning is not None
        assert "saga ai-setup" in warning

    def test_returns_none_when_no_hash_in_file(self, tmp_path):
        """Gracefully handles files without a hash (e.g. manually created)."""
        context_file = tmp_path / AI_CONTEXT_FILENAME
        context_file.write_text("# Manual content\n", encoding="utf-8")
        assert check_staleness(tmp_path) is None

    @patch(
        "dlt_saga.ai_setup_command._read_template",
        side_effect=FileNotFoundError("gone"),
    )
    def test_returns_none_when_template_unreadable(self, _mock, tmp_path):
        context_file = tmp_path / AI_CONTEXT_FILENAME
        context_file.write_text(
            "# Content\n<!-- template-hash: abc123def456 -->\n",
            encoding="utf-8",
        )
        assert check_staleness(tmp_path) is None


# ---------------------------------------------------------------------------
# run_ai_setup
# ---------------------------------------------------------------------------


class TestRunAiSetup:
    def test_generates_file(self, tmp_path):
        run_ai_setup(project_dir=tmp_path)
        output = tmp_path / AI_CONTEXT_FILENAME
        assert output.exists()
        content = output.read_text(encoding="utf-8")
        assert content.startswith("# dlt-saga AI Context (v")
        assert "## When to use this file" in content

    def test_overwrites_existing_file(self, tmp_path):
        output = tmp_path / AI_CONTEXT_FILENAME
        output.write_text("old content", encoding="utf-8")
        run_ai_setup(project_dir=tmp_path)
        content = output.read_text(encoding="utf-8")
        assert "old content" not in content
        assert content.startswith("# dlt-saga AI Context (v")

    def test_embeds_hash(self, tmp_path):
        run_ai_setup(project_dir=tmp_path)
        generated_hash = _get_generated_hash(tmp_path)
        assert generated_hash is not None
        assert len(generated_hash) == 12

    def test_no_staleness_after_generation(self, tmp_path):
        """Freshly generated file should not be stale."""
        run_ai_setup(project_dir=tmp_path)
        assert check_staleness(tmp_path) is None
