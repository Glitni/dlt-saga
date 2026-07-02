"""Unit tests for saga classification encoding into destination descriptions."""

import pytest

from dlt_saga.utility.column_docs import (
    compose_description,
    render_saga_block,
    strip_saga_block,
)


@pytest.mark.unit
class TestRenderSagaBlock:
    def test_empty_meta_renders_nothing(self):
        assert render_saga_block(None) == ""
        assert render_saga_block({}) == ""
        assert render_saga_block({"classification": []}) == ""

    def test_values_rendered_sorted(self):
        assert (
            render_saga_block({"classification": ["pii", "contact"]})
            == "[saga:classification=contact,pii]"
        )

    def test_keys_rendered_sorted(self):
        block = render_saga_block({"classification": ["pii"], "owner": "sales"})
        assert block == "[saga:classification=pii;owner=sales]"

    def test_scalar_value(self):
        assert render_saga_block({"owner": "sales"}) == "[saga:owner=sales]"

    def test_ordering_is_deterministic_regardless_of_input_order(self):
        a = render_saga_block({"classification": ["b", "a"]})
        b = render_saga_block({"classification": ["a", "b"]})
        assert a == b == "[saga:classification=a,b]"


@pytest.mark.unit
class TestStripSagaBlock:
    def test_none_and_empty(self):
        assert strip_saga_block(None) == ""
        assert strip_saga_block("") == ""

    def test_plain_description_untouched(self):
        assert strip_saga_block("Just a description") == "Just a description"

    def test_block_removed(self):
        composed = "Contact email [saga:classification=contact,pii]"
        assert strip_saga_block(composed) == "Contact email"

    def test_legacy_newline_block_removed(self):
        """strip must also recover human text from an older newline-separated block."""
        composed = "Contact email\n\n[saga:classification=pii]"
        assert strip_saga_block(composed) == "Contact email"

    def test_block_only(self):
        assert strip_saga_block("[saga:classification=pii]") == ""


@pytest.mark.unit
class TestComposeDescription:
    def test_human_and_classification(self):
        assert (
            compose_description("Contact email", {"classification": ["pii", "contact"]})
            == "Contact email [saga:classification=contact,pii]"
        )

    def test_composed_description_is_single_line(self):
        """No newline — some destinations write descriptions into unescaped SQL."""
        composed = compose_description("Contact email", {"classification": ["pii"]})
        assert "\n" not in composed

    def test_classification_only(self):
        assert (
            compose_description(None, {"classification": ["pii"]})
            == "[saga:classification=pii]"
        )

    def test_human_only(self):
        assert compose_description("Contact email", None) == "Contact email"

    def test_no_content(self):
        assert compose_description(None, None) == ""

    def test_idempotent_over_existing_block(self):
        """Composing a value that already carries a block replaces it, not appends."""
        once = compose_description("Contact email", {"classification": ["pii"]})
        twice = compose_description(once, {"classification": ["pii"]})
        assert once == twice == "Contact email [saga:classification=pii]"

    def test_recompose_with_changed_values_replaces_block(self):
        once = compose_description("Contact email", {"classification": ["pii"]})
        updated = compose_description(once, {"classification": ["contact"]})
        assert updated == "Contact email [saga:classification=contact]"
