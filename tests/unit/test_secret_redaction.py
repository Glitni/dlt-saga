"""Tests for process-wide secret redaction (logs + persisted metadata)."""

import logging
import sys

import pytest

from dlt_saga.utility.secrets import redaction
from dlt_saga.utility.secrets.redaction import (
    REDACTION_MASK,
    SecretRedactingFilter,
    redact,
    redaction_values,
    register_secret,
)
from dlt_saga.utility.secrets.resolver import SecretResolver
from dlt_saga.utility.secrets.resolver import (
    _reset_for_testing as _reset_resolver,
)


@pytest.fixture(autouse=True)
def _clean_registry():
    """Reset the redaction registry (and resolver) between tests."""
    redaction._reset_for_testing()
    _reset_resolver()
    yield
    redaction._reset_for_testing()
    _reset_resolver()


@pytest.mark.unit
class TestRegisterAndRedact:
    """register_secret + redact core behavior."""

    def test_registered_value_is_masked(self):
        register_secret("super-secret-token")
        assert redact("auth=super-secret-token") == f"auth={REDACTION_MASK}"

    def test_masks_all_occurrences(self):
        register_secret("abc123")
        assert redact("abc123 and abc123") == f"{REDACTION_MASK} and {REDACTION_MASK}"

    def test_unregistered_value_untouched(self):
        register_secret("secret-one")
        assert redact("nothing sensitive here") == "nothing sensitive here"

    def test_empty_registry_is_noop(self):
        assert redact("anything at all") == "anything at all"

    def test_short_values_not_registered(self):
        register_secret("ab")
        register_secret("abc")
        assert redaction_values() == ()
        # A 3-char string must not be globally masked.
        assert redact("abc def") == "abc def"

    def test_min_length_boundary_registers(self):
        register_secret("abcd")  # exactly _MIN_REDACTION_LENGTH
        assert redact("x abcd y") == f"x {REDACTION_MASK} y"

    def test_non_string_ignored(self):
        register_secret(12345)  # type: ignore[arg-type]
        register_secret(None)  # type: ignore[arg-type]
        assert redaction_values() == ()

    def test_redact_non_string_returned_as_is(self):
        register_secret("secret-value")
        assert redact(None) is None  # type: ignore[arg-type]
        assert redact("") == ""

    def test_substring_secret_masked_after_superstring(self):
        # A secret that is a substring of another must not leave a partial
        # secret behind: longest-first ordering guarantees full masking.
        register_secret("tokenABCDEF")
        register_secret("token")
        assert redact("value=tokenABCDEF") == f"value={REDACTION_MASK}"


@pytest.mark.unit
class TestSecretRedactingFilter:
    """The logging filter masks secrets across message, args, and tracebacks."""

    def _record(self, msg, args=(), exc_info=None):
        return logging.LogRecord(
            name="dlt_saga.test",
            level=logging.INFO,
            pathname=__file__,
            lineno=1,
            msg=msg,
            args=args,
            exc_info=exc_info,
        )

    def test_masks_rendered_message(self):
        register_secret("path-secret")
        filt = SecretRedactingFilter()
        record = self._record("GET https://host/path-secret/data")
        assert filt.filter(record) is True
        assert record.getMessage() == "GET https://host/***/data"

    def test_masks_positional_args(self):
        register_secret("arg-secret")
        filt = SecretRedactingFilter()
        record = self._record("url=%s", args=("arg-secret",))
        assert filt.filter(record) is True
        assert record.getMessage() == "url=***"

    def test_no_secret_leaves_record_unchanged(self):
        filt = SecretRedactingFilter()
        record = self._record("plain message")
        assert filt.filter(record) is True
        assert record.getMessage() == "plain message"

    def test_masks_exception_traceback(self):
        register_secret("exc-secret")
        filt = SecretRedactingFilter()
        try:
            raise ValueError("API request failed: https://host/exc-secret/x")
        except ValueError:
            record = self._record("request failed", exc_info=sys.exc_info())

        assert filt.filter(record) is True
        formatted = logging.Formatter("%(message)s").format(record)
        assert "exc-secret" not in formatted
        assert REDACTION_MASK in formatted

    def test_idempotent_across_repeated_filtering(self):
        # Two handlers share a record; the second pass must not double-mangle.
        register_secret("dup-secret")
        filt = SecretRedactingFilter()
        record = self._record("token=dup-secret")
        filt.filter(record)
        first = record.getMessage()
        filt.filter(record)
        assert record.getMessage() == first == "token=***"


@pytest.mark.unit
class TestResolverRegistersSecrets:
    """SecretResolver.resolve registers provider-resolved values only."""

    def test_resolved_secret_is_registered(self):
        SecretResolver.register_provider(
            "faketest", _StaticProvider("resolved-secret-value")
        )
        result = SecretResolver.resolve("faketest::my-secret")
        assert result == "resolved-secret-value"
        assert redact("leak: resolved-secret-value") == f"leak: {REDACTION_MASK}"

    def test_plain_passthrough_value_not_registered(self):
        # A non-URI plain string is returned as-is and must never be registered
        # (it is not necessarily a secret).
        assert SecretResolver.resolve("just-a-plain-string") == "just-a-plain-string"
        assert redaction_values() == ()


@pytest.mark.unit
class TestSecretStrRegistersOnUnwrap:
    """SecretStr.get_secret_value registers the value for redaction.

    Covers the gap the resolver leaves: a secret held as a hand-wrapped
    SecretStr (never routed through a provider URI) must still be masked once
    it is unwrapped and composed into a URL/log line.
    """

    def test_get_secret_value_registers_for_redaction(self):
        from dlt_saga.utility.secrets.secret_str import SecretStr

        secret = SecretStr("wrapped-brand-key")
        # Not registered until unwrapped — masking a still-wrapped secret is the
        # job of __str__/__repr__.
        assert redaction_values() == ()

        assert secret.get_secret_value() == "wrapped-brand-key"
        assert (
            redact("GET https://host/wrapped-brand-key/stats")
            == f"GET https://host/{REDACTION_MASK}/stats"
        )

    def test_short_wrapped_value_not_registered(self):
        from dlt_saga.utility.secrets.secret_str import SecretStr

        assert SecretStr("ab").get_secret_value() == "ab"
        assert redaction_values() == ()


class _StaticProvider:
    """Minimal SecretsProvider returning a fixed value, for tests."""

    def __init__(self, value: str):
        self._value = value

    def get_secret(self, name, project_or_scope="", version="latest"):
        return self._value
