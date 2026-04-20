"""Unit tests for environment variable helpers."""

import os
from unittest.mock import patch

import pytest

from dlt_saga.utility.env import get_env


@pytest.mark.unit
class TestGetEnv:
    def test_returns_value_when_set(self):
        with patch.dict(os.environ, {"SAGA_ENVIRONMENT": "prod"}, clear=True):
            assert get_env("SAGA_ENVIRONMENT") == "prod"

    def test_returns_default_when_unset(self):
        with patch.dict(os.environ, {}, clear=True):
            assert get_env("SAGA_ENVIRONMENT", "dev") == "dev"

    def test_returns_none_when_unset_no_default(self):
        with patch.dict(os.environ, {}, clear=True):
            assert get_env("SAGA_ENVIRONMENT") is None
