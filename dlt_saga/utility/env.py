"""Environment variable helpers."""

import os
from typing import Optional


def get_env(name: str, default: Optional[str] = None) -> Optional[str]:
    """Look up a SAGA_ environment variable.

    Args:
        name: The SAGA_* variable name.
        default: Value returned when the variable is not set.

    Returns:
        The value, or *default*.
    """
    return os.environ.get(name, default)
