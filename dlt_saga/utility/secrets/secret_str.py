"""Lightweight wrapper type that prevents credential leakage in logs, repr, and tracebacks."""

from typing import Optional, Union


class SecretStr:
    """A string wrapper that masks its value in repr, str, and f-string output.

    Use for credential fields (passwords, API keys, tokens) in config dataclasses.
    The actual value is only accessible via ``get_secret_value()``.

    Example::

        secret = SecretStr("my-api-key")
        print(secret)           # ******
        print(repr(secret))     # SecretStr('******')
        print(f"key={secret}")  # key=******
        secret.get_secret_value()  # "my-api-key"
    """

    __slots__ = ("_value",)

    def __init__(self, value: str) -> None:
        if not isinstance(value, str):
            raise TypeError(f"SecretStr expects str, got {type(value).__name__}")
        self._value = value

    def get_secret_value(self) -> str:
        """Return the actual secret value. Explicit call required."""
        return self._value

    def __repr__(self) -> str:
        return "SecretStr('******')"

    def __str__(self) -> str:
        return "******"

    def __eq__(self, other: object) -> bool:
        if isinstance(other, SecretStr):
            return self._value == other._value
        return NotImplemented

    def __hash__(self) -> int:
        return hash(self._value)

    def __bool__(self) -> bool:
        return bool(self._value)


def coerce_secret(value: Union[str, "SecretStr", None]) -> Optional[SecretStr]:
    """Coerce a value to SecretStr for use in dataclass ``__post_init__``.

    Args:
        value: Plain string (from YAML), existing SecretStr, or None.

    Returns:
        SecretStr wrapping the value, or None if value is None.

    Raises:
        TypeError: If value is not str, SecretStr, or None.
    """
    if value is None:
        return None
    if isinstance(value, SecretStr):
        return value
    if isinstance(value, str):
        return SecretStr(value)
    raise TypeError(f"Expected str, SecretStr, or None, got {type(value).__name__}")
