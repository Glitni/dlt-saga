"""Turn a ``credentials`` config value into Google service account sources."""

import json
from typing import Any, Dict


def service_account_credential_args(credentials: Any) -> Dict[str, Any]:
    """``get_credentials`` arguments for a ``credentials`` config value.

    Secret URIs are resolved wherever they appear (the whole value, or values
    inside a JSON object), so whether credentials are secret is decided by the
    value, not the field. A string that resolves to JSON is service account
    content; any other string is a key file path. ``None`` yields no
    arguments, leaving the pool's other sources (secret ID, ADC) to apply.
    """
    from dlt_saga.utility.secrets.resolver import SecretResolver

    if isinstance(credentials, dict):
        return {"credentials_dict": SecretResolver.resolve_dict(credentials)}
    if isinstance(credentials, str):
        resolved = SecretResolver.resolve(credentials)
        if resolved.lstrip().startswith("{"):
            return {"credentials_dict": json.loads(resolved)}
        return {"credentials_path": resolved}
    return {}
