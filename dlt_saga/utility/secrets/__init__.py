"""Secret resolution from multiple backends.

Provides a unified interface for resolving secrets from:
- Google Cloud Secret Manager (googlesecretmanager::project::secret_name)
- Environment variables (env::VAR_NAME)
- Custom providers via SecretsProvider ABC

Secrets are referenced using URI-style syntax in pipeline configs.
"""

from dlt_saga.utility.secrets.providers import (
    EnvVarSecretsProvider,
    GcpSecretsProvider,
    SecretsProvider,
)
from dlt_saga.utility.secrets.resolver import SecretResolver, resolve_secret
from dlt_saga.utility.secrets.secret_str import SecretStr, coerce_secret

__all__ = [
    "SecretResolver",
    "resolve_secret",
    "SecretStr",
    "coerce_secret",
    "SecretsProvider",
    "GcpSecretsProvider",
    "EnvVarSecretsProvider",
]
