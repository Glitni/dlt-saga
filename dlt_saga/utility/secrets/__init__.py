"""Secret resolution from multiple backends.

Provides a unified interface for resolving secrets from:
- Google Cloud Secret Manager (googlesecretmanager::project::secret_name)
- Azure Key Vault (azurekeyvault::vault-url::secret_name)
- Environment variables (env_secret::VAR_NAME)
- Custom providers via SecretsProvider ABC

Secrets are referenced using URI-style syntax in pipeline configs.

The re-exports are resolved on first attribute access (PEP 562), not at
package-import time. See ``_EXPORTS`` below for why that matters.
"""

from typing import TYPE_CHECKING

from dlt_saga.utility.lazy import lazy_exports

if TYPE_CHECKING:  # pragma: no cover - for type checkers only, never at runtime
    from dlt_saga.utility.secrets.providers import (
        EnvVarSecretsProvider,
        GcpSecretsProvider,
        SecretsProvider,
    )
    from dlt_saga.utility.secrets.redaction import (
        SecretRedactingFilter,
        redact,
        register_secret,
    )
    from dlt_saga.utility.secrets.resolver import SecretResolver, resolve_secret
    from dlt_saga.utility.secrets.secret_str import SecretStr, coerce_secret

# Public name -> submodule that defines it.
#
# Importing these submodules from the package ``__init__`` would deadlock.
# CPython takes the lock for ``pkg.sub`` *before* it imports ``pkg``
# (``importlib._bootstrap._find_and_load`` locks the full name, then
# ``_find_and_load_unlocked`` imports the parent), so with an eager ``__init__``
# two threads can close a cycle:
#
#     thread A   holds lock(pkg.secret_str)      waits for lock(pkg)
#     thread B   holds lock(pkg) [running init]  waits for lock(pkg.secret_str)
#
# and modules all over dlt_saga import these submodules directly
# (``dlt_saga.pipelines.api.config`` takes ``secret_str``,
# ``dlt_saga.pipelines.target.config`` takes ``redaction``). Pipeline modules
# are resolved on a thread pool, so the two sides really do race on first use.
# Resolving lazily means the package lock is never held while waiting for a
# submodule lock, which removes the cycle instead of reordering it.
_EXPORTS = {
    "EnvVarSecretsProvider": "providers",
    "GcpSecretsProvider": "providers",
    "SecretsProvider": "providers",
    "SecretRedactingFilter": "redaction",
    "redact": "redaction",
    "register_secret": "redaction",
    "SecretResolver": "resolver",
    "resolve_secret": "resolver",
    "SecretStr": "secret_str",
    "coerce_secret": "secret_str",
}

__all__ = [
    "SecretResolver",
    "resolve_secret",
    "SecretStr",
    "coerce_secret",
    "SecretsProvider",
    "GcpSecretsProvider",
    "EnvVarSecretsProvider",
    "SecretRedactingFilter",
    "redact",
    "register_secret",
]

__getattr__, __dir__ = lazy_exports(__name__, _EXPORTS, globals())
