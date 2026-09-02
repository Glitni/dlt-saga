"""Lazy package re-exports (PEP 562).

See ``dlt_saga.utility.secrets`` for the module-lock cycle these avoid. This
module imports nothing from ``dlt_saga`` and ``dlt_saga.utility`` is
docstring-only, so a package ``__init__`` can reach it without taking on a
lock-ordering constraint of its own.
"""

from collections.abc import Callable
from importlib import import_module
from typing import Any


def lazy_exports(
    package: str,
    exports: dict[str, str],
    namespace: dict[str, Any],
) -> tuple[Callable[[str], Any], Callable[[], list[str]]]:
    """Build the ``__getattr__``/``__dir__`` pair for *package*.

    *exports* maps each public name to the submodule that defines it, and
    *namespace* is the package's own ``globals()``, where resolved names are
    cached. The submodule names resolve as well, since an eager ``__init__``
    published them as package attributes when it imported them.
    """
    submodules = frozenset(exports.values())

    def __getattr__(name: str) -> Any:
        if name in submodules:
            value: Any = import_module(f"{package}.{name}")
        else:
            submodule = exports.get(name)
            if submodule is None:
                raise AttributeError(f"module {package!r} has no attribute {name!r}")
            value = getattr(import_module(f"{package}.{submodule}"), name)
        namespace[name] = value  # __getattr__ is only consulted on a miss
        return value

    def __dir__() -> list[str]:
        return sorted(set(namespace) | set(exports) | submodules)

    return __getattr__, __dir__
