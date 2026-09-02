"""Concurrent-import safety for packages whose submodules are public.

Importing two submodules of the same package from two threads must not
deadlock. CPython takes the lock for ``pkg.sub`` *before* it imports ``pkg``
(``importlib._bootstrap._find_and_load`` acquires the lock for the full name,
then ``_find_and_load_unlocked`` imports the parent), so a package whose
``__init__`` eagerly imports its own submodules closes a lock cycle:

    thread A   holds lock(pkg.a)                 waits for lock(pkg)
    thread B   holds lock(pkg)  [running init]   waits for lock(pkg.a)

CPython's own detector then raises ``_DeadlockError`` in one thread and leaves
half-initialised modules behind, so an unrelated ``from ... import Name``
elsewhere in the process fails afterwards with a confusing ``ImportError``.

The tests below run in a fresh interpreter so ``sys.modules`` is cold, and they
assert the invariant -- no deadlock, both modules fully initialised -- rather
than any particular import order, so they stay valid however the cycle is
broken.
"""

from __future__ import annotations

import ast
import json
import pathlib
import subprocess
import sys
import textwrap

import pytest

pytestmark = pytest.mark.unit

# Seconds the child gets before we call it hung. A healthy run is a few
# seconds; a deadlocked one either raises _DeadlockError or never finishes.
CHILD_TIMEOUT = 120.0

# The child interleaves the two imports deterministically rather than hoping a
# free-running race lands in a window microseconds wide:
#
#   1. thread A starts alone and imports LEFT, so it is certain to be the
#      thread that wins the contested package's lock and runs its ``__init__``;
#   2. a meta-path finder returns the contested package's real spec with its
#      loader wrapped, and the wrapper pauses in ``create_module`` -- the one
#      point where A already holds lock(pkg) and lock(pkg.left) but has not yet
#      published ``pkg`` in ``sys.modules``, so B must still queue for
#      lock(pkg). (The pause cannot go in ``find_spec``: that runs under the
#      global import lock, which would stall B instead of letting it block.)
#   3. thread B, released there, takes lock(pkg.right) and blocks on lock(pkg),
#      and A's ``__init__`` then walks into lock(pkg.right).
#
# The hook is the package's own load, which happens whatever the ``__init__``
# contains, so a fix cannot hide from the handshake: it simply means A stops
# asking for lock(pkg.right) while B holds it.
CHILD = textwrap.dedent(
    """
    import importlib
    import json
    import sys
    import threading
    import time
    import traceback
    from importlib.machinery import PathFinder

    LEFT, RIGHT, HOOK, WARM = sys.argv[1], sys.argv[2], sys.argv[3], sys.argv[4:]

    # Encourage the interpreter to switch threads at the smallest opportunity.
    sys.setswitchinterval(1e-6)

    # Warm the shared ancestors, so the only cold package is the contested one.
    for name in WARM:
        importlib.import_module(name)

    released = threading.Event()
    HANDOFF = 1.0  # seconds the second thread gets to reach its own import


    class _PausingLoader:
        \"\"\"Delegates to the real loader, pausing before the module is published.\"\"\"

        def __init__(self, inner):
            self._inner = inner

        def create_module(self, spec):
            released.set()
            time.sleep(HANDOFF)
            return self._inner.create_module(spec)

        def exec_module(self, module):
            return self._inner.exec_module(module)

        def __getattr__(self, item):
            return getattr(self._inner, item)


    class _PauseOnHook:
        \"\"\"Meta-path finder that only intervenes for the contested package.\"\"\"

        used = False

        @classmethod
        def find_spec(cls, fullname, path=None, target=None):
            if fullname != HOOK or cls.used:
                return None
            spec = PathFinder.find_spec(fullname, path, target)
            if spec is None or spec.loader is None:
                return None
            cls.used = True
            spec.loader = _PausingLoader(spec.loader)
            return spec


    sys.meta_path.insert(0, _PauseOnHook)

    errors = {}


    def _import(name, wait):
        try:
            if wait:
                # Falls through on timeout, so a fix that never loads the
                # contested package on this path cannot hang the test.
                released.wait(timeout=30.0)
            importlib.import_module(name)
        except BaseException:  # noqa: BLE001 - the point is to report anything
            errors[name] = traceback.format_exc()


    threads = [
        threading.Thread(target=_import, args=(LEFT, False), name="left"),
        threading.Thread(target=_import, args=(RIGHT, True), name="right"),
    ]
    for t in threads:
        t.start()
    for t in threads:
        t.join(timeout=60.0)

    result = {
        "errors": errors,
        "alive": [t.name for t in threads if t.is_alive()],
        "hook_fired": _PauseOnHook.used,
        "in_sys_modules": {n: (n in sys.modules) for n in (LEFT, RIGHT)},
    }

    # Fully initialised, not merely present: a module left half-executed by a
    # failed import can still answer ``in sys.modules``, and the symptom users
    # actually see is a name that has gone missing.
    unusable = {}
    for name in (LEFT, RIGHT):
        try:
            mod = importlib.import_module(name)
        except BaseException:  # noqa: BLE001
            unusable[name] = traceback.format_exc()
            continue
        if getattr(mod.__spec__, "_initializing", False):
            unusable[name] = "module is still initialising"
    result["unusable"] = unusable

    # The collateral damage from the production incident: a package left
    # half-imported makes an unrelated ``from ... import Name`` fail later.
    try:
        from dlt_saga.pipelines.base_pipeline import BasePipeline  # noqa: F401
    except BaseException:  # noqa: BLE001
        result["base_pipeline"] = traceback.format_exc()

    print("RESULT " + json.dumps(result))
    """
)

# (left, right, hook, warm) -- hook is the contested package both sides reach.
PAIRS = [
    pytest.param(
        "dlt_saga.utility.secrets.secret_str",
        "dlt_saga.utility.secrets.redaction",
        "dlt_saga.utility.secrets",
        ["dlt_saga.utility"],
        id="secrets-submodules",
    ),
    pytest.param(
        # The pair from the production incident: two pipeline config modules
        # that reach into different submodules of dlt_saga.utility.secrets.
        # Pipeline modules are resolved on a thread pool (see
        # dlt_saga.pipelines.registry), so this is a first-use race.
        "dlt_saga.pipelines.api.config",
        "dlt_saga.pipelines.target.config",
        "dlt_saga.utility.secrets",
        ["dlt_saga"],
        id="pipeline-configs",
    ),
    pytest.param(
        # Same shape, different package: dlt_saga.pipeline_config's __init__
        # eagerly imports submodules that other modules import directly too.
        "dlt_saga.pipeline_config.base_config",
        "dlt_saga.pipeline_config.naming",
        "dlt_saga.pipeline_config",
        ["dlt_saga"],
        id="pipeline-config-submodules",
    ),
    pytest.param(
        # dlt_saga.session imports both of these directly, and the
        # get_hook_registry() call sits in _execute_single_ingest, i.e. on a
        # worker thread.
        "dlt_saga.hooks.loader",
        "dlt_saga.hooks.registry",
        "dlt_saga.hooks",
        ["dlt_saga"],
        id="hooks-submodules",
    ),
]


@pytest.mark.parametrize("left,right,hook,warm", PAIRS)
def test_concurrent_submodule_import_does_not_deadlock(left, right, hook, warm):
    """Two threads importing sibling submodules must both finish."""
    try:
        proc = subprocess.run(
            [sys.executable, "-c", CHILD, left, right, hook, *warm],
            capture_output=True,
            text=True,
            timeout=CHILD_TIMEOUT,
        )
    except subprocess.TimeoutExpired:
        pytest.fail(
            f"importing {left} and {right} concurrently hung for "
            f"{CHILD_TIMEOUT}s -- module lock cycle"
        )

    line = next(
        (ln for ln in proc.stdout.splitlines() if ln.startswith("RESULT ")), None
    )
    assert line, (
        f"child produced no result\nstdout:\n{proc.stdout}\nstderr:\n{proc.stderr}"
    )
    result = json.loads(line[len("RESULT ") :])

    assert result["hook_fired"], (
        f"{hook} was never loaded during the race -- the test no longer "
        "exercises the window it was written for"
    )
    assert not result["alive"], f"threads still running: {result['alive']}"
    assert not result["errors"], "concurrent import raised:\n" + "\n".join(
        f"--- {name} ---\n{tb}" for name, tb in result["errors"].items()
    )
    assert all(result["in_sys_modules"].values()), (
        f"module missing from sys.modules after import: {result['in_sys_modules']}"
    )
    assert not result["unusable"], "module not fully initialised:\n" + "\n".join(
        f"--- {name} ---\n{tb}" for name, tb in result["unusable"].items()
    )
    assert "base_pipeline" not in result, (
        "unrelated import broke after the race:\n" + result.get("base_pipeline", "")
    )


def _dlt_saga_root() -> pathlib.Path:
    import dlt_saga

    return pathlib.Path(dlt_saga.__file__).parent


def _imported_dlt_saga_modules(nodes, module: str, is_init: bool) -> set[str]:
    """Absolute ``dlt_saga`` modules named by the import statements in *nodes*."""
    # A relative import resolves against the enclosing package: the package
    # itself inside an ``__init__``, the parent otherwise, one level up per
    # extra dot. Unresolved, ``from .loader import x`` is invisible here.
    package = module if is_init else module.rsplit(".", 1)[0]
    parts = package.split(".")
    names: set[str] = set()
    for node in nodes:
        if isinstance(node, ast.ImportFrom):
            up = node.level - 1 if node.level else 0
            if up >= len(parts):
                continue
            base = ".".join(parts[: len(parts) - up]) if node.level else ""
            if node.module:
                names.add(f"{base}.{node.module}" if base else node.module)
            elif base:
                names.add(base)
        elif isinstance(node, ast.Import):
            names.update(alias.name for alias in node.names)
    return {n for n in names if n == "dlt_saga" or n.startswith("dlt_saga.")}


def _module_name(root: pathlib.Path, path: pathlib.Path) -> tuple[str, bool]:
    parts = list(path.relative_to(root).with_suffix("").parts)
    is_init = parts[-1] == "__init__"
    if is_init:
        parts.pop()
    return ".".join(["dlt_saga", *parts]), is_init


def _scan_imports(root: pathlib.Path):
    """Return (submodules each package __init__ imports eagerly, importers)."""
    init_submodules: dict[str, set[str]] = {}
    importers: dict[str, set[str]] = {}
    for path in sorted(root.rglob("*.py")):
        module, is_init = _module_name(root, path)
        tree = ast.parse(path.read_text(encoding="utf-8"))
        if is_init:
            # Module scope only: ``if TYPE_CHECKING:`` and function-level
            # imports inside an __init__ never run at package-import time.
            init_submodules[module] = {
                name
                for name in _imported_dlt_saga_modules(tree.body, module, is_init)
                if name.startswith(f"{module}.")
            }
        # Any scope for the other side: a function-level import races too.
        for name in _imported_dlt_saga_modules(ast.walk(tree), module, is_init):
            importers.setdefault(name, set()).add(module)
    return init_submodules, importers


def test_no_package_init_eagerly_imports_a_directly_imported_submodule():
    """Guard the *class* of fault, not just the packages that hit it.

    A package ``__init__`` that imports its own submodule at module scope is
    only safe while nothing else imports that submodule directly: the moment
    both happen, two threads can take the two locks in opposite orders. This
    walks the source rather than the import graph so a new re-export cannot
    quietly reintroduce the cycle.
    """
    init_submodules, importers = _scan_imports(_dlt_saga_root())

    hazards = []
    for package, submodules in sorted(init_submodules.items()):
        for submodule in sorted(submodules):
            outsiders = sorted(
                m
                for m in importers.get(submodule, set())
                if m != package and not m.startswith(f"{package}.")
            )
            if outsiders:
                hazards.append(
                    f"{package}/__init__.py imports {submodule} eagerly, and "
                    f"{', '.join(outsiders)} import(s) it directly"
                )

    assert not hazards, (
        "module-lock cycle risk -- make the package re-export lazy "
        "(module-level __getattr__, PEP 562):\n  " + "\n  ".join(hazards)
    )


def test_secrets_package_exposes_its_public_api():
    """``__all__`` stays importable from the package, however it is wired."""
    import dlt_saga.utility.secrets as secrets

    for name in secrets.__all__:
        assert getattr(secrets, name) is not None, f"{name} missing from package"

    with pytest.raises(AttributeError):
        secrets.definitely_not_a_secret_helper

    # Package-level and submodule paths must still give the same objects.
    from dlt_saga.utility.secrets import SecretStr, redact
    from dlt_saga.utility.secrets.redaction import redact as redact_direct
    from dlt_saga.utility.secrets.secret_str import SecretStr as SecretStrDirect

    assert SecretStr is SecretStrDirect
    assert redact is redact_direct

    # ``from pkg import submodule`` (no attribute of that name) still resolves.
    from dlt_saga.utility.secrets import redaction, secret_str

    assert secret_str.SecretStr is SecretStr
    assert redaction.redact is redact


@pytest.mark.parametrize(
    "package,attributes",
    [
        (
            "dlt_saga.utility.secrets",
            ["providers", "redaction", "resolver", "secret_str"],
        ),
        ("dlt_saga.pipeline_config", ["base_config", "file_config", "naming"]),
        ("dlt_saga.destinations", ["base", "factory"]),
        ("dlt_saga.destinations.bigquery", ["access", "config", "destination"]),
        ("dlt_saga.destinations.duckdb", ["config", "destination"]),
        ("dlt_saga.hooks", ["loader", "registry"]),
    ],
)
def test_submodule_attributes_survive_in_a_cold_interpreter(package, attributes):
    """The submodules the eager ``__init__`` published stay reachable by name.

    Importing a submodule sets it as an attribute of its package, so with an
    eager ``__init__`` ``pkg.sub`` always worked. Code in the wild relies on
    that, so a lazy package has to keep answering -- checked in a subprocess
    because another test importing the submodule would hide a regression.
    """
    script = (
        "import importlib, sys\n"
        f"pkg = importlib.import_module({package!r})\n"
        f"for name in {attributes!r}:\n"
        "    assert getattr(pkg, name) is sys.modules[f'{pkg.__name__}.{name}'], name\n"
        "    assert name in dir(pkg), name\n"
        "print('OK')\n"
    )
    proc = subprocess.run(
        [sys.executable, "-c", script], capture_output=True, text=True, timeout=120
    )
    assert proc.returncode == 0 and "OK" in proc.stdout, (
        f"stdout:\n{proc.stdout}\nstderr:\n{proc.stderr}"
    )


def test_destination_factory_registers_builtins_when_reached_via_the_package():
    """Lazy re-export must not defer the factory's registration side effect."""
    from dlt_saga.destinations import DestinationFactory

    for builtin in ("bigquery", "duckdb"):
        DestinationFactory._check_registered(builtin)
