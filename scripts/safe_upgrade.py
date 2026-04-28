#!/usr/bin/env python3
"""
Safe dependency upgrade — skips brand-new versions that haven't had time to be vetted.

Usage:
    uv run python scripts/safe_upgrade.py
    uv run python scripts/safe_upgrade.py --min-age 14
    uv run python scripts/safe_upgrade.py --actions-only
    uv run python scripts/safe_upgrade.py --deps-only
"""

import argparse
import json
import os
import re
import subprocess
import sys
import tomllib
import urllib.error
import urllib.request
from datetime import datetime, timezone
from pathlib import Path

MIN_PYTHON = (3, 11)
if sys.version_info < MIN_PYTHON:
    sys.exit(f"Python {'.'.join(str(n) for n in MIN_PYTHON)}+ required")

DEFAULT_MIN_AGE_DAYS = 7
SECURITY_KEYWORDS = {
    "security",
    "malicious",
    "vulnerability",
    "cve",
    "exploit",
    "critical",
}


def _gh_headers(accept: str) -> dict[str, str]:
    headers = {"Accept": accept, "User-Agent": "safe-upgrade/1.0"}
    token = os.environ.get("GITHUB_TOKEN") or os.environ.get("GH_TOKEN")
    if token:
        headers["Authorization"] = f"Bearer {token}"
    return headers


def fetch_json(url: str, accept: str = "application/json") -> dict | None:
    is_github = "api.github.com" in url
    headers = (
        _gh_headers(accept)
        if is_github
        else {"Accept": accept, "User-Agent": "safe-upgrade/1.0"}
    )
    try:
        req = urllib.request.Request(url, headers=headers)
        with urllib.request.urlopen(req, timeout=10) as resp:
            return json.loads(resp.read())
    except urllib.error.HTTPError as e:
        if e.code in (403, 429) and is_github:
            token_hint = (
                ""
                if (os.environ.get("GITHUB_TOKEN") or os.environ.get("GH_TOKEN"))
                else " (set GITHUB_TOKEN to raise the limit)"
            )
            print(f"  GitHub rate limit hit{token_hint}", file=sys.stderr)
        return None
    except Exception:
        return None


def age_days(date_str: str) -> float:
    dt = datetime.fromisoformat(date_str.replace("Z", "+00:00"))
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return (datetime.now(timezone.utc) - dt).total_seconds() / 86400


def has_security_keywords(text: str) -> bool:
    return any(kw in text.lower() for kw in SECURITY_KEYWORDS)


def safety_check(age: float, release_notes: str, min_age: int) -> tuple[bool, str]:
    if age >= min_age:
        return True, "ok"
    if has_security_keywords(release_notes):
        return True, "security fix"
    return False, f"too new ({age:.0f}d old, min {min_age}d)"


# ── GitHub Actions ────────────────────────────────────────────────────────────

USES_RE = re.compile(
    r"^(\s*-?\s*uses:\s*)([a-zA-Z0-9_.-]+/[a-zA-Z0-9_.-]+)@([^\s#]+)(.*)"
)
SHA_RE = re.compile(r"^[0-9a-f]{40}$")
GH_ACCEPT = "application/vnd.github.v3+json"


def resolve_action(repo: str, ref: str) -> tuple[str, float, str] | None:
    """Returns (sha, age_days, release_notes) or None on failure."""
    data = fetch_json(
        f"https://api.github.com/repos/{repo}/commits/{ref}", accept=GH_ACCEPT
    )
    if not data:
        return None
    sha = data["sha"]
    age = age_days(data["commit"]["committer"]["date"])

    notes = ""
    rel = fetch_json(
        f"https://api.github.com/repos/{repo}/releases/tags/{ref}", accept=GH_ACCEPT
    )
    if rel and rel.get("body"):
        notes = rel["body"]
    else:
        rel = fetch_json(
            f"https://api.github.com/repos/{repo}/releases/latest", accept=GH_ACCEPT
        )
        if rel and rel.get("body"):
            notes = rel["body"]

    return sha, age, notes


def _parse_uses_line(line: str) -> tuple[str, str, str | None, str] | None:
    """Parse a `uses:` line into (repo, ref, current_sha, prefix). Returns None if not a uses line."""
    m = USES_RE.match(line)
    if not m:
        return None
    prefix, repo, current_ref, rest = m.groups()
    comment_m = re.search(r"#\s*(.+)", rest)
    ref = comment_m.group(1).strip() if comment_m else current_ref
    current_sha = current_ref if SHA_RE.match(current_ref) else None
    return repo, ref, current_sha, prefix


def _collect_unique_refs(wf_files: list[Path]) -> set[tuple[str, str]]:
    """Return the set of unique (repo, ref) pairs across all workflow files."""
    refs: set[tuple[str, str]] = set()
    for wf_file in wf_files:
        for line in wf_file.read_text(encoding="utf-8").splitlines():
            parsed = _parse_uses_line(line)
            if parsed:
                repo, ref, _, _ = parsed
                refs.add((repo, ref))
    return refs


def _rewrite_workflow_file(
    wf_file: Path,
    resolved: dict[tuple[str, str], tuple[str, float, str] | None],
    min_age: int,
    col: str,
) -> None:
    lines = wf_file.read_text(encoding="utf-8").splitlines()
    new_lines = []
    changed = False

    for line in lines:
        parsed = _parse_uses_line(line)
        if not parsed:
            new_lines.append(line)
            continue

        repo, ref, current_sha, prefix = parsed
        old_label = current_sha[:8] if current_sha else "unpinned"
        result = resolved.get((repo, ref))

        if result is None:
            print(col.format(repo, ref, old_label, "-", "-", "error (API)"))
            new_lines.append(line)
            continue

        new_sha, age, notes = result
        safe, reason = safety_check(age, notes, min_age)

        if not safe:
            status = f"HELD — {reason}"
            new_lines.append(line)
        elif new_sha == current_sha:
            status = "current"
            new_lines.append(line)
        else:
            status = "updated"
            new_lines.append(f"{prefix}{repo}@{new_sha} # {ref}")
            changed = True

        print(col.format(repo, ref, old_label, new_sha[:8], f"{age:.0f}d", status))

    if changed:
        wf_file.write_text("\n".join(new_lines) + "\n", encoding="utf-8")


def update_workflow_pins(min_age: int) -> None:
    workflow_dir = Path(".github/workflows")
    if not workflow_dir.exists():
        print("No .github/workflows directory found.\n")
        return

    wf_files = sorted(workflow_dir.glob("*.yml"))
    unique_refs = _collect_unique_refs(wf_files)
    resolved: dict[tuple[str, str], tuple[str, float, str] | None] = {
        (repo, ref): resolve_action(repo, ref) for (repo, ref) in unique_refs
    }

    col = "{:<45} {:<20} {:<10} {:<10} {:>7}  {}"
    print("\n## GitHub Actions\n")
    print(col.format("Action", "Ref", "Old SHA", "New SHA", "Age", "Status"))
    print("-" * 105)

    for wf_file in wf_files:
        _rewrite_workflow_file(wf_file, resolved, min_age, col)


# ── Python dependencies ───────────────────────────────────────────────────────

PKG_NAME_RE = re.compile(r"^([A-Za-z0-9]([A-Za-z0-9._-]*[A-Za-z0-9])?)")


def collect_packages(pyproject: dict) -> list[str]:
    deps: list[str] = []
    deps += pyproject.get("project", {}).get("dependencies", [])
    for group in pyproject.get("project", {}).get("optional-dependencies", {}).values():
        deps += group
    names = set()
    for dep in deps:
        m = PKG_NAME_RE.match(dep.strip())
        if m:
            names.add(re.sub(r"[-_.]+", "-", m.group(1).lower()))
    return sorted(names)


def load_locked_versions() -> dict[str, str]:
    """Parse uv.lock and return {normalised-name: version}."""
    lock_path = Path("uv.lock")
    if not lock_path.exists():
        return {}
    with open(lock_path, "rb") as f:
        data = tomllib.load(f)
    return {
        re.sub(r"[-_.]+", "-", pkg["name"].lower()): pkg["version"]
        for pkg in data.get("package", [])
        if "version" in pkg
    }


def resolve_pypi(pkg: str) -> tuple[str, float, str] | None:
    """Returns (latest_version, age_days, description) or None."""
    data = fetch_json(f"https://pypi.org/pypi/{pkg}/json")
    if not data:
        return None
    version = data["info"]["version"]
    releases = data.get("releases", {}).get(version, [])
    if not releases:
        return None
    upload_time = releases[0]["upload_time"]
    description = data["info"].get("description") or ""
    return version, age_days(upload_time), description


def update_python_deps(min_age: int) -> None:
    pyproject_path = Path("pyproject.toml")
    if not pyproject_path.exists():
        print("No pyproject.toml found.\n")
        return

    with open(pyproject_path, "rb") as f:
        pyproject = tomllib.load(f)

    project_name = re.sub(
        r"[-_.]+", "-", pyproject.get("project", {}).get("name", "").lower()
    )
    packages = [p for p in collect_packages(pyproject) if p != project_name]
    locked = load_locked_versions()

    col = "{:<35} {:<15} {:<15} {:>7}  {}"
    print("\n## Python packages\n")
    print(col.format("Package", "Locked", "Latest PyPI", "Age", "Status"))
    print("-" * 85)

    for pkg in packages:
        locked_ver = locked.get(pkg, "—")
        result = resolve_pypi(pkg)
        if result is None:
            print(col.format(pkg, locked_ver, "-", "-", "error (PyPI)"))
            continue

        latest_ver, age, description = result
        safe, reason = safety_check(age, description, min_age)

        if not safe:
            status = f"HELD — {reason}"
        elif latest_ver == locked_ver:
            status = "current"
        else:
            proc = subprocess.run(
                ["uv", "lock", "--upgrade-package", pkg],
                capture_output=True,
                text=True,
            )
            if proc.returncode != 0:
                err = (
                    proc.stderr.strip().splitlines()[-1]
                    if proc.stderr.strip()
                    else "unknown error"
                )
                status = f"error — {err[:50]}"
            else:
                new_locked = load_locked_versions().get(pkg, locked_ver)
                if new_locked != locked_ver:
                    status = f"upgraded ({locked_ver} → {new_locked})"
                else:
                    status = f"blocked by transitive constraint (stays at {locked_ver})"

        print(col.format(pkg, locked_ver, latest_ver, f"{age:.0f}d", status))


# ── Entry point ───────────────────────────────────────────────────────────────


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Upgrade dependencies, skipping versions newer than --min-age days."
    )
    parser.add_argument(
        "--min-age",
        type=int,
        default=DEFAULT_MIN_AGE_DAYS,
        help=f"Minimum version age in days to be considered safe (default: {DEFAULT_MIN_AGE_DAYS})",
    )
    parser.add_argument(
        "--actions-only", action="store_true", help="Only update GitHub Action pins"
    )
    parser.add_argument(
        "--deps-only", action="store_true", help="Only update Python dependencies"
    )
    args = parser.parse_args()

    if not args.deps_only:
        update_workflow_pins(args.min_age)
    if not args.actions_only:
        update_python_deps(args.min_age)


if __name__ == "__main__":
    main()
