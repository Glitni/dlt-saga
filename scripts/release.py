#!/usr/bin/env python3
"""Prepare a release PR. Usage: uv run python scripts/release.py X.Y.Z"""

import re
import subprocess
import sys


def run(*cmd: str) -> None:
    subprocess.run(cmd, check=True)


def check(*cmd: str) -> bool:
    return subprocess.run(cmd, capture_output=True).returncode == 0


def main() -> None:
    if len(sys.argv) < 2:
        print("Usage: mise run release X.Y.Z")
        sys.exit(1)

    version = sys.argv[1]
    tag = f"v{version}"

    if not re.match(r"^\d+\.\d+\.\d+$", version):
        print("Error: version must be X.Y.Z (e.g. 0.2.5)")
        sys.exit(1)

    if not check("git", "diff", "--quiet") or not check(
        "git", "diff", "--cached", "--quiet"
    ):
        print("Error: uncommitted changes — commit or stash before releasing")
        sys.exit(1)

    run("git", "checkout", "main")
    run("git", "pull")

    print(f"--- Changelog preview for {tag} ---")
    run("git", "cliff", "--unreleased", "--tag", tag)
    print("-----------------------------------\n")

    answer = input("Create release branch, bump version, and open PR? [y/N] ")
    if answer.strip().lower() != "y":
        print("Aborted.")
        sys.exit(0)

    run("git", "checkout", "-b", f"release/{tag}")
    run("git", "cliff", "--unreleased", "--tag", tag, "--prepend", "CHANGELOG.md")

    pyproject = open("pyproject.toml").read()
    pyproject = re.sub(
        r'^version = ".*"',
        f'version = "{version}"',
        pyproject,
        count=1,
        flags=re.MULTILINE,
    )
    open("pyproject.toml", "w").write(pyproject)

    run("git", "add", "CHANGELOG.md", "pyproject.toml")
    run("git", "commit", "-m", f"Bump: version to {version}")
    run("git", "push", "-u", "origin", f"release/{tag}")

    run(
        "gh",
        "pr",
        "create",
        "--title",
        f"Bump: version to {version}",
        "--body",
        f"Release preparation for {tag}. Merge when ready, then run `mise run tag {version}` to trigger the release.",
        "--base",
        "main",
    )

    print(f"\nPR created. After it is merged, run:\n  mise run tag {version}")


if __name__ == "__main__":
    main()
