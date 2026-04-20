#!/usr/bin/env python3
"""Push the release tag after the release PR is merged. Usage: uv run python scripts/tag.py X.Y.Z"""

import re
import subprocess
import sys


def run(*cmd: str) -> None:
    subprocess.run(cmd, check=True)


def main() -> None:
    if len(sys.argv) < 2:
        print("Usage: mise run tag X.Y.Z")
        sys.exit(1)

    version = sys.argv[1]
    tag = f"v{version}"

    if not re.match(r"^\d+\.\d+\.\d+$", version):
        print("Error: version must be X.Y.Z (e.g. 0.2.5)")
        sys.exit(1)

    run("git", "checkout", "main")
    run("git", "pull")

    pyproject = open("pyproject.toml").read()
    match = re.search(r'^version = "(.+)"', pyproject, re.MULTILINE)
    current = match.group(1) if match else "(unknown)"

    if current != version:
        print(
            f"Error: pyproject.toml version is {current}, not {version} — is the release PR merged?"
        )
        sys.exit(1)

    run("git", "tag", tag)
    run("git", "push", "origin", tag)

    print(f"Pushed {tag} — CI will publish to PyPI and create the GitHub Release.")


if __name__ == "__main__":
    main()
