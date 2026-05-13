#!/usr/bin/env python3
"""Prepare a release PR. Usage: uv run python scripts/release.py X.Y.Z"""

import re
import shutil
import subprocess
import sys


def run(*cmd: str) -> None:
    subprocess.run(cmd, check=True)


def check(*cmd: str) -> bool:
    return subprocess.run(cmd, capture_output=True).returncode == 0


def _origin_repo_slug() -> str | None:
    """Return ``owner/repo`` parsed from ``origin``, or ``None`` if unrecognised."""
    result = subprocess.run(
        ["git", "remote", "get-url", "origin"], capture_output=True, text=True
    )
    if result.returncode != 0:
        return None
    url = result.stdout.strip()
    m = re.search(r"github\.com[:/]([^/]+/[^/.]+?)(?:\.git)?/?$", url)
    return m.group(1) if m else None


def main() -> None:
    if len(sys.argv) < 2:
        print("Usage: mise run release X.Y.Z")
        sys.exit(1)

    version = sys.argv[1]
    tag = f"v{version}"

    if not re.match(r"^\d+\.\d+\.\d+$", version):
        print("Error: version must be X.Y.Z (e.g. 0.2.5)")
        sys.exit(1)

    if not check("git", "diff", "--quiet", "--", ":(exclude)uv.lock") or not check(
        "git", "diff", "--cached", "--quiet", "--", ":(exclude)uv.lock"
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
    run(
        "git",
        "cliff",
        "--unreleased",
        "--tag",
        tag,
        "--prepend",
        "CHANGELOG.md",
        "--strip",
        "header",
    )

    pyproject = open("pyproject.toml").read()
    pyproject = re.sub(
        r'^version = ".*"',
        f'version = "{version}"',
        pyproject,
        count=1,
        flags=re.MULTILINE,
    )
    open("pyproject.toml", "w").write(pyproject)

    run("uv", "lock")
    run("git", "add", "CHANGELOG.md", "pyproject.toml", "uv.lock")
    run("git", "commit", "-m", f"Bump: version to {version}")
    run("git", "push", "-u", "origin", f"release/{tag}")

    pr_title = f"Bump: version to {version}"
    pr_body = (
        f"Release preparation for {tag}. Merge when ready, "
        f"then run `mise run tag {version}` to trigger the release."
    )

    if shutil.which("gh"):
        run(
            "gh",
            "pr",
            "create",
            "--title",
            pr_title,
            "--body",
            pr_body,
            "--base",
            "main",
        )
        print(f"\nPR created. After it is merged, run:\n  mise run tag {version}")
    else:
        slug = _origin_repo_slug()
        compare_url = (
            f"https://github.com/{slug}/compare/main...release/{tag}?quick_pull=1"
            if slug
            else f"https://github.com/<owner>/<repo>/pull/new/release/{tag}"
        )
        print(
            "\ngh CLI not found — open the PR manually:\n"
            f"  URL:   {compare_url}\n"
            f"  Title: {pr_title}\n"
            f"  Body:  {pr_body}\n"
            f"\nAfter merging, run:\n  mise run tag {version}"
        )


if __name__ == "__main__":
    main()
