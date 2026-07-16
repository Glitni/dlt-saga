---
name: publish-release-notes
description: Publish reviewed local release-note drafts (release-notes/vX.Y.Z.md) to existing GitHub Releases via gh, for one or more dlt-saga versions. Use when the user asks to publish, push, or apply public/GitHub release notes after reviewing the drafts from create-release-notes. Never creates a release — edits existing ones only.
---

# Publish dlt-saga release notes to GitHub

Take the reviewed draft files produced by `create-release-notes` and set them as the body of the corresponding **existing** GitHub Releases, using `gh`. This is the deliberate, outward-facing publish step — the user has already reviewed the drafts.

## Hard rules

- **Never create a release.** No `gh release create`, ever. If a release doesn't exist for a version, skip it.
- **Never touch a version with no real content.** Missing or empty draft file → skip.
- Edits are a **wholesale body replace** (`gh release edit … --notes-file`), which is why the draft file must be the complete intended body. Re-running is idempotent.
- Editing a Release changes **live public content immediately**. Invoking this skill **is** the go-ahead — the user has reviewed the drafts (that's what `create-release-notes` is for) and asked to publish. Do not pause for a separate yes/no; just run the guards and publish.

## 1. Resolve the version(s)

Accept a single version (`0.24.0`) or a range (`0.20.0..0.26.0`), the same set the user drafted. For a range, expand to each `vX.Y.Z` tag via `git tag --sort=version:refname`.

## 2. Per-version guards (skip, don't fail)

For each version, check in order and **skip with a clear warning** if any fails — then continue to the next version:

1. **Draft file missing** — `release-notes/vX.Y.Z.md` doesn't exist → skip.
2. **Draft file empty** — file exists but is empty/whitespace-only → skip.
3. **Release doesn't exist** — `gh release view vX.Y.Z` fails → skip (do **not** create it).

Only versions that pass all three are candidates to publish.

## 3. Publish

**This skill executes the publish itself — it runs `gh release edit`. Do not stop at printing the command, and do not ask for confirmation: the invocation already is the go-ahead.** Just publish the candidates and report.

For each candidate, **run it yourself**:
```bash
gh release edit vX.Y.Z --notes-file release-notes/vX.Y.Z.md
```

Then report the result per version: published (with the release URL), or skipped (with the reason from the guards). If a guard skipped a version the user expected to publish, say so plainly so they can fix it.

## Notes

- If `gh` isn't authenticated (`gh auth status` fails), stop and tell the user to `gh auth login` — don't attempt a partial publish.
- The draft files are local-only (git-excluded) and are the source of truth for this step; this skill never regenerates content — for changes, the user edits the file or re-runs `create-release-notes`.
