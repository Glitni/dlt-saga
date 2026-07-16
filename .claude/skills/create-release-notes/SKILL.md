---
name: create-release-notes
description: Create public English GitHub Release notes for one or more dlt-saga releases, written to local files in release-notes/ for review. Use when the user asks to write, draft, create, or generate public/GitHub release notes for a saga version or range (e.g. "create release notes for 0.24.0", "draft GitHub notes for 0.20.0..0.26.0"). This is the public/English counterpart to saga-release-notes (which is the internal Slack post in Norwegian) — do not confuse the two. Publish the result with publish-release-notes.
---

# dlt-saga public release notes

Draft the **GitHub Release body** for one or more dlt-saga releases. The reader is an external open-source user evaluating an upgrade — someone who does not know the framework internals and has no access to internal channels. Write English, objective, and upgrade-focused.

This skill only **drafts** — it writes files for review. Publishing to GitHub is a separate step (the `publish-release-notes` skill).

## 1. Resolve the version(s)

Accept either a single version (`0.24.0`) or a range (`0.20.0..0.26.0`).

- Tags are `vX.Y.Z`. List them with `git tag --sort=version:refname`.
- **One file per release tag** in the requested set (patches included) — GitHub has one Release per tag, so notes are per-tag, not folded into the minor line (that folding is the *Slack* skill's job, not this one).
- For each tag `vX.Y.Z`, the previous tag is the one immediately before it in `git tag --sort=version:refname`. Use it for the commit range and the compare link.

## 2. Gather the facts (don't guess)

For each `vX.Y.Z`:
- Read that version's section in `CHANGELOG.md`.
- `git log --oneline vPREV..vX.Y.Z` for the actual PRs/commits. If the tag doesn't exist yet, use `vPREV..HEAD` and note it's a preview.
- **Exclude commits after the `Bump: version to X.Y.Z` commit** — they belong to the next version.

**Fact discipline (this is where drafts fail):**
- **Don't infer a bug's failure mechanism from its commit title, and don't dramatize it.** A one-line `fix:` tells you *what* changed, rarely *why the old behavior was bad*. State plainly what changed. If the consequence isn't verifiable from the diff/CHANGELOG, describe the change itself, not an invented scenario — or read the diff to confirm.
- Same for what a change *enables*: describe the mechanism, not an aspirational capability.
- Avoid severity framing ("trap", "collision", "silently") unless it's demonstrably true from the diff.
- **Resolve factual questions from the repo, don't ask the user or hedge.** "Is this command/feature new, or pre-existing?" is answerable: `git log --oneline --diff-filter=A -- '*name*'` for when a file first appeared, and the PR body (`gh pr view <n> --json title,body`) usually states it outright. Check before writing "new"; never leave it as an open question in the draft or ask the user.

## 3. Strip internal context (hard rule)

These notes are public. Never include:
- Named internal consumers, teams, pipelines, incidents, or Airflow activities.
- The internal review arc or the AI model's name (`Fable`), review "tiers", or `IMPROVEMENTS.md` (local-only file; don't reference it or its tier numbers).
- Anything phrased as "this broke for us" — frame every motivation at the **framework-user level** ("pipelines using native_load with S3 now …").

## 4. Output: one file per release

Write each release to `release-notes/vX.Y.Z.md` (create the dir if missing — it's local-only, git-excluded). The file **is the complete intended GitHub Release body** — what you write is exactly what gets published, so it must be self-contained (including the changelog list). Do **not** include an H1 title; GitHub already titles the Release `dlt-saga vX.Y.Z`.

Template:

```markdown
## X.Y.Z — YYYY-MM-DD

1–3 sentences: the theme of this release and who should care. No heading — the lead paragraph is self-evidently the summary.

### New

- **Feature** — one line on the capability and when you'd reach for it. Omit the section if there are no features.

### Upgrade notes

- Breaking or behavioral changes: what changed, and the exact migration step. Omit the section if there are none — but never trim it for brevity when it applies.

### Also fixed

- One line for the one or two fixes a user would care about; fold the rest into a clause or leave them to the changelog. Omit if nothing stands out.

### Full changelog

#### Breaking changes
- <mirror the CHANGELOG entry's groups for this version; drop empty groups>

#### Added
- ...

#### Fixed
- ...

**Full diff:** https://github.com/Glitni/dlt-saga/compare/vPREV...vX.Y.Z
```

Heading & date rules:
- **The version + date line is the only `##` heading, and it's the first line.** Copy that version's heading from `CHANGELOG.md` verbatim (`## X.Y.Z — YYYY-MM-DD`) — mirroring the CHANGELOG format keeps the two consistent and makes the note self-contained if read outside the release page. GitHub's auto-title already shows the version too; the mild redundancy is fine, and the authoritative date must not be dropped.
- **Consistent descending hierarchy under that one `##`.** The date is the only `##`; the narrative sections are `###` (`### New`, `### Upgrade notes`, `### Also fixed`, `### Full changelog`); the changelog groups nested under **Full changelog** are `####` (`#### Breaking changes`, `#### Added`, `#### Fixed`) — one level below their parent section, so nothing is ever a heading level that skips or contradicts its parent.
- **There is no "Highlights" heading** — the opening paragraph obviously is the summary, so it leads directly under the date with no label.

Rules:
- **Bias hard toward concision — length scales with significance, and most releases are short.** The **Full changelog** already lists everything with PR numbers, so the narrative carries *only what needs explaining*: breaking/behavioral changes, genuinely new capabilities, and the one or two fixes a user would actually care about. Do **not** re-itemize every fix in prose — that just doubles the reading. Fold routine fixes into a single line, or leave them to the changelog entirely.
- The narrative (lead paragraph + **New** / **Upgrade notes** / **Also fixed**) should be scannable in ~20 seconds. The lead summary is 1–3 sentences. If a release is under-the-hood, that lead paragraph may be all the narrative needs before the Full changelog.
- **Upgrade notes are the exception — never cut them for brevity.** Breaking changes, new config keys, and behavioral changes get whatever length clarity requires.
- Omit any section (bold label) that has no content. Section labels are a guide, not fixed — use **New** / **Upgrade notes** / **Also fixed** or similar; skip **New** when there are no features.
- For a trivial patch (dependency bump, docs), the lead paragraph + **Full changelog** is enough — don't manufacture substance.
- Keep inline-code formatting for flags, config keys, identifiers, and file names (`` `--select` ``, `` `write_disposition` ``, `` `native_load` ``).

## 5. Voice

- English, plain, objective. Actor–verb–object sentences; a non-specialist should follow.
- Describe capabilities and impact; **do not** address the reader with calls to action, except in **Upgrade notes**, where a concrete migration instruction is the point.
- No reassurance filler, no marketing adjectives ("powerful", "seamless"). State what it does.
- Flag security-relevant fixes plainly.
- **Upgrade guidance is the highest-value part** for an external reader — be explicit about new config keys, deprecations, and breaking changes.

## 6. Process

1. Write the file(s).
2. Report which files were written and, per release, note any judgment calls (what you excluded, patches deemed trivial, anything unverifiable you described conservatively).
3. Expect the user to review and edit — either by hand or by asking you to revise the files. Match their edits going forward.
4. Publishing is out of scope here — that's `publish-release-notes`.
