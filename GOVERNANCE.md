# Governance

## Project team

dlt-saga is maintained by [Glitni](https://glitni.no), a data consultancy.

| Maintainer | GitHub | Role |
|---|---|---|
| Sindre Grindheim | [@grindheim](https://github.com/grindheim) | Lead maintainer |

## Decision-making

Day-to-day decisions (bug fixes, minor features, docs) are made by maintainers
directly on pull requests.

Significant decisions — breaking changes, new extension points, major architectural
shifts — are made by maintainer consensus. These are often preceded by an RFC
(request for comments) thread in GitHub Discussions before implementation begins.

## Roadmap ownership

The roadmap is owned by Glitni. Community input is welcome via GitHub Discussions
(`Ideas` category) and GitHub Issues. The project team prioritises based on
adoption, impact, and alignment with the core mission: a config-driven,
cloud-agnostic dlt companion with a first-class SCD2 historization story.

## Accepting external contributions

**Plugin packages** (new pipeline sources, destination types, config sources) are
strongly encouraged and can be developed and published independently — no core
changes needed. See [docs/plugin-development-guide.md](docs/plugin-development-guide.md).

**Core contributions** require a PR approved by at least one maintainer. Large
features should be discussed in an issue or Discussion first to avoid wasted effort.

**Breaking changes** require an explicit maintainer decision and must be documented
in `CHANGELOG.md` with a migration path.

**Automated and agentic submissions** are subject to the same standards as human
contributions. Maintainers may close any issue or PR that appears to have been
submitted without human review, or that is part of a bulk automated campaign,
without detailed explanation. Accountability lies with the human who operated the
tool.

## Amendments

This document may be updated by the maintainers at any time. Significant changes
will be announced in GitHub Discussions.
