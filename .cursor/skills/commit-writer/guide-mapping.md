# Commit guide mapping

This file distills the commit guide into fast decisions for message writing.

## Core format

```text
type(optional-scope): imperative subject
```

- imperative verb
- lowercase style
- no trailing period
- one logical change per commit

## Type selection cheatsheet

- `feat`: add a new capability users/stakeholders care about
- `fix`: correct wrong behavior
- `perf`: improve speed with same behavior
- `docs`: docs and learning content
- `chore`: maintenance, deps, non-feature assets
- `refactor`: restructure internals with same behavior
- `test`: tests only
- `build`: build system/tooling
- `ci`: workflow/pipeline configuration
- `revert`: revert a prior commit

## Scope selection by repo type (portable)

Pick scope from the dominant changed area in the current repo:

- application/frontend: `ui`, `layout`, `styles`, `assets`, `seo`, `a11y`
- backend/services: `api`, `auth`, `db`, `server`, `worker`, `cache`
- data engineering: `bronze`, `silver`, `gold`, `sql`, `dbt`, `airflow`, `spark`
- infra/platform: `infra`, `docker`, `k8s`, `terraform`, `observability`, `ci`
- docs/business/legal: `docs`, `learning`, `guidelines`, `legal`, `fundraising`
- cross-cutting: `deps`, `config`, `tests`

If no scope adds clarity, omit scope instead of forcing one.

## Recruiter-facing quality signals

- specific subject lines with real technical nouns
- consistent type/scope usage over time
- no noisy commit subjects (`wip`, `updates`, `misc`)
- commit body only when it adds why/risk context

## If unsure

- broken/wrong vs new capability: `fix` vs `feat`
- user-visible vs maintainer-only: `feat` vs `chore`
- narrative docs vs binary assets: `docs(...)` vs `chore(assets)`

When uncertainty remains, pick the most honest dominant theme and mention tradeoff in rationale.

## Portability rule

- never rely on hardcoded repository paths
- infer context from the current repo's changed files and folders
- if the user supplies a guide file, treat that guide as highest-priority convention
