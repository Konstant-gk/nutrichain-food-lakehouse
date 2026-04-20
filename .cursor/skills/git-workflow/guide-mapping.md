# Git workflow guide mapping

Portable decision rules for branch, PR, pull, and merge timing.

## Branch timing

Create a branch when:
- starting any non-trivial feature or fix
- changing shared code
- preparing a reviewable change set

Use branch prefixes:
- `feature/` new capability
- `fix/` bug correction
- `docs/` documentation-only
- `chore/` maintenance/tooling

## Pull and push timing

- pull before starting work
- pull again before opening PR if branch is stale
- push after each logical checkpoint so PR reflects latest work

## PR timing

Open PR when:
- branch has one coherent theme
- commit history is understandable
- basic tests/lint checks pass (or status is explained)

## Merge timing

Merge when:
- review is complete (or self-reviewed in solo flow)
- CI/status checks are acceptable for project policy
- conflicts are resolved cleanly

## Strategy tradeoffs

- squash merge: clean main history
- merge commit: preserves branch topology
- rebase merge: linear history, stricter conflict handling

## Portability rule

- do not assume specific branch names beyond common defaults (`main`, optional integration branch)
- adapt commands to current repo conventions
- if the user provides a workflow guide, treat it as highest-priority convention
