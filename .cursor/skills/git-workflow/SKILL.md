---
name: git-workflow
description: Guide professional Git and GitHub workflow decisions for branching, pulling, pushing, pull requests, and merging. Use when the user asks when to create a branch, when to open PRs, how to merge safely, or how to run clean solo/team Git workflows.
---

# Git workflow coach

Provide practical Git/GitHub guidance for:
- branch strategy
- PR timing and hygiene
- pull/push/merge sequencing
- conflict-safe collaboration

## When this skill triggers

Use this skill when the user asks:
- when to create a branch
- when to merge
- when to open a pull request
- how to sync with `main`
- how to run professional GitHub workflow

## Operating workflow

### Step 1: identify context

Classify workflow context:
- solo vs team
- quick fix vs feature
- docs-only vs code change
- protected `main` vs unprotected

### Step 2: recommend branch strategy

Default:
- one logical change per branch
- branch prefixes: `feature/`, `fix/`, `docs/`, `chore/`
- keep branches short-lived

When change is tiny and truly isolated, allow direct branch + quick PR.

### Step 3: define command sequence

Standard sequence:
1. update base branch (`main` or integration branch)
2. create branch
3. commit focused changes
4. push branch
5. open PR
6. merge after checks/review
7. sync local `main` and delete branch

### Step 4: merge guidance

Recommend:
- small PRs with one theme
- clear PR title matching commit intent
- resolve conflicts before merge
- avoid long-lived diverged branches

Call out merge strategy tradeoffs when asked:
- squash (clean history)
- merge commit (full branch history)
- rebase merge (linear but stricter discipline)

### Step 5: return actionable output

Return:
1. recommended workflow path for this scenario
2. exact command block
3. risk notes (conflicts, stale branch, missing tests)
4. optional alternate path (fastest vs safest)

## Portable default workflow

```text
git checkout main
git pull
git checkout -b feature/short-name
# edit
git add .
git commit -m "type(scope): subject"
git push -u origin feature/short-name
# open PR to main
```

After merge:

```text
git checkout main
git pull
git branch -d feature/short-name
```

## Response template

```text
Recommended workflow
- ...

Commands
```bash
...
```

Why this timing
- Branch now because ...
- PR now because ...
- Merge when ...

Risk checks
- ...
```

## Professional guardrails

Never recommend:
- committing secrets
- vague commit messages
- merging with unresolved conflicts
- forcing push to shared main branches unless explicitly requested and understood

Prefer:
- pull before starting and before PR
- one branch per logical task
- PR-based merge flow, even for solo portfolio repos

## Additional reference

- For compact branch/PR decision rules, read [guide-mapping.md](guide-mapping.md).
