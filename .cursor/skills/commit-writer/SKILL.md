---
name: commit-writer
description: Write professional Conventional Commit messages from staged or described changes, aligned to a provided commit guide. Use when the user asks for commit text, "what should I write in the commit", commit title/body help, recruiter-ready git history, or PR title suggestions.
---

# Pro commit writer

Write commit messages that are:
- precise enough for engineers reviewing history
- clear enough for recruiters scanning project quality
- consistent with Conventional Commits and the user's guide
- repo-agnostic so this skill can be copied across repositories

## When this skill triggers

Use this skill when the user asks for:
- a commit message
- commit title + body
- `type(scope): subject` help
- "what do I write for this commit"
- polished, professional, recruiter-friendly git history

## Operating workflow

### Step 1: gather evidence

1. Read the files changed, or read the user summary if no diff is available.
2. If the user provided a commit guide file, read it and treat it as source of truth.
3. Identify the dominant change theme (one commit should represent one theme).
4. Infer scope names from changed folders/files; do not assume fixed repo structure.

If multiple unrelated themes exist, suggest splitting into separate commits.

### Step 2: classify correctly

Choose the most accurate commit `type`:
- `feat`: new user-visible capability
- `fix`: correcting wrong or broken behavior
- `docs`: documentation content changes
- `chore`: maintenance, dependencies, assets, non-feature upkeep
- `refactor`, `perf`, `test`, `build`, `ci`, `revert` as applicable

Choose a concise `scope` from the dominant area:
- examples: `deps`, `bronze`, `silver`, `api`, `ui`, `assets`, `docs`

### Step 3: write a strong subject line

Format:

```text
type(scope): imperative subject
```

Rules:
- imperative verb (`add`, `fix`, `update`, `replace`, `remove`)
- lowercase, no trailing period
- specific and concrete, avoid generic words like `update stuff`
- keep it short but informative

### Step 4: add body only when needed

Add a body when context matters:
- why this change exists
- what risk or tradeoff it handles
- test/migration notes (if relevant)

Keep body lines practical and scan-friendly.

### Step 5: produce final output options

Return:
1. one recommended commit message
2. two alternative subject lines
3. short rationale for type/scope choice

If useful, also include a PR title matching the recommended subject.

## Recruiter-quality guardrails

Never output:
- `wip`, `misc`, `updates`, `final`, `temp`, `quick fix`
- inflated claims not supported by the diff
- mixed unrelated changes in one message

Prefer:
- truthful and scoped language
- clean commit history that tells a project story
- wording that signals engineering discipline

## Response template

Use this exact structure unless the user asks for a different format:

```text
Recommended
type(scope): subject

[optional body]

Alternatives
- type(scope): subject
- type(scope): subject

Why this fits
- Type: ...
- Scope: ...
- Focus: ...
```

## Quick examples

### Example A: dependency bootstrap

Input: Added initial Python dependencies in `requirements.txt` for requests, airflow, pytest, dotenv.

Output:

```text
Recommended
chore(deps): add initial Python project dependencies

Alternatives
- build(deps): add base runtime and test dependencies
- chore(config): bootstrap dependency list for pipeline setup

Why this fits
- Type: This is maintenance setup, not a user-facing feature.
- Scope: The change is centered on dependencies.
- Focus: Single theme: project dependency baseline.
```

### Example B: bug in data layer

Input: Corrected duplicate rows from an incorrect join in silver transform.

Output:

```text
Recommended
fix(silver): remove duplicate rows caused by join mismatch

Alternatives
- fix(sql): correct join condition in silver transform
- fix(pipeline): prevent duplicate records in silver output

Why this fits
- Type: Behavior was incorrect and now corrected.
- Scope: Primary impact is the silver layer.
- Focus: Single bug-fix story with clear impact.
```

## Fallback behavior

If evidence is incomplete:
- ask for either `git diff --staged` summary or list of changed files
- provide a best-effort draft clearly labeled as draft

Do not pretend to have inspected changes you did not inspect.

## Additional reference

- For compact decision rules, read [guide-mapping.md](guide-mapping.md).
