---
name: repo-structure-organizer
description: Design or refactor repository folder structures using clear separation of code, data, docs, assets, config, and tests. Use when the user asks where files should go, how to organize a repo, how to restructure a messy repo, or how to set up a professional repository layout.
---

# Repo structure organizer

Organize repositories with:
- one purpose per folder
- clear separation of concerns
- portable conventions that work across domains

## When this skill triggers

Use this skill when the user asks:
- where specific files should be placed
- how to organize a new repository
- how to clean up/restructure an existing repo
- how to handle business/legal/testing/media assets
- how to standardize naming and `.gitignore`

## Operating workflow

### Step 1: inventory artifacts

Classify repository contents by type:
- code
- data
- docs
- assets/binaries
- config
- tests
- generated outputs
- optional business/legal/testing/fundraising materials

### Step 2: map artifacts to standard folders

Default placements:
- code -> `src/` (or `sql/`, `scripts/`)
- data -> `data/raw/`, `data/processed/`
- docs -> `docs/`
- binaries/deliverables -> `assets/`
- config templates -> `config/` or root `.env.example`
- tests -> `tests/`
- static web files -> `public/`

### Step 3: produce target structure

Return:
1. proposed folder tree
2. mapping table `current path -> target path`
3. naming conventions to enforce
4. `.gitignore` recommendations for safety and size control

### Step 4: migration plan

If restructuring existing repos:
- use `git mv` where possible to keep history
- update code/docs/config path references
- verify build/run/test after moves
- keep refactor in logical commits

### Step 5: validate final shape

Check:
- no catch-all folders (`misc`, `stuff`, `other`)
- lowercase folder names
- no secrets committed
- large/generated files ignored unless intentionally versioned

## Portable folder blueprint

```text
repo/
├── src/
├── data/
│   ├── raw/
│   └── processed/
├── docs/
├── assets/
├── config/
├── tests/
├── scripts/
├── templates/
├── README.md
└── .gitignore
```

Adjust per stack:
- web: add `public/`, `src/components/`, `src/styles/`
- data engineering: add `sql/` or `dbt/`, optional layer folders
- startup/business-heavy: add `docs/business/`, `docs/legal/`, `docs/testing/`, `docs/fundraising/`, `assets/videos/`

## Response template

```text
Recommended structure
[tree]

File placement rules
- ...

Migration plan
1) ...
2) ...
3) ...

Risk checks
- Secrets / PII: ...
- Large binaries: ...
- Broken paths to update: ...
```

## Professional guardrails

Never:
- place secrets in tracked files
- mix unrelated artifact types in one folder
- suggest committing generated build output by default

Prefer:
- practical, minimal top-level folders
- clear folder naming conventions
- explicit handling for legal/testing/survey/fundraising content when present

## Additional reference

- For quick mapping rules, read [guide-mapping.md](guide-mapping.md).
