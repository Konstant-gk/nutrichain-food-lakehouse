---
name: readme-structure-writer
description: Create or improve professional README structure and content for any repository using clear sections, setup instructions, architecture, and quality checks. Use when the user asks to write a README, improve README quality, add missing sections, or make a README recruiter/client ready.
---

# README structure writer

Build README files that are:
- scannable for humans
- complete enough to onboard fast
- credible for hiring managers, clients, and collaborators
- portable across repo types (app, data, ML, docs, startup)

## When this skill triggers

Use this skill when the user asks to:
- create a README from scratch
- improve or rewrite an existing README
- add missing README sections
- make README "professional", "portfolio-ready", "client-ready", or "recruiter-ready"
- document setup/run/deploy/testing in README

## Operating workflow

### Step 1: gather project context

Collect:
1. project purpose and target users
2. main stack and runtime requirements
3. run/test/deploy commands
4. key architecture/data flow
5. repo folder structure

If a README guide is provided by path, read it and treat it as source of truth.

### Step 2: choose section depth by project type

Always include:
- title + one-line tagline
- overview (problem, solution, key features)
- project structure
- getting started (prereqs, install, config, quick start)
- usage
- license

Include when relevant:
- architecture diagram or flow
- data model/layers
- data quality/testing
- deployment/CI-CD
- contributing
- docs links and changelog
- contact/support

### Step 3: write with professional structure

Rules:
- use clear headings and short sections
- prefer bullets/tables over dense paragraphs
- provide copy-paste commands
- document env vars via `.env.example` (never expose secrets)
- avoid "coming soon" placeholders without context

### Step 4: validate README quality

Before final output, verify:
- someone new can run project in under 15 minutes
- commands and paths look executable
- links are not obviously broken
- no secrets/credentials shown
- section order is logical and scannable

### Step 5: produce output

Return:
1. full README draft (or updated sections)
2. checklist of what is covered vs missing
3. optional "next improvements" list

## Portable section blueprint

Use this baseline order:

```text
# Project name
Tagline

## Overview
## Architecture
## Project structure
## Getting started
## Usage
## Testing / data quality
## Deployment
## Documentation
## Contributing
## License
## Contact
```

If sections are not relevant, remove them cleanly instead of leaving empty headers.

## Output template

Use this structure unless user asks otherwise:

```text
README draft
[Markdown content]

Coverage check
- Covered: ...
- Missing: ...

Optional next upgrades
- ...
```

## Recruiter/client quality guardrails

Never:
- fake production claims not supported by repository evidence
- include secrets, private keys, access tokens, or real credentials
- produce vague setup like "install dependencies" without commands

Prefer:
- explicit tooling and versions when known
- architecture/data flow clarity
- concise professional tone with technical precision

## Additional reference

- For compact rules and checklists, read [guide-mapping.md](guide-mapping.md).
