# README guide mapping

Portable rules distilled from professional README standards.

## Core principles

- scannable: headings, bullets, tables
- complete: setup, run, test, and core architecture
- honest: list limitations and prerequisites
- practical: copy-paste commands and env variable docs

## Minimum required sections

1. project name and one-line summary
2. overview (problem + solution)
3. project structure
4. getting started (prereqs, install, config, quick start)
5. usage
6. license

## Recommended sections (when relevant)

- architecture and data flow
- tech stack and system requirements
- testing / data quality
- deployment / CI-CD
- contributing
- documentation links
- contact/support

## Data-heavy project add-ons

- layer model (for example raw/clean/serving or bronze/silver/gold)
- key entities or marts
- quality checks and validation strategy
- orchestration/scheduling notes

## Common mistakes to avoid

- vague setup steps without commands
- missing `.env.example` instructions
- no folder tree or structure explanation
- unverified badges/links
- long unstructured text blocks

## Portability rule

- never rely on fixed repository paths
- infer structure from the current repo
- if the user provides a custom guide, treat that guide as highest-priority convention
