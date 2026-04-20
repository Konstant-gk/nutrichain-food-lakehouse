# Repo structure guide mapping

Portable placement rules for mixed repository content.

## Fast placement rules

- application code -> `src/`
- sql/etl scripts -> `sql/` or `etl/`
- datasets -> `data/raw/` and `data/processed/`
- docs and runbooks -> `docs/`
- presentations, media, binaries -> `assets/`
- tests -> `tests/`
- config templates -> `config/` or root `.env.example`
- generated output -> `output/`, `dist/`, or ignore in `.gitignore`

## Naming rules

- folders: lowercase (`kebab-case` or `snake_case`)
- avoid spaces in names
- prefer specific names over generic buckets

## Anti-patterns

- `misc/`, `stuff/`, `other/`
- mixed code+data+docs in one folder
- committing secrets or raw PII
- committing large generated artifacts by default

## Optional business/startup areas

- `docs/business/`
- `docs/legal/`
- `docs/testing/` (user, alpha, beta, guerrilla)
- `docs/fundraising/`
- `docs/surveys/` and `data/surveys/` (anonymized results)
- `assets/videos/` and `assets/videos/subs/`

## Portability rule

- infer placement from artifact type, not project-specific paths
- adapt layout to framework conventions when obvious
- if user provides a structure guide, use that guide as highest-priority convention
