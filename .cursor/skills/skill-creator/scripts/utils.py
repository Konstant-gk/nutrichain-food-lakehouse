"""Shared utilities for skill-creator scripts."""

import os
import shutil
import webbrowser
from pathlib import Path


def parse_skill_md(skill_path: Path) -> tuple[str, str, str]:
    """Parse a SKILL.md file, returning (name, description, full_content)."""
    content = (skill_path / "SKILL.md").read_text()
    lines = content.split("\n")

    if lines[0].strip() != "---":
        raise ValueError("SKILL.md missing frontmatter (no opening ---)")

    end_idx = None
    for i, line in enumerate(lines[1:], start=1):
        if line.strip() == "---":
            end_idx = i
            break

    if end_idx is None:
        raise ValueError("SKILL.md missing frontmatter (no closing ---)")

    name = ""
    description = ""
    frontmatter_lines = lines[1:end_idx]
    i = 0
    while i < len(frontmatter_lines):
        line = frontmatter_lines[i]
        if line.startswith("name:"):
            name = line[len("name:"):].strip().strip('"').strip("'")
        elif line.startswith("description:"):
            value = line[len("description:"):].strip()
            # Handle YAML multiline indicators (>, |, >-, |-)
            if value in (">", "|", ">-", "|-"):
                continuation_lines: list[str] = []
                i += 1
                while i < len(frontmatter_lines) and (frontmatter_lines[i].startswith("  ") or frontmatter_lines[i].startswith("\t")):
                    continuation_lines.append(frontmatter_lines[i].strip())
                    i += 1
                description = " ".join(continuation_lines)
                continue
            else:
                description = value.strip('"').strip("'")
        i += 1

    return name, description, content


def find_project_root(start: Path | None = None) -> Path:
    """Find project root by walking up and looking for .cursor/.Cursor."""
    current = (start or Path.cwd()).resolve()
    for parent in [current, *current.parents]:
        if (parent / ".cursor").is_dir() or (parent / ".Cursor").is_dir():
            return parent
    return current


def resolve_cursor_dir(project_root: Path) -> Path:
    """Return the Cursor config directory, preferring existing casing."""
    lower = project_root / ".cursor"
    upper = project_root / ".Cursor"
    if lower.is_dir():
        return lower
    if upper.is_dir():
        return upper
    return lower


def resolve_cursor_cli() -> str:
    """Resolve the Cursor CLI executable name/path."""
    # Allow explicit override for constrained environments.
    explicit = os.environ.get("CURSOR_CLI_PATH", "").strip()
    if explicit:
        return explicit

    for candidate in ("Cursor", "cursor", "cursor.cmd", "cursor.exe"):
        path = shutil.which(candidate)
        if path:
            return path

    # Fall back to the canonical name; subprocess will raise a clear error.
    return "Cursor"


def sanitized_env() -> dict[str, str]:
    """Environment for nested Cursor CLI calls."""
    env = dict(os.environ)
    # Different builds may use different casing for this guard var.
    env.pop("CursorCODE", None)
    env.pop("CURSORCODE", None)
    return env


def open_browser_safe(target: str) -> bool:
    """Best-effort browser open that never crashes callers."""
    try:
        return bool(webbrowser.open(target))
    except Exception:
        return False
