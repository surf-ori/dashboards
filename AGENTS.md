# AGENTS.md - Agentic Coding Guidelines

This repository contains [Marimo-based](https://marimo.io) dashboards that are exported to HTML/WebAssembly and deployed to GitHub Pages.

## Project Structure

```
dashboards/
├── .github/
│   ├── workflows/deploy.yml    # CI/CD for GitHub Pages deployment
│   ├── scripts/build.py         # Build script for exporting notebooks
│   └── templates/               # HTML templates for the index page
├── notebooks/
│   ├── cris-repository-overview/
│   │   ├── notebook.py          # Main marimo notebook
│   │   ├── metadata.json        # Notebook metadata
│   │   └── public/              # Static assets
│   ├── doa/
│   │   └── notebook.py
│   └── orcid-monitor/
│       └── notebook.py
└── index.html.j2               # Index page template
```

## Build Commands

### Build all notebooks for local testing
```bash
uv run .github/scripts/build.py --output-dir _site
```

### Run a single notebook in development mode
```bash
uvx marimo edit notebooks/[notebook-name]/notebook.py
```

### Export a single notebook to HTML/WASM
```bash
uvx marimo export html-wasm notebooks/[notebook-name]/notebook.py -o _site/[name].html
```

### Run all notebooks as apps (code hidden)
```bash
uv run .github/scripts/build.py
```

## Lint and Type Check

### Install development dependencies
```bash
# Install ruff (used in notebooks)
uv pip install ruff
```

### Run ruff linter on a notebook
```bash
uvx ruff check notebooks/[notebook-name]/notebook.py
```

### Auto-fix linting issues
```bash
uvx ruff check notebooks/[notebook-name]/notebook.py --fix
```

### Run marimo check on a notebook
```bash
uvx marimo check notebooks/[notebook-name]/notebook.py
```

## Testing

### Run pytest for notebooks that have tests
```bash
uv run pytest notebooks/[notebook-name]/
```

### Run a single test
```bash
uv run pytest notebooks/[notebook-name]/test_[name].py::test_function_name
```

### Run tests with verbose output
```bash
uv run pytest -v notebooks/[notebook-name]/
```

### Marimo notebook testing
When adding tests to notebooks, ensure `pytest` is in the dependencies and add test cells with functions starting with `test_`:

```python
# Add to script header dependencies
#     "pytest>=7.0.0",
```

Tests can run via: `pytest notebooks/[notebook-name]/notebook.py`

## Code Style Guidelines

### General Conventions

- **Python Version**: Python 3.12+ (check `requires-python` in notebook headers)
- **Package Manager**: Use `uv` for all dependency management
- **Marimo Version**: Always specify a minimum version (e.g., `marimo>=0.19.0`)

### Import Conventions

- Standard library imports first, then third-party, then local
- Use explicit imports (avoid `from x import *`)
- Group imports with a blank line between groups
- Import `marimo` first in notebook cells, then other packages

```python
# Standard library
from pathlib import Path
import json

# Third-party
import marimo as mo
import polars as pl
import altair as alt

# Local (if applicable)
from . import module
```

### Type Hints

- Use type hints for function signatures and return types
- Use `Union[X, Y]` or `X | Y` for union types (Python 3.10+)
- Use `List`, `Dict` from `typing` or use built-in `list`, `dict` (Python 3.9+)

```python
def process_data(path: Path) -> list[dict]:
    """Process data from the given path."""
    ...
```

### Naming Conventions

- **Variables/functions**: `snake_case` (e.g., `data_frame`, `process_data`)
- **Classes**: `PascalCase` (e.g., `DataProcessor`)
- **Constants**: `UPPER_SNAKE_CASE` (e.g., `MAX_ROWS`)
- **Files**: `snake_case.py`
- **Marimo notebooks**: Directory name matches notebook purpose (e.g., `cris-repository-overview/notebook.py`)

### Error Handling

- Use specific exception types rather than bare `except:`
- Include meaningful error messages
- Use `loguru` for logging in build scripts

```python
try:
    result = process_data(path)
except FileNotFoundError as e:
    logger.error(f"Data file not found: {path}")
    raise
```

### Marimo Notebook Specifics

- **Cell decorator**: Use `@app.cell` or `@app.cell(hide_code=True)` for hidden cells
- **Async cells**: Use `async def` when the cell performs async operations (e.g., `await micropip.install()`)
- **Metadata**: Each notebook directory must have a `metadata.json` file with at least:
  ```json
  {
    "display_name": "Notebook Title",
    "description": "Brief description"
  }
  ```
- **Script header**: Include the marimo dependency specification at the top:
  ```python
  # /// script
  # requires-python = ">=3.12"
  # dependencies = [
  #     "marimo",
  #     ...
  # ]
  # ///
  ```

### Marimo Best Practices

**Show all UI elements always.** Only change the data source in script mode.

- Sliders, buttons, widgets should always be created and displayed
- In script mode, just use synthetic/default data instead of waiting for user input
- Don't wrap everything in `if is_script_mode` conditionals
- Don't use try/except for normal control flow

**Don't guard cells with `if` statements.** Marimo's reactivity handles dependencies:

```python
# BAD - the if statement prevents the chart from showing
@app.cell
def _(plt, training_results):
    if training_results:  # WRONG
        fig, ax = plt.subplots()
        ax.plot(training_results['losses'])
        fig
    return

# GOOD - let marimo handle the dependency
@app.cell
def _(plt, training_results):
    fig, ax = plt.subplots()
    ax.plot(training_results['losses'])
    fig
    return
```

**Cell output rendering:** Marimo only renders the **final expression** of a cell. Indented or conditional expressions won't render:

```python
# BAD - indented expression won't render
@app.cell
def _(mo, condition):
    if condition:
        mo.md("This won't show!")  # WRONG
    return

# GOOD - final expression renders
@app.cell
def _(mo, condition):
    result = mo.md("Shown!") if condition else mo.md("Also shown!")
    result
    return
```

**Variable naming:** Avoid underscore-prefixing imports (e.g., `import numpy as np` not `import numpy as _np`). Only use `_prefix` for loop variables that would genuinely collide with another cell's outputs.

### Code Formatting

- Maximum line length: 88 characters (ruff default)
- Use Black-style formatting (ruff formatter is compatible)
- Use single quotes for strings unless double quotes are needed
- Leave one blank line between function definitions

### File Organization

1. Script header with dependencies
2. Imports
3. App initialization (`app = marimo.App(...)`)
4. Setup cells (async, hidden)
5. Data loading cells
6. UI component cells
7. Visualization cells
8. Output/display cells

### Git Conventions

- Commit messages: Use clear, concise descriptions
- Branch naming: `feature/description` or `fix/description`
- Do not commit: `_site/`, `__marimo__/`, `.env`, `uv.lock` (optional)

## Development Workflow

1. Create a new branch for your changes
2. Edit notebooks using `uv tool run marimo edit notebooks/[name]/notebook.py`
3. Run linting: `uv tool run ruff check notebooks/[name]/notebook.py`
4. Run marimo check: `uv tool run marimo check notebooks/[name]/notebook.py`
5. Test locally: `uv run .github/scripts/build.py --output-dir _site`
6. Commit and push changes
7. Deploy happens automatically on push to main branch

**Note:** `uvx` may not be on PATH in CI or sandbox environments. Use `uv tool run <tool>` as the reliable alternative.

## DuckLake SQL — Known Schema Facts and Gotchas

These apply to the SURF ORI DuckLake (DuckDB 1.5.2 + ducklake extension).

### openaire.publications schema

- `authors[]` has: `fullName, name, surname, rank, pid.id.{scheme,value}` — **no affiliations field**
- `organizations[]` at the publication level is the correct way to link publications to institutions: `.legalName`, `.pids[{scheme,value}]` — **no `.id` field** (do not try `unnest.id`, it will error)
- Filter by institution via `list_filter` on `organizations[].pids` with `scheme = 'ROR'` (uppercase), value is a full URI

### Critical UNNEST alias quirk (DuckDB + ducklake extension)

After `UNNEST(arr) AS alias`, struct fields are **only** accessible via the literal name `unnest`. Any other alias raises `Table "alias" does not have a column`. **Always use `AS unnest`.**

**Wrong — double UNNEST with custom alias (errors):**
```sql
UNNEST(w.authorships) AS a,
UNNEST(a.institutions) AS inst   -- fails: Table 'a' does not have column 'institutions'
```

**Correct — single UNNEST + `list_filter` for nested arrays:**
```sql
-- OpenAlex: works for an institution
SELECT COUNT(DISTINCT w.id)
FROM openalex.works AS w,
     UNNEST(w.authorships) AS unnest
WHERE array_length(list_filter(unnest.institutions, x -> x.ror = 'https://ror.org/04dkp9463')) > 0;

-- OpenAIRE: publications for a single institution (organizations[].pids, no .id field)
SELECT COUNT(DISTINCT pub.id)
FROM openaire.publications AS pub,
     UNNEST(pub.organizations) AS unnest
WHERE array_length(list_filter(
    unnest.pids,
    x -> x.scheme = 'ROR' AND x.value = 'https://ror.org/04dkp9463'
)) > 0;

-- OpenAIRE: multiple institutions — use list_contains() inside the lambda
SELECT COUNT(DISTINCT pub.id)
FROM openaire.publications AS pub,
     UNNEST(pub.organizations) AS unnest
WHERE array_length(list_filter(
    unnest.pids,
    x -> x.scheme = 'ROR' AND list_contains(['https://ror.org/aaa', 'https://ror.org/bbb'], x.value)
)) > 0;
```

### Performance facts (DuckLake on SURF Object Store)

No indexes or partition pruning for nested fields — every UNNEST query is a full Parquet scan over HTTPS:

| Table | Rows | Approx. scan time |
|---|---|---|
| `openalex.institutions` | 120 K | < 1 s |
| `cris.publications` | 2.4 M | seconds |
| `openaire.publications` | 206 M | 15–30 min |
| `openalex.works` | 364 M | 15+ min |
| `openaire.relations` | large | no faster than publications |

Prefer pre-computed fields where possible: `openalex.institutions.works_count` gives publication counts per institution in < 1 s.

### Skills and MCP server location

- Agent skills live in `.agents/skills/` (not `.claude/skills/`)
- MCP server config: `.claude/settings.json` with `mcpServers`
- The `ori-ducklake-sprouts` MCP server is installed via `uv tool install mcp-servers/ori-ducklake-mcp/`
