# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

Marimo-based interactive dashboards about Dutch open research (CRIS repositories, ORCID, Open Access journals, OAI-PMH status, Sprouts datasets), deployed as HTML/WebAssembly to GitHub Pages via GitHub Actions. Maintained by SURF's ORI team.

## Commands

### Development
```bash
uvx marimo edit notebooks/[notebook-name]/notebook.py   # Run single notebook in dev mode
uv run .github/scripts/build.py --output-dir _site      # Build all notebooks locally
python -m http.server -d _site                           # Serve built site at localhost:8000
```

### Export
```bash
uvx marimo export html-wasm --sandbox --mode run --no-show-code notebooks/[notebook-name]/notebook.py -o _site/[name].html
```

### Lint & Check
```bash
uvx ruff check notebooks/[notebook-name]/notebook.py        # Lint
uvx ruff check notebooks/[notebook-name]/notebook.py --fix  # Auto-fix
uvx marimo check notebooks/[notebook-name]/notebook.py      # Marimo validation
```

### Testing
```bash
uv run pytest notebooks/[notebook-name]/                              # All tests in a notebook
uv run pytest notebooks/[notebook-name]/test_[name].py::test_func    # Single test
```

## Architecture

Each notebook lives in `notebooks/[name]/` and consists of:
- `notebook.py` — Marimo notebook (Python file with `@app.cell` decorators)
- `metadata.json` — Title, image path, authors list (used by the build script to render the index page)
- `public/` — Static assets (screenshots, data files) referenced as `mo.notebook_location() / "public" / "file"`

The repo-level `public/` directory holds shared assets (ORCID icon, GitHub logo) used by the index page template (`index.html.j2`).

The build pipeline (`build.py`) reads each `metadata.json`, exports notebooks via `marimo export html-wasm`, then renders `index.html.j2` with Jinja2 into `_site/`. GitHub Actions deploys `_site/` to GitHub Pages on every push to `main`.

## Notebook Conventions

Every notebook must start with a PEP 723 inline script header:
```python
# /// script
# requires-python = ">=3.12"
# dependencies = [
#     "marimo>=0.19.0",
#     ...
# ]
# ///
```

**WASM setup cell:** Every notebook that uses packages not bundled by Pyodide must include an async setup cell as the first `@app.cell`:
```python
@app.cell(hide_code=True)
async def wasm_dependencies():
    import sys
    _ = None
    if 'pyodide' in sys.modules:
        import micropip
        await micropip.install(['polars', 'pyarrow', 'altair'])
        _ = 'wasm'
    return
```

**DuckDB / DuckLake:** Notebooks query data via `mo.sql()`, which uses a shared DuckDB connection. Attach the DuckLake catalog before querying:
```python
mo.sql(f"ATTACH '{url.value}' AS sprouts (TYPE ducklake, READ_ONLY); USE sprouts;")
# then query normally:
df = mo.sql("SELECT * FROM sprouts.schema.table LIMIT 100")
```

**Cell output:** Marimo only renders the **last expression** in a cell. Indented/conditional expressions are silent — use ternary instead:
```python
# BAD
if condition:
    mo.md("won't show")

# GOOD
mo.md("shown") if condition else mo.md("also shown")
```

**Reactivity over guards:** Do not use `if` to guard cells against missing data — Marimo handles dependency propagation. Do not wrap logic in `if is_script_mode` blocks; instead, use default/synthetic data in script mode while keeping all UI elements visible.

**Variable naming:** Do not prefix imports with underscore (use `import numpy as np`, not `_np`). Reserve `_prefix` only for loop variables that would genuinely collide with another cell's output.

**Cell order:**
1. Script header
2. Imports
3. `app = marimo.App(...)`
4. Async/hidden setup cells (WASM micropip installs)
5. Data loading
6. UI components
7. Visualizations
8. Output/display

## Code Style

- Python 3.12+ (`|` union syntax, built-in `list`/`dict` generics)
- Use `uv` for all dependency management; `uvx` for one-off tool runs
- Line length: 88 chars (ruff/Black compatible)
- Single quotes for strings unless double quotes are required
- Standard library → third-party → local import ordering

## metadata.json Schema

```json
{
  "title": "Display Title",
  "image": "public/screenshot-name.png",
  "authors": [
    {
      "name": "Author Name",
      "github": "https://github.com/handle",
      "orcid": "https://orcid.org/0000-0000-0000-0000"
    }
  ],
  "format": "app"
}
```

`github` and `orcid` must be full URLs — the index template links to them directly.

`format` options:
- `"app"` (default) — WASM export, code hidden, run mode
- `"notebook"` — WASM export, code visible, edit mode
- `"html"` — static HTML export (no WASM interactivity)
