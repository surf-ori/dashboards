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
uvx marimo export html-wasm notebooks/[notebook-name]/notebook.py -o _site/[name].html
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
- `public/` — Static assets (screenshots, data files)

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
4. Async/hidden setup cells
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
    { "name": "Author Name", "github": "handle", "orcid": "0000-0000-0000-0000" }
  ],
  "format": "app"
}
```

`format` defaults to `"app"` (code hidden); use `"html"` for document-style notebooks.
