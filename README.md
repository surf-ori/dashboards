# ORI Dashboards

Interactive dashboards exploring Dutch open research — CRIS repositories, ORCID coverage, Open Access journals, OAI-PMH endpoint health, and dataset availability. Built with [Marimo](https://marimo.io) and deployed as WebAssembly to GitHub Pages.

**Live site:** [surf-ori.github.io/dashboards](https://surf-ori.github.io/dashboards/)

---

## Dashboards

| Dashboard | Description |
|---|---|
| [Dutch CRIS / Repositories Overview](https://surf-ori.github.io/dashboards/cris-repository-overview.html) | Overview of Dutch CRIS systems and institutional repositories |
| [Diamond Open Access Journals](https://surf-ori.github.io/dashboards/doa-journals.html) | Diamond OA journals in the Netherlands from the DOAJ |
| [Dutch ORCID Monitor](https://surf-ori.github.io/dashboards/orcid-monitor.html) | ORCID adoption and coverage across Dutch institutions |
| [OAI-PMH Endpoint Status](https://surf-ori.github.io/dashboards/repository-status.html) | Live health status of Dutch repository OAI-PMH endpoints |
| [Datasets in the ORI Data Lake](https://surf-ori.github.io/dashboards/sprouts-overview.html) | Overview of datasets available in the SURF ORI Sprouts data lake |
| [ORI Datasets Overview (v2)](https://surf-ori.github.io/dashboards/sprouts-overview-2.html) | Editable notebook — live catalog browsing and ad-hoc SQL querying of the ORI DuckLake |
| [ORI Data Quality](https://surf-ori.github.io/dashboards/ori-data-quality.html) | Data quality metrics across ORI datasets |

---

## Project structure

```
dashboards/
├── notebooks/
│   └── [name]/
│       ├── notebook.py       # Marimo notebook
│       ├── metadata.json     # Title, authors, screenshot path
│       └── public/           # Static assets referenced by the notebook
├── .github/
│   ├── workflows/deploy.yml  # Builds and deploys to GitHub Pages on push to main
│   ├── scripts/build.py      # Exports all notebooks to HTML/WASM
│   └── templates/            # Jinja2 templates for the index page
└── mcp-servers/
    └── ori-ducklake-mcp/     # MCP server for querying the ORI DuckLake catalog
```

Each notebook is self-contained: its dependencies are declared in a [PEP 723](https://peps.python.org/pep-0723/) inline script header and installed automatically by `uv`.

---

## Development

### Prerequisites

- [uv](https://docs.astral.sh/uv/) — Python package manager

### Run a notebook locally

```bash
uvx marimo edit notebooks/[notebook-name]/notebook.py
```

### Build and preview the full site

```bash
uv run .github/scripts/build.py --output-dir _site
python -m http.server -d _site
# Open http://localhost:8000
```

### Lint and validate a notebook

```bash
uv tool run ruff check notebooks/[notebook-name]/notebook.py
uv tool run marimo check notebooks/[notebook-name]/notebook.py
```

---

## Adding a notebook

1. Create a branch named after the notebook (e.g. `my-new-dashboard`).
2. Add `notebooks/[name]/notebook.py` — a Marimo notebook with a PEP 723 header listing dependencies.
3. Add `notebooks/[name]/metadata.json`:
   ```json
   {
     "title": "Dashboard Title",
     "image": "public/screenshot.png",
     "authors": [
       {
         "name": "Your Name",
         "github": "https://github.com/handle",
         "orcid": "https://orcid.org/0000-0000-0000-0000"
       }
     ],
     "format": "app"
   }
   ```
4. Open a pull request to `main` — the deploy pipeline picks up the new notebook automatically.

> [!TIP]
> Use `"format": "app"` for interactive dashboards (code hidden, run mode) and `"format": "notebook"` to show the code for educational notebooks.

---

## Data access

Notebooks query the **SURF ORI DuckLake** — a DuckDB-backed data lake on SURF Object Store containing datasets from OpenAIRE, OpenAlex, CRIS systems, and more. The `ori-ducklake-mcp` server (in `mcp-servers/`) exposes the catalog via the [Model Context Protocol](https://modelcontextprotocol.io) for use in AI-assisted development.

---

## Deployment

Every push to `main` triggers a GitHub Actions workflow that:

1. Exports all notebooks to HTML/WebAssembly via `marimo export html-wasm`
2. Renders the index page from a Jinja2 template
3. Deploys the `_site/` directory to GitHub Pages

Each notebook has its own branch (e.g. `cris-repository-overview`, `doa-journals`). Work on a notebook in its branch, then open a PR to `main` to trigger a deploy.

---

## Maintained by

[SURF ORI](https://www.surf.nl/en/services/surf-research-information) — the Open Research Information team at SURF, the Dutch national research and education network.
