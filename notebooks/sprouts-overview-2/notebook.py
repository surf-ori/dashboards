# /// script
# requires-python = ">=3.13"
# dependencies = [
#     "altair==6.0.0",
#     "duckdb==1.5.2",
#     "marimo>=0.20.2",
#     "numpy==2.4.3",
#     "polars[pyarrow]==1.39.3",
#     "requests==2.32.5",
# ]
# ///

import marimo

__generated_with = "0.23.2"
app = marimo.App(
    width="medium",
    app_title="Open Research Information | Datasets Overview",
)


@app.cell(hide_code=True)
async def system():
    import marimo as mo
    import sys

    if "pyodide" in sys.modules:
        import micropip
        await micropip.install(["polars", "duckdb"])

    import requests
    import json
    import duckdb
    import polars as pl

    return duckdb, json, mo, pl, requests


@app.cell(hide_code=True)
def header(mo):
    mo.md("""
    <div style="
        display: flex;
        align-items: center;
        justify-content: space-between;
        padding-bottom: 0.5rem;
        border-bottom: 1px solid #e5e5e5;
        margin-bottom: 1rem;
    ">
        <div>
            <h1 style="margin: 0;">
                Open Research Information | Datasets Overview
            </h1>
            <div style="color: #666; font-size: 0.9rem;">
                Overview of available and actively queryable ORI datasets.
            </div>
        </div>
        <img
            src="https://www.surf.nl/themes/surf/logo.svg"
            alt="SURF logo"
            style="height: 100px;"
        />
    </div>
    """)
    return


@app.cell(hide_code=True)
def introduction(mo):
    mo.md(r"""
    This dashboard is part of the [**PID to Portal project**](https://communities.surf.nl/en/open-research-information/article/from-pid-to-portal-strengthening-the-open-research-information) from SURF and UNL. Our goal is to create an overview of available and actively queryable Open Research Information Resources / Datasets, that the ORI community can start using freely. [Code available here.](https://github.com/surf-ori/sprouts/)
    [![Open in molab](https://marimo.io/molab-shield.svg)](https://molab.marimo.io/github/surf-ori/dashboards/blob/sprouts-overview-2/notebooks/sprouts-overview-2/notebook.py)
    """)
    return


@app.cell(hide_code=True)
def background(mo):
    _background = mo.md("""
    In our approach, we aim to avoid BigTech. We curate the queryable databases ourselves but welcome others to share their data catalogs. We utilize the DuckLake catalog from DuckDB and store the actual data as Parquet files on an S3-compatible object store. This separation of storage and compute helps us keep costs low.

    This catalog lists all the ORI data resources, their tables and columns. It also holds information about the changes over time of all the data resources, like deleted, updated and added records, and schema changes.

    This allows you to 'time-travel' in the data, and not only use the last state. For example detecting when an article flips from open access to closed access.

    **Compute:** You can query the datasets directly from your browser (no login required)! When you query the databases, a portion of the requested data is transferred over HTTPS to your local machine where the SQL operations are performed. Larger data requests result in longer transfer times, and the speed of your local machine affects the query completion time. The size of your machine is up to you.

    **Existing Components:** At SURF, we also provide ready-made [services](https://www.surf.nl/en/services) for SQL computation, such as a Marimo notebook in a virtual machine on SURF Research Cloud or a Superset dashboard on a Kubernetes cluster.

    **Inspiring initiatives:** Below, you will see the ORI data resources we currently curate. This overview was inspired by the [ORION-DBS initiative](https://orion-dbs.community/), [Swemetrics](https://swemetrics.se/) and [Science Datalake](https://j0nasw.github.io/science-datalake/).

    **Datasets wishlist:** OpenAIRE, OpenALEX, OpenAPC, ROR, Harvest metadata from CRISes, Harvest metadata from Repositories, Crossref, SURF Journal Catalogue, CWTS Leiden Ranking, ORCID, DOAJ, DOAB, OpenCitations, DataCite, PKP beacon.
    """)
    mo.accordion({"Background": _background}, lazy=True)
    return


@app.cell(hide_code=True)
def catalog_section(mo):
    mo.md(r"""
    ## ORI data catalog

    This URL gives you access to the ORI data catalog. Copy this URL to attach it as a ducklake to your own query engine.
    """)
    return


@app.cell(hide_code=True)
def catalog_location(mo):
    url = mo.ui.text_area(value='https://objectstore.surf.nl/cea01a7216d64348b7e51e5f3fc1901d:sprouts/catalog.ducklake')
    url
    return (url,)


@app.cell(hide_code=True)
def catalog_loading(mo, url):
    _df = mo.sql(
        f"""
        ATTACH 'ducklake:{url.value}' as sprouts;
        USE sprouts;
        """
    )
    return


@app.cell(hide_code=True)
def catalog_statistics_data(mo):
    quick_statistics = mo.sql(
        """
        SELECT table_name, record_count, file_size_bytes
            FROM __ducklake_metadata_sprouts.ducklake_table_stats
            FULL JOIN __ducklake_metadata_sprouts.ducklake_table
            USING (table_id)
        """,
        output=False
    )
    return (quick_statistics,)


@app.cell(hide_code=True)
def catalog_statistics_cards(datasets, mo, pl, quick_statistics, tables):
    def _to_int(value):
        return int(value) if value is not None else 0

    def _format_gb(size_bytes):
        return f"{size_bytes / (1024 ** 3):,.2f} GB"

    datasets_count = datasets.height
    total_records = _to_int(quick_statistics.select(pl.col("record_count").fill_null(0).sum()).item())
    volume_bytes = _to_int(tables.select(pl.col("file_size_bytes").fill_null(0).sum()).item())

    mo.hstack([
        mo.stat(value=f"{datasets_count:,}", label="Datasets", caption="Number of datasets", bordered=True),
        mo.stat(value=f"{total_records:,}", label="Total records", caption="Total number of records across all datasets", bordered=True),
        mo.stat(value=_format_gb(volume_bytes), label="Data volume", caption="Total data volume in GB", bordered=True),
    ], widths="equal", align="center")
    return


@app.cell(hide_code=True)
def data_section(mo):
    mo.md(r"""
    ## ORI datasets, tables, columns
    """)
    return


@app.cell
def data_datasets(mo):
    datasets = mo.sql(
        """
        FROM __ducklake_metadata_sprouts.ducklake_schema
        WHERE schema_name != 'main';
        """,
        output=False
    )
    return (datasets,)


@app.cell
def data_tables(mo):
    tables = mo.sql(
        """
        SELECT *
        FROM __ducklake_metadata_sprouts.ducklake_table t
        JOIN __ducklake_metadata_sprouts.ducklake_table_stats s
        ON t.table_id = s.table_id
        JOIN __ducklake_metadata_sprouts.ducklake_tag c
        ON t.table_id = c.object_id
        WHERE key = 'comment'
        """,
        output=False
    )
    return (tables,)


@app.cell
def data_columns(mo):
    columns = mo.sql(
        """
        SELECT *
        FROM __ducklake_metadata_sprouts.ducklake_column c
        JOIN __ducklake_metadata_sprouts.ducklake_column_tag t
        ON c.column_id = t.column_id
        """,
        output=False
    )
    return (columns,)


@app.cell
def data_columns_latest(columns):
    latest_columns = (
        columns.sort('begin_snapshot')
               .unique(subset=['table_id', 'column_id'], keep='first')
    )
    return (latest_columns,)


@app.cell
def data_schema_tabs(datasets, mo):
    options = {row['schema_name']: '' for row in datasets.to_dicts()}
    initial = datasets['schema_name'][0]
    selector = mo.ui.tabs(options, value=initial)
    selector
    return (selector,)


@app.cell
def data_schema_details(datasets, latest_columns, mo, pl, quick_statistics, selector, tables):
    # For the selected dataset show the tables as accordions and within each accordion show the list of columns, their types and a description

    selected_schema_id = datasets.filter(pl.col('schema_name') == selector.value)['schema_id'][0]
    filtered_tables = tables.filter(pl.col('schema_id') == selected_schema_id)

    accordion_data = {}

    for _row in filtered_tables.to_dicts():
        _table_id = _row['table_id']
        _table_name = _row['table_name']
        _record_count = quick_statistics.filter(pl.col('table_name') == _table_name).to_dict()['record_count'][0]

        _cols = latest_columns.filter(pl.col('table_id') == _table_id)
        _records = _cols.select(['column_name', 'column_type', 'value']).to_dicts()

        accordion_data[f'{_table_name} ({_record_count} records)'] = mo.ui.table(data=_records)

    mo.accordion(accordion_data, lazy=True)
    return


@app.cell(hide_code=True)
def query_section(mo):
    mo.md(r"""
    ## Query the datasets yourself

    Below you can run queries yourself — use the table names and columns you see above and start exploring the data live!

    The query runs in your browser via DuckDB. You have access to all the data; the only limits are the CPU and RAM of your machine.
    """)
    return


@app.cell
def query_editor(mo):
    initial_code = """SELECT *
FROM openapc.apc
LIMIT 100
"""
    editor = mo.ui.code_editor(value=initial_code, language="sql").form(submit_button_label="Run")
    editor
    return (editor,)


@app.cell
def query_results(duckdb, editor, mo):
    mo.ui.table(duckdb.sql(editor.value)) if editor.value else mo.md("_Run the query above to see results._")
    return


if __name__ == "__main__":
    app.run()
  return


if __name__ == "__main__":
    app.run()
