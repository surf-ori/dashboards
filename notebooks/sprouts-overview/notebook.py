# /// script
# requires-python = ">=3.13"
# dependencies = [
#     "altair==6.0.0",
#     "anywidget==0.9.21",
#     "duckdb==1.5.2",
#     "fsspec==2026.2.0",
#     "marimo>=0.20.2",
#     "numpy==2.4.2",
#     "polars[pyarrow]==1.38.1",
#     "pydantic-ai==1.63.0",
#     "requests==2.32.5",
#     "sqlglot==29.0.1",
#     "traitlets==5.14.3",
# ]
# ///

import marimo

__generated_with = "0.21.1"
app = marimo.App(width="medium", app_title="Open Research Information | Datasets Overview")


@app.cell
def _():
    # # Install the packages when running in WASM
    # import micropip
    # await micropip.install(["polars","duckdb"])
    # import pyodide.http
    return


@app.cell
def _():
    import duckdb
    import marimo as mo
    import polars as pl
    import altair as alt
    import anywidget
    import traitlets

    return anywidget, mo, pl, traitlets


@app.cell
def _():
    from fsspec.implementations.http import HTTPFileSystem

    return


@app.cell(hide_code=True)
def _(mo):
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
def _(mo):
    mo.md(r"""
    This dashboard is part of the [**PID to Portal project**](https://communities.surf.nl/en/open-research-information/article/from-pid-to-portal-strengthening-the-open-research-information) from SURF and UNL.

    Our goals is to create an overview of available and actively queryable Open Research Information Resources / Datasets, that the ORI community can start using freely. [Code available.](https://github.com/surf-ori/sprouts/)
    """)
    return


@app.cell
def _(mo):
    background = mo.md("""
    ## Background                       

    In our approach, we aim to avoid BigTech. We curate the queryable databases ourselves but welcome others to share their data catalogs. We utilize the DuckLake catalog from DuckDB and store the actual data as Parquet files on an S3-compatible object store. This separation of storage and compute helps us keep costs low.

    This catalog lists all the ORI data resoures, their tables and columns. also it holds information about the changes in time of all the data resources, like  deleted, updated and added records, and schema changes.

    This alows you to 'time-travel' in the data, and not only use the last state. For example detecting when an article flips from open access to closed access.

    **Compute:** You can query the datasets directly from your browser (no login required)! When you query the databases, a portion of the requested data is transferred over HTTPS to your local machine where the SQL operations are performed. Larger data requests result in longer transfer times, and the speed of your local machine affects the query completion time. The size of your machine is up to you.

    At SURF, we also provide ready-made [services](https://www.surf.nl/en/services) for SQL computation, such as a Marimo notebook in a virtual machine on SURF Research Cloud or a Superset dashboard on a Kubernetes cluster.

    Below, you will see the ORI data resources we currently curate. (This overview was inspired by the [ORION-DBS initiative](https://orion-dbs.community/).)

    We want to add the following datasets: OpenAIRE, OpenALEX, OpenAPC, ROR, Harvest metadata from CRISes, Harvest metdata from Repositories, Crossref, SURF Journal Catalogue, CWTS Leiden Ranking, ORCID, DOAJ, DOAB, OpenCitations, DataCite, PKP beacon.
    """)
    mo.sidebar(
        background
    )
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ## ORI data catalog

    This URL gives you access to the ORI data catalog. Copy this URL to attach it as a ducklake to your own query engine.
    """)
    return


@app.cell
def _(mo):
    url = mo.ui.text(value='https://objectstore.surf.nl/cea01a7216d64348b7e51e5f3fc1901d:sprouts/catalog.ducklake', full_width=True)
    url
    return (url,)


@app.cell
def _(mo, url):
    _df = mo.sql(
        f"""
        ATTACH 'ducklake:{url.value}' as sprouts;
        USE sprouts;
        """
    )
    return


@app.cell
def _():
    # res = await pyodide.http.pyfetch(url)
    # with open('/catalog.ducklake', 'wb') as f:
    #     f.write(await res.bytes())
    return


@app.cell
def _(mo, url):
    sprouts_df = mo.sql(
        f"""
        -- ATTACH '{url.value}' as __ducklake_metadata_sprouts;
        """
    )
    return


@app.cell
def _(mo):
    quick_statistics = mo.sql(
        f"""
        SELECT table_name, record_count, file_size_bytes
            FROM __ducklake_metadata_sprouts.ducklake_table_stats
            FULL JOIN __ducklake_metadata_sprouts.ducklake_table
        	USING (table_id)
        """,
        output=False
    )
    return (quick_statistics,)


@app.cell
def _(datasets, mo, pl, quick_statistics, tables):
    # Summary Statistics of ORI Datasets

    def _format_gb(size_bytes):
            return f"{size_bytes / (1024 ** 3):,.2f} GB"

    datasets_count = datasets.height

    total_records = quick_statistics.select(pl.col("record_count").sum()).item()

    volume_bytes = tables.select(pl.col("file_size_bytes").sum()).item()


    datasets_stat = mo.stat(
        value=f"{datasets_count:,}",
        label="Datasets",
        caption="Number of datasets",
        bordered=True
    )

    total_records_stat = mo.stat(
        value=f"{total_records:,}",
        label="Total records",
        caption="Total number of records across all datasets",
        bordered=True
    )

    volume_bytes_stat = mo.stat(
        value=_format_gb(volume_bytes),
        label="Data volume",
        caption="Total data volume in GB",
        bordered=True
    )

    mo.hstack([datasets_stat, total_records_stat, volume_bytes_stat], widths="equal", align="center")
    return


@app.cell(hide_code=True)
def _():
    ## ORI data resources
    return


@app.cell
def _(mo):
    datasets = mo.sql(
        f"""
        FROM __ducklake_metadata_sprouts.ducklake_schema
        WHERE schema_name != 'main';
        """,
        output=False
    )
    return (datasets,)


@app.cell(hide_code=True)
def _():
    ## ORI tables
    return


@app.cell
def _(mo):
    tables = mo.sql(
        f"""
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


@app.cell(hide_code=True)
def _():
    ## ORI columns
    return


@app.cell
def _(mo):
    columns = mo.sql(
        f"""
        SELECT *
        FROM __ducklake_metadata_sprouts.ducklake_column c
        JOIN __ducklake_metadata_sprouts.ducklake_column_tag t
        ON c.column_id = t.column_id
        """,
        output=False
    )
    return (columns,)


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ## ORI datasets, tables, columns
    """)
    return


@app.cell
def _(columns):
    # polars DataFrame operations: sort and take first row of each group
    latest_columns = (
        columns.sort('begin_snapshot')
               .unique(subset=['table_id', 'column_id'], keep='first')
    )
    return (latest_columns,)


@app.cell
def _(dataset_details, datasets, mo):
    # build a tabs selector from polars DataFrame
    options = {
        row['schema_name']: dataset_details(row['schema_name']) 
        for row in datasets.to_dicts()
    }
    initial = datasets['schema_name'][0]
    selector = mo.ui.tabs(options, value=initial)
    selector
    return


@app.cell
def _():
    # TODO: add information about the selected dataset, at least the date_Created and date_lastUpdated, Also the description, and the link to the orginal source, and the licence

    #"Description, dateCreated, dateLastUpdated, link to source, Licence"
    return


@app.cell
def _(datasets, latest_columns, mo, pl, quick_statistics, tables):
    def dataset_details(schema_name):
        # For the selected dataset show the tables as accordeons and within each accordion show the list of colums, their types and a description

        # Determine the selected schema ID based on the user's selection
        selected_schema_id = datasets.filter(pl.col('schema_name') == schema_name)['schema_id'][0]

        # Filter tables that belong to the selected schema
        filtered_tables = tables.filter(pl.col('schema_id') == selected_schema_id)

        # Initialize a dictionary to hold the accordion data
        accordion_data = {}

        # Iterate over each table in the filtered tables
        for row in filtered_tables.to_dicts():
            table_id = row['table_id']
            table_name = row['table_name']
            record_count = quick_statistics.filter(pl.col('table_name') == table_name).to_dict()['record_count'][0]

            # Filter columns that belong to the current table
            cols = latest_columns.filter(pl.col('table_id') == table_id)

            # Select relevant column details and convert to dictionaries
            records = cols.select(['column_name', 'column_type', 'value']).to_dicts()

            # Create a marimo UI table for the current table's columns and add it to the accordion data
            accordion_data[f'{table_name} ({record_count} records)'] = mo.ui.table(data=records)

        # Display the accordion with the collected table data
        return mo.accordion(accordion_data)

    return (dataset_details,)


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ## Query the data right here

    Below you can run queries yourself, use the table names and colums you see above, and start exploring the data live!

    This of course will run in your browser. You have access to all the data, you are now not limited to you imagination, but the limitation now the CPU and RAM of your computer.
          
    As a start, try for example the following query: `SELECT institution, avg(euro) FROM openapc.apc GROUP BY institution ORDER BY avg(euro) DESC;`
    """)
    return


@app.cell
def _(mo):
    mo.iframe('''
    <iframe src="https://shell.duckdb.org/#queries=v0,ATTACH-'ducklake%3Ahttps%3A%2F%2Fobjectstore.surf.nl%2Fcea01a7216d64348b7e51e5f3fc1901d%3Asprouts%2Fcatalog.ducklake'-AS-sprouts~-USE-sprouts~" style="width: 100%; height: 450px"></iframe>
    ''', height=500)
    return


@app.cell
def _(mo):
    # initial_code = """SELECT * 
    # FROM openapc.apc 
    # LIMIT 100"""

    # editor = mo.ui.code_editor(value=initial_code, language="sql").form(submit_button_label="Run")
    # editor
    return (editor,)


@app.cell
def _():
    # mo.ui.table(duckdb.sql(editor.value))
    return


@app.cell
def _(DuckLake, mo):
    # ducklake = mo.ui.anywidget(DuckLake())
    # ducklake
    return (ducklake,)


@app.cell
def _(ducklake, editor):
    # ducklake.query = editor.value
    return


@app.cell
def _(ducklake, mo, pl):
    # (
    #     mo.callout(ducklake.diagnostics['message'], kind='danger')
    #     if ducklake.diagnostics['status'] == 'error' else
    #     mo.ui.table(pl.DataFrame(ducklake.result, orient='row'))         
    # )
    return


@app.cell
def _(anywidget, traitlets):
    class DuckLake(anywidget.AnyWidget):
      # Widget front-end JavaScript code
      _esm = """
        import * as duckdb from "https://cdn.jsdelivr.net/npm/@duckdb/duckdb-wasm@1.33.1-dev36.0/+esm";
        import { tableToIPC } from 'https://cdn.jsdelivr.net/npm/apache-arrow@21.1.0/+esm';

        async function getDb() {
            // @ts-ignore
            //if (window._db) return window._db;
            const JSDELIVR_BUNDLES = duckdb.getJsDelivrBundles();

            // Select a bundle based on browser checks
            const bundle = await duckdb.selectBundle(JSDELIVR_BUNDLES);

            const worker_url = URL.createObjectURL(
                new Blob([`importScripts("${bundle.mainWorker}");`], {
                    type: "text/javascript",
                })
            );

            // Instantiate the asynchronous version of DuckDB-wasm
            const worker = new Worker(worker_url);

            const logger = new duckdb.ConsoleLogger();
            const db = new duckdb.AsyncDuckDB(logger, worker);

            await db.instantiate(bundle.mainModule, bundle.pthreadWorker, (progress) => {});
            URL.revokeObjectURL(worker_url);
            //window._db = db;
            return db;
        }

        async function getConnection() {
            const db = await getDb();
            var conn = await db.connect();
            return conn;
        }

        async function attachDucklake(conn, ducklake_url) {
            await executeQuery(conn, "LOAD httpfs;");
            await executeQuery(conn, "LOAD ducklake;");
            await executeQuery(conn, "USE memory; DETACH db;");
            await executeQuery(conn, "ATTACH 'ducklake:" + ducklake_url + "' as db; USE db;")
            return conn
        }

        async function executeQuery(conn, query, setup_queries = "") {
                var error = null;
                const startTime = performance.now();
                var results = await conn.query(setup_queries + query).catch((e) => {error = e; console.log(e);});
                const runTime = performance.now() - startTime;

                return {results, error, runTime};
        }

        async function render({ model, el }) {
          var conn = await getConnection();
          await attachDucklake(conn, "https://objectstore.surf.nl/cea01a7216d64348b7e51e5f3fc1901d:sprouts/catalog.ducklake");
          model.on("change:query", async () => {
              BigInt.prototype.toJSON = function () {
                  return { $bigint: this.toString() };
                };
              model.set("diagnostics", {status: "running", message: ""});
              model.save_changes();
              let msg = model.get("query");
              let res = await executeQuery(conn, msg);
              console.log(res)
              if (res.error !== null) {
                  model.set("diagnostics", {status: "error", message: res.error.message});
                  model.set("result", []);
              } else {
                  model.set("diagnostics", {status: "success", message: ""});
                  model.set("result", res.results.toArray().map((row) => row.toJSON()));
              }
              model.save_changes();
         });
        }

        export default { render };
      """

      _css = """
        .counter-button {
          background-image: linear-gradient(to right, #a1c4fd, #c2e9fb);
          border: 0;
          border-radius: 10px;
          padding: 10px 50px;
          color: white;
        }
        """

      query = traitlets.Unicode("").tag(sync=True)
      diagnostics = traitlets.Dict({"status": "", "message": ""}).tag(sync=True)
      result = traitlets.List([]).tag(sync=True)

    return (DuckLake,)


if __name__ == "__main__":
    app.run()
