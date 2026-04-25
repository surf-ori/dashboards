# /// script
# requires-python = ">=3.12"
# dependencies = [
#     "altair==6.1.0",
#     "duckdb==1.5.2",
#     "marimo>=0.10.0",
#     "openpyxl==3.1.5",
#     "pandas==3.0.2",
#     "polars[pyarrow]==1.39.3",
#     "pyarrow==24.0.0",
#     "sqlglot==30.6.0",
# ]
# ///

import marimo

__generated_with = "0.23.3"
app = marimo.App(width="full", app_title="ORI Data Quality Dashboard")


@app.cell(hide_code=True)
async def wasm_dependencies():
    # install packages not bundled by the WASM export (duckdb is handled by mo.sql)
    import sys
    import io
    import openpyxl
    _ = None
    if 'pyodide' in sys.modules:
        import micropip
        await micropip.install(['polars', 'pyarrow', 'altair', 'openpyxl'])
        _ = 'wasm'
    return io, openpyxl, sys


@app.cell(hide_code=True)
def imports():
    # import all third-party libraries used throughout the notebook
    import marimo as mo
    import polars as pl
    import altair as alt
    from datetime import date

    return alt, date, mo, pl


@app.cell(hide_code=True)
def catalog_url(mo):
    # text input widget for the ORI DuckLake catalog URL
    url = mo.ui.text(
        value='https://objectstore.surf.nl/cea01a7216d64348b7e51e5f3fc1901d:sprouts/catalog.ducklake',
        full_width=True,
    )
    mo.accordion({"DuckLake catalog URL": url})
    return (url,)


@app.cell(hide_code=True)
def attach_catalog(mo, url):
    # attach the ORI DuckLake catalog to the shared mo.sql DuckDB connection
    mo.sql(
        f"""
        ATTACH '{url.value}' AS sprouts (TYPE ducklake, READ_ONLY);
        USE sprouts;
        """
    )
    return


@app.cell(hide_code=True)
async def load_nl_baseline(io, openpyxl, pl, sys):
    # fetch the Dutch research organisations baseline list from Zenodo (DOI: 10.5281/zenodo.18957154)
    # and annotate each org with its Barcelona Declaration signatory status

    _ZENODO_URL = 'https://zenodo.org/api/records/18957154/files/nl-orgs-baseline.xlsx/content'

    # ROR IDs of NL Barcelona Declaration signatories (source: barcelona-declaration.org/signatories_by_country/)
    _BARCELONA_RORS = {
        'https://ror.org/02e2c7k09',  # Delft University of Technology
        'https://ror.org/04jsz6e67',  # Dutch Research Council (NWO)
        'https://ror.org/02w4jbg70',  # KB, National Library of the Netherlands
        'https://ror.org/027bh9e22',  # Leiden University
        'https://ror.org/00rbjv475',  # Netherlands eScience Center
        'https://ror.org/043c0p156',  # Royal Netherlands Academy of Arts and Sciences (KNAW)
        'https://ror.org/009vhk114',  # SURF
        'https://ror.org/04dkp9463',  # University of Amsterdam
        'https://ror.org/012p63287',  # University of Groningen
        'https://ror.org/04pp8hn57',  # Utrecht University
        'https://ror.org/008xxew50',  # Vrije Universiteit Amsterdam
    }

    if 'pyodide' in sys.modules:
        import pyodide.http
        _resp = await pyodide.http.pyfetch(_ZENODO_URL)
        _raw = await _resp.bytes()
    else:
        import urllib.request
        with urllib.request.urlopen(_ZENODO_URL) as _r:
            _raw = _r.read()

    _wb = openpyxl.load_workbook(io.BytesIO(_raw), read_only=True)
    _ws = _wb['nl-orgs']
    _all_rows = list(_ws.iter_rows(values_only=True))
    _data_rows = [r for r in _all_rows[1:] if r[0] and r[3]]  # skip header + empty rows

    nl_baseline_df = pl.DataFrame({
        'full_name': [r[0] for r in _data_rows],
        'acronym':   [r[1] or '' for r in _data_rows],
        'grouping':  [r[3] for r in _data_rows],
        'ror':       [r[5] if r[5] and r[5] != 'nvt' else None for r in _data_rows],
    }).with_columns(
        pl.col('ror').is_in(_BARCELONA_RORS).fill_null(False).alias('barcelona_signatory'),
    )
    return (nl_baseline_df,)


@app.cell(hide_code=True)
def load_nl_institutions(mo):
    # query NL education and funder institutions from the OpenAlex institutions catalog table
    nl_inst_df = mo.sql(
        """
        -- Load data about Dutch Institutions in OpenAlex
        SELECT
            display_name,
            ror,
            type,
            works_count,
            cited_by_count,
            ids.ror      IS NOT NULL AS has_ror,
            ids.grid     IS NOT NULL AS has_grid,
            ids.wikidata IS NOT NULL AS has_wikidata,
            ids.wikipedia IS NOT NULL AS has_wikipedia,
            homepage_url IS NOT NULL AS has_homepage,
            ids.grid      AS grid_id,
            ids.wikidata  AS wikidata_id,
            ids.wikipedia AS wikipedia_url,
            homepage_url
        FROM openalex.institutions
        WHERE country_code = 'NL'
          AND type IN ('education', 'funder')
          AND works_count > 1000
        ORDER BY works_count DESC
        """,
        output=False
    )
    return (nl_inst_df,)


@app.cell(hide_code=True)
def load_openaire_orgs(mo, pl):
    # query NL organisations from the OpenAIRE organizations catalog table and extract PIDs
    oa_orgs_df = mo.sql("""
    SELECT
        legalName,
        legalShortName,
        websiteUrl,
        id AS openaire_id,
        pids
    FROM openaire.organizations
    WHERE country.code = 'NL'
    """, output=False)
    # Extract ROR from pids list
    oa_orgs_df = oa_orgs_df.with_columns(
        pl.col('pids').list.eval(
            pl.when(pl.element().struct.field('scheme').str.to_uppercase() == 'ROR')
            .then(pl.element().struct.field('value'))
        ).list.drop_nulls().list.first().alias('ror'),
        pl.col('pids').list.eval(
            pl.when(pl.element().struct.field('scheme').str.to_uppercase() == 'ISNI')
            .then(pl.element().struct.field('value'))
        ).list.drop_nulls().list.first().alias('isni'),
        pl.col('pids').list.eval(
            pl.when(pl.element().struct.field('scheme').str.to_uppercase() == 'GRID')
            .then(pl.element().struct.field('value'))
        ).list.drop_nulls().list.first().alias('grid'),
        pl.col('pids').list.eval(
            pl.when(pl.element().struct.field('scheme').str.to_uppercase() == 'WIKIDATA')
            .then(pl.element().struct.field('value'))
        ).list.drop_nulls().list.first().alias('wikidata'),
    )
    return (oa_orgs_df,)


@app.cell(hide_code=True)
def load_source_record_counts(mo, nl_baseline_df, oa_orgs_df, org_select, pl):
    # Fast record counts for selected organisations across sources.
    # Avoid scanning the huge works/publications tables (364 M and 206 M rows).

    _sel_rors = (
        nl_baseline_df
        .filter(pl.col('full_name').is_in(org_select.value))
        ['ror'].drop_nulls().to_list()
    )
    _rors_clause = ', '.join(f"'{r}'" for r in _sel_rors) if _sel_rors else "''"

    # OpenAlex: SUM pre-computed works_count from the institutions table (120 K rows).
    # Scanning openalex.works (364 M rows) to count via UNNEST took 15+ minutes.
    openalex_works_df = mo.sql(f"""
    SELECT COALESCE(SUM(works_count), 0) AS openalex_works_count
    FROM openalex.institutions
    WHERE ror IN ({_rors_clause})
    """, output=False)

    # OpenAIRE: count matched organisations from oa_orgs_df (already in memory, 448 K rows).
    # Scanning openaire.publications (206 M rows) via UNNEST took 15+ minutes.
    # The org count gives an idea of OpenAIRE presence without a full table scan.
    openaire_pubs_df = (
        oa_orgs_df
        .filter(pl.col('ror').is_in(_sel_rors))
        .select(pl.len().alias('openaire_pubs_count'))
    )

    # CRIS: COUNT(*) on 2.4 M rows filtered by repository_info.ror — fast.
    cris_pubs_df = mo.sql(f"""
    SELECT COUNT(*) AS cris_pubs_count
    FROM cris.publications
    WHERE repository_info.ror IS NOT NULL
      AND repository_info.ror IN ({_rors_clause})
    """, output=False)
    return cris_pubs_df, openaire_pubs_df, openalex_works_df


@app.cell(hide_code=True)
def load_works_completeness(mo, pl):
    # aggregate field-level completeness from first OpenAlex works shard (data_0) via direct parquet read
    # the full works table spans 732 files; querying data_0 only keeps this cell fast
    _WORKS_URL = 'https://objectstore.surf.nl/cea01a7216d64348b7e51e5f3fc1901d:sprouts/data/openalex/works/data_0.parquet'
    _raw = mo.sql(f"""
    SELECT
        COUNT(*)::BIGINT                                             AS total,
        COUNT(doi)::BIGINT                                           AS has_doi,
        COUNT(language)::BIGINT                                      AS has_language,
        COUNT(type)::BIGINT                                          AS has_type,
        COUNT_IF(open_access.is_oa)::BIGINT                          AS is_oa,
        COUNT_IF(array_length(funders) > 0)::BIGINT                  AS has_funder,
        COUNT(license)::BIGINT                                       AS has_license,
        COUNT(abstract_inverted_index)::BIGINT                       AS has_abstract,
        COUNT(primary_location.source.issn_l)::BIGINT                AS has_issn,
        COUNT_IF(array_length(corresponding_author_ids) > 0)::BIGINT AS has_corresponding_author,
        COUNT(publication_year)::BIGINT                              AS has_year,
        COUNT_IF(array_length(concepts) > 0)::BIGINT                 AS has_concepts
    FROM read_parquet('{_WORKS_URL}')
    """, output=False)

    total_works = _raw['total'][0]
    works_compl_df = pl.DataFrame({
        'field': [
            'DOI', 'Language', 'Publication Type', 'Open Access Status',
            'Funder / Grant', 'License', 'Abstract', 'ISSN (Journal)',
            'Corresponding Author', 'Publication Year', 'Topics / Concepts'
        ],
        'label': [
            'DOI', 'Language', 'Type', 'OA Status',
            'Funder', 'License', 'Abstract', 'ISSN',
            'Corresp. Author', 'Year', 'Concepts'
        ],
        'has_value': [
            _raw['has_doi'][0], _raw['has_language'][0], _raw['has_type'][0],
            _raw['is_oa'][0],   _raw['has_funder'][0],  _raw['has_license'][0],
            _raw['has_abstract'][0], _raw['has_issn'][0], _raw['has_corresponding_author'][0],
            _raw['has_year'][0], _raw['has_concepts'][0],
        ],
        'total': [total_works] * 11,
    }).with_columns(
        (pl.col('has_value') * 100.0 / pl.col('total')).round(1).alias('pct'),
        (pl.col('total') - pl.col('has_value')).alias('missing'),
    )
    return (works_compl_df,)


@app.cell(hide_code=True)
def constants():
    # define static lookup lists and brand colours used across UI and charts
    CERIF_ENTITIES = [
        'Outputs: Publications',
        'Outputs: Products',
        'Core: Organisation units',
        'Core: Persons',
        'Core: Projects',
    ]
    SOURCES = ['OpenAlex', 'OpenAIRE', 'Crossref', 'ORCID', 'ROR', 'DataCite']
    PUB_TYPES = ['All', 'Journal Article', 'Conference Paper', 'Book Chapter', 'Preprint', 'Thesis', 'Report']
    GROUPING_LABELS = {
        'UNL':   'UNL — Research Universities',
        'VH':    'VH — Universities of Applied Sciences',
        'KNAW':  'KNAW — Royal Academy Institutes',
        'NFU':   'NFU — University Medical Centres',
        'NWO':   'NWO — Research Council',
        'NWO-i': 'NWO-i — NWO Institutes',
        'GOV':   'GOV — Government Research Orgs',
        'INDP':  'INDP — Independent Institutes',
        'UN':    'UN — Other / International',
    }
    return CERIF_ENTITIES, GROUPING_LABELS, PUB_TYPES, SOURCES


@app.cell(hide_code=True)
def ui_base_filters(CERIF_ENTITIES, GROUPING_LABELS, PUB_TYPES, SOURCES, mo):
    # create sector/type/source widgets — these must exist before org_select so that
    # org_select options can be filtered reactively by group_select and barcelona_toggle
    group_select = mo.ui.multiselect(
        options=list(GROUPING_LABELS.keys()),
        value=list(GROUPING_LABELS.keys()),
        label=f"{mo.icon('lucide:layers')} Sector Group",
    )
    barcelona_toggle = mo.ui.switch(
        value=False,
        label=f"{mo.icon('lucide:award')} Barcelona Declaration signatories only",
    )
    entity_select = mo.ui.multiselect(
        options=CERIF_ENTITIES,
        value=['Outputs: Publications'],
        label=f"{mo.icon('lucide:boxes')} CERIF Entity",
    )
    source_select = mo.ui.multiselect(
        options=SOURCES,
        value=['OpenAlex'],
        label=f"{mo.icon('lucide:database')} Primary Source",
    )
    pub_type_select = mo.ui.multiselect(
        options=PUB_TYPES,
        value=['All'],
        label=f"{mo.icon('lucide:file-type')} Type",
    )
    return (
        barcelona_toggle,
        entity_select,
        group_select,
        pub_type_select,
        source_select,
    )


@app.cell(hide_code=True)
def ui_org_select(barcelona_toggle, group_select, mo, nl_baseline_df, pl):
    # build org_select options filtered by the current sector group and Barcelona toggle
    _opts = nl_baseline_df
    if group_select.value:
        _opts = _opts.filter(pl.col('grouping').is_in(group_select.value))
    if barcelona_toggle.value:
        _opts = _opts.filter(pl.col('barcelona_signatory'))
    _org_options = sorted(_opts['full_name'].to_list())
    _default = [v for v in ['University of Amsterdam'] if v in _org_options]
    org_select = mo.ui.multiselect(
        options=_org_options,
        value=_default,
        label=f"{mo.icon('lucide:landmark')} Organisation",
    )
    return (org_select,)


@app.cell(hide_code=True)
def selected_org(
    barcelona_toggle,
    group_select,
    nl_baseline_df,
    nl_inst_df,
    org_select,
    pl,
):
    # apply sector-group and Barcelona Declaration filters to the baseline
    _filtered = nl_baseline_df
    if group_select.value:
        _filtered = _filtered.filter(pl.col('grouping').is_in(group_select.value))
    if barcelona_toggle.value:
        _filtered = _filtered.filter(pl.col('barcelona_signatory'))
    filtered_baseline = _filtered

    # resolve selected org names → RORs → OpenAlex rows (for KPI cards)
    _sel_rors = (
        nl_baseline_df
        .filter(pl.col('full_name').is_in(org_select.value))
        ['ror'].drop_nulls().to_list()
    )
    sel_org = nl_inst_df.filter(pl.col('ror').is_in(_sel_rors))
    return filtered_baseline, sel_org


@app.cell(hide_code=True)
def header(mo):
    # render the dashboard title and subtitle as markdown
    mo.md("""
    # ORI Data Quality Dashboard
    **Metadata completeness, coverage & enrichment for Dutch research organisations**
    *(OpenAlex · OpenAIRE · Crossref · ORCID — Barcelona Declaration on Open Research Information)*
    """)
    return


@app.cell(hide_code=True)
def sidebar(
    barcelona_toggle,
    entity_select,
    group_select,
    mo,
    org_select,
    pub_type_select,
    source_select,
):
    # stack filter dropdowns and mount them in the marimo sidebar
    filters = mo.vstack([
        mo.md("### Filters"),
        mo.md("---"),
        org_select,
        mo.md("---"),
        group_select,
        barcelona_toggle,
        mo.md("---"),
        entity_select,
        source_select,
        pub_type_select,
    ], gap=1, align='end')
    mo.sidebar(filters, width="350px")
    return


@app.cell(hide_code=True)
def overview(
    cris_pubs_df,
    filtered_baseline,
    mo,
    nl_baseline_df,
    oa_orgs_df,
    openaire_pubs_df,
    openalex_works_df,
    org_select,
    pl,
):
    # build the overview tab: KPI stat cards, baseline org table, and getting-started guide
    _n_baseline    = nl_baseline_df.height
    _n_filtered    = filtered_baseline.height
    _n_barcelona   = nl_baseline_df.filter(pl.col('barcelona_signatory')).height
    _n_oa_orgs     = oa_orgs_df.height

    # Get record counts from sources
    _openalex_works = openalex_works_df['openalex_works_count'][0] if openalex_works_df.height > 0 else 0
    _openaire_pubs = openaire_pubs_df['openaire_pubs_count'][0] if openaire_pubs_df.height > 0 else 0
    _cris_pubs = cris_pubs_df['cris_pubs_count'][0] if cris_pubs_df.height > 0 else 0

    # Build caption showing selected institution(s), truncated if too many
    _sel = org_select.value
    _sel_caption = (
        ', '.join(_sel[:2]) + (f' +{len(_sel)-2} more' if len(_sel) > 2 else '')
        if _sel else '(none selected)'
    )

    _kpis = mo.hstack([
        mo.stat(
            value=f"{_n_filtered}",
            label="NL Research Orgs (filtered)",
            caption=f"{_n_baseline} total in Zenodo baseline",
            bordered=True,
        ),
        mo.stat(
            value=f"{_n_barcelona}",
            label="Barcelona Declaration NL",
            caption="Signatories matched by ROR",
            bordered=True,
        ),
        mo.stat(
            value=f"{_openalex_works:,}",
            label="OpenAlex Works (est.)",
            caption=_sel_caption,
            bordered=True,
        ),
        mo.stat(
            value=f"{_openaire_pubs:,}",
            label="OpenAIRE Orgs matched",
            caption=_sel_caption,
            bordered=True,
        ),
        mo.stat(
            value=f"{_cris_pubs:,}",
            label="CRIS Publications",
            caption=_sel_caption,
            bordered=True,
        ),
        mo.stat(
            value="7",
            label="Sources monitored",
            caption="OpenAlex, OpenAIRE, Crossref, ORCID, ROR, DataCite, CRIS",
            bordered=True,
        ),
    ], gap=3, wrap=True)

    _SECTOR_FULL = {
        'UNL': 'Research Universities', 'VH': 'Univ. Applied Sciences',
        'KNAW': 'Royal Academy Institutes', 'NFU': 'Univ. Medical Centres',
        'NWO': 'Research Council', 'NWO-i': 'NWO Institutes',
        'GOV': 'Government Research', 'INDP': 'Independent Institutes',
        'UN': 'Other / International',
    }
    _org_tbl_data = filtered_baseline.with_columns(
        pl.col('grouping').replace(_SECTOR_FULL).alias('Sector'),
        pl.when(pl.col('barcelona_signatory')).then(pl.lit('✓')).otherwise(pl.lit('')).alias('Barcelona'),
    ).select([
        pl.col('full_name').alias('Organisation'),
        pl.col('acronym').alias('Acronym'),
        pl.col('Sector'),
        pl.col('ror').alias('ROR'),
        pl.col('Barcelona'),
    ])

    _guide = mo.md("""
    ## Getting started

    Use the **filters** (left panel) to focus on a specific organisation, sector group, or Barcelona Declaration signatories.
    The tabs below explore different aspects of data quality:

    - **Completeness** — which metadata fields are present, and for what share of records
    - **Coverage** — which records appear in which open sources, and where there are gaps
    - **Accuracy** — how correctly formatted and valid the field values are
    - **Enrichment** — opportunities to add missing identifiers from external registries

    Data is drawn from the ORI DuckLake catalog (Parquet snapshots on SURF Object Store).
    *Part of the [PID to Portal](https://communities.surf.nl/en/open-research-information) project.*
    """)

    _overview_content = mo.vstack([
        _kpis,
        mo.md(f"### NL Research Organisations *(source: [Zenodo baseline](https://zenodo.org/records/18957154))* — {_n_filtered} of {_n_baseline} shown"),
        mo.ui.table(_org_tbl_data.to_pandas(), selection=None, page_size=15),
        _guide,
    ], gap=4)
    _overview_content
    return


@app.cell(hide_code=True)
def completeness(
    alt,
    entity_select,
    mo,
    nl_inst_df,
    org_select,
    pl,
    sel_org,
    works_compl_df,
):
    # build the completeness tab: publication field completeness chart and institution identifier completeness
    # -----------------------------------------------------------------------
    # Publications completeness (works sample from OpenAlex)
    # -----------------------------------------------------------------------
    _pub_bar = (
        alt.Chart(works_compl_df.to_pandas())
        .mark_bar()
        .encode(
            x=alt.X('pct:Q', title='Completeness %', scale=alt.Scale(domain=[0, 100])),
            y=alt.Y('field:N', sort='-x', title=''),
            color=alt.Color(
                'pct:Q',
                scale=alt.Scale(
                    domain=[0, 50, 80, 100],
                    range=['#e74c3c', '#f39c12', '#f39c12', '#2ecc71'],
                ),
                legend=None,
            ),
            tooltip=[
                alt.Tooltip('field:N', title='Field'),
                alt.Tooltip('pct:Q', title='Completeness %', format='.1f'),
                alt.Tooltip('has_value:Q', title='Records with field', format=','),
                alt.Tooltip('missing:Q', title='Missing', format=','),
            ],
        )
        .properties(
            title=f'Publication field completeness — OpenAlex sample ({works_compl_df["total"][0]:,} works)',
            height=320,
            width='container',
        )
    )

    # Text labels
    _pub_text = (
        alt.Chart(works_compl_df.to_pandas())
        .mark_text(dx=4, align='left')
        .encode(
            x=alt.X('pct:Q'),
            y=alt.Y('field:N', sort='-x'),
            text=alt.Text('pct:Q', format='.1f'),
        )
    )

    _pub_chart = (_pub_bar + _pub_text).configure_view(strokeWidth=0)

    # -----------------------------------------------------------------------
    # Metric cards (top fields)
    # -----------------------------------------------------------------------
    _top_fields = works_compl_df.sort('pct').head(5)

    def _status_color(pct):
        return '#2ecc71' if pct >= 80 else ('#f39c12' if pct >= 50 else '#e74c3c')

    _metric_stats = mo.hstack([
        mo.stat(
            value=f"{row['pct']:.0f}%",
            label=row['label'],
            caption=f"{row['missing']:,} missing",
            bordered=True,
        )
        for row in _top_fields.to_dicts()
    ], gap=2, wrap=True)

    # -----------------------------------------------------------------------
    # Institutions identifier completeness
    # -----------------------------------------------------------------------
    _id_fields = ['has_ror', 'has_grid', 'has_wikidata', 'has_wikipedia', 'has_homepage']
    _id_labels = ['ROR', 'GRID', 'Wikidata', 'Wikipedia', 'Homepage URL']
    _id_pcts   = [
        round(nl_inst_df[f].sum() * 100 / nl_inst_df.height, 1)
        for f in _id_fields
    ]
    _inst_compl_df = pl.DataFrame({
        'Identifier': _id_labels,
        'pct':        _id_pcts,
        'count':      [nl_inst_df[f].sum() for f in _id_fields],
        'total':      [nl_inst_df.height] * 5,
    })
    _inst_bar = (
        alt.Chart(_inst_compl_df.to_pandas())
        .mark_bar()
        .encode(
            x=alt.X('pct:Q', title='% of NL institutions', scale=alt.Scale(domain=[0, 105])),
            y=alt.Y('Identifier:N', sort='-x', title=''),
            color=alt.Color(
                'pct:Q',
                scale=alt.Scale(domain=[0, 50, 80, 100], range=['#e74c3c', '#f39c12', '#f39c12', '#2ecc71']),
                legend=None,
            ),
            tooltip=[
                alt.Tooltip('Identifier:N'),
                alt.Tooltip('pct:Q', format='.1f', title='%'),
                alt.Tooltip('count:Q', title='# institutions'),
                alt.Tooltip('total:Q', title='total NL institutions'),
            ],
        )
        .properties(
            title=f'Institution identifier completeness — {nl_inst_df.height} NL institutions (OpenAlex)',
            height=200, width='container',
        )
    )

    # -----------------------------------------------------------------------
    # Selected org detail
    # -----------------------------------------------------------------------
    _sel_fields = []
    for _f, _lbl in zip(_id_fields, _id_labels):
        _val = sel_org[_f].any() if sel_org.height > 0 else False
        _sel_fields.append({'Identifier': _lbl, 'Present': '✓' if _val else '✗'})

    _sel_tbl = mo.ui.table(
        pl.DataFrame(_sel_fields).to_pandas(),
        selection=None,
        label=f"Identifier completeness — {', '.join(org_select.value)}",
    ) if sel_org.height > 0 else mo.md('_(no institution selected)_')

    # -----------------------------------------------------------------------
    # SQL query reference
    # -----------------------------------------------------------------------
    _sql = mo.callout(mo.md("""
    **Query used (OpenAlex institutions):**
    ```sql
    SELECT
    display_name,
    ids.ror      IS NOT NULL AS has_ror,
    ids.grid     IS NOT NULL AS has_grid,
    ids.wikidata IS NOT NULL AS has_wikidata,
    ids.wikipedia IS NOT NULL AS has_wikipedia,
    homepage_url IS NOT NULL AS has_homepage
    FROM read_parquet(
      'https://objectstore.surf.nl/.../data/openalex/institutions/data_0.parquet'
    )
    WHERE country_code = 'NL' AND type IN ('education', 'funder')
    ```
    """), kind='info')

    # -----------------------------------------------------------------------
    # Assemble completeness tab
    # -----------------------------------------------------------------------
    _pub_section = mo.vstack([
        mo.md('### Publications metadata completeness *(OpenAlex sample — first parquet shard)*'),
        mo.md('*Fields ranked by completeness. Select a bar to inspect missing records.*'),
        _metric_stats,
        _pub_chart,
    ], gap=2)

    _inst_section = mo.vstack([
        mo.md('### Organisation identifier completeness *(OpenAlex NL institutions)*'),
        _inst_bar,
        _sel_tbl,
    ], gap=2)

    _completeness_content = mo.vstack([
        _pub_section if any(v.startswith('Outputs') for v in entity_select.value) else _inst_section,
        mo.accordion({'SQL query details': _sql}),
    ], gap=4)

    _completeness_content
    return


@app.cell(hide_code=True)
def coverage(alt, mo, nl_inst_df, oa_orgs_df, pl):
    # build the coverage tab: cross-source institution presence and PID completeness comparison
    # -----------------------------------------------------------------------
    # Join OpenAlex institutions with OpenAIRE organizations by ROR
    # -----------------------------------------------------------------------
    _openalex_nl = nl_inst_df.select([
        pl.col('display_name').alias('Institution'),
        pl.col('ror'),
        pl.col('works_count').alias('OpenAlex works'),
        pl.col('has_grid').alias('OpenAlex GRID'),
        pl.col('has_wikidata').alias('OpenAlex Wikidata'),
    ])

    _openaire_nl = oa_orgs_df.filter(pl.col('ror').is_not_null()).select([
        pl.col('ror'),
        pl.col('legalName').alias('OpenAIRE name'),
        pl.col('isni').is_not_null().alias('OpenAIRE ISNI'),
        pl.col('grid').is_not_null().alias('OpenAIRE GRID'),
        pl.col('wikidata').is_not_null().alias('OpenAIRE Wikidata'),
    ])

    _coverage_df = _openalex_nl.join(_openaire_nl, on='ror', how='left').with_columns(
        pl.col('OpenAIRE name').is_not_null().alias('In OpenAIRE'),
    )

    # -----------------------------------------------------------------------
    # Coverage bar: how many NL universities are in OpenAlex vs OpenAIRE
    # -----------------------------------------------------------------------
    _n_in_openalex  = _coverage_df.height
    _n_in_openaire  = _coverage_df.filter(pl.col('In OpenAIRE')).height
    _n_only_openalex = _n_in_openalex - _n_in_openaire

    _bar_data = pl.DataFrame({
        'Source': ['OpenAlex', 'OpenAIRE'],
        'Count':  [_n_in_openalex, _n_in_openaire],
        'Description': ['NL education institutions', 'Matched via ROR'],
    })

    _cov_bar = (
        alt.Chart(_bar_data.to_pandas())
        .mark_bar(cornerRadius=4)
        .encode(
            x=alt.X('Source:N', title=''),
            y=alt.Y('Count:Q', title='# NL institutions'),
            color=alt.Color('Source:N', scale=alt.Scale(
                domain=['OpenAlex', 'OpenAIRE'],
                range=['#009de0', '#2ecc71'],
            )),
            tooltip=['Source:N', 'Count:Q', 'Description:N'],
        )
        .properties(title='NL institution coverage per source', height=220, width=300)
    )

    # -----------------------------------------------------------------------
    # PID completeness: OpenAlex vs OpenAIRE side-by-side
    # -----------------------------------------------------------------------
    _pid_data = pl.DataFrame({
        'PID': ['ROR', 'GRID', 'Wikidata'],
        'OpenAlex %': [
            round(_coverage_df['has_ror'].sum() * 100 / _coverage_df.height, 1) if 'has_ror' in _coverage_df.columns else 100.0,
            round(nl_inst_df['has_grid'].sum() * 100 / nl_inst_df.height, 1),
            round(nl_inst_df['has_wikidata'].sum() * 100 / nl_inst_df.height, 1),
        ],
        'OpenAIRE %': [
            round(_openaire_nl.height * 100 / _openalex_nl.height, 1),
            round(_openaire_nl['OpenAIRE GRID'].sum() * 100 / _openaire_nl.height, 1),
            round(_openaire_nl['OpenAIRE Wikidata'].sum() * 100 / _openaire_nl.height, 1),
        ],
    })

    _pid_long = _pid_data.unpivot(
        index='PID', on=['OpenAlex %', 'OpenAIRE %'], variable_name='Source', value_name='pct'
    )

    _pid_chart = (
        alt.Chart(_pid_long.to_pandas())
        .mark_bar()
        .encode(
            x=alt.X('pct:Q', title='% with identifier', scale=alt.Scale(domain=[0, 105])),
            y=alt.Y('PID:N', title=''),
            color=alt.Color('Source:N', scale=alt.Scale(
                domain=['OpenAlex %', 'OpenAIRE %'],
                range=['#009de0', '#2ecc71'],
            )),
            yOffset='Source:N',
            tooltip=['PID:N', 'Source:N', alt.Tooltip('pct:Q', format='.1f')],
        )
        .properties(title='PID completeness: OpenAlex vs OpenAIRE (NL institutions)', height=200, width='container')
    )

    # -----------------------------------------------------------------------
    # Coverage table
    # -----------------------------------------------------------------------
    _cov_tbl_df = _coverage_df.select([
        'Institution',
        pl.lit('✓').alias('OpenAlex'),
        pl.when(pl.col('In OpenAIRE')).then(pl.lit('✓')).otherwise(pl.lit('✗')).alias('OpenAIRE'),
        pl.when(pl.col('OpenAlex GRID')).then(pl.lit('✓')).otherwise(pl.lit('✗')).alias('GRID'),
        pl.when(pl.col('OpenAlex Wikidata')).then(pl.lit('✓')).otherwise(pl.lit('✗')).alias('Wikidata'),
        pl.col('OpenAlex works'),
    ])

    _cov_stats = mo.hstack([
        mo.stat(value=str(_n_in_openalex), label="In OpenAlex", caption="NL education institutions", bordered=True),
        mo.stat(value=str(_n_in_openaire), label="Matched in OpenAIRE", caption="via ROR linkage", bordered=True),
        mo.stat(value=str(_n_only_openalex), label="Only in OpenAlex", caption="Not found in OpenAIRE", bordered=True),
    ], gap=3)

    _coverage_content = mo.vstack([
        mo.md('### Coverage — NL institutions across open sources'),
        mo.md('*Comparing which organisations appear in OpenAlex and OpenAIRE, matched via ROR.*'),
        _cov_stats,
        mo.hstack([
            _cov_bar,
            _pid_chart,
        ], gap=4),
        mo.md('#### Institution × source coverage matrix'),
        mo.ui.table(_cov_tbl_df.to_pandas(), selection=None, page_size=20),
        mo.callout(
            mo.md("**Tip:** Use the ROR to link the same organisation across sources. "
                  "Organisations appearing only in OpenAlex may need to be registered in OpenAIRE's "
                  "[provide.openaire.eu](https://provide.openaire.eu)."),
            kind='info',
        ),
    ], gap=4)

    _coverage_content
    return


@app.cell(hide_code=True)
def accuracy(alt, mo, nl_inst_df, pl, works_compl_df):
    # build the accuracy tab: identifier format validation checks (ROR, GRID, Wikidata)
    # -----------------------------------------------------------------------
    # Identifier format validation (pattern checks)
    # -----------------------------------------------------------------------

    # DOI format: should start with "10."
    # ROR format: should match https://ror.org/[9 chars]
    # ORCID format: 0000-0000-0000-000X

    # For institutions: check ROR format validity
    _ror_df = nl_inst_df.select([
        pl.col('display_name'),
        pl.col('ror'),
    ]).with_columns([
        pl.col('ror').str.starts_with('https://ror.org/').alias('ror_format_valid'),
        pl.col('ror').str.len_chars().alias('ror_length'),
    ])

    _ror_valid_pct = round(_ror_df['ror_format_valid'].sum() * 100 / _ror_df.height, 1)

    _acc_data = pl.DataFrame({
        'Check': [
            'ROR format valid (https://ror.org/...)',
            'GRID format present',
            'Wikidata format valid (Q number)',
        ],
        'Pass %': [
            _ror_valid_pct,
            round(nl_inst_df['has_grid'].sum() * 100 / nl_inst_df.height, 1),
            round(
                nl_inst_df.filter(
                    pl.col('has_wikidata') & pl.col('wikidata_id').str.starts_with('Q')
                ).height * 100 / nl_inst_df.filter(pl.col('has_wikidata')).height
                if nl_inst_df.filter(pl.col('has_wikidata')).height > 0 else 0, 1
            ),
        ],
        'Source': ['OpenAlex NL institutions', 'OpenAlex NL institutions', 'OpenAlex NL institutions'],
    })

    _acc_chart = (
        alt.Chart(_acc_data.to_pandas())
        .mark_bar(cornerRadius=4)
        .encode(
            x=alt.X('Pass %:Q', scale=alt.Scale(domain=[0, 105]), title='Pass %'),
            y=alt.Y('Check:N', sort='-x', title=''),
            color=alt.Color(
                'Pass %:Q',
                scale=alt.Scale(domain=[0, 50, 80, 100], range=['#e74c3c', '#f39c12', '#f39c12', '#2ecc71']),
                legend=None,
            ),
            tooltip=['Check:N', alt.Tooltip('Pass %:Q', format='.1f')],
        )
        .properties(title='Identifier format accuracy checks', height=160, width='container')
    )

    # Works completeness vs "accuracy proxy": type+language+doi together
    _works_completeness_score = round(
        works_compl_df.filter(pl.col('field').is_in(['DOI', 'Language', 'Publication Type']))['pct'].mean(), 1
    )

    _acc_kpis = mo.hstack([
        mo.stat(
            value=f"{_ror_valid_pct:.0f}%",
            label="ROR format valid",
            caption="Institutions with correct ROR URI",
            bordered=True,
        ),
        mo.stat(
            value=f"{_works_completeness_score:.0f}%",
            label="Core fields avg (works)",
            caption="DOI + Language + Type completeness",
            bordered=True,
        ),
        mo.stat(
            value="⚙ Coming soon",
            label="ORCID checksum validation",
            caption="Requires author-level query",
            bordered=True,
        ),
    ], gap=3)

    _accuracy_content = mo.vstack([
        mo.md('### Accuracy — identifier format & value validation'),
        mo.md('*Accuracy checks whether field values conform to the expected format or registry standard.*'),
        _acc_kpis,
        _acc_chart,
        mo.accordion({
            'About these checks': mo.md("""
    **ROR:** Must match `https://ror.org/` followed by a 9-character alphanumeric identifier.
    Example: `https://ror.org/04dkp9463`

    **ORCID:** Must match pattern `0000-0000-0000-000X` (16 digits with dashes, Luhn checksum).

    **DOI:** Must start with `10.` followed by registrant code and suffix.

    **Wikidata:** Entity IDs should start with `Q` followed by digits.

    Full record-level accuracy validation (DOI resolver, ORCID API check) is planned for a future version.
            """),
        }),
    ], gap=4)

    _accuracy_content
    return


@app.cell(hide_code=True)
def enrichment(mo, nl_inst_df, pl, works_compl_df):
    # build the enrichment tab: prioritised recommendations to fill metadata gaps
    # -----------------------------------------------------------------------
    # Derive enrichment opportunities from completeness data
    # -----------------------------------------------------------------------
    _missing_pub_fields = works_compl_df.filter(pl.col('pct') < 80).sort('pct').to_dicts()
    _missing_inst_fields = [
        f for f, has in [
            ('GRID', nl_inst_df['has_grid'].mean() < 0.95),
            ('Wikidata', nl_inst_df['has_wikidata'].mean() < 0.95),
            ('Wikipedia', nl_inst_df['has_wikipedia'].mean() < 0.95),
            ('Homepage URL', nl_inst_df['has_homepage'].mean() < 0.95),
        ] if has
    ]

    INTERVENTIONS = [
        {
            'field': 'DOI',
            'priority': 'High',
            'title': 'Register missing DOIs via Crossref',
            'description': (
                f"{works_compl_df.filter(pl.col('field')=='DOI')['missing'][0]:,} works lack a DOI. "
                'Register publications with Crossref to obtain DOIs. '
                'This dramatically improves discoverability and linking across sources.'
            ),
            'effort': 'Medium',
            'impact': 'High',
            'action_url': 'https://www.crossref.org/documentation/',
            'action_label': 'Crossref Docs',
        },
        {
            'field': 'ORCID',
            'priority': 'High',
            'title': 'Link ORCID profiles to publications',
            'description': (
                'Author ORCIDs are missing from many records. '
                'Enable institutional ORCID integration to auto-link author profiles to outputs. '
                'Improves attribution and reporting quality.'
            ),
            'effort': 'Low',
            'impact': 'High',
            'action_url': 'https://orcid.org/organizations/integrators',
            'action_label': 'ORCID Integration Guide',
        },
        {
            'field': 'Funder',
            'priority': 'Medium',
            'title': 'Enrich grant metadata via OpenAIRE',
            'description': (
                f"{works_compl_df.filter(pl.col('field')=='Funder / Grant')['missing'][0]:,} works lack funder information. "
                'Use the OpenAIRE API to match publications to funded projects and add grant DOIs.'
            ),
            'effort': 'High',
            'impact': 'Medium',
            'action_url': 'https://api.openaire.eu/',
            'action_label': 'OpenAIRE API',
        },
        {
            'field': 'License',
            'priority': 'Medium',
            'title': 'Add license information to open access works',
            'description': (
                f"{works_compl_df.filter(pl.col('field')=='License')['missing'][0]:,} works lack license metadata. "
                'Check Unpaywall and re-harvest OA location data to obtain license strings.'
            ),
            'effort': 'Low',
            'impact': 'Medium',
            'action_url': 'https://unpaywall.org/',
            'action_label': 'Unpaywall API',
        },
        {
            'field': 'Abstract',
            'priority': 'Low',
            'title': 'Harvest abstracts from publisher APIs',
            'description': (
                f"{works_compl_df.filter(pl.col('field')=='Abstract')['missing'][0]:,} works have no abstract. "
                'Enrich via Crossref, Semantic Scholar, or Europe PMC APIs.'
            ),
            'effort': 'Medium',
            'impact': 'Low',
            'action_url': 'https://api.semanticscholar.org/',
            'action_label': 'Semantic Scholar API',
        },
        {
            'field': 'Wikidata',
            'priority': 'Low',
            'title': 'Add Wikidata IDs to institutions',
            'description': (
                f"{nl_inst_df.filter(~pl.col('has_wikidata')).height} NL institutions are missing Wikidata IDs. "
                'Wikidata enables linked data connections across multiple knowledge bases.'
            ),
            'effort': 'Low',
            'impact': 'Low',
            'action_url': 'https://www.wikidata.org/',
            'action_label': 'Wikidata',
        },
    ]

    _priority_color = {'High': '#e74c3c', 'Medium': '#f39c12', 'Low': '#95a5a6'}
    _effort_label   = {'High': '🔴 High effort', 'Medium': '🟡 Medium', 'Low': '🟢 Low effort'}

    def _make_item(iv):
        return mo.vstack([
            mo.hstack([
                mo.md(f"**[{iv['priority']}]** {iv['title']}"),
                mo.md(f"*{_effort_label[iv['effort']]}* · impact: **{iv['impact']}**"),
            ], justify='space-between', gap=2),
            mo.md(iv['description']),
            mo.md(f"→ [{iv['action_label']}]({iv['action_url']})"),
        ], gap=1)

    _items = [_make_item(iv) for iv in INTERVENTIONS]

    _enrichment_content = mo.vstack([
        mo.md('### Enrichment opportunities'),
        mo.md('*Priority recommendations to improve metadata completeness, ranked by impact.*'),
        mo.hstack([
            mo.stat(value=str(sum(1 for i in INTERVENTIONS if i['priority'] == 'High')),
                    label="High-priority actions", bordered=True),
            mo.stat(value=str(sum(1 for i in INTERVENTIONS if i['priority'] == 'Medium')),
                    label="Medium-priority", bordered=True),
            mo.stat(value=str(sum(1 for i in INTERVENTIONS if i['priority'] == 'Low')),
                    label="Low-priority", bordered=True),
        ], gap=3),
        mo.accordion({iv['title']: _make_item(iv) for iv in INTERVENTIONS}),
        mo.callout(
            mo.md("**Next steps:** Implement the suggested enrichment pipelines and re-check this dashboard. "
                  "Questions? Contact [ori-team@surf.nl](mailto:ori-team@surf.nl)."),
            kind='success',
        ),
    ], gap=4)

    _enrichment_content
    return


@app.cell(hide_code=True)
def divider(mo):
    # render a horizontal rule to visually separate the main content from the footer
    mo.md("""
    ---
    """)
    return


@app.cell(hide_code=True)
def footer(date, mo, org_select):
    # render a small footer with attribution, selected organisation, and today's date
    mo.md(f"""
    <div style="font-size:.75rem;color:#999;display:flex;justify-content:space-between;flex-wrap:wrap;">
    <span>ORI Quality Dashboard · PID to Portal · SURF ORI team</span>
    <span>Organisation: {', '.join(org_select.value) or '(none)'} · Data: OpenAlex / OpenAIRE via SURF DuckLake · {date.today()}</span>
    </div>
    """)
    return


if __name__ == "__main__":
    app.run()
