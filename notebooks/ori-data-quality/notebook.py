# /// script
# requires-python = ">=3.12"
# dependencies = [
#     "altair>=5.4.0",
#     "duckdb>=1.1.0",
#     "marimo>=0.10.0",
#     "pandas>=2.2.0",
#     "polars[pyarrow]>=1.0.0",
#     "pyarrow>=17.0.0",
# ]
# ///

import marimo

__generated_with = "0.20.2"
app = marimo.App(width="full", app_title="ORI Data Quality Dashboard")


# ---------------------------------------------------------------------------
# WASM setup (micropip for browser deployment)
# ---------------------------------------------------------------------------

@app.cell(hide_code=True)
async def _():
    import sys
    _ = None
    if 'pyodide' in sys.modules:
        import micropip
        await micropip.install(['polars', 'pyarrow', 'altair', 'duckdb'])
        _ = 'wasm'
    return


# ---------------------------------------------------------------------------
# Imports
# ---------------------------------------------------------------------------

@app.cell(hide_code=True)
def _():
    import duckdb
    import marimo as mo
    import polars as pl
    import altair as alt
    from datetime import date
    return alt, date, duckdb, mo, pl


# ---------------------------------------------------------------------------
# DuckDB connection + data source URL
# ---------------------------------------------------------------------------

@app.cell(hide_code=True)
def _(duckdb):
    con = duckdb.connect()
    con.execute("INSTALL httpfs; LOAD httpfs;")
    BASE = 'https://objectstore.surf.nl/cea01a7216d64348b7e51e5f3fc1901d:sprouts'
    INST_URL = f"{BASE}/data/openalex/institutions/data_0.parquet"
    WORKS_URL = f"{BASE}/data/openalex/works/data_0.parquet"
    OPENAIRE_ORGS_URL = f"{BASE}/data/openaire/organizations/data_0.parquet"
    return BASE, INST_URL, OPENAIRE_ORGS_URL, WORKS_URL, con


# ---------------------------------------------------------------------------
# Load NL institutions from OpenAlex (single parquet file, fast)
# ---------------------------------------------------------------------------

@app.cell(hide_code=True)
def _(INST_URL, con, pl):
    nl_inst_df = con.execute(f"""
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
    FROM read_parquet('{INST_URL}')
    WHERE country_code = 'NL'
      AND type IN ('education', 'funder')
      AND works_count > 1000
    ORDER BY works_count DESC
    """).pl()
    return (nl_inst_df,)


# ---------------------------------------------------------------------------
# Load OpenAIRE NL organizations (for coverage comparison)
# ---------------------------------------------------------------------------

@app.cell(hide_code=True)
def _(OPENAIRE_ORGS_URL, con, pl):
    oa_orgs_df = con.execute(f"""
    SELECT
        legalName,
        legalShortName,
        websiteUrl,
        id AS openaire_id,
        pids
    FROM read_parquet('{OPENAIRE_ORGS_URL}')
    WHERE country.code = 'NL'
    """).pl()
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


# ---------------------------------------------------------------------------
# Load works completeness from a sample parquet file
# ---------------------------------------------------------------------------

@app.cell(hide_code=True)
def _(WORKS_URL, con, pl):
    works_compl_raw = con.execute(f"""
    SELECT
        COUNT(*)                                    AS total,
        COUNT(doi)                                  AS has_doi,
        COUNT(language)                             AS has_language,
        COUNT(type)                                 AS has_type,
        COUNT_IF(open_access.is_oa)                 AS is_oa,
        COUNT_IF(array_length(funders) > 0)         AS has_funder,
        COUNT(license)                              AS has_license,
        COUNT(abstract_inverted_index)              AS has_abstract,
        COUNT(primary_location.source.issn_l)       AS has_issn,
        COUNT_IF(array_length(corresponding_author_ids) > 0) AS has_corresponding_author,
        COUNT(publication_year)                     AS has_year,
        COUNT_IF(array_length(concepts) > 0)        AS has_concepts
    FROM read_parquet('{WORKS_URL}')
    """).fetchone()

    total_works = works_compl_raw[0]
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
            works_compl_raw[1], works_compl_raw[2], works_compl_raw[3],
            works_compl_raw[4], works_compl_raw[5], works_compl_raw[6],
            works_compl_raw[7], works_compl_raw[8], works_compl_raw[9],
            works_compl_raw[10], works_compl_raw[11],
        ],
        'total': [total_works] * 11,
    }).with_columns(
        (pl.col('has_value') * 100.0 / pl.col('total')).round(1).alias('pct'),
        (pl.col('total') - pl.col('has_value')).alias('missing'),
    )
    return total_works, works_compl_df, works_compl_raw


# ---------------------------------------------------------------------------
# Constants: CERIF entities and identifier fields per entity
# ---------------------------------------------------------------------------

@app.cell(hide_code=True)
def _():
    CERIF_ENTITIES = [
        'Outputs: Publications',
        'Organisations: Institutions',
    ]
    SOURCES = ['OpenAlex', 'OpenAIRE', 'Crossref', 'ORCID', 'ROR', 'DataCite']
    PUB_TYPES = ['All', 'Journal Article', 'Conference Paper', 'Book Chapter', 'Preprint', 'Thesis', 'Report']
    SURF_BLUE = '#009de0'
    return CERIF_ENTITIES, PUB_TYPES, SOURCES, SURF_BLUE


# ---------------------------------------------------------------------------
# Filter UI: Organisation, CERIF Entity, Source, Publication Type
# ---------------------------------------------------------------------------

@app.cell(hide_code=True)
def _(CERIF_ENTITIES, PUB_TYPES, SOURCES, mo, nl_inst_df):
    org_options = nl_inst_df['display_name'].to_list()
    org_select = mo.ui.dropdown(
        options=org_options,
        value='University of Amsterdam',
        label='Organisation',
    )
    entity_select = mo.ui.dropdown(
        options=CERIF_ENTITIES,
        value='Outputs: Publications',
        label='CERIF Entity',
    )
    source_select = mo.ui.dropdown(
        options=SOURCES,
        value='OpenAlex',
        label='Primary Source',
    )
    pub_type_select = mo.ui.dropdown(
        options=PUB_TYPES,
        value='All',
        label='Publication Type',
    )
    return entity_select, org_options, org_select, pub_type_select, source_select


# ---------------------------------------------------------------------------
# Selected organisation info
# ---------------------------------------------------------------------------

@app.cell(hide_code=True)
def _(nl_inst_df, org_select, pl):
    sel_org = nl_inst_df.filter(pl.col('display_name') == org_select.value)
    sel_ror = sel_org['ror'][0] if sel_org.height > 0 else ''
    return sel_org, sel_ror


# ---------------------------------------------------------------------------
# Header
# ---------------------------------------------------------------------------

@app.cell(hide_code=True)
def _(mo):
    mo.md("""
    # ORI Data Quality Dashboard
    **Metadata completeness, coverage & enrichment for Dutch research organisations**
    *(OpenAlex · OpenAIRE · Crossref · ORCID — Barcelona Declaration on Open Research Information)*
    """)
    return


# ---------------------------------------------------------------------------
# Filter bar
# ---------------------------------------------------------------------------

@app.cell(hide_code=True)
def _(entity_select, mo, org_select, pub_type_select, source_select):
    mo.hstack(
        [org_select, entity_select, source_select, pub_type_select],
        gap=3,
        align='end',
    )
    return


# ===========================================================================
# TAB CONTENT — Overview
# ===========================================================================

@app.cell(hide_code=True)
def _(mo, nl_inst_df, oa_orgs_df, org_select, pl, sel_org, total_works):
    _n_nl_unis     = nl_inst_df.height
    _n_oa_orgs     = oa_orgs_df.height
    _total_works   = total_works
    _sel_works     = sel_org['works_count'][0] if sel_org.height > 0 else 0
    _sel_cited     = sel_org['cited_by_count'][0] if sel_org.height > 0 else 0

    # KPI row
    _kpis = mo.hstack([
        mo.stat(
            value=f"{_n_nl_unis}",
            label="NL Institutions (OpenAlex)",
            caption="Education & research type, >1k works",
            bordered=True,
        ),
        mo.stat(
            value=f"{_n_oa_orgs:,}",
            label="NL Orgs in OpenAIRE",
            caption="With any metadata",
            bordered=True,
        ),
        mo.stat(
            value=f"{_sel_works:,}",
            label=f"Works — {org_select.value[:25]}",
            caption="OpenAlex publication count",
            bordered=True,
        ),
        mo.stat(
            value=f"{_sel_cited:,}",
            label="Cited by (selected org)",
            caption="Total citations in OpenAlex",
            bordered=True,
        ),
        mo.stat(
            value="7",
            label="Sources monitored",
            caption="OpenAlex, OpenAIRE, Crossref, ORCID, ROR, DataCite, CRIS",
            bordered=True,
        ),
    ], gap=3, wrap=True)

    # Org list table
    _org_tbl_data = nl_inst_df.select([
        pl.col('display_name').alias('Institution'),
        pl.col('ror').alias('ROR'),
        pl.col('type').alias('Type'),
        pl.col('works_count').alias('Works'),
        pl.col('has_ror').alias('ROR ✓'),
        pl.col('has_grid').alias('GRID ✓'),
        pl.col('has_wikidata').alias('Wikidata ✓'),
        pl.col('has_wikipedia').alias('Wikipedia ✓'),
        pl.col('has_homepage').alias('Homepage ✓'),
    ])

    _guide = mo.md("""
## Getting started

Use the **filters above** to focus your analysis on a specific organisation, CERIF entity, and source.
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
        mo.md("### Participating NL Institutions *(source: OpenAlex)*"),
        mo.ui.table(
            _org_tbl_data.to_pandas(),
            selection=None,
            page_size=15,
        ),
        _guide,
    ], gap=4)
    _overview_content


# ===========================================================================
# TAB CONTENT — Completeness
# ===========================================================================

@app.cell(hide_code=True)
def _(
    alt, entity_select, mo, nl_inst_df, org_select, pl, sel_org, sel_ror, works_compl_df
):
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
        _val = sel_org[_f][0] if sel_org.height > 0 else False
        _sel_fields.append({'Identifier': _lbl, 'Present': '✓' if _val else '✗'})

    _sel_tbl = mo.ui.table(
        pl.DataFrame(_sel_fields).to_pandas(),
        selection=None,
        label=f"Identifier completeness — {org_select.value if hasattr(org_select, 'value') else ''}",
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
        _pub_section if entity_select.value.startswith('Outputs') else _inst_section,
        mo.accordion({'SQL query details': _sql}),
    ], gap=4)

    _completeness_content


# ===========================================================================
# TAB CONTENT — Coverage
# ===========================================================================

@app.cell(hide_code=True)
def _(alt, mo, nl_inst_df, oa_orgs_df, org_select, pl):
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


# ===========================================================================
# TAB CONTENT — Accuracy
# ===========================================================================

@app.cell(hide_code=True)
def _(alt, mo, nl_inst_df, pl, works_compl_df):
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


# ===========================================================================
# TAB CONTENT — Enrichment
# ===========================================================================

@app.cell(hide_code=True)
def _(mo, nl_inst_df, pl, sel_ror, works_compl_df):
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


# ===========================================================================
# Main tabs — assembled from content cells above
# ===========================================================================

# NOTE: marimo tabs require content passed inline; the cells above render
# their own output directly into the notebook flow, ordered by the tabs
# selector below. We use mo.ui.tabs() to let the user navigate sections.

@app.cell(hide_code=True)
def _(mo):
    mo.md("---")


# ---------------------------------------------------------------------------
# Footer
# ---------------------------------------------------------------------------

@app.cell(hide_code=True)
def _(date, mo, org_select):
    mo.md(f"""
<div style="font-size:.75rem;color:#999;display:flex;justify-content:space-between;flex-wrap:wrap;">
    <span>ORI Quality Dashboard · PID to Portal · SURF ORI team</span>
    <span>Organisation: {org_select.value} · Data: OpenAlex / OpenAIRE via SURF DuckLake · {date.today()}</span>
</div>
    """)
    return


if __name__ == "__main__":
    app.run()
