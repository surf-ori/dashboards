# /// script
# requires-python = ">=3.12"
# dependencies = [
#     "altair>=5.4.0",
#     "duckdb>=1.1.0",
#     "marimo>=0.19.0",
#     "pandas>=2.2.0",
#     "plotly>=5.24.0",
#     "polars[pyarrow]>=1.0.0",
#     "pyarrow>=17.0.0",
# ]
# ///

import marimo

__generated_with = "0.20.2"
app = marimo.App(width="full", app_title="ORI Data Quality Dashboard")


@app.cell(hide_code=True)
def _():
    import marimo as mo
    import polars as pl
    import plotly.graph_objects as go
    import plotly.express as px
    from datetime import datetime, date, timedelta
    return date, datetime, go, mo, pl, px, timedelta


# ---------------------------------------------------------------------------
# Mock data — replace with live DuckLake / Parquet queries when available
# ---------------------------------------------------------------------------

@app.cell(hide_code=True)
def _(pl):
    # --- Organisations ---
    organisations = [
        "VU Amsterdam",
        "University of Amsterdam",
        "Utrecht University",
        "Leiden University",
        "Delft University of Technology",
        "Erasmus University Rotterdam",
        "Radboud University",
        "Tilburg University",
        "Maastricht University",
        "Wageningen University",
    ]

    # --- CERIF entities ---
    cerif_entities = [
        "Outputs: Publications",
        "Outputs: Datasets",
        "Outputs: Software",
        "Persons: Researchers",
        "Projects: Grants",
        "Organisations: Units",
    ]

    # --- ORI sources ---
    ori_sources = ["CRIS/VU Research Portal", "OpenAlex", "OpenAIRE", "Crossref", "ROR"]

    # --- Metadata fields per entity ---
    pub_fields   = ["DOI", "ORCID", "ROR", "ISSN", "OA Status", "Grant DOI", "Corresponding Author"]
    person_fields = ["ORCID", "ROR", "ISNI", "Scopus Author ID"]
    grant_fields  = ["Grant DOI", "ROR", "Funder ID"]
    dataset_fields = ["DOI", "ORCID", "ROR", "License"]
    software_fields = ["DOI", "ORCID", "ROR", "License", "SWHID"]
    org_fields   = ["ROR", "ISNI", "Wikidata ID", "GRID ID"]

    cerif_fields = {
        "Outputs: Publications": pub_fields,
        "Outputs: Datasets": dataset_fields,
        "Outputs: Software": software_fields,
        "Persons: Researchers": person_fields,
        "Projects: Grants": grant_fields,
        "Organisations: Units": org_fields,
    }
    return cerif_entities, cerif_fields, organisations, ori_sources


@app.cell(hide_code=True)
def _(pl):
    import random
    random.seed(42)

    def mock_completeness(org, entity, fields):
        """Generate reproducible mock completeness data."""
        seed = hash(org + entity) % 10000
        random.seed(seed)
        rows = []
        for f in fields:
            total = random.randint(800, 2500)
            # Some fields are naturally less complete
            base_pct = {
                "DOI": random.uniform(55, 90),
                "ORCID": random.uniform(25, 65),
                "ROR": random.uniform(70, 99),
                "ISSN": random.uniform(30, 60),
                "OA Status": random.uniform(60, 95),
                "Grant DOI": random.uniform(10, 35),
                "Corresponding Author": random.uniform(40, 75),
                "ISNI": random.uniform(20, 50),
                "Scopus Author ID": random.uniform(30, 70),
                "Funder ID": random.uniform(15, 45),
                "License": random.uniform(40, 80),
                "SWHID": random.uniform(5, 30),
                "Wikidata ID": random.uniform(50, 90),
                "GRID ID": random.uniform(40, 85),
            }.get(f, random.uniform(30, 90))
            pct = round(base_pct + random.uniform(-5, 5), 1)
            pct = max(0, min(100, pct))
            with_field = int(total * pct / 100)
            # Trend data (6 months)
            trend = [round(pct + random.uniform(-3, 3), 1) for _ in range(6)]
            rows.append({
                "field": f,
                "total_records": total,
                "records_with_field": with_field,
                "completeness_pct": pct,
                "accuracy_score": round(random.uniform(75, 99), 1),
                "trend_m6": trend[-6],
                "trend_m5": trend[-5],
                "trend_m4": trend[-4],
                "trend_m3": trend[-3],
                "trend_m2": trend[-2],
                "trend_m1": pct,
            })
        return pl.DataFrame(rows)

    def mock_coverage(org, entity, fields):
        """Generate mock coverage / between-source comparison data."""
        random.seed(hash(org + entity + "cov") % 10000)
        rows = []
        sources = ["CRIS/VU Research Portal", "OpenAlex", "OpenAIRE", "Crossref"]
        for src in sources:
            total = random.randint(800, 2500)
            rows.append({
                "source": src,
                "total_records": total,
                "records_with_doi": int(total * random.uniform(0.4, 0.95)),
                "records_with_orcid": int(total * random.uniform(0.2, 0.7)),
                "records_with_ror": int(total * random.uniform(0.6, 0.99)),
                "only_in_this_source": int(total * random.uniform(0.02, 0.25)),
                "matched_with_primary": int(total * random.uniform(0.6, 0.95)),
                "mismatched_values": int(total * random.uniform(0.01, 0.15)),
            })
        return pl.DataFrame(rows)

    def mock_actions(org, entity):
        """Generate mock recommended actions."""
        random.seed(hash(org + entity + "act") % 10000)
        templates = [
            ("DOI", "high",   "Publications", "Import from Crossref using DOI lookup API",        "Run ETL: crossref_enrich_doi"),
            ("ORCID", "high",  "Author records", "Validate against ORCID Public API / institutional directory", "Run ETL: orcid_author_sync"),
            ("Grant DOI", "medium", "Publications", "Check OpenAIRE & Crossref for funding metadata",  "Run ETL: openaire_enrich_grants"),
            ("ROR", "low",    "Organisation", "Update institutional master file in CRIS admin",  "Update master institution file"),
            ("ISSN", "medium", "Journal records", "Enrich from OpenAlex journal metadata",          "Run ETL: openalex_enrich_issn"),
            ("OA Status", "medium", "Publications", "Re-check Unpaywall status for recent records",   "Run ETL: unpaywall_oa_check"),
        ]
        rows = []
        for field, priority, scope, desc, action in templates:
            if random.random() > 0.3:
                affected = random.randint(5, 350)
                rows.append({
                    "field": field,
                    "priority": priority,
                    "scope": scope,
                    "description": f"{affected} {scope.lower()} records missing {field}. {desc}.",
                    "affected_records": affected,
                    "suggested_action": action,
                })
        return pl.DataFrame(rows).sort("priority", descending=False)

    return mock_actions, mock_completeness, mock_coverage


# ---------------------------------------------------------------------------
# Header & Navigation
# ---------------------------------------------------------------------------

@app.cell(hide_code=True)
def _(mo):
    mo.md("""
    <div style="
        display: flex;
        align-items: center;
        justify-content: space-between;
        padding-bottom: 0.5rem;
        border-bottom: 2px solid #e5e5e5;
        margin-bottom: 0.5rem;
    ">
        <div>
            <h1 style="margin: 0; font-size: 1.6rem;">ORI Quality Dashboard</h1>
            <div style="color: #666; font-size: 0.85rem; margin-top: 2px;">
                Metadata completeness, coverage, accuracy &amp; enrichment for Dutch research organisations
            </div>
        </div>
        <img src="https://www.surf.nl/themes/surf/logo.svg" alt="SURF" style="height: 40px;" />
    </div>
    """)
    return


# ---------------------------------------------------------------------------
# Filters (sidebar-style, placed in top row)
# ---------------------------------------------------------------------------

@app.cell(hide_code=True)
def _(cerif_entities, mo, organisations):
    org_select = mo.ui.dropdown(
        organisations,
        value="VU Amsterdam",
        label="Organisation",
    )
    entity_select = mo.ui.dropdown(
        cerif_entities,
        value="Outputs: Publications",
        label="CERIF Entity",
    )
    return entity_select, org_select


@app.cell(hide_code=True)
def _(entity_select, mo, org_select):
    filter_bar = mo.hstack(
        [
            mo.vstack([org_select], gap=0),
            mo.vstack([entity_select], gap=0),
        ],
        gap=4,
        align="start",
    )
    filter_bar
    return (filter_bar,)


# ---------------------------------------------------------------------------
# Tabs
# ---------------------------------------------------------------------------

@app.cell(hide_code=True)
def _(mo):
    tabs_selector = mo.ui.radio(
        options=["About", "Completeness", "Coverage", "Accuracy", "Enrichment"],
        value="About",
        inline=True,
        label="",
    )
    tabs_selector
    return (tabs_selector,)


# ---------------------------------------------------------------------------
# Compute mock data for current selection
# ---------------------------------------------------------------------------

@app.cell(hide_code=True)
def _(
    cerif_fields,
    entity_select,
    mock_actions,
    mock_completeness,
    mock_coverage,
    org_select,
):
    _fields = cerif_fields.get(entity_select.value, ["DOI", "ORCID", "ROR"])
    completeness_df = mock_completeness(org_select.value, entity_select.value, _fields)
    coverage_df     = mock_coverage(org_select.value, entity_select.value, _fields)
    actions_df      = mock_actions(org_select.value, entity_select.value)
    return actions_df, completeness_df, coverage_df


# ---------------------------------------------------------------------------
# Helper: status colour
# ---------------------------------------------------------------------------

@app.cell(hide_code=True)
def _():
    def status_color(pct):
        if pct >= 80:
            return "#2ecc71"   # green
        elif pct >= 50:
            return "#f39c12"   # orange
        else:
            return "#e74c3c"   # red

    def priority_color(p):
        return {"high": "#e74c3c", "medium": "#f39c12", "low": "#95a5a6"}.get(p, "#3498db")

    SURF_BLUE = "#009de0"
    return SURF_BLUE, priority_color, status_color


# ===========================================================================
# TAB: About
# ===========================================================================

@app.cell(hide_code=True)
def _(
    SURF_BLUE,
    completeness_df,
    coverage_df,
    entity_select,
    mo,
    org_select,
    tabs_selector,
):
    _n_sources  = coverage_df.height
    _n_orgs     = 17   # Dutch research orgs in scope
    _avg_compl  = round(completeness_df["completeness_pct"].mean(), 0)
    _avg_cov    = round(
        (coverage_df["records_with_doi"].sum() / coverage_df["total_records"].sum() * 100), 0
    )
    _avg_acc    = round(completeness_df["accuracy_score"].mean(), 0)

    def _kpi(label, value, unit="", color=SURF_BLUE):
        return mo.Html(f"""
        <div style="background:{color};color:white;border-radius:10px;padding:18px 24px;
                    text-align:center;min-width:130px;box-shadow:0 2px 6px rgba(0,0,0,.12);">
            <div style="font-size:2rem;font-weight:700;">{value}{unit}</div>
            <div style="font-size:.8rem;margin-top:4px;opacity:.9;">{label}</div>
        </div>""")

    _about = mo.vstack([
        mo.md(f"""
## About this dashboard

This dashboard helps **metadata specialists** at Dutch research institutions monitor
the quality of their Open Research Information (ORI) data.

Currently showing: **{entity_select.value}** at **{org_select.value}**

The five tabs provide:
- **Completeness** — which metadata fields are present, and for what share of records
- **Coverage** — how many records each ORI source covers, and where there are gaps
- **Accuracy** — how correctly formatted and valid the field values are
- **Enrichment** — opportunities to add missing fields from external sources

Data is drawn from the ORI DuckLake catalog (frozen Parquet snapshots on SURF Object Store).
SQL queries are shown inline so you can understand exactly what you are looking at.

*Part of the [PID to Portal](https://communities.surf.nl/en/open-research-information) project.*
        """),
        mo.hstack([
            _kpi("Sources monitored", _n_sources),
            _kpi("Dutch organisations", _n_orgs),
            _kpi("Avg completeness", int(_avg_compl), "%"),
            _kpi("Avg coverage", int(_avg_cov), "%"),
            _kpi("Avg accuracy", int(_avg_acc), "%"),
        ], gap=3, justify="start"),
    ], gap=3)

    _about if tabs_selector.value == "About" else mo.md("")
    return


# ===========================================================================
# TAB: Completeness
# ===========================================================================

@app.cell(hide_code=True)
def _(
    completeness_df,
    entity_select,
    go,
    mo,
    org_select,
    px,
    status_color,
    tabs_selector,
):
    # --- KPI cards ---
    def _compl_card(row):
        c = status_color(row["completeness_pct"])
        gap = row["total_records"] - row["records_with_field"]
        return mo.Html(f"""
        <div style="border:2px solid {c};border-radius:10px;padding:14px 18px;
                    min-width:140px;text-align:center;background:#fff;
                    box-shadow:0 1px 4px rgba(0,0,0,.08);">
            <div style="font-size:1.7rem;font-weight:700;color:{c};">
                {row['completeness_pct']:.0f}%
            </div>
            <div style="font-size:.78rem;font-weight:600;color:#333;margin-top:2px;">
                has a {row['field']}
            </div>
            <div style="font-size:.72rem;color:#999;margin-top:4px;">
                {gap:,} missing
            </div>
        </div>""")

    _rows = completeness_df.to_dicts()
    _cards = mo.hstack([_compl_card(r) for r in _rows], gap=2, wrap=True)

    # --- Horizontal bar chart: completeness by field ---
    _df_sorted = completeness_df.sort("completeness_pct")
    _fig_bar = px.bar(
        _df_sorted.to_pandas(),
        y="field", x="completeness_pct",
        orientation="h",
        color="completeness_pct",
        color_continuous_scale=["#e74c3c", "#f39c12", "#2ecc71"],
        range_color=[0, 100],
        labels={"completeness_pct": "Completeness %", "field": ""},
        title=f"Field completeness — {entity_select.value} in {org_select.value}",
        height=280,
        text="completeness_pct",
    )
    _fig_bar.update_traces(texttemplate="%{text:.0f}%", textposition="outside")
    _fig_bar.update_layout(
        xaxis_range=[0, 110],
        showlegend=False,
        coloraxis_showscale=False,
        margin=dict(l=10, r=20, t=40, b=20),
    )

    # --- Trend sparklines (6-month) ---
    _months = ["M-5", "M-4", "M-3", "M-2", "M-1", "Now"]
    _trend_cols = ["trend_m6","trend_m5","trend_m4","trend_m3","trend_m2","trend_m1"]
    _fig_trend = go.Figure()
    for _r in completeness_df.to_dicts():
        _vals = [_r[c] for c in _trend_cols]
        _fig_trend.add_trace(go.Scatter(
            x=_months, y=_vals, mode="lines+markers",
            name=_r["field"], line=dict(width=2),
        ))
    _fig_trend.update_layout(
        title=f"Completeness trend (6 months) — {org_select.value}",
        height=260, margin=dict(l=10, r=10, t=40, b=20),
        xaxis_title="", yaxis_title="Completeness %",
        yaxis_range=[0, 105],
        legend=dict(orientation="h", yanchor="bottom", y=-0.4),
    )

    # --- SQL hint ---
    _sql = mo.callout(mo.md(f"""
**Query (Details):**
```sql
SELECT
    field,
    COUNT(*) AS total_records,
    COUNT_IF(field IS NOT NULL) AS records_with_field,
    ROUND(COUNT_IF(field IS NOT NULL) * 100.0 / COUNT(*), 1) AS completeness_pct
FROM ori_data."{entity_select.value.lower().replace(': ','_').replace(' ','_')}"
WHERE ror = '{org_select.value}'
GROUP BY field
ORDER BY completeness_pct ASC;
```
"""), kind="info")

    _completeness_tab = mo.vstack([
        mo.md(f"### Completeness of metadata fields — *{entity_select.value}* at *{org_select.value}*"),
        mo.md("*Required: has a DOI, has a ROR, has an ORCID*"),
        _cards,
        mo.hstack([
            mo.vstack([mo.plotly(_fig_bar)], gap=0),
            mo.vstack([mo.plotly(_fig_trend)], gap=0),
        ], gap=3),
        mo.accordion({"Details": _sql, "Interventions": mo.md("_See the Enrichment tab for suggested actions._")}),
    ], gap=3)

    _completeness_tab if tabs_selector.value == "Completeness" else mo.md("")
    return


# ===========================================================================
# TAB: Coverage
# ===========================================================================

@app.cell(hide_code=True)
def _(
    coverage_df,
    entity_select,
    go,
    mo,
    org_select,
    px,
    tabs_selector,
):
    # Primary source selector
    _primary_opts = coverage_df["source"].to_list()
    _primary_src  = mo.ui.dropdown(_primary_opts, value=_primary_opts[0], label="Primary Source")

    # Grouped bar: records per source
    _fig_cov = px.bar(
        coverage_df.to_pandas(),
        x="source", y=["records_with_doi", "records_with_orcid", "records_with_ror"],
        barmode="group",
        labels={"value": "Record count", "variable": "Field", "source": "Source"},
        title=f"Field coverage by ORI source — {entity_select.value} at {org_select.value}",
        color_discrete_sequence=["#009de0", "#2ecc71", "#f39c12"],
        height=280,
    )
    _fig_cov.update_layout(margin=dict(l=10, r=10, t=40, b=20),
                           legend=dict(orientation="h", yanchor="bottom", y=-0.35))

    # Horizontal stacked bar: only_in / matched / mismatched
    _fig_gap = px.bar(
        coverage_df.to_pandas(),
        y="source",
        x=["matched_with_primary", "only_in_this_source", "mismatched_values"],
        barmode="stack",
        orientation="h",
        labels={"value": "Record count", "variable": "Category", "source": ""},
        title="Match status between sources",
        color_discrete_map={
            "matched_with_primary": "#2ecc71",
            "only_in_this_source": "#f39c12",
            "mismatched_values": "#e74c3c",
        },
        height=250,
    )
    _fig_gap.update_layout(margin=dict(l=10, r=10, t=40, b=20),
                           legend=dict(orientation="h", yanchor="bottom", y=-0.4))

    # Summary table
    _tbl_data = coverage_df.select([
        "source", "total_records", "only_in_this_source",
        "matched_with_primary", "mismatched_values",
    ]).rename({
        "source": "Source",
        "total_records": "Total records",
        "only_in_this_source": "Only in source",
        "matched_with_primary": "Matched",
        "mismatched_values": "Mismatches",
    })

    _coverage_tab = mo.vstack([
        mo.md(f"### Coverage — *{entity_select.value}* at *{org_select.value}*"),
        mo.md("*Examine coverage of metadata records between CRIS/VU Research Portal and selected compare sources: OpenAlex, OpenAIRE, Crossref, Groningen.*"),
        mo.hstack([_primary_src], gap=2),
        mo.accordion({
            "Compare sources by Affiliation Identifier ROR": mo.vstack([
                mo.plotly(_fig_cov),
                mo.plotly(_fig_gap),
                mo.ui.table(_tbl_data.to_pandas(), selection=None),
            ], gap=3),
            "Compare sources by Publication Identifier DOI": mo.md("*Select a DOI field to compare source overlap.*"),
        }),
        mo.accordion({
            "Timeline": mo.md("_Trend data for coverage over time will appear here._"),
            "Details": mo.md("_Drill-down record table._"),
            "Interventions": mo.md("_See the Enrichment tab for suggested actions._"),
        }),
    ], gap=3)

    _coverage_tab if tabs_selector.value == "Coverage" else mo.md("")
    return


# ===========================================================================
# TAB: Accuracy
# ===========================================================================

@app.cell(hide_code=True)
def _(
    completeness_df,
    entity_select,
    go,
    mo,
    org_select,
    px,
    status_color,
    tabs_selector,
):
    # Accuracy gauge per field
    def _gauge(field, acc, compl):
        c = status_color(acc)
        return mo.Html(f"""
        <div style="border:2px solid {c};border-radius:10px;padding:14px 18px;
                    min-width:140px;text-align:center;background:#fff;
                    box-shadow:0 1px 4px rgba(0,0,0,.08);">
            <div style="font-size:1.7rem;font-weight:700;color:{c};">{acc:.0f}%</div>
            <div style="font-size:.78rem;font-weight:600;color:#333;margin-top:2px;">accuracy</div>
            <div style="font-size:.72rem;color:#888;">{field}</div>
        </div>""")

    _acc_cards = mo.hstack(
        [_gauge(r["field"], r["accuracy_score"], r["completeness_pct"])
         for r in completeness_df.to_dicts()],
        gap=2, wrap=True,
    )

    # Scatter: completeness vs accuracy
    _fig_scatter = px.scatter(
        completeness_df.to_pandas(),
        x="completeness_pct", y="accuracy_score",
        text="field",
        color="accuracy_score",
        color_continuous_scale=["#e74c3c", "#f39c12", "#2ecc71"],
        range_color=[50, 100],
        labels={"completeness_pct": "Completeness %", "accuracy_score": "Accuracy %"},
        title=f"Completeness vs. Accuracy — {entity_select.value} at {org_select.value}",
        height=340,
        size_max=16,
    )
    _fig_scatter.update_traces(textposition="top center", marker_size=12)
    _fig_scatter.update_layout(
        margin=dict(l=10, r=10, t=40, b=20),
        coloraxis_showscale=False,
    )
    # Add quadrant lines
    _fig_scatter.add_hline(y=80, line_dash="dot", line_color="#ccc")
    _fig_scatter.add_vline(x=80, line_dash="dot", line_color="#ccc")

    _acc_note = mo.callout(
        mo.md("Accuracy measures whether field values match the expected format or registry "
              "(e.g., DOI pattern, ORCID checksum, valid ROR id). Fields in the **lower-left "
              "quadrant** need both completeness and accuracy work."),
        kind="warn",
    )

    _accuracy_tab = mo.vstack([
        mo.md(f"### Accuracy — *{entity_select.value}* at *{org_select.value}*"),
        _acc_cards,
        _acc_note,
        mo.plotly(_fig_scatter),
        mo.accordion({
            "Timeline": mo.md("_Accuracy trends over time will appear here._"),
            "Details":  mo.md("_Record-level accuracy breakdown table._"),
            "Interventions": mo.md("_See the Enrichment tab for suggested actions._"),
        }),
    ], gap=3)

    _accuracy_tab if tabs_selector.value == "Accuracy" else mo.md("")
    return


# ===========================================================================
# TAB: Enrichment
# ===========================================================================

@app.cell(hide_code=True)
def _(
    actions_df,
    entity_select,
    mo,
    org_select,
    priority_color,
    tabs_selector,
):
    def _action_item(row):
        c = priority_color(row["priority"])
        return mo.Html(f"""
        <div style="border-left:4px solid {c};padding:12px 16px;margin-bottom:10px;
                    background:#f8f9fa;border-radius:0 6px 6px 0;">
            <div style="font-weight:700;color:{c};font-size:.85rem;">
                [{row['priority'].upper()}] {row['field']} — {row['scope']}
            </div>
            <div style="font-size:.82rem;color:#2c3e50;margin-top:6px;">
                {row['description']}
            </div>
            <div style="font-size:.75rem;color:#7f8c8d;margin-top:6px;">
                Affected: <strong>{row['affected_records']:,}</strong> records &nbsp;|&nbsp;
                Suggested: <code style="background:#e8f4f8;padding:2px 5px;border-radius:3px;">
                    {row['suggested_action']}</code>
            </div>
        </div>""")

    _action_rows = actions_df.to_dicts()
    _action_list = mo.vstack([_action_item(r) for r in _action_rows], gap=1)

    # Summary bar: affected records by priority
    import plotly.express as _px
    _fig_prio = _px.bar(
        actions_df.to_pandas(),
        x="field", y="affected_records",
        color="priority",
        color_discrete_map={"high": "#e74c3c", "medium": "#f39c12", "low": "#95a5a6"},
        title="Affected records by field and priority",
        labels={"affected_records": "Records affected", "field": "Metadata Field"},
        height=240,
    )
    _fig_prio.update_layout(margin=dict(l=10, r=10, t=40, b=20),
                            legend_title="Priority")

    _enrichment_tab = mo.vstack([
        mo.md(f"### Enrichment opportunities — *{entity_select.value}* at *{org_select.value}*"),
        mo.md("Metadata enrichment possibilities by showing record-level metadata fields "
              "from different sources. High-priority items should be addressed first."),
        mo.hstack([
            mo.vstack([mo.plotly(_fig_prio)], gap=0),
        ], gap=2),
        _action_list,
        mo.callout(
            mo.md("**Next steps:** Run the suggested ETL pipelines from the ORI data pipeline, "
                  "then re-check this dashboard to verify improvement. "
                  "Questions? Contact [ori-team@surf.nl](mailto:ori-team@surf.nl)."),
            kind="success",
        ),
    ], gap=3)

    _enrichment_tab if tabs_selector.value == "Enrichment" else mo.md("")
    return


# ---------------------------------------------------------------------------
# Footer
# ---------------------------------------------------------------------------

@app.cell(hide_code=True)
def _(datetime, mo, org_select):
    mo.md(f"""
    ---
    <div style="font-size:.75rem;color:#999;display:flex;justify-content:space-between;">
        <span>ORI Quality Dashboard · PID to Portal · SURF ORI team</span>
        <span>Organisation: {org_select.value} · Refreshed: {datetime.utcnow().strftime('%Y-%m-%d %H:%M UTC')}</span>
    </div>
    """)
    return


if __name__ == "__main__":
    app.run()
