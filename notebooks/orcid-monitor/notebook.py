# /// script
# requires-python = ">=3.11"
# dependencies = [
#     "altair==6.0.0",
#     "marimo>=0.19.0",
#     "openpyxl==3.1.5",
#     "pandas==2.3.3",
#     "pydantic-ai==1.67.0",
# ]
# ///

import marimo

__generated_with = "0.20.4"
app = marimo.App(width="full", app_title="Dutch ORCiD Monitor")

async with app.setup(hide_code=True):
    try:
        import micropip
    except ModuleNotFoundError:
        micropip = None

    if micropip is not None:
        await micropip.install(["openpyxl", "pandas"])

    import altair as alt
    import marimo as mo
    import pandas as pd


@app.cell(hide_code=True)
def _():
    # add constants

    DATA_URL = (
        "https://docs.google.com/spreadsheets/d/e/"
        "2PACX-1vSVbrjJLpVfZ_zzzHHCuyuoy29OXla9R17XtzcbOEnIDc9I4-3k_7AyNjh5Ab04t9T54ge8idgpfMWi/"
        "pub?output=xlsx"
    )

    TOTAL_RESEARCHERS = "Aantal onderzoekers"
    CRIS_REGISTRATIONS = "Aantal ORCiD registraties in het CRIS van Onderzoekers"
    CRIS_EXPORTS = "Aantal ORCiD Export Koppelingen in het CRIS van Onderzoekers"
    ORCID_DATABASE = "Aantal Onderzoekers in de ORCiD database"

    ABSOLUTE_METRICS = [
        TOTAL_RESEARCHERS,
        CRIS_REGISTRATIONS,
        CRIS_EXPORTS,
        ORCID_DATABASE,
    ]

    RELATIVE_METRICS = [
        CRIS_REGISTRATIONS,
        CRIS_EXPORTS,
        ORCID_DATABASE,
    ]

    DEFAULT_RELATIVE_METRIC = CRIS_REGISTRATIONS
    return (
        ABSOLUTE_METRICS,
        DATA_URL,
        DEFAULT_RELATIVE_METRIC,
        RELATIVE_METRICS,
        TOTAL_RESEARCHERS,
    )


@app.cell(hide_code=True)
def _():
    mo.md("""
    <div style="
        display: flex;
        align-items: center;
        justify-content: space-between;
        gap: 1.5rem;
        padding-bottom: 0.75rem;
        border-bottom: 1px solid #e5e5e5;
        margin-bottom: 1rem;
    ">
        <div>
            <h1 style="margin: 0;">Dutch ORCiD Monitor</h1>
            <div style="color: #666; font-size: 0.95rem; margin-top: 0.35rem;">
                Tijdlijn van ORCiD-adoptie in Nederlandse CRIS-systemen
            </div>
        </div>
        <div style="display: flex; align-items: center; gap: 1rem;">
            <img
                src="public/ORCID-iD_icon_unauth_vector.svg"
                alt="ORCID logo"
                style="height: 42px;"
            />
            <img
                src="https://www.surf.nl/themes/surf/logo.svg"
                alt="SURF logo"
                style="height: 42px;"
            />
        </div>
    </div>
    """)
    return


@app.cell(hide_code=True)
def _():
    mo.md("""
    Deze dashboardweergave gebruikt de SURF ORCiD monitor spreadsheet als bron.
    Kies in de sidebar welke instellingen je wilt vergelijken en bepaal vervolgens
    of de y-as absolute aantallen of relatieve waarden ten opzichte van
    `Aantal onderzoekers` toont.
    """)
    return


@app.cell(hide_code=True)
def _(ABSOLUTE_METRICS, DATA_URL):
    survey_data = pd.read_excel(DATA_URL).rename(columns=lambda col: str(col).strip())
    survey_data["Tijdstempel"] = pd.to_datetime(
        survey_data["Tijdstempel"], errors="coerce"
    )
    survey_data["Datum van meting"] = pd.to_datetime(
        survey_data["Datum van meting"], errors="coerce"
    ).dt.normalize()
    survey_data["Selecteer je Universiteit"] = survey_data[
        "Selecteer je Universiteit"
    ].fillna("Onbekend")
    survey_data["Selecteer je CRIS product"] = survey_data[
        "Selecteer je CRIS product"
    ].fillna("Onbekend")

    for column in ABSOLUTE_METRICS:
        survey_data[column] = pd.to_numeric(survey_data[column], errors="coerce")

    universities = sorted(survey_data["Selecteer je Universiteit"].dropna().unique())
    cris_products = sorted(
        survey_data["Selecteer je CRIS product"].dropna().unique()
    )
    min_measurement_date = survey_data["Datum van meting"].min().date()
    max_measurement_date = survey_data["Datum van meting"].max().date()
    return (
        cris_products,
        max_measurement_date,
        min_measurement_date,
        survey_data,
        universities,
    )


@app.cell(hide_code=True)
def _():
    metric_mode = mo.ui.radio(
        options=["Relatief", "Absoluut"],
        value="Relatief",
        label=f"{mo.icon('lucide:chart-line')} Y-as type",
    )
    return (metric_mode,)


@app.cell(hide_code=True)
def _(
    ABSOLUTE_METRICS,
    DEFAULT_RELATIVE_METRIC,
    RELATIVE_METRICS,
    TOTAL_RESEARCHERS,
    metric_mode,
):
    metric_options = (
        RELATIVE_METRICS if metric_mode.value == "Relatief" else ABSOLUTE_METRICS
    )
    default_metric = (
        DEFAULT_RELATIVE_METRIC
        if metric_mode.value == "Relatief"
        else TOTAL_RESEARCHERS
    )

    metric_selector = mo.ui.dropdown(
        options=metric_options,
        value=default_metric,
        label=f"{mo.icon('lucide:type')} Y-as metric",
        full_width=True,
    )
    return (metric_selector,)


@app.cell(hide_code=True)
def _(cris_products, max_measurement_date, min_measurement_date, universities):
    university_filter = mo.ui.multiselect(
        options=universities,
        value=[],
        label=f"{mo.icon('lucide:landmark')} Universiteit",
        full_width=True,
    )

    cris_filter = mo.ui.multiselect(
        options=cris_products,
        value=[],
        label=f"{mo.icon('lucide:database')} CRIS product",
        full_width=True,
    )

    start_date = mo.ui.date(
        start=min_measurement_date,
        stop=max_measurement_date,
        value=min_measurement_date,
        label=f"{mo.icon('lucide:calendar')} Vanaf",
        full_width=True,
    )

    end_date = mo.ui.date(
        start=min_measurement_date,
        stop=max_measurement_date,
        value=max_measurement_date,
        label=f"{mo.icon('lucide:calendar')} Tot en met",
        full_width=True,
    )
    return cris_filter, end_date, start_date, university_filter


@app.cell(hide_code=True)
def _(
    cris_filter,
    end_date,
    metric_mode,
    metric_selector,
    start_date,
    university_filter,
):
    filters_card = mo.vstack(
        [
            mo.md("### Filters"),
            mo.md(
                "Laat een selectie leeg om alle waarden te tonen. De relatieve modus deelt altijd door `Aantal onderzoekers`."
            ),
            mo.md("---"),
            metric_mode,
            metric_selector,
            mo.md("---"),
            university_filter,
            cris_filter,
            mo.md("---"),
            start_date,
            end_date,
        ],
        gap=1,
    )

    mo.sidebar(
        item=mo.vstack(
            [
                mo.md(
                    """
                    <div style="
                        background: #f5f5f5;
                        padding: 12px;
                        border-radius: 10px;
                        border: 1px solid #e5e5e5;
                    ">
                    """
                ),
                filters_card,
                mo.md("</div>"),
            ],
            gap=0,
        ),
        footer=mo.md(
            "[SURF | Open Research Information](https://github.com/surf-ori/dashboards)"
        ),
        width="380px",
    )
    return


@app.cell(hide_code=True)
def _(cris_filter, end_date, start_date, survey_data, university_filter):
    filtered_survey_data = survey_data.copy()

    if university_filter.value:
        filtered_survey_data = filtered_survey_data[
            filtered_survey_data["Selecteer je Universiteit"].isin(
                university_filter.value
            )
        ]

    if cris_filter.value:
        filtered_survey_data = filtered_survey_data[
            filtered_survey_data["Selecteer je CRIS product"].isin(cris_filter.value)
        ]

    selected_start = pd.Timestamp(start_date.value)
    selected_end = pd.Timestamp(end_date.value)

    if selected_start > selected_end:
        selected_start, selected_end = selected_end, selected_start

    filtered_survey_data = filtered_survey_data[
        filtered_survey_data["Datum van meting"].between(selected_start, selected_end)
    ].copy()
    return (filtered_survey_data,)


@app.cell(hide_code=True)
def _(
    ABSOLUTE_METRICS,
    TOTAL_RESEARCHERS,
    filtered_survey_data,
    metric_mode,
    metric_selector,
):
    timeline_data = (
        filtered_survey_data.groupby("Datum van meting", as_index=False)[ABSOLUTE_METRICS]
        .sum(min_count=1)
        .sort_values("Datum van meting")
    )

    if not timeline_data.empty:
        if metric_mode.value == "Relatief":
            denominator = timeline_data[TOTAL_RESEARCHERS].replace({0: pd.NA})
            timeline_data["metric_value"] = timeline_data[metric_selector.value] / denominator
            y_axis_title = f"{metric_selector.value} / {TOTAL_RESEARCHERS}"
            y_axis_format = ".0%"
        else:
            timeline_data["metric_value"] = timeline_data[metric_selector.value]
            y_axis_title = metric_selector.value
            y_axis_format = ",.0f"

        timeline_data = timeline_data.dropna(subset=["metric_value"]).copy()
    else:
        timeline_data["metric_value"] = []
        y_axis_title = metric_selector.value
        y_axis_format = ",.0f"
    return


app._unparsable_cell(
    """
    if filtered_survey_data.empty or timeline_data.empty:
        mo.md(
            \"\"\"
            ## Selectie zonder resultaten
            Pas de filters in de sidebar aan om metingen in de tijdlijn te tonen.
            \"\"\"
        )
        return

    latest_point = timeline_data.iloc[-1]
    latest_metric_value = latest_point[\"metric_value\"]
    latest_measurement_date = latest_point[\"Datum van meting\"]

    if metric_mode.value == \"Relatief\":
        latest_value = \"{:.1%}\".format(latest_metric_value)
    else:
        latest_value = f\"{latest_metric_value:,.0f}\"

    summary_cards = mo.hstack(
        [
            mo.stat(
                label=\"Metingen in selectie\",
                value=f\"{len(filtered_survey_data):,}\",
                bordered=True,
            ),
            mo.stat(
                label=\"Universiteiten in selectie\",
                value=int(
                    filtered_survey_data[\"Selecteer je Universiteit\"].nunique()
                ),
                bordered=True,
            ),
            mo.stat(
                label=\"Laatste meetdatum\",
                value=latest_measurement_date.strftime(\"%Y-%m-%d\"),
                bordered=True,
            ),
            mo.stat(
                label=metric_selector.value,
                value=latest_value,
                bordered=True,
            ),
        ],
        widths=\"equal\",
        align=\"center\",
    )

    mo.vstack(
        [
            mo.md(\"## Overzicht\"),
            summary_cards,
        ],
        gap=1,
    )
    """,
    column=None, disabled=False, hide_code=True, name="_"
)


app._unparsable_cell(
    r"""
    if filtered_survey_data.empty or timeline_data.empty:
        return

    if metric_mode.value == "Relatief":
        y_axis_label = f"{y_axis_title} (%)"
    else:
        y_axis_label = y_axis_title

    timeline_chart = alt.Chart(timeline_data).mark_line(
        color="#0f766e",
        point=False,
        strokeWidth=3,
    ).encode(
        x=alt.X(
            "Datum van meting:T",
            title="Tijdlijn",
            axis=alt.Axis(format="%b %Y", labelAngle=-30),
        ),
        y=alt.Y(
            "metric_value:Q",
            title=y_axis_label,
            axis=alt.Axis(format=y_axis_format),
        ),
        tooltip=[
            alt.Tooltip("Datum van meting:T", title="Datum"),
            alt.Tooltip("metric_value:Q", title=y_axis_title, format=y_axis_format),
            alt.Tooltip(f"{TOTAL_RESEARCHERS}:Q", title=TOTAL_RESEARCHERS, format=",.0f"),
            alt.Tooltip(f"{CRIS_REGISTRATIONS}:Q", title=CRIS_REGISTRATIONS, format=",.0f"),
            alt.Tooltip(f"{CRIS_EXPORTS}:Q", title=CRIS_EXPORTS, format=",.0f"),
            alt.Tooltip(f"{ORCID_DATABASE}:Q", title=ORCID_DATABASE, format=",.0f"),
        ],
    )

    timeline_points = alt.Chart(timeline_data).mark_circle(
        color="#0f766e",
        size=90,
    ).encode(
        x="Datum van meting:T",
        y="metric_value:Q",
        tooltip=[
            alt.Tooltip("Datum van meting:T", title="Datum"),
            alt.Tooltip("metric_value:Q", title=y_axis_title, format=y_axis_format),
        ],
    )

    mo.vstack(
        [
            mo.md("## Tijdlijn"),
            mo.md(
                "De grafiek telt de gekozen selectie per meetdatum op. In relatieve modus is de waarde de geselecteerde teller gedeeld door `Aantal onderzoekers`."
            ),
            (timeline_chart + timeline_points)
            .properties(height=460, width="container")
            .configure_axis(labelFontSize=12, titleFontSize=13)
            .configure_view(strokeOpacity=0),
        ],
        gap=1,
    )
    """,
    column=None, disabled=False, hide_code=True, name="_"
)


if __name__ == "__main__":
    app.run()
