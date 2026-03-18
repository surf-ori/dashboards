# /// script
# requires-python = ">=3.11"
# dependencies = [
#     "altair==6.0.0",
#     "marimo>=0.19.0",
#     "numpy>=1.26.0",
#     "openpyxl==3.1.5",
#     "pandas==2.3.3",
#     "pydantic-ai==1.67.0",
# ]
# ///

import marimo

__generated_with = "0.21.0"
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
def constants():
    # Title: Constants
    # Purpose: Define shared data source and metric labels used across the notebook.

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
        CRIS_EXPORTS,
        CRIS_REGISTRATIONS,
        DATA_URL,
        DEFAULT_RELATIVE_METRIC,
        ORCID_DATABASE,
        RELATIVE_METRICS,
        TOTAL_RESEARCHERS,
    )


@app.cell(hide_code=True)
def header():
    # Title: Header
    # Purpose: Render the notebook header with title, subtitle, and logos.

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
    # Title: Introduction
    # Purpose: Explain the data source and how the dashboard should be read.

    mo.md("""
    Dit dashboard gebruikt de Nationale ORCiD monitor [spreadsheet](https://docs.google.com/spreadsheets/d/e/2PACX-1vSVbrjJLpVfZ_zzzHHCuyuoy29OXla9R17XtzcbOEnIDc9I4-3k_7AyNjh5Ab04t9T54ge8idgpfMWi/pub?output=html) als bron. Ingevuld door de instellingen via dit [*web formulier*](https://docs.google.com/forms/d/e/1FAIpQLScxSZqFRWBO_sniAKiDL5mEiEEtkfcs2XOhWlV-BHnOzd_qXw/viewform).
    Kies in de sidebar welke instellingen je wilt vergelijken en bepaal vervolgens
    of de y-as absolute aantallen of relatieve waarden ten opzichte van
    `Aantal onderzoekers` toont.
    """)
    return


@app.cell(hide_code=True)
def survey_data(ABSOLUTE_METRICS, DATA_URL):
    # Title: Survey Data
    # Purpose: Load the spreadsheet, normalize key columns, and derive filter bounds.

    # Load the spreadsheet and trim column names so downstream lookups stay stable.
    survey_data = pd.read_excel(DATA_URL).rename(columns=lambda col: str(col).strip())

    # Convert timestamp columns to pandas datetimes and normalize measurement dates.
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

    # Ensure metric columns are numeric before aggregation.
    for column in ABSOLUTE_METRICS:
        survey_data[column] = pd.to_numeric(survey_data[column], errors="coerce")

    # Build sorted filter options and the available date range for the sidebar.
    universities = sorted(survey_data["Selecteer je Universiteit"].dropna().unique())
    cris_products = sorted(survey_data["Selecteer je CRIS product"].dropna().unique())
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
def metric_mode_control():
    # Title: Metric Mode Control
    # Purpose: Let the user switch between relative and absolute y-axis values.

    metric_mode = mo.ui.radio(
        options=["Relatief", "Absoluut"],
        value="Relatief",
        label=f"{mo.icon('lucide:chart-line')} Y-as type",
    )
    return (metric_mode,)


@app.cell(hide_code=True)
def metric_selector_control(
    ABSOLUTE_METRICS,
    DEFAULT_RELATIVE_METRIC,
    RELATIVE_METRICS,
    TOTAL_RESEARCHERS,
    metric_mode,
):
    # Title: Metric Selector Control
    # Purpose: Show the valid metric choices for the selected y-axis mode.

    # Limit the dropdown options to metrics that make sense for the current mode.
    metric_options = (
        RELATIVE_METRICS if metric_mode.value == "Relatief" else ABSOLUTE_METRICS
    )
    default_metric = (
        DEFAULT_RELATIVE_METRIC
        if metric_mode.value == "Relatief"
        else TOTAL_RESEARCHERS
    )

    # Create the dropdown with a mode-specific default value.
    metric_selector = mo.ui.dropdown(
        options=metric_options,
        value=default_metric,
        label=f"{mo.icon('lucide:type')} Y-as metric",
        full_width=True,
    )
    return (metric_selector,)


@app.cell(hide_code=True)
def filter_controls(
    cris_products,
    max_measurement_date,
    min_measurement_date,
    universities,
):
    # Title: Filter Controls
    # Purpose: Build the sidebar widgets for university, CRIS product, and date range.

    # University filter is optional; an empty selection means "all universities".
    university_filter = mo.ui.multiselect(
        options=universities,
        value=[],
        label=f"{mo.icon('lucide:landmark')} Universiteit",
        full_width=True,
    )

    # CRIS product filter is optional; an empty selection means "all products".
    cris_filter = mo.ui.multiselect(
        options=cris_products,
        value=[],
        label=f"{mo.icon('lucide:database')} CRIS product",
        full_width=True,
    )

    # Date inputs are constrained to the measurement dates available in the source.
    start_date = mo.ui.date(
        start=min_measurement_date,
        stop=max_measurement_date,
        value=min_measurement_date,
        label=f"{mo.icon('lucide:calendar')} Vanaf",
        full_width=True,
    )

    # The end date uses the same bounds and defaults to the latest measurement.
    end_date = mo.ui.date(
        start=min_measurement_date,
        stop=max_measurement_date,
        value=max_measurement_date,
        label=f"{mo.icon('lucide:calendar')} Tot en met",
        full_width=True,
    )
    return cris_filter, end_date, start_date, university_filter


@app.cell(hide_code=True)
def date_granularity_control():
    # Title: Date Granularity Control
    # Purpose: Let the user choose how measurements are bucketed on the timeline.

    date_granularity = mo.ui.dropdown(
        options=["Dag", "Week", "Maand", "Kwartaal", "Jaar"],
        value="Maand",
        label=f"{mo.icon('lucide:calendar-range')} Datumgranulariteit",
        full_width=True,
    )
    return (date_granularity,)


@app.cell(hide_code=True)
def projection_control():
    projection_toggle = mo.ui.radio(
        options=["Nee", "Ja"],
        value="Nee",
        label=f"{mo.icon('lucide:trending-up')} Projectie",
    )
    projection_years = mo.ui.dropdown(
        options=[0, 2, 5, 10],
        value=5,
        label=f"{mo.icon('lucide:calendar')} Projectie jaren",
        full_width=True,
    )
    return projection_toggle, projection_years


@app.cell(hide_code=True)
def sidebar_layout(
    cris_filter,
    date_granularity,
    end_date,
    metric_mode,
    metric_selector,
    projection_toggle,
    projection_years,
    start_date,
    university_filter,
):
    # Title: Sidebar
    # Purpose: Arrange the controls and explanatory copy inside the app sidebar.

    # Group the controls into a single card so the sidebar reads as one unit.
    filters_card = mo.vstack(
        [
            mo.md("### Filters"),
            mo.md(
                "Laat een selectie leeg om alle waarden te tonen. De relatieve modus deelt altijd door `Aantal onderzoekers`."
            ),
            mo.md("---"),
            metric_mode,
            metric_selector,
            date_granularity,
            mo.md("---"),
            university_filter,
            cris_filter,
            mo.md("---"),
            start_date,
            end_date,
            mo.md("---"),
            projection_toggle,
            projection_years,
        ],
        gap=1,
    )

    # Render the filter card inside the sidebar and keep a persistent footer link.
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
def filtered_survey_dataset(
    cris_filter,
    end_date,
    start_date,
    survey_data,
    university_filter,
):
    # Title: Filtered Survey Data
    # Purpose: Apply the current sidebar selections to the loaded survey dataset.

    # Start from the full dataset and progressively narrow it down.
    filtered_survey_data = survey_data.copy()

    # Filter by university only when the user made an explicit selection.
    if university_filter.value:
        filtered_survey_data = filtered_survey_data[
            filtered_survey_data["Selecteer je Universiteit"].isin(
                university_filter.value
            )
        ]

    # Filter by CRIS product only when the user made an explicit selection.
    if cris_filter.value:
        filtered_survey_data = filtered_survey_data[
            filtered_survey_data["Selecteer je CRIS product"].isin(cris_filter.value)
        ]

    # Normalize the chosen date range so reversed inputs still behave predictably.
    selected_start = pd.Timestamp(start_date.value)
    selected_end = pd.Timestamp(end_date.value)

    if selected_start > selected_end:
        selected_start, selected_end = selected_end, selected_start

    # Keep only rows that fall inside the selected measurement window.
    filtered_survey_data = filtered_survey_data[
        filtered_survey_data["Datum van meting"].between(selected_start, selected_end)
    ].copy()
    return (filtered_survey_data,)


@app.cell(hide_code=True)
def timeline_dataset(
    ABSOLUTE_METRICS,
    TOTAL_RESEARCHERS,
    date_granularity,
    filtered_survey_data,
    metric_mode,
    metric_selector,
):
    # Title: Timeline Data
    # Purpose: Keep the latest university measurement per time bucket and derive
    # both university and national-average timeline series.

    granularity_config = {
        "Dag": {"freq": "D", "label": "Dag"},
        "Week": {"freq": "W-SUN", "label": "Week"},
        "Maand": {"freq": "M", "label": "Maand"},
        "Kwartaal": {"freq": "Q", "label": "Kwartaal"},
        "Jaar": {"freq": "Y", "label": "Jaar"},
    }
    selected_granularity = granularity_config[date_granularity.value]

    if filtered_survey_data.empty:
        timeline_data = pd.DataFrame(
            columns=[
                "bucket_date",
                "period_label",
                "series_label",
                "series_type",
                "Datum van meting",
                "universities_in_average",
                *ABSOLUTE_METRICS,
                "metric_value",
            ]
        )
        series_order = ["Landelijk gemiddelde"]
    else:
        university_measurements = filtered_survey_data.copy()
        period_index = university_measurements["Datum van meting"].dt.to_period(
            selected_granularity["freq"]
        )
        university_measurements["bucket_date"] = period_index.dt.start_time

        # Within each university and time bucket, keep only the latest submitted measurement.
        university_measurements = (
            university_measurements.sort_values(
                [
                    "Selecteer je Universiteit",
                    "bucket_date",
                    "Tijdstempel",
                    "Datum van meting",
                ]
            )
            .drop_duplicates(
                subset=["Selecteer je Universiteit", "bucket_date"], keep="last"
            )
            .copy()
        )

        if date_granularity.value == "Dag":
            university_measurements["period_label"] = university_measurements[
                "bucket_date"
            ].dt.strftime("%Y-%m-%d")
        elif date_granularity.value == "Week":
            university_measurements["period_label"] = (
                "Week van "
                + university_measurements["bucket_date"].dt.strftime("%Y-%m-%d")
            )
        elif date_granularity.value == "Maand":
            university_measurements["period_label"] = university_measurements[
                "bucket_date"
            ].dt.strftime("%Y-%m")
        elif date_granularity.value == "Kwartaal":
            university_measurements["period_label"] = (
                "Q"
                + university_measurements["bucket_date"].dt.quarter.astype(str)
                + " "
                + university_measurements["bucket_date"].dt.strftime("%Y")
            )
        else:
            university_measurements["period_label"] = university_measurements[
                "bucket_date"
            ].dt.strftime("%Y")

        university_series = university_measurements[
            [
                "bucket_date",
                "period_label",
                "Datum van meting",
                "Selecteer je Universiteit",
                *ABSOLUTE_METRICS,
            ]
        ].copy()
        university_series["series_label"] = university_series[
            "Selecteer je Universiteit"
        ]
        university_series["series_type"] = "Universiteit"
        university_series["universities_in_average"] = pd.NA

        national_series = (
            university_measurements.groupby(
                ["bucket_date", "period_label"], as_index=False
            )
            .agg(
                {
                    "Datum van meting": "max",
                    "Selecteer je Universiteit": "nunique",
                    **{metric: "mean" for metric in ABSOLUTE_METRICS},
                }
            )
            .rename(
                columns={
                    "Selecteer je Universiteit": "universities_in_average",
                }
            )
        )
        national_series["series_label"] = "Landelijk gemiddelde"
        national_series["series_type"] = "Landelijk gemiddelde"

        timeline_data = pd.concat(
            [
                university_series.drop(columns=["Selecteer je Universiteit"]),
                national_series,
            ],
            ignore_index=True,
            sort=False,
        )
        series_order = [
            "Landelijk gemiddelde",
            *sorted(university_series["series_label"].dropna().unique()),
        ]

    # Derive the plotted metric from the per-series absolute metrics.
    if metric_mode.value == "Relatief":
        denominator = timeline_data[TOTAL_RESEARCHERS].replace({0: pd.NA})
        timeline_data["metric_value"] = (
            timeline_data[metric_selector.value] / denominator
        )
        y_axis_title = f"{metric_selector.value} / {TOTAL_RESEARCHERS}"
        y_axis_format = ".0%"
    else:
        timeline_data["metric_value"] = timeline_data[metric_selector.value]
        y_axis_title = metric_selector.value
        y_axis_format = ",.0f"

    timeline_data = (
        timeline_data.dropna(subset=["metric_value"])
        .sort_values(["bucket_date", "series_label"])
        .copy()
    )
    return (
        series_order,
        timeline_data,
        university_series,
        y_axis_format,
        y_axis_title,
    )


@app.cell(hide_code=True)
def summary_overview(
    filtered_survey_data,
    metric_mode,
    metric_selector,
    timeline_data,
):
    # Title: Summary Overview
    # Purpose: Show key stats for the current selection or an empty-state message.

    # Render a clear message when the current filters produce no usable timeline data.
    if filtered_survey_data.empty or timeline_data.empty:
        summary_content = mo.md(
            """
            ## Selectie zonder resultaten
            Pas de filters in de sidebar aan om metingen in de tijdlijn te tonen.
            """
        )
    else:
        summary_series = timeline_data[
            timeline_data["series_label"] == "Landelijk gemiddelde"
        ]
        if summary_series.empty:
            summary_series = timeline_data

        # Read the latest available point so the summary reflects the newest measurement.
        latest_point = summary_series.sort_values("bucket_date").iloc[-1]
        latest_metric_value = latest_point["metric_value"]
        latest_period_label = latest_point["period_label"]

        # Format the selected metric according to the current mode.
        if metric_mode.value == "Relatief":
            latest_value = "{:.1%}".format(latest_metric_value)
        else:
            latest_value = f"{latest_metric_value:,.0f}"

        # Compose the summary cards with counts, coverage, and the latest metric value.
        summary_cards = mo.hstack(
            [
                mo.stat(
                    label="Metingen in selectie",
                    value=f"{len(filtered_survey_data):,}",
                    bordered=True,
                ),
                mo.stat(
                    label="Universiteiten in selectie",
                    value=int(
                        filtered_survey_data["Selecteer je Universiteit"].nunique()
                    ),
                    bordered=True,
                ),
                mo.stat(
                    label="Laatste periode",
                    value=latest_period_label,
                    bordered=True,
                ),
                mo.stat(
                    label=metric_selector.value,
                    value=latest_value,
                    bordered=True,
                ),
            ],
            widths="equal",
            align="center",
        )

        # Render the summary section above the timeline chart.
        summary_content = mo.vstack(
            [
                mo.md("## Overzicht"),
                summary_cards,
            ],
            gap=1,
        )

    summary_content
    return


@app.cell(hide_code=True)
def timeline_chart(
    CRIS_EXPORTS,
    CRIS_REGISTRATIONS,
    ORCID_DATABASE,
    TOTAL_RESEARCHERS,
    date_granularity,
    filtered_survey_data,
    metric_mode,
    projection_toggle,
    projection_years,
    series_order,
    timeline_data,
    y_axis_format,
    y_axis_title,
):
    # Title: Timeline Chart
    # Purpose: Plot the aggregated measurements over time for the selected metric.

    from numpy import polyfit, polyval

    tooltip_fields = [
        alt.Tooltip("series_label:N", title="Categorie"),
        alt.Tooltip("period_label:N", title="Periode"),
        alt.Tooltip("Datum van meting:T", title="Laatste meting in bucket"),
        alt.Tooltip("metric_value:Q", title=y_axis_title, format=y_axis_format),
        alt.Tooltip(
            f"{TOTAL_RESEARCHERS}:Q",
            title=TOTAL_RESEARCHERS,
            format=",.0f",
        ),
        alt.Tooltip(
            f"{CRIS_REGISTRATIONS}:Q",
            title=CRIS_REGISTRATIONS,
            format=",.0f",
        ),
        alt.Tooltip(
            f"{CRIS_EXPORTS}:Q",
            title=CRIS_EXPORTS,
            format=",.0f",
        ),
        alt.Tooltip(
            f"{ORCID_DATABASE}:Q",
            title=ORCID_DATABASE,
            format=",.0f",
        ),
        alt.Tooltip(
            "universities_in_average:Q",
            title="Universiteiten in gemiddelde",
            format=",.0f",
        ),
    ]

    # Skip chart rendering when the active filters do not produce any timeline points.
    if filtered_survey_data.empty or timeline_data.empty:
        timeline_content = mo.md("")
    else:
        # Add a percentage hint to the axis label when the chart is in relative mode.
        if metric_mode.value == "Relatief":
            y_axis_label = f"{y_axis_title} (%)"
        else:
            y_axis_label = y_axis_title

        x_axis_format = {
            "Dag": "%d %b %Y",
            "Week": "%d %b %Y",
            "Maand": "%b %Y",
            "Kwartaal": "%b %Y",
            "Jaar": "%Y",
        }[date_granularity.value]

        # Draw one line per university and a separate national-average line.
        timeline_chart = (
            alt.Chart(timeline_data)
            .mark_line(
                point=False,
                strokeWidth=3,
            )
            .encode(
                x=alt.X(
                    "bucket_date:T",
                    title=f"Tijdlijn ({date_granularity.value.lower()})",
                    axis=alt.Axis(format=x_axis_format, labelAngle=-30),
                ),
                y=alt.Y(
                    "metric_value:Q",
                    title=y_axis_label,
                    axis=alt.Axis(format=y_axis_format),
                ),
                color=alt.Color(
                    "series_label:N",
                    title="Categorie",
                    sort=series_order,
                ),
                strokeDash=alt.StrokeDash(
                    "series_type:N",
                    legend=None,
                    scale=alt.Scale(
                        domain=["Landelijk gemiddelde", "Universiteit"],
                        range=[[10, 5], [1, 0]],
                    ),
                ),
                tooltip=tooltip_fields,
            )
        )

        # Overlay points so individual measurements remain easy to inspect.
        timeline_points = (
            alt.Chart(timeline_data)
            .mark_circle(
                size=70,
            )
            .encode(
                x="bucket_date:T",
                y="metric_value:Q",
                color=alt.Color("series_label:N", title="Categorie", sort=series_order),
                tooltip=tooltip_fields,
            )
        )

        # Generate projection trend lines if enabled
        chart_layers = [timeline_chart, timeline_points]

        if projection_toggle.value == "Ja" and projection_years.value > 0:
            projection_data = []
            years_ahead = projection_years.value

            granularity_freq = {
                "Dag": "D",
                "Week": "W",
                "Maand": "ME",
                "Kwartaal": "Q",
                "Jaar": "Y",
            }[date_granularity.value]

            for series_label in timeline_data["series_label"].unique():
                series_df = timeline_data[
                    timeline_data["series_label"] == series_label
                ].copy()
                if len(series_df) < 2:
                    continue

                series_df = series_df.sort_values("bucket_date")
                x_dates = series_df["bucket_date"]
                y_values = series_df["metric_value"]

                x_ordinal = x_dates.apply(lambda d: d.toordinal()).values
                y_vals = y_values.values

                try:
                    coeffs = polyfit(x_ordinal, y_vals, 1)
                except Exception:
                    continue

                last_date = x_dates.max()
                periods_ahead = {
                    "Dag": years_ahead * 365,
                    "Week": years_ahead * 52,
                    "Maand": years_ahead * 12,
                    "Kwartaal": years_ahead * 4,
                    "Jaar": years_ahead,
                }[date_granularity.value]

                future_dates = pd.date_range(
                    start=last_date, periods=periods_ahead + 1, freq=granularity_freq
                )[1:]

                future_ordinal = (
                    future_dates.to_series().apply(lambda d: d.toordinal()).values
                )
                future_values = polyval(coeffs, future_ordinal)

                for fd, fv in zip(future_dates, future_values):
                    projection_data.append(
                        {
                            "bucket_date": fd,
                            "metric_value": fv,
                            "series_label": series_label,
                            "series_type": series_df["series_type"].iloc[0],
                            "period_label": f"Projectie +{years_ahead}j",
                            "Datum van meting": fd,
                            "universities_in_average": pd.NA,
                            TOTAL_RESEARCHERS: pd.NA,
                            CRIS_REGISTRATIONS: pd.NA,
                            CRIS_EXPORTS: pd.NA,
                            ORCID_DATABASE: pd.NA,
                            "is_projection": True,
                        }
                    )

            if projection_data:
                projection_df = pd.DataFrame(projection_data)

                projection_line = (
                    alt.Chart(projection_df)
                    .mark_line(
                        point=False,
                        strokeWidth=2,
                        strokeDash=[4, 4],
                    )
                    .encode(
                        x=alt.X(
                            "bucket_date:T",
                            axis=alt.Axis(format=x_axis_format, labelAngle=-30),
                        ),
                        y=alt.Y(
                            "metric_value:Q",
                            axis=alt.Axis(format=y_axis_format),
                        ),
                        color=alt.Color(
                            "series_label:N",
                            title="Categorie",
                            sort=series_order,
                        ),
                        strokeDash=alt.StrokeDash(
                            "series_type:N",
                            legend=None,
                            scale=alt.Scale(
                                domain=["Landelijk gemiddelde", "Universiteit"],
                                range=[[10, 5], [1, 0]],
                            ),
                        ),
                        tooltip=tooltip_fields,
                    )
                )
                chart_layers.append(projection_line)

        # Render the explanatory text together with the combined chart.
        timeline_content = mo.vstack(
            [
                mo.md("## Tijdlijn"),
                mo.md(
                    "Per universiteit toont de grafiek steeds de laatst ingestuurde meting binnen de gekozen periode. De lijn `Landelijk gemiddelde` is het gemiddelde van alle universiteiten die in die periode een meting hebben."
                ),
                alt.layer(*chart_layers)
                .properties(height=400, width=600)
                .configure_axis(labelFontSize=12, titleFontSize=13)
                .configure_view(strokeOpacity=0),
            ],
            gap=1,
        )

    timeline_content
    return


@app.cell
def timeline_data_table(university_series):
    mo.ui.table(university_series)
    return


if __name__ == "__main__":
    app.run()
