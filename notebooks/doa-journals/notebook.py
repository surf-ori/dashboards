# /// script
# requires-python = ">=3.12"
# dependencies = [
#     "altair==6.0.0",
#     "fastexcel==0.19.0",
#     "marimo",
#     "openpyxl==3.1.5",
#     "polars==1.38.1",
#     "pyarrow==23.0.1",
# ]
# ///

import marimo

__generated_with = "0.21.1"
app = marimo.App(
    width="full",
    app_title="Diamond Open Access journals in the Netherlands",
)


@app.cell
async def _():
    import micropip
    await micropip.install(['polars', 'altair', 'openpyxl'])

    import marimo as mo
    import polars as pl
    import altair as alt

    return alt, mo, pl


@app.cell
def _(mo):
    mo.md(r"""
    # Diamond Open Access journals in the Netherlands
    """)
    return


@app.cell
def _(domain_chart, mo, platform_chart, publisher_chart, years_chart):
    mo.vstack([
        mo.hstack([platform_chart, publisher_chart]),
        mo.hstack([domain_chart, years_chart])
    ])
    return


@app.cell
def _(journals, mo):
    get_state, set_state = mo.state(journals)
    return get_state, set_state


@app.cell
def _(mo):
    mo.md(r"""
    This dashboard is based on the dataset **Livio, C & Kramer, B (2025)**: A curated list of Diamond OA journals in the Netherlands. *Version 2, Zenodo,* [doi: 10.5281/zenodo.17185088](https://doi.org/10.5281/zenodo.17185088).
    """)
    return


@app.cell
def _(mo):
    data_selector = mo.ui.dropdown({
        'data from Google Sheet': ('https://docs.google.com/spreadsheets/d/1CaHEgS6Nu9OAeyEHe9tmCpMgnhRr196zFp2gsRw20YM/export?format=xlsx', 'Included Diamond OA Journals'),
        'data from Zenodo': ('https://zenodo.org/api/records/17195184/files/Diamond%20OA%20-%20NL%20Journals%20-%20WGNM%20-%20FINAL%20-%20SHAREABLE-version%202.xlsx/content', 'Included DOA Journals')
    }, value='data from Zenodo')
    data_selector
    return (data_selector,)


@app.cell
def _(data_selector, pl):
    data_path, sheet_name = data_selector.value

    journals = pl.read_excel(data_path, sheet_name=sheet_name, engine='openpyxl').fill_null('unknown')
    journals
    return (journals,)


@app.cell
def _(alt, get_state, journals, mo, set_state):
    years_chart = mo.ui.altair_chart(
        alt.Chart(journals if len(get_state()) == 0 else get_state())
        .mark_bar()
        .encode(
            x=alt.X(field='DOAJ - Year OA', type='quantitative'),
            y=alt.Y(aggregate='count', type='quantitative', title='Number of journals'),
            color=alt.Color(field='Model', type='nominal'),
            tooltip=[
                alt.Tooltip(field='DOAJ - Year OA', format='.0f'),
                alt.Tooltip(field='Model'),
                alt.Tooltip(aggregate='count', title='Number of journals')
            ]
        )
        .properties(height=300, width=300),
        on_change=set_state
    )
    return (years_chart,)


@app.cell
def _(alt, get_state, journals, mo, set_state):
    domain_chart = mo.ui.altair_chart(
        alt.Chart(journals if len(get_state()) == 0 else get_state())
        .mark_arc(innerRadius=80)
        .encode(
            color=alt.Color(field='OpenAlex - domain', type='nominal'),
            theta=alt.Theta(aggregate='count', type='quantitative'),
            tooltip=[
                alt.Tooltip(field='OpenAlex - domain'),
                alt.Tooltip(aggregate='count', title='Number of journals'),
                # alt.Tooltip(field='Journal Title')
            ]
        )
        .properties(height=300, width=300),
        on_change=set_state
    )
    return (domain_chart,)


@app.cell
def _(alt, get_state, journals, mo, set_state):
    publisher_chart = mo.ui.altair_chart(
        alt.Chart(journals if len(get_state()) == 0 else get_state())
        .mark_arc(innerRadius=80)
        .encode(
            color=alt.Color(field='Publisher', type='nominal'),
            theta=alt.Theta(aggregate='count', type='quantitative'),
            tooltip=[
                # alt.Tooltip(aggregate='count'),
                alt.Tooltip(field='Publisher'),
                alt.Tooltip(field='Journal Title')
            ]
        )
        .properties(height=300, width=300),
        on_change=set_state
    )
    return (publisher_chart,)


@app.cell
def _(alt, get_state, journals, mo, set_state):
    platform_chart = mo.ui.altair_chart(
        alt.Chart(journals if len(get_state()) == 0 else get_state())
        .mark_arc(innerRadius=80)
        .encode(
            color=alt.Color(field='Technical platform', type='nominal'),
            theta=alt.Theta(aggregate='count', type='quantitative'),
            tooltip=[
                alt.Tooltip(field='Technical platform'),
                alt.Tooltip(aggregate='count', title='Number of journals'),
                # alt.Tooltip(field='Publisher')
            ]
        )
        .properties(height=300, width=300),
        on_change=set_state
    )
    return (platform_chart,)


@app.cell
def _():
    return


if __name__ == "__main__":
    app.run()
