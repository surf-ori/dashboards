# /// script
# requires-python = ">=3.12"
# dependencies = [
#     "altair==6.0.0",
#     "fastexcel==0.19.0",
#     "marimo==0.21.1",
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
def _(mo):
    mo.hstack([
            mo.md('[::streamline-plump:return-3-remix:: Back to all dashboards](.)'),
            mo.image('public/DiamondOpenAccess_expertise-center_logo_RGB_v1.svg', height=70)
        ], justify='space-between')
    return


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

    This dashboard showcases the curated list of Diamond OA journals in the Netherlands compiled by the working group Mapping and Monitoring Diamond OA for the Dutch Expertise Centre Diamond OA.
    The goal of this dashboard is to serve as a curation and monitoring tool for anyone involved in the Dutch Diamond OA ecosystem. It provides an overview of which journals exist, who runs them, and on which technical platform, and helps identify gaps in registry coverage.

    Are you a Diamond OA journal or publisher and would like to be featured in our dashboard? Let us know by filling in this Google Form: [Submit/update your journal](https://docs.google.com/forms/d/e/1FAIpQLSeW_A4OiY3TiHnMQILBtt-aOShhrJm3_c53zHcjMIhbAtJVzg/viewform?usp=sharing&ouid=107608145513868902468)

    This dashboard is based on the dataset **Livio, C & Kramer, B (2025)**: A curated list of Diamond OA journals in the Netherlands. *Version 2, Zenodo,* [doi: 10.5281/zenodo.17185088](https://doi.org/10.5281/zenodo.17185088).
    """)
    return


@app.cell
def _(mo, reset):
    mo.ui.button(label='reset all selections', on_click=reset)
    return


@app.cell
def _(get_selected_publishers, mo, publishers, set_selected_publishers):
    publisher_selector = mo.ui.multiselect(options=publishers, value=get_selected_publishers(), on_change=set_selected_publishers)
    return (publisher_selector,)


@app.cell
def _(mo, publisher_selector, selection):
    mo.hstack([
        mo.vstack([
            mo.md(f'Select one or more publishers: {publisher_selector}'),
        ]),
        mo.hstack(
        [mo.stat(label=f'Total number of journals', value=selection.height)] + 
        [
            mo.stat(label=f'Journals in {source}',
                    value=selection[f'in_{source}'].sum(),
                    caption=f'{selection[f'in_{source}'].sum()/selection.height*100:.0f} % of total',
                    bordered=True)
            for source in ['DOAJ', 'DDH', 'OpenAlex']   
        ], justify='end')
    ], justify='center')
    return


@app.cell
def _(connection_chart, domain_chart, mo, platform_chart, years_chart):
    mo.vstack([
        mo.hstack([platform_chart, connection_chart]),
        mo.hstack([domain_chart, years_chart])
    ])
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    Below you find the data that you selected through the interactive charts. You can download it in various formats.
    """)
    return


@app.cell
def _(pl, selection):
    selection.select(
        pl.exclude([
            'UUID', 'ISSN', 'ISSN-L', 'Type', 'Discontinued journal', 'include', ' exlusion criteria', 'notes on inclusion/exclusion',
            'Journal in DDH (Yes/No)', 'in_DDH', 'in_OpenAlex', 'in_DOAJ', 'OpenAlex - % NL affiliations 2022-2024',
            'OpenAlex - % non-en 2022-2024'
        ]),
        pl.col('OpenAlex - % NL affiliations 2022-2024').map_elements(lambda ratio: f'{ratio*100:.1f} %', return_dtype=pl.String),
        pl.col('OpenAlex - % non-en 2022-2024').map_elements(lambda ratio: f'{ratio*100:.1f} %', return_dtype=pl.String)
    )
    return


@app.cell
def _(pl):
    data_path, sheet_name = ('https://docs.google.com/spreadsheets/d/19RDdKVJoWXF35MiyOYLKTqXT1cGhBEA33sbEjTSxMcY/export?format=xlsx', 'Included Diamond OA Journals')

    journals_all = (
        pl
        .read_excel(data_path, sheet_name=sheet_name, engine='openpyxl')
        .fill_null('unknown')
        .with_columns(
            pl.col('DOAJ - Year added').cast(pl.String),
            in_DDH=pl.col('Diamond Discovery Hub ID').eq('unknown').not_(),
            in_OpenAlex=pl.col('OpenAlex ID').eq('unknown').not_(),
            in_DOAJ=pl.col('DOAJ ID').eq('unknown').not_(),
            NL_connection=pl.col('NL connection')#.str.split(by=',')#.str.replace(r'\(.+?\) ?', '')
        )
    )
    return (journals_all,)


@app.cell
def _(get_selected_publishers, journals_all, mo, pl):
    if len(get_selected_publishers()) > 0:
        journals = journals_all.filter(pl.col('Publisher').is_in(get_selected_publishers()))
    else:
        journals = journals_all
    get_state, set_state = mo.state(journals)
    return get_state, journals, set_state


@app.cell
def _(get_state, journals):
    selection = (journals if len(get_state()) == 0 else get_state())
    return (selection,)


@app.cell
def _(journals_all, mo):
    publishers = journals_all['Publisher'].unique().sort().to_list()
    get_selected_publishers, set_selected_publishers = mo.state([])
    return get_selected_publishers, publishers, set_selected_publishers


@app.cell
def _(journals_all, set_selected_publishers, set_state):
    def reset(x):
        set_selected_publishers([])
        set_state(journals_all)

    return (reset,)


@app.cell
def _(alt, get_state, journals, mo, set_state):
    database_chart = mo.ui.altair_chart(
        alt.Chart(journals if len(get_state()) == 0 else get_state())
        .mark_bar(innerRadius=80)
        .encode(
            alt.Color(field='in_DOAJ', type='nominal'),
            alt.X(field='Journal Title', type='nominal'),
            alt.Y(field='in_DOAJ'),
            # alt.Y(field='in_OpenAlex'),
            tooltip=[
                alt.Tooltip(field='OpenAlex - domain'),
                alt.Tooltip(aggregate='count', title='Number of journals'),
                # alt.Tooltip(field='Journal Title')
            ]
        )
        .properties(height=300, width=800),
        on_change=set_state
    )
    # database_chart
    return


@app.cell
def _(alt, get_state, journals, mo, set_state):
    connection_chart = mo.ui.altair_chart(
        alt.Chart(journals if len(get_state()) == 0 else get_state(), title='Connection to the Netherlands')
        .mark_arc(innerRadius=80)
        .encode(
            color=alt.Color(field='NL_connection', type='nominal'),
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
    return (connection_chart,)


@app.cell
def _(alt, get_state, journals, mo, set_state):
    years_chart = mo.ui.altair_chart(
        alt.Chart(journals if len(get_state()) == 0 else get_state(), title='Year added to DOAJ')
        .mark_bar()
        .encode(
            x=alt.X(field='DOAJ - Year added', type='temporal'),
            y=alt.Y(aggregate='count', type='quantitative', title='Number of journals'),
            tooltip=[
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
        alt.Chart(journals if len(get_state()) == 0 else get_state(), title='OpenAlex domains')
        .mark_arc(innerRadius=80)
        .encode(
            color=alt.Color(field='OpenAlex - domain', type='nominal').legend(title=None),
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
    platforms = (journals if len(get_state()) == 0 else get_state())

    platform_base_chart = (
        alt.Chart(platforms, title='Technical platform')
        .encode(
            x=alt.X(aggregate='count', type='quantitative').axis(None),
            y=alt.Y(field='Technical platform', type='ordinal', title=None).sort(aggregate='count'),
            text=alt.Text(aggregate='count', type='quantitative'),
            tooltip=[
                alt.Tooltip(field='Technical platform'),
                alt.Tooltip(aggregate='count', title='Number of journals'),
                # alt.Tooltip(field='Publisher')
            ]
        )

    )

    platform_chart = mo.ui.altair_chart(
        (
            platform_base_chart
            .mark_bar(
                cornerRadiusTopRight=3,
                cornerRadiusBottomRight=3,
                height=15
            )
            +
            platform_base_chart
            .mark_text(align='left', dx=2) 
        ).configure_view(stroke=None)
         .properties(width=300),
        on_change=set_state
    )
    return (platform_chart,)


@app.cell
def _(mo):
    mo.Html('''
    <footer class="mt-10 pt-6 border-t border-gray-200 text-center text-sm text-gray-600">
        This dashboard was made by:
        <div class="italic text-gray-500 text-sm">Chiara Livio  <a href="https://orcid.org/0000-0003-1219-1775"><img src="public/ORCID-iD_icon_unauth_vector.svg" alt="Orcid link" class="inline h-4"></a></div>
        <div class="italic text-gray-500 text-sm">Till Bey  <a href="https://orcid.org/0000-0001-7509-9875"><img src="public/ORCID-iD_icon_unauth_vector.svg" alt="Orcid link" class="inline h-4"></a> <a href="https://github.com/tillbey"><img src="public/GitHub_Invertocat_Black.svg" alt="GitHub logo" class="inline h-4"></a></div>
        <p class="mb-2">
          <p class="mb-2">Run by the <a href="https://www.surf.nl/themas/open-science/open-research-information" target="_blank" class="text-blue-500 hover:underline">SURF Open Science Innovation team</a>.
          For feedback you can <a href="https://github.com/surf-ori/dashboards/issues" target="_blank" class="text-blue-500 hover:underline">raise an issue here</a>.
          Source code under <a href="https://github.com/surf-ori/dashboards" target="_blank" class="text-blue-500 hover:underline">https://github.com/surf-ori/dashboards</a>.
          This website is licenced as CC-BY. Data powering the dashboard may have a different licence.</p>
          <img src="https://www.surf.nl/themes/surf/logo.svg" alt="SURF logo" class="w-20 h-auto mx-auto mb-3">
        </footer>
           ''')
    return


if __name__ == "__main__":
    app.run()
