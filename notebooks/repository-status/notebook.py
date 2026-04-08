# /// script
# requires-python = ">=3.10"
# dependencies = [
#     "beautifulsoup4==4.14.3",
#     "fastexcel==0.19.0",
#     "marimo",
#     "polars==1.39.3",
#     "requests==2.33.0",
#     "xmltodict==1.0.4",
# ]
# ///

import marimo

__generated_with = "0.22.5"
app = marimo.App(width="medium")


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    # Dutch repositories – OAI-PMH endpoint status
    """)
    return


@app.cell
def _():
    import marimo as mo
    import requests
    import polars as pl
    from bs4 import BeautifulSoup
    import xmltodict
    import json
    import os
    import time

    return mo, pl, requests, time, xmltodict


@app.cell
def _(mo, time):
    mo.md(f"""
    Updated at {time.strftime('%Y-%m-%d %H:%M:%S', time.localtime())}.
    """)
    return


@app.cell
def _(pl):
    repositories = (
        pl
        .read_excel('https://zenodo.org/records/19470205/files/nl_orgs_openaire_datasources_with_endpoint_public.xlsx')
        .with_columns(
            url=pl.col('oai_endpoint').str.json_decode(dtype=pl.List(pl.String)).list.get(0, null_on_oob=True)
        )
        .filter(pl.col('url').is_not_null())
        .group_by('url')
        .agg(
            name=pl.col('Name').first(),
            type=pl.col('Type').str.join(delimiter='/')
        )
    )
    return (repositories,)


@app.cell
def _(requests, time, xmltodict):
    class Repository:

        def __init__(self, url, name):
            self.url = url
            self.name = name

        def identify(self, format='raw-response'):
            return self._get(format=format, params={'verb': 'Identify'})

        def listMetadataFormats(self, format='raw-response'):
            return self._get(format=format, params={'verb': 'ListMetadataFormats'})

        def listSets(self, format='raw-response'):
            return self._get(format=format, params={'verb': 'ListSets'})

        def listIdentifiers(self, metadataPrefix, from_=None, until=None, set=None, resumptionToken=None, format='raw-response'):
            return self._get(format=format, params=
                                {'verb': 'ListIdentifiers', 'metadataPrefix': metadataPrefix,
                                 'from': from_, 'until': until, 'set': set, 'resumptionToken': resumptionToken}
                            )

        def listRecords(self, metadataPrefix, from_=None, until=None, set=None, resumptionToken=None, format='raw-response'):
            return self._get(format=format, params=
                                {'verb': 'ListRecords', 'metadataPrefix': metadataPrefix,
                                 'from': from_, 'until': until, 'set': set, 'resumptionToken': resumptionToken}
                            )

        def getRecord(self, identifier, metadataPrefix, format='raw-response'):
            return self._get(format=format, params=
                                {'verb': 'GetRecord', 'identifier': identifier, 'metadataPrefix': metadataPrefix}
                            )

        def base_information(self):
            identify = self.identify(format='json')
            if identify['status'] == 'error':
                return {
                        'url': self.url,
                        'name': self.name,
                        'status': 'error',
                        'error_type': identify['type'],
                        'error_message': identify['message'],
                        'response_time': None,
                        'baseURL': None,
                        'identified_name': None,
                        'metadataFormats': None,
                        'sets': None
                }
            else:

                info = identify['response']['OAI-PMH']['Identify']
                listMetadataFormats = self.listMetadataFormats(format='json')
                metadataFormats = listMetadataFormats['response']['OAI-PMH']['ListMetadataFormats']['metadataFormat']
                metadataFormats = metadataFormats if type(metadataFormats) is list else [metadataFormats]
                listSets = self.listSets(format='json')
                sets = listSets['response']['OAI-PMH'].get('ListSets', {'set': []})['set']
                sets = sets if type(sets) is list else [sets]
                # responses = {
                #                 f'''Identify response ({identify['response_time']:.3f} s)''':
                #                     mo.json(identify['response']),
                #                 f'''ListMetadataFormats response ({listMetadataFormats['response_time']:.3f} s)''':
                #                     mo.json(listMetadataFormats['response']),
                #                 f'''ListSets response ({listSets['response_time']:.3f} s)''':
                #                     mo.json(listSets['response'])
                #             }
                total_response_time = identify['response_time'] + listMetadataFormats['response_time'] + listSets['response_time']

                return {
                        'url': self.url,
                        'name': self.name,
                        'status': 'alive',
                        'error_type': None,
                        'error_message': None,
                        'response_time': total_response_time,
                        'baseURL': info['baseURL'],
                        'identified_name': info['repositoryName'],
                        'metadataFormats': [format['metadataPrefix'] for format in metadataFormats],
                        'sets': [s['setSpec'] for s in sets if 'year' not in s['setSpec']]
                }

        def full_harvest(self, base_path):
                    # for verb in verbs:
            #     res = verbs[verb]()
                # dataset_path = os.path.join(base_path, self.url, verb)
                # os.makedirs(dataset_path, exist_ok=True)
                # with open(os.path.join(dataset_path, 'response.xml'), 'wb') as f:
                #     res = verbs[verb]()
                #     f.write(res.content)
            yield 1

        def _get(self, params, format):
            if format not in ('raw-response', 'xml', 'json'):
                raise Exception('format must be one of raw-response, xml, json')
            if format == 'raw-response':
                return requests.get(self.url, params=params, timeout=20).text
            try:
                t0 = time.time()
                res = requests.get(self.url, params=params, timeout=20)
                response_time = time.time() - t0
            except Exception as e:
                return {
                    'url': self.url,
                    'status': 'error',
                    'type': type(e).__name__,
                    'message': str(e),
                }
            else:
                try:
                    res.raise_for_status()
                except requests.HTTPError as e:
                    return {
                        'url': res.url,
                        'status': 'error',
                        'type': type(e).__name__,
                        'message': str(e),
                        'response_time': response_time,
                        'response': res.text
                    }
                else:
                    return {
                        'url': res.url,
                        'status': 'success',
                        'response_time': response_time,
                        'response': xmltodict.parse(res.text) if format == 'json' else res.text
                    }

        def __repr__(self):
            return f"OAIEndpoint('{self.url}')"

    return (Repository,)


@app.cell
def _(Repository, mo):
    def health_report(name, url):
        repository = Repository(url, name)
        identify = repository.identify(format='json')
        if identify['status'] == 'error':
            return mo.md(f'''
             <details style="margin: 0pt">
                <summary>
                    <b>{name}</b> ({url})
                    <div style="float: right">{mo.icon('solar:close-circle-outline', color='crimson', size=30)}</div>
                </summary>
                <div style="margin: 20pt">
                    <h4>Error</h4>
                    <code>{identify['message']}</code>
                    <h4>Response</h4>
                    <code>{mo.Html(identify.get('response', '').replace('<', '&lt;').replace('>', '&gt;'))}</code>
                </div>
             </details>
            ''')
        else:
            info = identify['response']['OAI-PMH']['Identify']
            listMetadataFormats = repository.listMetadataFormats(format='json')
            metadataFormats = listMetadataFormats['response']['OAI-PMH']['ListMetadataFormats']['metadataFormat']
            metadataFormats = metadataFormats if type(metadataFormats) is list else [metadataFormats]
            listSets = repository.listSets(format='json')
            sets = listSets['response']['OAI-PMH'].get('ListSets', {'set': []})['set']
            sets = sets if type(sets) is list else [sets]
            responses = {
                            f'''Identify response ({identify['response_time']:.3f} s)''':
                                mo.json(identify['response']),
                            f'''ListMetadataFormats response ({listMetadataFormats['response_time']:.3f} s)''':
                                mo.json(listMetadataFormats['response']),
                            f'''ListSets response ({listSets['response_time']:.3f} s)''':
                                mo.json(listSets['response'])
                        }
            total_response_time = identify['response_time'] + listMetadataFormats['response_time'] + listSets['response_time']
            return mo.md(f'''
             <details style="margin: 0pt">
                <summary>
                    <b>{name}</b> ({info['baseURL']})
                    <div style="float: right">{mo.icon('solar:check-circle-bold', color='forestgreen', size=30)}</div>
                    <div style="float: right; margin-right: 20pt">{total_response_time:.3f} s</div>
                </summary>
                <div style="margin: 20pt">
                    <table>
                        <tr>
                            <th style="text-align: start">Name</th>
                            <td style="text-align: start; width: 100%">{info['repositoryName']}</td>
                        </tr>
                        <tr>
                            <th style="text-align: start">URL</th>
                            <td style="text-align: start">{info['baseURL']}</td>
                        </tr>
                        <tr>
                            <th style="text-align: start">Metadata formats</th>
                            <td style="text-align: start">
                                {', '.join(sorted(format['metadataPrefix'] for format in metadataFormats))}
                            </td>
                        </tr>
                        <tr>
                            <th style="text-align: start">Sets</th>
                            <td style="text-align: start">
                                {', '.join(sorted(s['setSpec'] for s in sets if 'year' not in s['setSpec']))}
                            </td>
                        </tr>
                    </table>
                </div>
             </details>
            ''')
                    # {mo.accordion(responses)}

    # health_report(repository_choice.value['name'], repository_choice.value['url'])
    return (health_report,)


@app.cell
def _():
    # repository_choice = mo.ui.dropdown(
    #     {repository['name']: repository for repository in repositories.to_dicts()}
    # )
    # repository_choice
    return


@app.cell
def _():
    # health_report(repository_choice.value['name'], repository_choice.value['url'])
    return


@app.cell
def _():
    # r.base_information()
    return


@app.cell
def _():
    # r = Repository(repository_choice.value['url'], repository_choice.value['name'])
    # r.listRecords(format='json', metadataPrefix='oai_openaire')
    return


@app.cell
def _():
    # repository_information = pl.DataFrame([
    #         Repository(repository['url'], repository['name']).base_information()
    #          for repository in repositories.to_dicts()
    # ])
    return


@app.cell
def _():
    # repository_information.filter((
    #     pl.col('metadataFormats').list.contains('resgtr')
    #       # pl.col('metadataFormats').list.contains('oai_cerif_openaire')
    #     # | pl.col('metadataFormats').list.contains('oai_openaire')
    #     # | pl.col('metadataFormats').list.contains('cerif_openaire')
    #     # | pl.col('metadataFormats').list.contains('oai_datacite')
    #     # | pl.col('metadataFormats').list.contains('oai_datacite')
    #     # | pl.col('metadataFormats').list.contains('nl_didl')
    #     | pl.col('metadataFormats').list.contains('didl')
    #     # | pl.col('metadataFormats').list.contains('didl_mods')
    #     # | pl.col('metadataFormats').list.contains('oai_dc')
    # ))
    return


@app.cell
def _():
    # def choose_arguments(metadataFormats, sets):
    #     if [s for s in sets if s.startswith('openaire')]:

    #     elif len(sets) == 0
    return


@app.cell
def _(health_report, loading_icon, mo, repositories):
    for repository in repositories.to_dicts():
        mo.output.append(
         mo.md(f'''
                 <details style="margin: 0pt">
                    <summary>
                        <b>{repository['name']}</b> ({repository['url']})
                        <div style="float: right">{loading_icon}</div>
                    </summary>
                    <div style="margin: 20pt">
                    </div>
                 </details>
                ''')                   

        )

    for n, repository in enumerate(repositories.to_dicts()):    
        mo.output.replace_at_index(health_report(repository['name'], repository['url']), n)


    # for n, repository in enumerate(repositories.to_dicts()):    
    #     mo.Thread(target=lambda: mo.output.append(health_report(repository['name'], repository['url']))).start()
    return


@app.cell
def _(mo):
    loading_icon = mo.Html('''
    <style>
    .lds-ripple,
    .lds-ripple div {
      box-sizing: border-box;
    }
    .lds-ripple {
      display: inline-block;
      position: relative;
      width: 30px;
      height: 30px;
    }
    .lds-ripple div {
      position: absolute;
      border: 2px solid currentColor;
      opacity: 1;
      border-radius: 50%;
      animation: lds-ripple 1s cubic-bezier(0, 0.2, 0.8, 1) infinite;
    }
    .lds-ripple div:nth-child(2) {
      animation-delay: -0.5s;
    }
    @keyframes lds-ripple {
      0% {
        top: 13px;
        left: 13px;
        width: 4px;
        height: 4px;
        opacity: 0;
      }
      4.9% {
        top: 13px;
        left: 13px;
        width: 4px;
        height: 4px;
        opacity: 0;
      }
      5% {
        top: 13px;
        left: 13px;
        width: 4px;
        height: 4px;
        opacity: 1;
      }
      100% {
        top: 0;
        left: 0;
        width: 30px;
        height: 30px;
        opacity: 0;
      }
    }
    </style>
    <div class="lds-ripple"><div></div><div></div></div>
    ''')
    return (loading_icon,)


@app.cell
def _():
    # import xml.etree.ElementTree as ET
    return


@app.cell
def _():
    # ET.fromstring(response3).findall('.//{http://www.openarchives.org/OAI/2.0/}resumptionToken')[0]
    return


@app.cell
def _():
    # BeautifulSoup(response3, 'xml').find_all('resumptionToken')
    return


@app.cell
def _():
    # import time
    # def task(title):
    #     # mo.output.append(mo.md(title))
    #     with mo.status.progress_bar(total=4, title=title, completion_title='done') as bar:
    #         for i in range(4):
    #             time.sleep(0.5)
    #             bar.update()

    # for t in range(10):
    #     mo.Thread(target=lambda: task(str(t))).start()

    # # for x in range(10):
    # #     mo.output.append(mo.md(str(x)))

    # # mo.output.replace_at_index(mo.md('test'), 3)
    return


if __name__ == "__main__":
    app.run()
