from concurrent.futures import ThreadPoolExecutor
from datetime import datetime
from pathlib import Path
from pprint import pprint as print
import re
from urllib.request import urlopen, urlretrieve
from urllib.error import HTTPError, URLError


year = datetime.now().strftime('%Y')

infos = {
    'WCOTA': [

    ],

    'SRAG': [
        'dataset/srag-2009-2012',
        'dataset/srag-2013-2018',
        'dataset/srag-2019',
        'dataset/srag-2020',
        'dataset/srag-2021-a-2023',
    ],

    'SINDROME_GRIPAL': [
        f'dataset/notificacoes-de-sindrome-gripal-leve-{dt}'
        for dt in range(2020, int(year) + 1)

    ],

    'VACINACAO': [
        'dataset/covid-19-vacinacao'
    ]
}

datalake = Path('datalake') / \
        f'raw_{datetime.now().strftime("%Y-%m-%d")}'

for folder in infos:
    for sub in infos[folder]:
        datalake.joinpath(folder, sub.split('/')[-1]) \
                .mkdir(parents=True, exist_ok=True)


opendatasus = 'https://opendatasus.saude.gov.br/'


ptt_href = re.compile('<a href="https://\\w+.*\\.csv[" ]( |title="https://\w.*\\.csv">|>(href="https://\\w+.*(\.csv|)|\w+.*</a>))')


def download(url, savepath):
    with urlopen(url) as req_download:
        with open(savepath, 'wb') as bff:
            for chunk in req_download:
                bff.write(chunk)


def get_url_name(url, pattern):
    with urlopen(url) as req:
        for content in req:
            if (match := pattern.search(content.decode())):
                resource = '/'.join(match.group() \
                    .replace('"', '') \
                    .replace(' ', '') \
                    .split('/')[3:]
                )

                with urlopen(url + '/' + resource) as new_req:
                    for content in new_req:
                        if (match := ptt_href.search(content.decode())):
                            yield match.group() \
                                    .replace('"', '')


rgx_link = re.compile('https://\w+.*\.csv', re.IGNORECASE)
rgx_nm = re.compile(
    '(uf-\w{2} - lote \d{1,3}|influd\w+.*\.csv|dados completos - parte \d{1,3})', re.IGNORECASE
)

for db_name in infos:
    for endpoint in infos[db_name]:
        url = f'{opendatasus}{endpoint}'
        pattern = re.compile(
            f'"/{endpoint}/resource/\\w+.*" '
        )

        try:
            resources = {content for content in get_url_name(url, pattern)}
        except URLError:
            pass
        except HTTPError:
            pass

        if len(resources):
            with ThreadPoolExecutor(max_workers=4) as pool:
                for url in resources:
                    if (name := rgx_nm.search(url)):
                        print(endpoint.split('/')[-1])
                        filename = name.group()
                        if not filename.endswith('.csv'):
                            filename += '.csv'
                        if (link := rgx_link.search(url)):
                            # pool.submit(urlretrieve,
                            #         link.group(),
                            #         datalake.joinpath(db_name, filename))
                            pool.submit(
                                download,
                                link.group(),
                                datalake.joinpath(
                                    db_name,
                                    endpoint.split('/')[-1],
                                    filename
                                )
                            )
