from pathlib import Path
from concurrent.futures import ThreadPoolExecutor
import multiprocessing
import re
from urllib import request
# from tqdm import tqdm


urls = {
    'SRAG': [
        'dataset/srag-2009-2012',
        'dataset/srag-2013-2018',
        'dataset/srag-2019',
        'dataset/srag-2020',
        'dataset/srag-2021-a-2023'
    ],

    'Wesley Cota': [],

    'Sindrome Gripal': [],

    'Vacinação covid-19': [],
}


opendatasus = 'https://opendatasus.saude.gov.br/'


def get_middle_link_title(text):
    link = text.split('title')[0] \
        .replace('"', '') \
        .replace(' ', '')

    title = text.split('title')[1] \
        .replace('=', '') \
        .replace('"', '') \
        .replace(' ', '')

    # print(link, title)
    return link, title


def return_link_title(srag):
    dataset = set()
    pattern = re.compile(srag + '/resource/\\w+.* title="SRAG.\\w+.*"')

    with request.urlopen(opendatasus + srag) as conn:
        for chunk in conn:
            if (match := re.search(pattern, chunk.decode())) is not None:
                link, title = get_middle_link_title(match.group())

                with request.urlopen(opendatasus + link) as middle:
                    for chunk in middle:
                        decoded = chunk.decode()
                        url_csv = re.compile('"https://\\w+.*\\.csv"')

                        if (match := url_csv.search(decoded)) is not None:
                            url = match.group().split(' ')[0].replace('"', '')
                            dataset.add(
                                (url, title.split('/')[0].split('-')[0])
                            )
    return dataset


def download(url, filename):
    with request.urlopen(url) as conn:
        with open(filename + '.csv', 'wb') as fcsv:
            # pbar = tqdm(desc=filename.split('/')[-1],
            #             total=int(conn.headers['content-length']),
            #             unit_scale=True,
            #             unit_divisor=1024,
            #             unit='B')
            for line in conn:
                fcsv.write(line)
                # pbar.update(len(line))
        # pbar.close()


def run(databases):
    cpus = multiprocessing.cpu_count()
    with ThreadPoolExecutor(max_workers=cpus) as executor:
        for db in databases:
            pamepi = Path().home().joinpath('PAMepi/') / db
            pamepi.mkdir(parents=True, exist_ok=True)
            for endpoint in urls[db]:
                for url, title in list(return_link_title(endpoint)):
                    executor.submit(download, url, str(pamepi.joinpath(title)))
