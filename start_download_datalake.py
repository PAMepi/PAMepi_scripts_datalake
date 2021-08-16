#!/usr/bin/env python3


try:
    import os
    import json
    import gzip
    import csv
    from multiprocessing.pool import ThreadPool
    import requests
    from bs4 import BeautifulSoup
    from tqdm import tqdm
except Exception as e:
    print(f'Biblioteca {e} não foi carregada')


SCRIPT_FOLDER = os.path.dirname(__file__)
DATALAKE_FOLDERS = [
    'data-brasil_io',
    'data-cidacs_ibp_setores_censitarios',
    'data-google_mobility',
    'data-jusbrasil_decretos_brasil',
    'data-notificacao_sindrome_gripal',
    'data-ocupacao_hospitalares',
    'data-sindrome_respiratoria_aguda_grave_incluindo_covid',
    'data-datasus_vacinacao_brasil',
    'metadata',
]

data_links = [
    'data-cidacs_ibp_setores_censitarios/,https://cidacs.bahia.fiocruz.br/ibp/wp-content/uploads/2021/04/Deprivation_Brazil_2010_CensusSectors.xlsx',
    'data-datasus_vacinacao_brasil/,https://s3-sa-east-1.amazonaws.com/ckan.saude.gov.br/PNI/vacina/completo/2021-07-29/part-00000-940212ba-96ea-4a70-ae52-d06a34e9000e-c000.csv',
    'data-jusbrasil_decretos_brasil/,https://files.slack.com/files-pri/T027JUQ225A-F028Q6K66MT/download/2021_07_12_decretos.csv',
    'data-brasil_io/,https://data.brasil.io/dataset/covid19/caso_full.csv.gz',
    'data-google_mobility/,https://www.gstatic.com/covid19/mobility/Global_Mobility_Report.csv',
    'data-ocupacao_hospitalares/,https://s3-sa-east-1.amazonaws.com/ckan.saude.gov.br/Leitos/2021-07-28/esus-vepi.LeitoOcupacao.csv',
    'data-sindrome_respiratoria_aguda_grave_incluindo_covid/,https://s3-sa-east-1.amazonaws.com/ckan.saude.gov.br/SRAG/2021/INFLUD21-26-07-2021.csv',
]

url = 'https://opendatasus.saude.gov.br/dataset/casos-nacionais/resource/30c7902e-fe02-4986-b69d-906ca4c2ec36'
part_url_csv = 'https://s3-sa-east-1.amazonaws.com/ckan.saude.gov.br/'



# this function are in development...
"""
def write_meta():
    def write_data(filename):
        unique_headers = set()
        try:
            with open(filename, 'r', encoding='utf-8') as fin:
                csvin = csv.reader(fin)
                unique_headers.update(next(csvin, []))
                f.write(json.dumps(unique_headers) + '\n')
        except TypeError:
            print(filename)

    with open(os.path.join(SCRIPT_FOLDER, '../metadata/meta.json'),
              'w+', encoding='utf-8') as f:

        group_folders = [os.path.join(SCRIPT_FOLDER, f'../{folder}/')
               for folder in DATALAKE_FOLDERS]

        for folder in group_folders:
            for itm in os.listdir(folder):
                write_data(os.path.join(SCRIPT_FOLDER, folder + itm))
"""


def download_url(url):
    path, url = url.split(',')
    file_name_start_pos = url.rfind('/') + 1
    file_name = url[file_name_start_pos:]

    r = requests.get(url, stream=True)
    if r.status_code:
        with open(os.path.join(SCRIPT_FOLDER, f'../{path + file_name}'),
                  'wb+') as file, tqdm(
            desc=file_name,
            total=int(r.headers['Content-Length']),
            unit='iB',
            unit_scale=True,
            unit_divisor=1024,
        ) as bar:
            for data in r.iter_content(chunk_size=1024):
                size = file.write(data)
                bar.update(size)


def create_datalake_folders():
    for folder in DATALAKE_FOLDERS:
        data_lake_folder = os.path.join(SCRIPT_FOLDER, f'../{folder}/')

        try:
            os.makedirs(data_lake_folder)
            print('Directory ', data_lake_folder, ' Created')
        except FileExistsError:
            print('Directory ', data_lake_folder, ' already exists')


if __name__ == '__main__':
    r = requests.get(url)
    soup = BeautifulSoup(r.content, 'html.parser')
    li = soup.findAll('li', class_='nav-item')
    a = [a.find('a') for a in li]

    for href in a:
        try:
            if href.find('span').text[0:5] == 'dados':
                data_links.append('data-notificacao_sindrome_gripal/,'
                                  + part_url_csv
                                  + href.find('span').text)
        except AttributeError:
            pass

    create_datalake_folders()
    downloads = ThreadPool(4).imap_unordered(download_url, data_links)

    for download in downloads:
        print(r)
