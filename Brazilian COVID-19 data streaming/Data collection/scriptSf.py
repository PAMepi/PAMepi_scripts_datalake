#!/usr/bin/env python3

import os
import pathlib
from datetime import datetime
import requests
from bs4 import BeautifulSoup
from tqdm import tqdm

from config import DATALAKE


raw = 'raw_data_covid19_version-' + datetime.now().strftime('%Y-%m-%d')
output_sg = os.path.join(DATALAKE, raw, 'data-notificacao_sindrome_gripal')

url = 'https://opendatasus.saude.gov.br/dataset/'

list_endpoints = [
    'notificacoes-de-sindrome-gripal-leve-2020',
    'notificacoes-de-sindrome-gripal-leve-2021',
    'notificacoes-de-sindrome-gripal-leve-2022',
]

ufs = [
    'AC', 'AP', 'AM', 'PA', 'RO', 'RR', 'TO',
    'AL', 'BA', 'CE', 'MA', 'PB', 'PE', 'PI',
    'RN', 'SE', 'DF', 'GO', 'MT', 'MS', 'ES',
    'MG', 'RJ', 'SP', 'PR', 'RS', 'SC'
]

for endpoint in list_endpoints:
    part_parth = endpoint.replace('notificacoes-de-', '')
    folder_dataset = os.path.join(output_sg, part_parth)
    pathlib.Path(folder_dataset).mkdir(parents=True, exist_ok=True)

    r = requests.get(os.path.join(url, endpoint))
    soup = BeautifulSoup(r.text, 'html.parser')
    tag_a = soup.findAll('a')
    list_uf_text = list(map(lambda x: 'Dados ' + x, ufs))
    data_url = {}

    for tag in tag_a:
        string = tag.text.lstrip('\n').rstrip('\n').lstrip(' ').rstrip(' ')[0:8]

        for string_uf in list_uf_text:
            if string_uf == string:

                href = tag['href']
                data_url[string] = href.lstrip('/dataset/')

    for csv_url in data_url.values():
        r = requests.get(os.path.join(url, csv_url))
        soup = BeautifulSoup(r.text, 'html.parser')
        tag_a = soup.findAll('a')

        for tag in tag_a:
            if tag['href'].endswith('.csv'):
                file_csv = requests.get(tag['href'], stream=True)

                with open(os.path.join(folder_dataset, tag.text), 'wb') as f, tqdm(
                    desc=tag.text,
                    total=int(file_csv.headers['Content-Length']),
                    unit='iB',
                    unit_scale=True,
                    unit_divisor=1024
                ) as bar:
                    for content in file_csv.iter_content(chunk_size=1024):
                        size = f.write(content)
                        bar.update(size)
