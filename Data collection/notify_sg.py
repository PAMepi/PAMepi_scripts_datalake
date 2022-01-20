#!/usr/bin/env python3

import os
import requests
from bs4 import BeautifulSoup
from tqdm import tqdm

datalake = '/media/fabio/19940C2755DB566F/PAMepi/datalake'
raw = 'raw_data_covid19_version-2022-01-20'
output_sg = os.path.join(datalake, raw, 'data-notificacao_sindrome_gripal')

url = 'https://opendatasus.saude.gov.br/dataset/'
dataset = 'notificacoes-de-sindrome-gripal'

r = requests.get(os.path.join(url, dataset))

soup = BeautifulSoup(r.text, 'html.parser')

tag_a = soup.findAll('a')

ufs = [
    'AC', 'AP', 'AM', 'PA', 'RO', 'RR', 'TO',
    'AL', 'BA', 'CE', 'MA', 'PB', 'PE', 'PI',
    'RN', 'SE', 'DF', 'GO', 'MT', 'MS', 'ES',
    'MG', 'RJ', 'SP', 'PR', 'RS', 'SC'
]

list_uf_text = list(map(lambda x: 'Dados ' + x, ufs))

data_url = {}

for tag in tag_a:
    string = tag.text.lstrip('\n').rstrip('\n').lstrip(' ').rstrip(' ')[0:8]

    for string_uf in list_uf_text:
        if string_uf == string:
            href = tag['href']
            data_url[string] = href.lstrip('/dataset/')

for csv_url in data_url.values():
    r = requests.get(os.path.join(url, csv_url))#list(data_url.values())[0]))

    soup = BeautifulSoup(r.text, 'html.parser')

    tag_a = soup.findAll('a')

    for tag in tag_a:
        if tag['href'].endswith('.csv'):
            file_csv = requests.get(tag['href'], stream=True)

            with open(os.path.join(output_sg, tag.text), 'wb') as f, tqdm(
                desc=tag.text,
                total=int(file_csv.headers['Content-Length']),
                unit='iB',
                unit_scale=True,
                unit_divisor=1024
            ) as bar:
                for content in file_csv.iter_content(chunk_size=1024):
                    size = f.write(content)
                    bar.update(size)
