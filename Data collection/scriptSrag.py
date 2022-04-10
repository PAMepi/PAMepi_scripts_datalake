#!/usr/bin/env python3

import os
import pathlib
from datetime import datetime
import requests
from bs4 import BeautifulSoup
from tqdm import tqdm

from config import datalake


# set folders
raw = 'raw_data_covid19_version-' + datetime.now().strftime('%Y-%m-%d')
output_srag = os.path.join(datalake, raw, 'data-sindrome_respiratoria_aguda_grave_incluindo_covid/')

pathlib.Path(output_srag).mkdir(parents=True, exist_ok=True)

url = 'https://opendatasus.saude.gov.br/dataset/'
endpoints = [
    'srag-2019',
    'srag-2020',
    'srag-2021-e-2022',
]

for endpoint in endpoints:
    r = requests.get(os.path.join(url, endpoint))

    soup = BeautifulSoup(r.text, 'html.parser')

    tag_a = soup.findAll('a', class_='heading')

    link = ''
    for tag in tag_a:
        string = tag.text.lstrip('\n').rstrip('\n').lstrip(' ').rstrip(' ')[0:4]
        if string == 'SRAG':
            link = '/'.join(tag['href'].split('/')[2:])


    r = requests.get(os.path.join(url, link))

    soup = BeautifulSoup(r.text, 'html.parser')

    tag_a = soup.findAll('a', class_='resource-url-analytics')

    for tag in tag_a:
        if tag['href'].endswith('.csv') and tag['href'] == tag.text:
            file_csv = requests.get(tag['href'], stream=True)
            name = ''.join(tag.text.split('/')[-1:])

            with open(os.path.join(output_srag, name), 'wb') as f, tqdm(
                desc=tag.text,
                total=int(file_csv.headers['Content-Length']),
                unit='iB',
                unit_scale=True,
                unit_divisor=1024
            ) as bar:
                for content in file_csv.iter_content(chunk_size=1024):
                    size = f.write(content)
                    bar.update(size)
