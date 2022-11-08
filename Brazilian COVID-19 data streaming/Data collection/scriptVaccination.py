import os
import pathlib
from datetime import datetime
import requests
from bs4 import BeautifulSoup
from tqdm import tqdm

from config import DATALAKE


# set folders
raw = 'raw_data_covid19_version-' + datetime.now().strftime('%Y-%m-%d')
output_vacc = os.path.join(DATALAKE, raw, 'data-vacinacao_brasil/')

pathlib.Path(output_vacc).mkdir(parents=True, exist_ok=True)

url = 'https://opendatasus.saude.gov.br'
vacc = 'dataset/covid-19-vacinacao'

r = requests.get(os.path.join(url, vacc))

soup = BeautifulSoup(r.text, 'html.parser')

tag_a = soup.findAll('a', class_='heading')

link = ''
for tag in tag_a:
    if 'Dados Completos' in tag.text:
        link = tag['href']

r = requests.get(url + link)

soup = BeautifulSoup(r.text, 'html.parser')

tag_a = soup.findAll('a')

for tag in tag_a:
    if tag['href'].endswith('.csv'):# and 'Dados Completos' in tag.text:
        file_csv = requests.get(tag['href'], stream=True)
        name = ''.join(tag.text.split('/')[-1:])

        with open(os.path.join(output_vacc, name), 'wb') as f, tqdm(
            desc=tag.text,
            total=int(file_csv.headers['Content-Length']),
            unit='iB',
            unit_scale=True,
            unit_divisor=1024
        ) as bar:
            for content in file_csv.iter_content(chunk_size=1024):
                size = f.write(content)
                bar.update(size)
