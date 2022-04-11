import os
from datetime import datetime
import pathlib
import requests
from bs4 import BeautifulSoup
from tqdm import tqdm

from config import DATALAKE

# set folders
'ocupacao_hospitalar'

raw = 'raw_data_covid19_version-' + datetime.now().strftime('%Y-%m-%d')
folder_out = os.path.join(DATALAKE, raw, 'ocupacao_hospitalar')

pathlib.Path(folder_out).mkdir(parents=True, exist_ok=True)

url = 'https://opendatasus.saude.gov.br/dataset/'
hospitalar = 'registro-de-ocupacao-hospitalar-covid-19'

r = requests.get(os.path.join(url, hospitalar))

# parsing the page
soup = BeautifulSoup(r.text, 'html.parser')

# pickup all elements <a> in page
tag_a = soup.findAll('a', class_='heading')

link = ''
for tag in tag_a:
    # cleaning spaces in blank and \n on element
    if 'Registro de Ocupação' in tag.text:
        link = '/'.join(tag['href'].split('/')[2:])

        r = requests.get(os.path.join(url, link))

        soup = BeautifulSoup(r.text, 'html.parser')

        # pickup all elements <a> in page
        tag_a = soup.findAll('a')

        for tag in tag_a:
            if 'LeitoOcupacao' in tag.text:
                file_csv = requests.get(tag['href'], stream=True)
                name = ''.join(tag.text.split('/')[-1:])
         
                 # opening the file and defining config for progress bar
                with open(os.path.join(folder_out, name), 'wb') as f, tqdm(
                    desc=name,
                    total=int(file_csv.headers['Content-Length']),
                    unit='iB',
                    unit_scale=True,
                    unit_divisor=1024
                ) as bar:
                    # iterating chunk and writing on file opened
                    for content in file_csv.iter_content(chunk_size=1024):
                        size = f.write(content)
                        # updating progress bar
                        bar.update(size)
