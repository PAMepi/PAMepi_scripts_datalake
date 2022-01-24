import os
import pathlib
from datetime import datetime
import requests
from bs4 import BeautifulSoup
from tqdm import tqdm


# set folders
datalake = '/media/fabio/19940C2755DB566F/PAMepi/datalake'
raw = 'raw_data_covid19_version-' + datetime.now().strftime('%Y-%m-%d')
output_vacc = os.path.join(datalake, raw, 'data-datasus_vacinacao_brasil/')

pathlib.Path(output_vacc).mkdir(parents=True, exist_ok=True)

url = 'https://opendatasus.saude.gov.br/dataset/'
vacc = 'covid-19-vacinacao'

r = requests.get(os.path.join(url, vacc))

# parsing the page
soup = BeautifulSoup(r.text, 'html.parser')

# pickup all elements <a> in page
tag_a = soup.findAll('a', class_='heading')

vac_string = 'Registros de Vacinação'

n = len(vac_string)

link = ''
for tag in tag_a:
    # cleaning spaces in blank and \n on element
    string = tag.text.lstrip('\n').rstrip('\n').lstrip(' ').rstrip(' ')[0:n]
    if string == vac_string:
        link = '/'.join(tag['href'].split('/')[2:])


r = requests.get(os.path.join(url, link))

soup = BeautifulSoup(r.text, 'html.parser')

# pickup all elements <a> in page
tag_a = soup.findAll('a')

for tag in tag_a:
    if tag['href'].endswith('.csv') and tag.text == 'Dados Completos':
        file_csv = requests.get(tag['href'], stream=True)
        name = ''.join(tag.text.split('/')[-1:])

        # opening the file and defining config for progress bar
        with open(os.path.join(output_vacc, name), 'wb') as f, tqdm(
            desc=tag.text,
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
