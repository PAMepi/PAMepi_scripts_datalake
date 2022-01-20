#!/usr/bin/env python3

import os
import requests
from bs4 import BeautifulSoup
from tqdm import tqdm


# set folders
datalake = '/media/fabio/19940C2755DB566F/PAMepi/datalake'
raw = 'raw_data_covid19_version-2022-01-20'
output_sg = os.path.join(datalake, raw, 'data-notificacao_sindrome_gripal')

# main url
url = 'https://opendatasus.saude.gov.br/dataset/'

# endpoint that matter
dataset = 'notificacoes-de-sindrome-gripal'

# getting the page
r = requests.get(os.path.join(url, dataset))

# parsing the page
soup = BeautifulSoup(r.text, 'html.parser')

# pickup all elements <a> in page
tag_a = soup.findAll('a')

# list uf
ufs = [
    'AC', 'AP', 'AM', 'PA', 'RO', 'RR', 'TO',
    'AL', 'BA', 'CE', 'MA', 'PB', 'PE', 'PI',
    'RN', 'SE', 'DF', 'GO', 'MT', 'MS', 'ES',
    'MG', 'RJ', 'SP', 'PR', 'RS', 'SC'
]

# concating string 'Dados' with uf in ufs
list_uf_text = list(map(lambda x: 'Dados ' + x, ufs))

# declaring dictionary
data_url = {}

# find by text containing 'Dados *'
for tag in tag_a:
    # cleaning spaces in blank and \n on element
    string = tag.text.lstrip('\n').rstrip('\n').lstrip(' ').rstrip(' ')[0:8]

    # comparing text on element web with list of ufs
    for string_uf in list_uf_text:
        if string_uf == string:
            # adding string and href to dictionary
            href = tag['href']
            data_url[string] = href.lstrip('/dataset/')

# iterating urls
for csv_url in data_url.values():
    # getting the new page
    r = requests.get(os.path.join(url, csv_url))

    # parsing the content in page
    soup = BeautifulSoup(r.text, 'html.parser')

    # pickup all elements <a>
    tag_a = soup.findAll('a')

    # iterating in list of <a>
    for tag in tag_a:
        # searching for text containing .csv in final
        if tag['href'].endswith('.csv'):
            # getting the file like in stream
            file_csv = requests.get(tag['href'], stream=True)

            # opening the file and defining config for progress bar
            with open(os.path.join(output_sg, tag.text), 'wb') as f, tqdm(
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
