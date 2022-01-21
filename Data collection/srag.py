import os
import requests
from bs4 import BeautifulSoup
from tqdm import tqdm


# set folders
datalake = '/media/fabio/19940C2755DB566F/PAMepi/datalake'
raw = 'raw_data_covid19_version-2022-01-20'
output_srag = os.path.join(datalake, raw, 'data-sindrome_respiratoria_aguda_grave_incluindo_covid/')

# main url
url = 'https://opendatasus.saude.gov.br/dataset/'

srag_2020 = 'srag-2020-banco-de-dados-de-sindrome-respiratoria-aguda-grave-incluindo-dados-da-covid-19'

srag_2021 = 'bd-srag-2021'

# getting the page
for srag in [srag_2020, srag_2021]:
    r = requests.get(os.path.join(url, srag))

    # parsing the page
    soup = BeautifulSoup(r.text, 'html.parser')

    # pickup all elements <a> in page
    tag_a = soup.findAll('a', class_='heading')

    link = ''
    for tag in tag_a:
        # cleaning spaces in blank and \n on element
        string = tag.text.lstrip('\n').rstrip('\n').lstrip(' ').rstrip(' ')[0:4]
        if string == 'SRAG':
            link = '/'.join(tag['href'].split('/')[2:])


    r = requests.get(os.path.join(url, link))

    soup = BeautifulSoup(r.text, 'html.parser')

    # pickup all elements <a> in page
    tag_a = soup.findAll('a', class_='resource-url-analytics')

    for tag in tag_a:
        if tag['href'].endswith('.csv') and tag['href'] == tag.text:
            file_csv = requests.get(tag['href'], stream=True)
            name = ''.join(tag.text.split('/')[-1:])

            # opening the file and defining config for progress bar
            with open(os.path.join(output_srag, name), 'wb') as f, tqdm(
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
