#!/usr/bin/env python3

import os
from multiprocessing.pool import ThreadPool
from requests import request
from bs4 import BeautifulSoup
from tqdm import tqdm


class ScrapeOpenDatasus(object):
    """Classe desenhada para coletar dados do OpenDatasus de forma dinâmica,
    visto que nem todas as tabelas tem seus registros atualizados com a mesma
    periodicidade.


    As tabelas disponíveis até o momento são:
        - Google Mobility : 'google_mobility'
        - Brasil.io: 'brasil_io'
        - Ocupação Hospitalar Covid-19: 'ocupacao_hospitalar'
        - SRAG 2021: 'srag'
        - Notificação Sindrome Gripal: 'sindrome_gripal'
        - Registros de Vacinação: 'vacinacao_covid'


    Guia rapido de como usar:

    # Criar a instancia e escolhendo uma tabela.

        scrape = ScrapeOpenDatasus('google_mobility')

        # ou

        scrape = ScrapeOpenDatasus(['brasil_io',
                                    'ocupacao_hospitalar',
                                    'google_mobility',
                                    'srag',
                                    'sindrome_gripal',
                                    'vacinacao_covid'
                                   ])


    # Escolhendo um diretório para armazenar os arquivos. Caso nada seja
    escolhido, o download será guardado na home do usuário.

        scrape.set_directory('/home/fabio/Documentos/')

    # Realizando o download. A quantidade de arquivos que podem ser baixados
    ao mesmo tempo é 1, porém este número pode ser alterado.

        scrape.get_data()

        # ou

        scrape.get_data(4)
    """

    # Link padrão do opendatasus
    opendatasus = 'https://opendatasus.saude.gov.br/dataset/'

    # Link estatico google mobility
    mobility = 'https://www.gstatic.com/covid19/mobility/Global_Mobility_Report.csv'

    # Link estatico brasil.io
    brasil_io = 'https://data.brasil.io/dataset/covid19/caso_full.csv.gz'

    # Padrões que serão concatenados com o link do opendatasus, caso o usuário
    # escolha uma tabela valida.
    databases = {
        'ocupacao_hospitalar': 'registro-de-ocupacao-hospitalar',
        'vacinacao_covid': 'covid-19-vacinacao',
        'srag': 'bd-srag-2021',
        'sindrome_gripal': 'casos-nacionais'
    }

    def __init__(self, bases: [str, list]):
        """Recebe um texto ou uma lista, contendo o nome dos bancos
        que o usuario desejar baixar"""
        self.bases = bases
        self.__path = os.path.expanduser('~/')
        self.__download_url = []

    def get_data(self, task: int=1):
        """Função que realiza o download dos dados. a variavel task recebe
        um inteiro que vai definir quantos arquivos podem ser baixados ao
        mesmo tempo
        """
        if 'google_mobility' in self.bases:
            self.bases.remove('google_mobility')
            self.__download_url.append(self.mobility)

        if 'brasil_io' in self.bases:
            self.bases.remove('brasil_io')
            self.__download_url.append(self.brasil_io)

        if isinstance(self.bases, str):
            home_url = self.opendatasus + self.databases[self.bases]

            self.__scrape(home_url, self.bases)

        elif isinstance(self.bases, list):
            home_url = [
                self.opendatasus
                + self.databases[base]
                for base in self.bases
            ]

            [self.__scrape(url, base)
             for url, base in zip(home_url, self.bases)]

        downloads = ThreadPool(4).imap_unordered(
            self.__download, self.__download_url
        )
        for download in downloads:
            print(download)

    def __scrape(self, url, base):
        def ocupacao_hospitalar(page):
            self.__download_url.append(
                page.find('a', class_='resource-url-analytics')['href']
            )

            return self.__download_url

        def srag(page):
            self.__download_url.append(
                page.find('a', class_='resource-url-analytics')['href']
            )
            return self.__download_url

        def vacinacao_covid(page):
            for element in page.findAll('a'):
                if element.text == 'Dados Completos':
                    self.__download_url.append(element['href'])

            return self.__download_url

        def sindrome_gripal(page):
            url = 'https://s3-sa-east-1.amazonaws.com/ckan.saude.gov.br/'
            for element in page.findAll('a'):
                if element.text[0:6] == 'dados-':
                    self.__download_url.append(url + element.text)

            return self.__download_url

        r = request('GET', url)
        page = BeautifulSoup(r.content, 'html.parser')

        url = ''
        for link in page.findAll('a', class_='heading'):
            if link.find('span').text == 'CSV':
                url = link['href']

        url = self.opendatasus[:-9] + url

        functions = {
            'ocupacao_hospitalar': ocupacao_hospitalar,
            'vacinacao_covid': vacinacao_covid,
            'srag': srag,
            'sindrome_gripal': sindrome_gripal,
        }

        r = request('GET', url)
        page = BeautifulSoup(r.content, 'html.parser')

        return functions[base](page)

    def __download(self, url):
        file_name_start_pos = url.rfind('/') + 1
        file_name = url[file_name_start_pos:]

        r = request('GET', url, stream=True)
        if r.status_code:
            with open(f'{self.__path + file_name}', 'wb+') as file, tqdm(
                desc=file_name,
                total=int(r.headers['Content-Length']),
                unit='iB',
                unit_scale=True,
                unit_divisor=1024,
            ) as bar:
                for data in r.iter_content(chunk_size=1024):
                    size = file.write(data)
                    bar.update(size)

    def set_directory(self, path: [str]):
        """Recebe um texto como caminho onde deseja salvar seus downloads"""
        self.__path = os.path.expanduser(path)


if __name__ == '__main__':
    bot = ScrapeOpenDatasus(
        'ocupacao_hospitalar'
        # 'srag',
        # 'vacinacao_covid',
        # 'sindrome_gripal'
        # 'google_mobility',
        # 'brasil_io'
    )
    bot.set_directory('/home/fabio/Documentos/dados_covid/')
    bot.get_data(4)
