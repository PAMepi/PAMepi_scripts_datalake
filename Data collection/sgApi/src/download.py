#!/usr/bin/env python3

from concurrent.futures import ThreadPoolExecutor
import os
import csv
import re
from pathlib import Path
from elasticsearch import Elasticsearch
from elasticsearch_dsl import Q, Search
from tqdm import tqdm
import pandas as pd
from unidecode import unidecode


local = os.path.dirname(__file__)
data = os.path.join(local, 'codigos_municipios.csv')

df = pd.read_csv(data)

df['Estado'] = df['Estado'].apply(lambda x: unidecode(x).lower())


host = 'https://elasticsearch-saps.saude.gov.br/'
idx= 'desc-esus-notifica-estado-*'

user = 'user-public-notificacoes'
passwd = 'Za4qNXdyQNSa9YaA'

columns = {
    'dataInicioSintomas': 'date',
    'idade': 'age',
    'sexo': 'sex',
    'municipioIBGE': 'mun_res',
    'resultadoTeste': 'result'
}


def create_connection(dt_min, dt_max):
    es = Elasticsearch(hosts=host, http_auth=(user, passwd), timeout=120)

    s = Search(using=es).params(request_timeout=60 * 5)
    s = s.source(list(columns.keys()))
    s = s.filter(
        'range',
        dataInicioSintomas={
            'gte': dt_min,
            'lte': dt_max,
        }
    )

    return s

def positivo(path, mun_res, cod, dt_min, dt_max):
    name = path + '/positivo/'
    Path(name).mkdir(parents=True, exist_ok=True)
    session = create_connection(dt_min, dt_max)

    session = session.query(
        Q('regexp', municipioIBGE=cod)
    ).query(
        Q('match', resultadoTeste='Positivo')
    )

    return session, name + mun_res + '.csv'


def negativo(path, mun_res, cod, dt_min, dt_max):
    name = path + '/negativo/'
    Path(name).mkdir(parents=True, exist_ok=True)
    session = create_connection(dt_min, dt_max)

    session = session.query(
        Q('regexp', municipioIBGE=cod)
    ).query(
        Q('match', resultadoTeste='Negativo')
    )

    return session, name + mun_res + '.csv'


def inconclusivo(path, mun_res, cod, dt_min, dt_max):
    name = path + '/inconclusivo/'
    Path(name).mkdir(parents=True, exist_ok=True)
    session = create_connection(dt_min, dt_max)

    session = session.query(
        Q('regexp', municipioIBGE=cod)
    ).query(
        Q('match', resultadoTeste='Inconclusivo ou Indeterminado')
    )

    return session, name + mun_res + '.csv'


def nao_detectavel(path, mun_res, cod, dt_min, dt_max):
    name = path + '/nao_detectavel/'
    Path(name).mkdir(parents=True, exist_ok=True)
    session = create_connection(dt_min, dt_max)

    session = session.query(
        Q('regexp', municipioIBGE=cod)
    ).query(
        Q('match', resultadoTeste='Inconclusivo ou Indeterminado')
    )

    return session, name + mun_res + '.csv'


def write(session, name):

    with open(name, 'w') as f:
        filecsv = csv.DictWriter(f, fieldnames=list(columns.values()))
        filecsv.writeheader()

        for hit in tqdm(session.scan(),
                        total=session.count(),
                        desc=name.split('/')[-1]):
            data = hit.to_dict()
            for key, n_key in columns.items():
                data[n_key] = data.pop(key)
            data['date'] = re.findall(r'\d{4}-\d{2}-\d{2}', data['date'])[0]
            data['mun_res'] = re.findall(r'\d{6}', data['mun_res'])[0]
            filecsv.writerow(data)


def main(path, dt_min, dt_max):
    funcoes = [
        positivo,
        negativo,
        inconclusivo,
        nao_detectavel,
    ]

    with ThreadPoolExecutor(max_workers=4) as executor:
        for mun_res, cod in zip(df['Estado'], df['Codigo']):
            for funcao in funcoes:
                executor.submit(
                    write,
                    *funcao(path, mun_res, cod, dt_min, dt_max)
                )
