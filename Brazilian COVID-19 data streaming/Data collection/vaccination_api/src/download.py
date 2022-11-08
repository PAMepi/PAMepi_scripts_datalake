#!/usr/bin/env python3

from concurrent.futures import ThreadPoolExecutor
import os
import re
import csv
from pathlib import Path
import pandas as pd
from unidecode import unidecode
from elasticsearch import Elasticsearch
from elasticsearch_dsl import Q, Search
from tqdm import tqdm


local = os.path.dirname(__file__)
data = os.path.join(local, 'codigos_municipios.csv')

df = pd.read_csv(data)

df['Estado'] = df['Estado'].apply(lambda x: unidecode(x).lower())

url = 'https://imunizacao-es.saude.gov.br/'
index = 'desc-imunizacao'

auth = ('imunizacao_public', 'qlto5t&7r_@+#Tlstigi')

columns = {
    'vacina_dataAplicacao': 'date',
    'paciente_id': 'id',
    'paciente_idade': 'age',
    'paciente_enumSexoBiologico': 'sex',
    'paciente_endereco_coIbgeMunicipio': 'mun_res',
    'vacina_nome': 'vacina',
    'vacina_descricao_dose': 'dose'
}


def create_connection(date_min, date_max):
    es = Elasticsearch(hosts=url, http_auth=auth)

    s = Search(using=es, index=index).params(request_timeout=60 * 5)

    s = s.source(list(columns.keys()))
    s = s.filter(
        'range',
        vacina_dataAplicacao={
            'gte': date_min,
            'lte': date_max
        }
    )

    return s


def primeira_dose(path, mun_res, cod, date_min, date_max):

    name = path + '/primeira_dose/'
    Path(name).mkdir(parents=True, exist_ok=True)
    session = create_connection(date_min, date_max)

    res = Q('regexp', paciente_endereco_coIbgeMunicipio=cod)
    query = (
        Q('match', vacina_descricao_dose='1ª Dose') |
        Q('match', vacina_descricao_dose='Dose Inicial')
    )

    session = session.query(res)
    session = session.query(query)

    return session, name + mun_res + '.csv'


def segunda_dose(path, mun_res, cod, date_min, date_max):

    name = path + '/segunda_dose/'
    Path(name).mkdir(parents=True, exist_ok=True)
    session = create_connection(date_min, date_max)

    res = Q('regexp', paciente_endereco_coIbgeMunicipio=cod)
    query = Q('match', vacina_descricao_dose='2ª Dose')

    session = session.query(res)
    session = session.query(query)

    return session, name + mun_res + '.csv'


def unica_dose(path, mun_res, cod, date_min, date_max):

    name = path + '/unica_dose/'
    Path(name).mkdir(parents=True, exist_ok=True)
    session = create_connection(date_min, date_max)

    res = Q('regexp', paciente_endereco_coIbgeMunicipio=cod)
    query = Q('match', vacina_descricao_dose='Única')

    session = session.query(res)
    session = session.query(query)

    return session, name + mun_res + '.csv'


def nao_registrada(path, mun_res, cod, date_min, date_max):
    name = path + '/nao_registrada/'
    Path(name).mkdir(parents=True, exist_ok=True)
    session = create_connection(date_min, date_max)

    res = Q('regexp', paciente_endereco_coIbgeMunicipio=cod)
    query = Q('match', vacina_descricao_dose='Dose')

    session = session.query(res)
    session = session.query(query)

    return session, name + mun_res + '.csv'


def reforco_dose(path, mun_res, cod, date_min, date_max):
    name = path + '/reforco_dose/'
    Path(name).mkdir(parents=True, exist_ok=True)
    session = create_connection(date_min, date_max)

    res = Q('regexp', paciente_endereco_coIbgeMunicipio=cod)
    query = (
        Q('match', vacina_descricao_dose='Dose Adicional') |
        Q('match', vacina_descricao_dose='Reforço') |

        Q('match', vacina_descricao_dose='1º Reforço') |
        Q('match', vacina_descricao_dose='2º Reforço') |
        Q('match', vacina_descricao_dose='3º Reforço') |
        Q('match', vacina_descricao_dose='4º Reforço') |

        Q('match', vacina_descricao_dose='3ª Dose') |
        Q('match', vacina_descricao_dose='4ª Dose') |
        Q('match', vacina_descricao_dose='5ª Dose') |

        Q('match', vacina_descricao_dose='1ª Dose Revacinação') |
        Q('match', vacina_descricao_dose='2ª Dose Revacinação') |
        Q('match', vacina_descricao_dose='3ª Dose Revacinação') |
        Q('match', vacina_descricao_dose='4ª Dose Revacinação')
    )

    session = session.query(res)
    session = session.query(query)

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
            filecsv.writerow(data)


def main(path, date_min, date_max):
    funcoes = [
        nao_registrada,
        primeira_dose,
        segunda_dose,
        unica_dose,
        reforco_dose
    ]

    with ThreadPoolExecutor(max_workers=4) as executor:
        for mun_res, cod in zip(df['Estado'], df['Codigo']):
            for funcao in funcoes:
                executor.submit(
                    write,
                    *funcao(path, mun_res, cod, date_min, date_max)
                )
