#!/usr/bin/env python3

from os import path
import json
import csv
import requests
from requests.auth import HTTPBasicAuth


class ApiOpendatasus:

    database_api = {
        'vacinacao': {
            'url': 'https://imunizacao-es.saude.gov.br/',
            'user': 'imunizacao_public',
            'passwd': 'qlto5t&7r_@+#Tlstigi'
            },
        'leitos': {
            'url': 'https://elastic-leitos.saude.gov.br/',
            'user': 'user-api-leitos',
            'passwd': 'aQbLL3ZStaTr38tj'
        },
        'sindrome_gripal': {
            'url': 'https://elasticsearch-saps.saude.gov.br/',
            'user': 'user-public-notificacoes',
            'passwd': 'Za4qNXdyQNSa9YaA'
        }
    }

    header = {
        'content-type': 'application/json',
        'accept': 'application/json'
    }
    TEMPLATES = path.join(path.dirname(__file__), 'templates')


    def __init__(self, database, **kwargs):
        self.database = database
        self.kwargs = kwargs

    def get_data(self, filename, template, scroll='3m', **kwargs):

        url, user, passwd = self.database_api[self.database].values()

        with open(path.join(self.TEMPLATES, template)) as p:
            data = json.load(p)

        r = requests.post(path.join(url, '_search?scroll=' + scroll),
                          auth=HTTPBasicAuth(user, passwd),
                          json=data,
                          headers=self.header)

        scroll_id = r.json()['_scroll_id']

        fieldnames = sorted(list(r.json()['hits']['hits'][0]['_source']))

        with open(filename, 'w') as file:
            csvfile = csv.DictWriter(file, fieldnames=fieldnames)
            csvfile.writeheader()

            sort = {}
            for hit in r.json()['hits']['hits']:
                for value in hit.values():
                    if isinstance(value, dict):
                        for i in value:
                            sort[i] = value[i]
                        csvfile.writerow(sort)

            while r.json()['hits']['hits']:
                r = requests.post(
                    path.join(url, '_search/scroll'),
                    auth=HTTPBasicAuth(user, passwd),
                    json={'scroll': '3m', 'scroll_id': scroll_id},
                    headers=self.header,
                )

                scroll_id = r.json()['_scroll_id']
                sort = {}
                for hit in r.json()['hits']['hits']:
                    for value in hit.values():
                        if isinstance(value, dict):
                            for i in value:
                                sort[i] = value[i]
                            csvfile.writerow(sort)


if __name__ == '__main__':
    api = ApiOpendatasus('vacinacao')
    api.get_data('/media/fabio/19940C2755DB566F/PAMepi/datalake/raw_data_covid19_version-2021-12-05/data-datasus_vacinacao_brasil/vacinacao.csv', template='vacc_full.json')
