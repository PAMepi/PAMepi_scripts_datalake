import json
import csv
import requests
from requests.auth import HTTPBasicAuth
from PySide6.QtCore import Signal, QThread


class PyOpenDatasus(QThread):

    pbarVal = Signal(int)

    def __init__(self):#, api, state, date_min=None, date_max=None):
        super().__init__()
        # self.api = api
        # self.state = state
        # self.date_min = date_min
        # self.date_max = date_max

    def search(self, api, state, date_min=None, date_max=None):
        self.api = api
        self.state = state
        self.date_min = date_min
        self.date_max = date_max

    def __read_json(self):
        with open('api.json', 'r') as filejson:
            df = json.load(filejson)['apis'][self.api]
            return df

    def __check_if_date_isNone(self):
        if self.date_min is None or self.date_max is None:
            return True
        else:
            return False

    def __create_form(self):
        auth = None
        body = None

        fields = self.__read_json()['fields']

        if self.api == 'notificacao_sg':
            auth = HTTPBasicAuth(
                'user-public-notificacoes', 'Za4qNXdyQNSa9YaA'
            )
            if self.__check_if_date_isNone():
                    body = {
                        {
                            'query': {
                                'match_all': {
                                },
                            },
                            '_source': fields
                        }
                    }
            else:
                body = {
                    'size': 10000,
                    'query': {
                        'range': {
                            'dataNotificacao': {
                                'gte': self.date_min,
                                'lte': self.date_max
                            }
                        }
                    },
                    '_source': fields,
                }


        elif self.api == 'vacinacao':
            auth = HTTPBasicAuth(
                'imunizacao_public',
                'qlto5t&7r_@+#Tlstigi'
            )
            if self.__check_if_date_isNone():
                    body = {
                        'query': {
                            'match': {
                                'estabelecimento_uf': self.state
                            }
                        },
                        '_source': fields
                    }
            else:
                body = {
                    'size': 10000,
                    'query': {
                        'bool': {
                            'must': [{
                                'match': {
                                    'estabelecimento_uf': self.state
                                }
                            },
                                {
                                    'range': {
                                        'vacina_dataAplicacao': {
                                            'gte': self.date_min,
                                            'lte': self.date_max
                                        }
                                    }
                                }
                                     ]
                        }
                    },
                    '_source': fields
                }

        elif self.api == 'leitos_covid19':
            auth = HTTPBasicAuth(
                'user-api-leitos', 'aQbLL3ZStaTr38tj'
            )
            if self.__check_if_date_isNone():
                    body = {
                        'query': {
                            'match': {
                                'estadoSigla': self.state
                            }
                        },
                        '_source': fields
                    }
            else:
                body = {
                    'size': 10000,
                    'query': {
                        'bool': {
                            'must': [{
                                'match': {
                                    'estadoSigla': self.state
                                }
                            },
                                {
                                    'range': {
                                        'dataNotificacaoOcupacao': {
                                            'gte': self.date_min,
                                            'lte': self.date_max
                                        }
                                    }
                                }
                                     ]
                        }
                    },
                    '_source': fields
                }

        return auth, body

    def download(self, filename):

        url, idx = self.__read_json()['url'], self.__read_json()['idx']
        auth, body = self.__create_form()

        if self.api == 'notificacao_sg':
            r = requests.post(
                url +
                idx.replace('*', self.state) +
                '?scroll=3m',
                auth=auth,
                json=body
            )

        else:
            r = requests.post(
                url +
                idx +
                '?scroll=3m',
                auth=auth,
                json=body
            )

        sid = r.json()['_scroll_id']
        scroll_size = r.json()['hits']['total']['value']

        data = r.json()['hits']['hits']

        total = scroll_size
        count = len(data)
        # ratio = round((float(total / count) * 100 - 6), 1)
        ratio = round((count * 100 / total), 1)

        header = list(data[0]['_source'].keys())

        with open(filename, 'w') as f:
            print('sttart')
            filecsv = csv.DictWriter(f,
                                     fieldnames=header,
                                     extrasaction='ignore')
            filecsv.writeheader()

            while scroll_size > 0:

                count += len(data)
                ratio = round((float(count * 100 / total)), 1)
                percent = int(round(100 * ratio / (100 - 6), 1))
                self.pbarVal.emit(percent)

                for hits in data:
                    filecsv.writerow(hits['_source'])

                body = {
                    'scroll': '3m',
                    'scroll_id': sid,
                }

                r = requests.post(url + '_search/scroll',
                                  auth=auth, json=body)

                try:
                    sid = r.json()['_scroll_id']
                    scroll_size = len(r.json()['hits']['hits'])
                    data = r.json()['hits']['hits']
                except KeyError:
                    break

                self.pbarVal.emit(100)
        print('done')


if __name__ == '__main__':
    pyapi = PyOpenDatasus()
    pyapi.connect('notificacao_sg', 'ro', '2021-09-13', 'now')
    # test = PyOpenDatasus('notificacao_sg', 'ro', '2021-09-06', 'now')
    # test.download('notf.csv')
    pyapi.download('notf.csv')
