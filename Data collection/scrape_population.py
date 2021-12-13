#!/usr/bin/env python3

import requests
import pandas as pd


ufs = {
    'Acre': 12, 'Alagoas': '27', 'Amapá': '16', 'Amazonas': '13', 'Bahia': 29,
    'Ceará': 23, 'Distrito Federal': 53, 'Espírito Santo': 32, 'Goiás': 52,
    'Maranhão': 21, 'Mato Grosso': 51, 'Mato Grosso do Sul': 50,
    'Minas Gerais': 31, 'Pará': 15, 'Paraíba': 25, 'Paraná': 41,
    'Pernambuco': 26, 'Piauí': 22, 'Rio Grande do Norte': 24,
    'Rio Grande do Sul': 43, 'Rio de Janeiro': 33, 'Rondônia': 11,
    'Roraima': 14, 'Santa Catarina': 42, 'São Paulo': 35, 'Sergipe': 28,
    'Tocantins': 17

}

result = {'Estado': [], 'Cod_Uf': [], 'Populacao': []}

for uf, cod in ufs.items():
    api = f'https://servicodados.ibge.gov.br/api/v1/projecaopopulacao?cod={cod}'
    r = requests.get(api)
    result['Estado'].append(uf)
    result['Cod_Uf'].append(cod)
    result['Populacao'].append(r.json()['projecao']['populacao'])

dataframe = pd.DataFrame(result)
dataframe.to_csv('population.csv', index=False)
