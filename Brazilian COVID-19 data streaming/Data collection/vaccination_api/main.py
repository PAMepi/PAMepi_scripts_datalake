import os
from pathlib import Path
import asyncio
import csv
from elasticsearch import AsyncElasticsearch
from elasticsearch.helpers import async_scan
from tqdm.asyncio import tqdm


host = 'https://imunizacao-es.saude.gov.br:443'
index = 'desc-imunizacao'

auth = ('imunizacao_public', 'qlto5t&7r_@+#Tlstigi')


columns = [
    'paciente_id',
    'estalecimento_noFantasia',
    'vacina_lote',
    'estabelecimento_municipio_codigo',
    'estabelecimento_valor',
    'vacina_nome',
    'paciente_endereco_coPais',
    'paciente_nacionalidade_enumNacionalidade',
    'vacina_categoria_codigo',
    'vacina_fabricante_referencia',
    'paciente_idade',
    'vacina_descricao_dose',
    'paciente_endereco_coIbgeMunicipio',
    'vacina_grupoAtendimento_codigo',
    'paciente_racaCor_codigo',
    'estabelecimento_uf',
    'estabelecimento_razaoSocial',
    'vacina_numDose',
    'sistema_origem',
    'paciente_dataNascimento',
    'paciente_endereco_uf',
    'vacina_fabricante_nome',
    'paciente_endereco_cep',
    'id_sistema_origem',
    'paciente_endereco_nmPais',
    'vacina_categoria_nome',
    'paciente_endereco_nmMunicipio',
    'estabelecimento_municipio_nome',
    'vacina_codigo',
    'paciente_enumSexoBiologico',
    'vacina_grupoAtendimento_nome',
    'paciente_racaCor_valor',
    'vacina_dataAplicacao',
]


async def get_total_id(client, index, code):
    total = await client.count(
        index=index,
        query={
            'constant_score': {
                'filter': {
                    'bool': {
                        'must': [{
                            'regexp': {
                                'estabelecimento_municipio_codigo': code + '.*'
                            }},
                                 { 'term': {
                                     'status': 'final'
                                 }
                            }
                        ]
                    }
                }
            }
        },
    )

    return total['count']


async def collect_data(client, index, code, filename):
    total = await get_total_id(client, index, code)

    with open(filename, 'w') as f:
        csvfile = csv.DictWriter(f, fieldnames=columns)
        csvfile.writeheader()

        async for doc in tqdm(
            async_scan(
                client=client,
                index=index,
                source_includes=columns,
                size=10000,
                query={
                    'query': {
                        'constant_score': {
                            'filter': {
                                'bool': {
                                    'must': [{
                                        'regexp': {
                                            'estabelecimento_municipio_codigo': code + '.*'
                                        }
                                    }, {
                                        'term': {
                                            'status': 'final'
                                        }
                                    }]
                                }
                            }
                        }
                    }
                },
            ),
            desc=filename.split('/')[-1],
            total=total
        ):
            csvfile.writerow(doc['_source'])

        await client.close()


async def main():
    home = os.path.dirname(__file__)
    ref_code = os.path.join(home, 'codigos.txt')
    download = os.path.join(home, 'VACC')

    Path(download).mkdir(parents=True, exist_ok=True)

    client = AsyncElasticsearch(
        hosts=host, basic_auth=auth, request_timeout=10000,
        retry_on_timeout=True, max_retries=50
    )

    tasks = []

    with open(ref_code, 'r') as fc:
        for line in fc:
            code, uf = line.rstrip().split(',')
            code = str(code) + '.*'
            filename = os.path.join(download, uf + '.csv')

            tasks.append(collect_data(client, index, code, filename))


    await asyncio.gather(*tasks)


if __name__ == '__main__':
    asyncio.run(main())
