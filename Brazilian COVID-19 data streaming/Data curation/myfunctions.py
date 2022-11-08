import re
from datetime import datetime
from pyspark.sql.functions import udf
from unidecode import unidecode


@udf()
def corrige_data(col):
    try:
        if re.match(r'\d{2}/\d{2}/\d{4}', col):
            _ = datetime.strptime(col, '%d/%m/%Y').date()
            return _.strftime('%Y-%m-%d')
        elif re.match(r'\d{4}/\d{2}/\d{2}', col):
            _ = datetime.strptime(col, '%Y/%m/%d').date()
            return _.strftime('%Y-%m-%d')
        else:
            return col
    except TypeError:
        return col


@udf()
def remove_caracter_special(col):
    try:
        return unidecode(col).lower()
    except AttributeError:
        return col


@udf()
def renomeia_vacina(col):
    if col.find('astrazeneca'):
        return 1
    elif col.find('coronavac'):
        return 2
    elif col.find('pfizer'):
        return 3
    elif col.find('janssen'):
        return 4
    else:
        return col


@udf()
def strings_sg_para_numeros(col):
    try:
        return {
            # profissionalSaude / profissionalSeguranca
            'sim': 1,
            'nao': 2,

            # racaCor
            'branca': 1,
            'preta': 2,
            'amarela': 3,
            'parda': 4,
            'indigena': 5,

            # resultadoTeste
            'positivo': 1,
            'negativo': 2,
            'nao detectavel': 3,
            'inconclusivo ou indeterminado': 9,
            'inconclusivo': 9,
            'indeterminado': 9,

            # sexo
            'masculino': 1,
            'feminino': 2,

            # tipoTeste
            'rt-pcr': 1,
            'imunoensaio por eletroquimioluminescencia eclia': 2,
            'imunoensaio por eletroquimioluminescencia - eclia igG': 2,
            'quimioluminescencia - clia': 2,
            'teste rapido - anticorpo': 3,
            'teste rapido - antigeno': 4,
            'enzimaimunoensaio-elisa igm': 5,

            # classificacaoFinal
            'confirmado': 1,
            'confirmado clinico-epidemiologico': 2,
            'confirmado por criterio clinico': 3,
            'confirmado laboratorial': 4,
            'confirmacao laboratorial': 4,
            'confirmado clinico-imagem': 5,
            'sindrome gripal nao especificada': 6,
            'descartado': 7,

            # estadoTeste
            'solicitado': 1,
            'coletado': 2,
            'concluido': 3,
            'exame nao solicitado': 4,

            # evolucaoCaso
            'obito': 1,
            'cura': 2,
            'internado': 3,
            'internado em uti': 4,
            'cancelado': 5,
            'em tratamento domiciliar': 6,

            'ignorado': 9,
        }[col]
    except KeyError:
        return col
