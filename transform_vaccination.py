#!/usr/bin/env python3

import os
from datetime import datetime
from pyspark import SparkConf
from pyspark.sql import SparkSession
import pyspark.sql.functions as F
import pyspark.sql.types as T


# define PATHS
DATALAKE = '/media/fabio/19940C2755DB566F/PAMepi/datalake/'
RAW = 'raw_data_covid19_version-2021-12-05'
PAMEPI = os.path.join(DATALAKE, RAW)
TEMP = os.path.join(PAMEPI, 'tmp')
OUTPUT = os.path.join(PAMEPI, 'vacc_transformation')

date = datetime.now().strftime('%Y-%m-%d')

# creating conf for spark
conf = SparkConf().setAll([
    ('spark.executor.cores', '8'),
    ('spark.driver.memory', '8g',),
    ('spark.local.dir', TEMP)])

# set spark session
spark = SparkSession.builder.config(conf=conf).getOrCreate()

# read dataset vaccination
df = spark.read.csv(os.path.join(PAMEPI, 'data-datasus_vacinacao_brasil'),
                    header=True, sep=';')

# dictionary with name for replace
columns = {
    'paciente_id': 'id',
    'paciente_idade': 'idade',
    'paciente_datanascimento': 'dt_nasc',
    'paciente_enumsexobiologico': 'sexo',
    'paciente_racacor_codigo': 'raca',
    'paciente_endereco_coibgemunicipio': 'mun_res',
    'paciente_endereco_nmmunicipio': 'nome_mun_res',
    'paciente_endereco_uf': 'uf_res',
    'vacina_dataaplicacao': 'date',
    'vacina_descricao_dose': 'dose',
}

# replace names in dictionary
for old, new in columns.items():
    df = df.withColumnRenamed(old, new)

# set to datetype
df = df.withColumn('date', F.col('date').cast(T.DateType()))

# filter date
df = df.filter((F.col('date') >= '2021-01-17') &
               (F.col('date') <= date))

# this is obvious
def fix_string(string):
    try:
        return string.lower().replace(u'\xa0', ' ') \
            .replace('á', 'a').replace('à', 'a').replace('ã', 'a') \
            .replace('â', 'a').replace('é', 'e').replace('è', 'e') \
            .replace('ẽ', 'e').replace('ê', 'e').replace('í', 'i') \
            .replace('ì', 'i').replace('ĩ', 'i').replace('î', 'i') \
            .replace('ó', 'o').replace('ò', 'o').replace('õ', 'o') \
            .replace('ô', 'o').replace('ú', 'u').replace('ù', 'u') \
            .replace('ũ', 'u').replace('û', 'u').replace('ç', 'c') \
            .lstrip(' ').rstrip(' ')
    except AttributeError:
        return None

# explicit is better that implicit
fix_string_udf = F.udf(fix_string, T.StringType())

### fixing some stuff ###
df = df.withColumn('dose', fix_string_udf('dose'))

df = df.withColumn('nome_mun_res', fix_string_udf('nome_mun_res'))

df = df.withColumn('uf_res', fix_string_udf('uf_res'))

df = df.withColumn('estabelecimento_uf', fix_string_udf('estabelecimento_uf'))

df = df.withColumn('estabelecimento_razaosocial',
                   fix_string_udf('estabelecimento_razaosocial'))

df = df.withColumn('estabelecimento_municipio_nome',
                   fix_string_udf('estabelecimento_municipio_nome'))

df = df.withColumn('vacina_grupoatendimento_codigo',
                   fix_string_udf('vacina_grupoatendimento_codigo'))

df = df.withColumn('vacina_grupoatendimento_nome',
                   fix_string_udf('vacina_grupoatendimento_nome'))

df = df.withColumn('vacina_categoria_nome',
                   fix_string_udf('vacina_categoria_nome'))

df = df.withColumn('vacina_fabricante_nome',
                   fix_string_udf('vacina_fabricante_nome'))

df = df.withColumn('vacina_nome',
                   fix_string_udf('vacina_nome'))
### end ###



# setting columns to grouping but removing the columns "date" default
columns = df.columns
columns.remove('date')

# aggregating
df.groupby(columns).pivot('dose').agg(F.first('id')).coalesce(1) \
    .write.csv(OUTPUT, header=True)
