#!/usr/bin/env python3

import os
from datetime import datetime
from pyspark import SparkConf
from pyspark.sql import SparkSession
import pyspark.sql.functions as F
import pyspark.sql.types as T
import pandas as pd


# date = '2021-11-25'
date = datetime.today().now().strftime('%Y-%m-%d')
datalake = '/media/fabio/19940C2755DB566F/PAMepi/datalake/'
raw = f'raw_data_covid19_version-{date}/'

preprocess = os.path.join(datalake, raw, 'preprocess/')
groupby = os.path.join(datalake, raw, 'group_')

conf = SparkConf().setAll(
    [
        ('spark.driver.memory', '12g'),
        ('spark.driver.cores', '6'),
    ]
)
spark = SparkSession.builder.config(conf=conf).getOrCreate()

def recorta_codigo(codigo):
    try:
        return codigo[:6]
    except AttributeError:
        return None

recorta_codigo_udf = F.udf(recorta_codigo, T.StringType())

col_date = spark.createDataFrame(
    [(x.strftime('%Y-%m-%d'),)
     for x in pd.date_range('2020-02-01', freq='D', end=date)]
).toDF('date')

# referencia tabela ibge
ref = spark.read.csv('ref', header=True)

ref = ref.withColumnRenamed('uf', 'uf_res') \
    .withColumnRenamed('mun_name', 'nome_mun_res')

ref = ref.withColumn('mun_res', recorta_codigo_udf('mun_cod_full'))
useful_ref = ref.select('uf_res', 'nome_mun_res', 'mun_res')
useful_ref = col_date.join(useful_ref)

# sindrome gripal

cols_sg = [
    'newCases_SG',
    'new_des_SG',
    'new_undefined_SG',
    'newDeath_SG',
    'new_recovered_SG',
    'UtiEvolution_SG'
]


df = spark.read.csv(os.path.join(groupby, 'sg_mun'), header=True)
dataset = useful_ref.join(df, ['date', 'mun_res'], how='left')

dataset.groupby('date', 'uf_res', 'sexo', 'age_group').agg(
    *[F.count(c).astype('int').alias(c) for c in cols_sg]
).coalesce(1).write.mode('overwrite') \
    .csv(os.path.join(groupby, 'sg_uf'),  header=True)

# dataset.groupby('date', 'uf_res', 'mun_res',
#                 'nome_mun_res', 'sexo', 'age_group').agg(
#     *[F.count(c).astype('int').alias(c) for c in cols_sg]
# ).coalesce(1).write.mode('overwrite') \
#     .csv(os.path.join(groupby, 'sg_total'),  header=True)


# hospitalização
df = spark.read.csv(os.path.join(groupby, 'hosp_total'), header=True)
cols_hosp = [
    'ocupacaoSuspeitoCli_HOS',
    'ocupacaoSuspeitoUti_HOS',
    'ocupacaoConfirmadoCli_HOS',
    'ocupacaoConfirmadoUti_HOS',
    'saidaSuspeitaObitos_HOS',
    'saidaSuspeitaAltas_HOS',
    'saidaConfirmadaObitos_HOS',
    'saidaConfirmadaAltas_HOS'
]
df = df.drop('estado')
dataset = useful_ref.join(df, ['date', 'nome_mun_res'], how='left')

dataset.groupby('date', 'uf_res').agg(*[
    F.sum(c).astype('int').alias(c) for c in cols_hosp
]).coalesce(1).write.mode('overwrite') \
    .csv(os.path.join(groupby, 'hosp_uf'), header=True)

dataset.groupby('date', 'mun_res').agg(*[
    F.sum(c).astype('int').alias(c) for c in cols_hosp
]).coalesce(1).write.mode('overwrite') \
    .csv(os.path.join(groupby, 'hosp_mun'), header=True)
