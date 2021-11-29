#!/usr/bin/env python3


import os
from pyspark import SparkConf
from pyspark.sql import SparkSession
import pyspark.sql.functions as F

date = '2021-11-25'
datalake = '/media/fabio/19940C2755DB566F/PAMepi/datalake/'
raw = f'raw_data_covid19_version-{date}/'
preprocess = os.path.join(datalake, raw, 'preprocess/')
groupby = os.path.join(datalake, raw, 'group_')

conf = SparkConf().setAll(
    [
        ('spark.driver.memory', '12g'),
        ('spark.driver.cores', '6')
    ]
)

spark = SparkSession.builder.config(conf=conf).getOrCreate()

vac_uf = spark.read.csv(os.path.join(groupby, 'vacc_uf'), header=True)
sg_uf = spark.read.csv(os.path.join(groupby, 'sg_uf'), header=True)
srag_uf = spark.read.csv(os.path.join(groupby, 'srag_uf'), header=True)
wcota_uf = spark.read.csv(os.path.join(groupby, 'wcota_uf'), header=True)
hosp_uf = spark.read.csv(os.path.join(groupby, 'hosp_uf'), header=True)

vac_mun = spark.read.csv(os.path.join(groupby, 'vacc_mun'), header=True)
sg_mun = spark.read.csv(os.path.join(groupby, 'sg_mun'), header=True)
srag_mun = spark.read.csv(os.path.join(groupby, 'srag_mun'), header=True)
wcota_mun = spark.read.csv(os.path.join(groupby, 'wcota_mun'), header=True)
hosp_mun = spark.read.csv(os.path.join(groupby, 'hosp_mun'), header=True)


uf_datasets = [vac_uf, sg_uf, srag_uf, wcota_uf, hosp_uf]
mun_datasets = [vac_mun, sg_mun, srag_mun, wcota_mun, hosp_mun]

vac_uf = vac_uf.drop('raca', 'age_group', 'sexo')
vac_mun = vac_mun.drop('raca', 'age_group', 'sexo')

sg_uf = sg_uf.drop('sexo', 'age_group')
sg_mun = sg_mun.drop('sexo', 'age_group')

uf_final = sg_uf.join(hosp_uf, ['date', 'uf_res'], how='left') \
    .join(vac_uf, ['date', 'uf_res'], how='left') \
    .join(wcota_uf, ['date', 'uf_res'], how='left')

uf_final.coalesce(1).write.mode('overwrite').csv(os.path.join(groupby, f'aggregate_uf-{date}'), header=True)

mun_final = sg_mun.join(hosp_mun, ['date', 'mun_res'], how='left') \
    .join(vac_mun, ['date', 'mun_res'], how='left') \
    .join(wcota_mun, ['date', 'mun_res'], how='left')

mun_final.coalesce(1).write.mode('overwrite').csv(os.path.join(groupby, f'aggregate_mun-{date}'), header=True)
