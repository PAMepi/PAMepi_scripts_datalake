#!/usr/bin/env python3

import os
from pyspark import SparkConf
from pyspark.sql import SparkSession, Window
import pyspark.sql.functions as F


conf = SparkConf().setAll(
    [
        ('spark.driver.memory', '12g'),
        ('spark.driver.cores', '6')
    ]
)

date = '2021-11-25'
datalake = '/media/fabio/19940C2755DB566F/PAMepi/datalake/'
raw = f'raw_data_covid19_version-{date}/'
preprocess = os.path.join(datalake, raw, 'preprocess/')
groupby = os.path.join(datalake, raw, 'group_')
visualization = os.path.join(groupby, f'visualization-{date}')

spark = SparkSession.builder.config(conf=conf).getOrCreate()

df1 = spark.read.csv(os.path.join(groupby, 'vacc_uf'), header=True)
df2 = spark.read.csv(os.path.join(groupby, 'vacc_mun'), header=True)

df1.filter((df1.date >= '2021-01-17') &
           (df1.date <= date) &
           (df1.uf_res.isNotNull()))

df2.filter((df2.date >= '2021-01-17') &
           (df2.date <= date) &
           (df2.mun_res.isNotNull()))

window_uf = Window.partitionBy('uf_res') \
    .orderBy('date') \
    .rangeBetween(Window.unboundedPreceding, 0)

window_mun = Window.partitionBy('mun_res') \
    .orderBy('date') \
    .rangeBetween(Window.unboundedPreceding, 0)

df1.withColumn('cum_sum_daily', F.sum('total_dose_VAC').over(window_uf)) \
    .filter(df1['uf_res'].isNotNull()) \
    .coalesce(1).write.mode('overwrite') \
    .csv(os.path.join(visualization, 'vac_uf'), header=True)

df2.withColumn('cum_sum_daily', F.sum('total_dose_VAC').over(window_mun)) \
    .filter(df2['mun_res'].isNotNull()) \
    .coalesce(1).write.mode('overwrite') \
    .csv(os.path.join(visualization, 'vac_mun'), header=True)
