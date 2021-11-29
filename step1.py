#!/usr/bin/env python3

# importing libraries
import os
from datetime import datetime
import yaml
from pyspark.sql import SparkSession

from funcoes import union_all, rename_columns


# creating spark session
spark = SparkSession.builder.appName('app').getOrCreate()

# date = '2021-11-25'
date = datetime.today().now().strftime('%Y-%m-%d')
PAMEPI = '/media/fabio/19940C2755DB566F/PAMepi/datalake/'
# version = f"raw_data_covid19_version-{datetime.now().strftime('%Y-%m-%d')}/"
version = f"raw_data_covid19_version-{date}/"

datalake = os.path.join(PAMEPI, version)
output_datalake = os.path.join(datalake, 'preprocess')

# some changes based in params.yml
def preprocess(dataset, sep, enc, cols, output):
    df = spark.read.csv(dataset, sep=sep, header=True, encoding=enc)
    df = rename_columns(df, cols)
    df.select(*list(cols.values())) \
        .write.mode('overwrite') \
        .csv(os.path.join(output_datalake, output), header=True)

# if multiple files this solves the problem
def concat(dataset, sep, enc, cols, output):
    dfs = []
    folder = os.path.join(datalake, dataset)
    for file in os.listdir(folder):
        dfs.append(
            spark.read.csv(folder + '/' + file,
                           sep=sep, header=True,
                           encoding=enc)
        )

    df = union_all(dfs)
    df = rename_columns(df, cols)
    df.select(*list(cols.values())) \
        .write.mode('overwrite') \
        .csv(os.path.join(output_datalake, output), header=True)

# load config
with open('params.yml', 'r') as f:
    params = yaml.safe_load(f)

# apply function
for dataset in os.listdir(datalake):
    try:
        sep, functions, enc, columns, output = (params[dataset]['sep'],
                                                params[dataset]['function'],
                                                params[dataset]['enc'],
                                                params[dataset]['cols'],
                                                params[dataset]['output'])
        for func in functions:
            globals()[func](datalake + dataset, sep, enc, columns, output)
    except KeyError:
        pass
