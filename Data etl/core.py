"""
Esse arquivo tem como finalidade poupar tempo de configuracao com spark e
definir algumas funções que sejam usadas com uma certa frequencia.
"""

from pyspark import SparkConf
from pyspark.sql import SparkSession
import pyspark.sql.functions as F
import pyspark.sql.types as T


conf = SparkConf().setAll(
    [('spark.driver.memory', '6G'),
     ('spark.executor.memory', '2G'),
     ('spark.executor.cores', '6'),
     ('spark.local.dir', './tmp'),
     ('spark.sql.debug.maxToStringFields', '100'),
     ('spark.app.name', 'core')]
)


spark = SparkSession.builder.config(conf=conf).getOrCreate()
