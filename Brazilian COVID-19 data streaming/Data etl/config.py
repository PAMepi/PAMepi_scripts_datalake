import os
import psutil
from pyspark import SparkConf
from pyspark.sql import SparkSession


mem_base = psutil.virtual_memory()
thread_base = int(round(psutil.cpu_count() / 5, 0))


cores = str((thread_base * 6) - 1)
ram = str((int(round((mem_base.total / 1024**3), 0)) // 2) -1) + 'g'
mem_driver = str(int(round(0.40 * (mem_base.total / 1024**3), 0))) + 'g'
tmp = os.path.expanduser('~/tmp')


conf = SparkConf().setAll([
    ('spark.local.dir', tmp),
    ('spark.driver.cores', '5'),
    ('spark.driver.memory', mem_driver),
    ('spark.executor.cores', cores),
    ('spark.executor.memory', ram),
])


spark = SparkSession.builder.config(conf=conf).getOrCreate()
