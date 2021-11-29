#!/usr/bin/env python3

# carregando bibliotecas
import os
from pyspark import SparkConf
from pyspark.sql import SparkSession
import pyspark.sql.functions as F
import pyspark.sql.types as T

from funcoes import (
    converte_data, converte_sexo, padroniza_texto, correcao_de_idade,
    cria_faixa_etaria
)

# setando uma data fixa para escolha da pasta
date = '2021-11-25'
# criando caminho para diretorio do datalake
datalake = '/media/fabio/19940C2755DB566F/PAMepi/datalake/'
raw = f'raw_data_covid19_version-{date}/'
preprocess = os.path.join(datalake, raw, 'preprocess/')
groupby = os.path.join(datalake, raw, 'group_')

# configurações para sessão spark
conf = SparkConf().setAll(
    [
        ('spark.driver.memory', '12g'),
        ('spark.driver.cores', '6')
    ]
)

# criando sessão
spark = SparkSession.builder.config(conf=conf).getOrCreate()

# carregando banco de vacinação e fazendo alguns ajustes
df = spark.read.csv(preprocess + 'vacc_preprocess', header=True)
df = padroniza_texto(df, ['sexo', 'dose', 'nome_mun_res', 'uf_res'])
df = converte_sexo(df, 'sexo')
df = cria_faixa_etaria(df, 'idade')
df = df.withColumn('n', F.lit(1))

# agrupamento para visualização - vacinação

# 1 - estado e municipio

df.groupby('date', 'mun_res', 'nome_mun_res',
           'uf_res', 'raca', 'age_group', 'sexo') \
    .agg(F.count(F.when(F.col('num_dose') != 'dose', F.col('n'))).astype('int') \
         .alias('num_dose_reg_VAC'),

         F.count(F.when(F.col('num_dose') == 'dose', F.col('n'))).astype('int') \
         .alias('num_dose_non_reg_VAC'),

         F.count(F.col('n')).astype('int') \
         .alias('total_dose_VAC'),

         F.count(F.when(
             (F.col('dose') == '1ª dose') | (F.col('dose') == 'dose inicial'),
             F.col('n'))).astype('int').alias('num_pri_reg_VAC'),

         F.count(F.when(
             (F.col('dose') == '2ª dose') | (F.col('dose') == 'unica'),
             F.col('n'))).astype('int').alias('num_sec_uni_reg_VAC'),

         F.count(F.when(
             (F.col('dose') == '3ª dose') |
             (F.col('dose') == '1º reforco') |
             (F.col('dose') == 'dose adicional') |
             (F.col('dose') == 'reforco'),
             F.col('n'))).astype('int').alias('num_reinforcment_VAC'),

         F.count(F.when(F.col('dose') == '1ª dose revacinacao', F.col('n'))) \
         .astype('int').alias('num_pri_revac_VAC'),

         F.count(F.when(F.col('dose') == '2ª dose revacinacao', F.col('n'))) \
                 .astype('int').alias('num_sec_revac_VAC')) \
    .coalesce(1).write.mode('overwrite') \
    .csv(os.path.join(groupby, 'vacc_total'), header=True)

# 2 - municipio
df.groupby('date', 'mun_res', 'raca', 'age_group', 'sexo') \
    .agg(F.count(F.when(F.col('num_dose') != 'dose', F.col('n'))).astype('int') \
         .alias('num_dose_reg_VAC'),

         F.count(F.when(F.col('num_dose') == 'dose', F.col('n'))).astype('int') \
         .alias('num_dose_non_reg_VAC'),

         F.count(F.col('n')).astype('int') \
         .alias('total_dose_VAC'),

         F.count(F.when(
             (F.col('dose') == '1ª dose') | (F.col('dose') == 'dose inicial'),
             F.col('n'))).astype('int').alias('num_pri_reg_VAC'),

         F.count(F.when(
             (F.col('dose') == '2ª dose') | (F.col('dose') == 'unica'),
             F.col('n'))).astype('int').alias('num_sec_uni_reg_VAC'),

         F.count(F.when(
             (F.col('dose') == '3ª dose') |
             (F.col('dose') == '1º reforco') |
             (F.col('dose') == 'dose adicional') |
             (F.col('dose') == 'reforco'),
             F.col('n'))).astype('int').alias('num_reinforcment_VAC'),

         F.count(F.when(F.col('dose') == '1ª dose revacinacao', F.col('n'))) \
         .astype('int').alias('num_pri_revac_VAC'),

         F.count(F.when(F.col('dose') == '2ª dose revacinacao', F.col('n'))) \
                 .astype('int').alias('num_sec_revac_VAC')) \
    .coalesce(1).write.mode('overwrite') \
    .csv(os.path.join(groupby, 'vacc_mun'), header=True)

# 3 - estado
df.groupby('date', 'uf_res', 'raca', 'age_group', 'sexo') \
    .agg(F.count(F.when(F.col('num_dose') != 'dose', F.col('n'))).astype('int') \
         .alias('num_dose_reg_VAC'),

         F.count(F.when(F.col('num_dose') == 'dose', F.col('n'))).astype('int') \
         .alias('num_dose_non_reg_VAC'),

         F.count(F.col('n')).astype('int') \
         .alias('total_dose_VAC'),

         F.count(F.when(
             (F.col('dose') == '1ª dose') | (F.col('dose') == 'dose inicial'),
             F.col('n'))).astype('int').alias('num_pri_reg_VAC'),

         F.count(F.when(
             (F.col('dose') == '2ª dose') | (F.col('dose') == 'unica'),
             F.col('n'))).astype('int').alias('num_sec_uni_reg_VAC'),

         F.count(F.when(
             (F.col('dose') == '3ª dose') |
             (F.col('dose') == '1º reforco') |
             (F.col('dose') == 'dose adicional') |
             (F.col('dose') == 'reforco'),
             F.col('n'))).astype('int').alias('num_reinforcment_VAC'),

         F.count(F.when(F.col('dose') == '1ª dose revacinacao', F.col('n'))) \
         .astype('int').alias('num_pri_revac_VAC'),

         F.count(F.when(F.col('dose') == '2ª dose revacinacao', F.col('n'))) \
                 .astype('int').alias('num_sec_revac_VAC')) \
    .coalesce(1).write.mode('overwrite') \
    .csv(os.path.join(groupby, 'vacc_uf'), header=True)

# carregando dados municiapis do wesley cota
df = spark.read.csv(preprocess + 'wcota_preprocess', header=True)
df = padroniza_texto(df, ['uf_res', 'nome_mun_res'])
df = df.filter(df.nome_mun_res != 'total')
df = df.withColumn(
    'mun_res', F.udf(lambda x: x[:6], T.StringType())(F.col('mun_res'))
)
df = df.withColumn(
    'nome_mun_res',
    F.udf(lambda x: x[:-3], T.StringType())(F.col('nome_mun_res'))
)

df.groupby('date', 'uf_res', 'nome_mun_res', 'mun_res') \
    .agg(F.count('newDeaths').astype('int').alias('newDeaths_WC'),
         F.sum('deaths').astype('int').alias('deaths_WC'),
         F.count('newCases').astype('int').alias('newCases_WC'),
         F.sum('totalCases').astype('int').alias('totalCases_WC')
    ).orderBy('date').write.mode('overwrite') \
    .csv(os.path.join(groupby, 'wcota_total'), header=True)

df.groupby('date', 'uf_res') \
    .agg(F.count('newDeaths').astype('int').alias('newDeaths_WC'),
         F.sum('deaths').astype('int').alias('deaths_WC'),
         F.count('newCases').astype('int').alias('newCases_WC'),
         F.sum('totalCases').astype('int').alias('totalCases_WC')
    ).orderBy('date').write.mode('overwrite') \
    .csv(os.path.join(groupby, 'wcota_uf'), header=True)

df.groupby('date', 'mun_res') \
    .agg(F.count('newDeaths').astype('int').alias('newDeaths_WC'),
         F.sum('deaths').astype('int').alias('deaths_WC'),
         F.count('newCases').astype('int').alias('newCases_WC'),
         F.sum('totalCases').astype('int').alias('totalCases_WC')
    ).orderBy('date').write.mode('overwrite') \
    .csv(os.path.join(groupby, 'wcota_mun'), header=True)


# carregando dados de sindrome gripal
df = spark.read.csv(preprocess + 'sg_preprocess', header=True)

df = df.withColumn('n', F.lit(1))

for col in df.columns:
    if col.startswith('data'):
        df = df.withColumn(col, F.col(col).cast(T.DateType()))

df = df.withColumn(
    'date', F.when(F.col('dataInicioSintomas').isNotNull(),
                   F.col('dataInicioSintomas')) \

    .when((F.col('dataInicioSintomas').isNull()) |
          (F.col('dataTeste').isNotNull()), F.col('dataTeste')) \

    .when((F.col('dataInicioSintomas').isNull()) &
          (F.col('dataTeste').isNull()), F.col('dataNotificacao')) \
    .otherwise('dataNotificacao')
)

df = padroniza_texto(
    df, ['classificacaoFinal', 'evolucaoCaso', 'municipio',
         'municipioNotificacao', 'estado', 'estadoNotificacao', 'sexo']
)

df = correcao_de_idade(df, 'idade', 'dataNascimento')
df = cria_faixa_etaria(df, 'idade')
df = converte_sexo(df, 'sexo')

df = df.withColumn(
    'mun_res',
    F.when(F.col('municipioIBGE').isNotNull(), F.col('municipioIBGE')) \
    .when(F.col('municipioIBGE').isNull(), F.col('municipioNotificacaoIBGE')))

df = df.withColumn(
    'nome_mun_res',
    F.when(F.col('municipio').isNotNull(), F.col('municipio')) \
    .when(F.col('municipio').isNull(), F.col('municipioNotificacao')))

df = df.withColumn(
    'classificacaoFinal',
    F.when(F.col('classificacaoFinal') == 'confirmado', 1)\
    .when((F.col('classificacaoFinal') == 'confirmado clinico epidemiologico')|
          (F.col('classificacaoFinal') == \
           'confirmacao clinico epidemiologico'),
          2) \
    .when(F.col('classificacaoFinal') == 'confirmado por criterio clinico',
          3) \
    .when((F.col('classificacaoFinal') == 'confirmado laboratorial') |
          (F.col('classificacaoFinal') == 'confirmacao laboratorial '), 4) \
    .when(F.col('classificacaoFinal') == 'confirmado clinico imagem', 5) \
    .when(F.col('classificacaoFinal') == 'sindrome gripal nao especificada',
          6) \
    .when(F.col('classificacaoFinal') == 'descartado', 7)
)

df = df.withColumn(
    'evolucaoCaso',
    F.when(F.col('evolucaoCaso') == 'obito', 1) \
    .when(F.col('evolucaoCaso') == 'cura', 2) \
    .when((F.col('evolucaoCaso') == 'internado em uti') |
          (F.col('evolucaoCaso') == 'internado'), 3) \
    .when(F.col('evolucaoCaso') == 'cancelado', 4) \
    .when(F.col('evolucaoCaso') == 'em tratamento domiciliar', 5) \
    .when(F.col('evolucaoCaso') == 'ignorado', 9)
)

df = df.filter((F.col('date') >= '2020-02-01') &
                 (F.col('date') <= date))

df = df.withColumn('mun_res', F.udf(lambda x: x[:6], T.StringType())('mun_res'))

df.groupby(
    'date', 'mun_res', 'sexo', 'age_group') \
    .agg(
        F.count(F.when((F.col('classificacaoFinal') != 7) |
                       (F.col('classificacaoFinal').isNotNull()),
                       F.col('n'))) \
        .astype('int').alias('newCases_SG'),

        F.count(F.when(F.col('classificacaoFinal') == 7, F.col('n'))) \
        .astype('int').alias('new_des_SG'),

        F.count(F.when(F.col('classificacaoFinal').isNull(), F.col('n')))\
        .astype('int').alias('new_undefined_SG'),

        F.count(F.when(F.col('evolucaoCaso') == 1, F.col('n'))) \
        .astype('int').alias('newDeath_SG'),

        F.count(F.when(F.col('evolucaoCaso') == 2, F.col('n'))) \
        .astype('int').alias('new_recovered_SG'),

        F.count(F.when(F.col('evolucaoCaso') == 3, F.col('n'))) \
        .astype('int').alias('UtiEvolution_SG'),
    ).orderBy('date').coalesce(1).write.mode('overwrite') \
    .csv(os.path.join(groupby, 'sg_mun'), header=True)


# carregando dados de hospitalização
df = spark.read.csv(preprocess + 'hosp_preprocess', header=True)
df = padroniza_texto(df, ['estado', 'nome_mun_res'])
df = df.withColumn('date', F.col('date').cast(T.DateType()))
df = df.filter((df.date >= '2020-02-01') & (df.date <= date))

df.groupby('date', 'estado', 'nome_mun_res') \
    .agg(
        *[
            F.sum(c).astype('int').alias(c)
            for c in df.columns if c.endswith('_HOS')
        ]).coalesce(1).write.mode('overwrite') \
    .csv(os.path.join(groupby, 'hosp_total'), header=True)

# carregando dados de srag
df = spark.read.csv(preprocess + 'srag_preprocess', header=True)

df = df.withColumn('n', F.lit(1))

df = converte_data(df, ['dt_notific', 'dt_sin_pri', 'dt_coleta'])

df = df.withColumn(
    'date', F.when(F.col('dt_sin_pri').isNotNull(), F.col('dt_sin_pri')) \
    .when((F.col('dt_sin_pri').isNull()) & (F.col('dt_coleta').isNotNull()),
          F.col('dt_coleta')) \
    .otherwise('dt_notific'))

df = padroniza_texto(df, ['id_municip', 'uf_res', 'id_mn_resi'])

df = df.withColumn(
    'mun_res', F.when(F.col('co_mun_res').isNotNull(), F.col('co_mun_res')) \
    .when(F.col('co_mun_res').isNull(), F.col('co_mun_not'))\
    .otherwise(F.col('co_mun_res')))

df = df.withColumn(
    'nome_mun_res', F.when(F.col('id_mn_resi').isNotNull(),
                           F.col('id_mn_resi')) \
    .when(F.col('id_mn_resi').isNull(), F.col('id_municip')) \
    .otherwise(F.col('id_mn_resi')))


df.groupby('date', 'uf_res', 'mun_res', 'nome_mun_res') \
    .agg(
        F.count(F.when(F.col('classi_fin') == 5, F.col('n'))) \
        .astype('int').alias('newCases_SRAG'),

        F.count(F.when(
            (F.col('uti') == 2) & (F.col('dt_interna').isNotNull()),
            F.col('n'))).astype('int').alias('hosp_clinc_SRAG'),

        F.count(F.when((F.col('uti') == 1) & (
                       (F.col('dt_entuti').isNotNull()) |
            (F.col('dt_saiduti').isNotNull())
        ),
            F.col('n'))).astype('int').alias('hosp_ICU_SRAG'),

        F.count(F.when((F.col('classi_fin') == 5) & (
                       (F.col('dt_entuti').isNotNull()) |
                       (F.col('dt_saiduti').isNotNull())), F.col('n'))
                ).astype('int').alias('ocup_ICU_SRAG'),

        F.count(F.when((F.col('classi_fin') == 5) & (
                       (F.col('dt_evoluca').isNotNull()) |
                       (F.col('dt_entuti').isNotNull()) |
                       (F.col('dt_saiduti').isNotNull())) , F.col('n'))
                ).astype('int').alias('ocup_clinc_SRAG'),

        F.count(F.when(
            (F.col('classi_fin') == 5) & (F.col('evolucao') == 2) & (
                (F.col('dt_evoluca').isNotNull()) |
                (F.col('dt_encerra').isNotNull())), F.col('n'))
                ).astype('int').alias('newDeath_SRAG'),

        F.count(F.when(
            (F.col('classi_fin') == 5) & (F.col('evolucao') == 1) & (
                (F.col('dt_evoluca').isNotNull()) |
                (F.col('dt_encerra').isNotNull())), F.col('n'))
                ).astype('int').alias('newRecovered_SRAG'),

        F.count(
            F.when(
                (F.col('classi_fin') == 5) & (F.col('evolucao') == 2) &
                (F.col('uti') == 1) & (
                    (F.col('dt_evoluca').isNotNull()) |
                     (F.col('dt_encerra').isNotNull())), F.col('n'))
        ).astype('int').alias('newDeath_ICU_SRAG'),

        F.count(
            F.when(
                (F.col('classi_fin') == 5) & (F.col('evolucao') == 1) &
                (F.col('uti') == 1) & (
                    (F.col('dt_evoluca').isNotNull() |
                     (F.col('dt_encerra').isNotNull()))), F.col('n'))
        ).astype('int').alias('newRecovered_ICU_SRAG'),

        F.count(
            F.when(
                (F.col('classi_fin') == 5) & (F.col('evolucao') == 2) &
                (F.col('uti') == 2) & (
                    (F.col('dt_evoluca').isNotNull() |
                     (F.col('dt_encerra').isNotNull()))), F.col('n'))
        ).astype('int').alias('newDeath_clinic_SRAG'),

        F.count(
            F.when(
                (F.col('classi_fin') == 5) & (F.col('evolucao') == 1) &
                (F.col('uti') == 2) & (
                    (F.col('dt_evoluca').isNotNull() |
                     (F.col('dt_encerra').isNotNull()))), F.col('n'))
        ).astype('int').alias('newRecovered_clinic_SRAG')

        ).orderBy('date').coalesce(1).write.mode('overwrite') \
            .csv(os.path.join(groupby, 'srag_total'), header=True)


df.groupby('date', 'uf_res') \
    .agg(
        F.count(F.when(F.col('classi_fin') == 5, F.col('n'))) \
        .astype('int').alias('newCases_SRAG'),

        F.count(F.when(
            (F.col('uti') == 2) & (F.col('dt_interna').isNotNull()),
            F.col('n'))).astype('int').alias('hosp_clinc_SRAG'),

        F.count(F.when((F.col('uti') == 1) & (
                       (F.col('dt_entuti').isNotNull()) |
            (F.col('dt_saiduti').isNotNull())
        ),
            F.col('n'))).astype('int').alias('hosp_ICU_SRAG'),

        F.count(F.when((F.col('classi_fin') == 5) & (
                       (F.col('dt_entuti').isNotNull()) |
                       (F.col('dt_saiduti').isNotNull())), F.col('n'))
                ).astype('int').alias('ocup_ICU_SRAG'),

        F.count(F.when((F.col('classi_fin') == 5) & (
                       (F.col('dt_evoluca').isNotNull()) |
                       (F.col('dt_entuti').isNotNull()) |
                       (F.col('dt_saiduti').isNotNull())) , F.col('n'))
                ).astype('int').alias('ocup_clinc_SRAG'),

        F.count(F.when(
            (F.col('classi_fin') == 5) & (F.col('evolucao') == 2) & (
                (F.col('dt_evoluca').isNotNull()) |
                (F.col('dt_encerra').isNotNull())), F.col('n'))
                ).astype('int').alias('newDeath_SRAG'),

        F.count(F.when(
            (F.col('classi_fin') == 5) & (F.col('evolucao') == 1) & (
                (F.col('dt_evoluca').isNotNull()) |
                (F.col('dt_encerra').isNotNull())), F.col('n'))
                ).astype('int').alias('newRecovered_SRAG'),

        F.count(
            F.when(
                (F.col('classi_fin') == 5) & (F.col('evolucao') == 2) &
                (F.col('uti') == 1) & (
                    (F.col('dt_evoluca').isNotNull()) |
                     (F.col('dt_encerra').isNotNull())), F.col('n'))
        ).astype('int').alias('newDeath_ICU_SRAG'),

        F.count(
            F.when(
                (F.col('classi_fin') == 5) & (F.col('evolucao') == 1) &
                (F.col('uti') == 1) & (
                    (F.col('dt_evoluca').isNotNull() |
                     (F.col('dt_encerra').isNotNull()))), F.col('n'))
        ).astype('int').alias('newRecovered_ICU_SRAG'),

        F.count(
            F.when(
                (F.col('classi_fin') == 5) & (F.col('evolucao') == 2) &
                (F.col('uti') == 2) & (
                    (F.col('dt_evoluca').isNotNull() |
                     (F.col('dt_encerra').isNotNull()))), F.col('n'))
        ).astype('int').alias('newDeath_clinic_SRAG'),

        F.count(
            F.when(
                (F.col('classi_fin') == 5) & (F.col('evolucao') == 1) &
                (F.col('uti') == 2) & (
                    (F.col('dt_evoluca').isNotNull() |
                     (F.col('dt_encerra').isNotNull()))), F.col('n'))
        ).astype('int').alias('newRecovered_clinic_SRAG')

        ).orderBy('date').coalesce(1).write.mode('overwrite') \
            .csv(os.path.join(groupby, 'srag_uf'), header=True)


df.groupby('date', 'mun_res', ) \
    .agg(
        F.count(F.when(F.col('classi_fin') == 5, F.col('n'))) \
        .astype('int').alias('newCases_SRAG'),

        F.count(F.when(
            (F.col('uti') == 2) & (F.col('dt_interna').isNotNull()),
            F.col('n'))).astype('int').alias('hosp_clinc_SRAG'),

        F.count(F.when((F.col('uti') == 1) & (
                       (F.col('dt_entuti').isNotNull()) |
            (F.col('dt_saiduti').isNotNull())
        ),
            F.col('n'))).astype('int').alias('hosp_ICU_SRAG'),

        F.count(F.when((F.col('classi_fin') == 5) & (
                       (F.col('dt_entuti').isNotNull()) |
                       (F.col('dt_saiduti').isNotNull())), F.col('n'))
                ).astype('int').alias('ocup_ICU_SRAG'),

        F.count(F.when((F.col('classi_fin') == 5) & (
                       (F.col('dt_evoluca').isNotNull()) |
                       (F.col('dt_entuti').isNotNull()) |
                       (F.col('dt_saiduti').isNotNull())) , F.col('n'))
                ).astype('int').alias('ocup_clinc_SRAG'),

        F.count(F.when(
            (F.col('classi_fin') == 5) & (F.col('evolucao') == 2) & (
                (F.col('dt_evoluca').isNotNull()) |
                (F.col('dt_encerra').isNotNull())), F.col('n'))
                ).astype('int').alias('newDeath_SRAG'),

        F.count(F.when(
            (F.col('classi_fin') == 5) & (F.col('evolucao') == 1) & (
                (F.col('dt_evoluca').isNotNull()) |
                (F.col('dt_encerra').isNotNull())), F.col('n'))
                ).astype('int').alias('newRecovered_SRAG'),

        F.count(
            F.when(
                (F.col('classi_fin') == 5) & (F.col('evolucao') == 2) &
                (F.col('uti') == 1) & (
                    (F.col('dt_evoluca').isNotNull()) |
                     (F.col('dt_encerra').isNotNull())), F.col('n'))
        ).astype('int').alias('newDeath_ICU_SRAG'),

        F.count(
            F.when(
                (F.col('classi_fin') == 5) & (F.col('evolucao') == 1) &
                (F.col('uti') == 1) & (
                    (F.col('dt_evoluca').isNotNull() |
                     (F.col('dt_encerra').isNotNull()))), F.col('n'))
        ).astype('int').alias('newRecovered_ICU_SRAG'),

        F.count(
            F.when(
                (F.col('classi_fin') == 5) & (F.col('evolucao') == 2) &
                (F.col('uti') == 2) & (
                    (F.col('dt_evoluca').isNotNull() |
                     (F.col('dt_encerra').isNotNull()))), F.col('n'))
        ).astype('int').alias('newDeath_clinic_SRAG'),

        F.count(
            F.when(
                (F.col('classi_fin') == 5) & (F.col('evolucao') == 1) &
                (F.col('uti') == 2) & (
                    (F.col('dt_evoluca').isNotNull() |
                     (F.col('dt_encerra').isNotNull()))), F.col('n'))
        ).astype('int').alias('newRecovered_clinic_SRAG')

        ).orderBy('date').coalesce(1).write.mode('overwrite') \
            .csv(os.path.join(groupby, 'srag_total'), header=True)


df.groupby('date', 'uf_res') \
    .agg(
        F.count(F.when(F.col('classi_fin') == 5, F.col('n'))) \
        .astype('int').alias('newCases_SRAG'),

        F.count(F.when(
            (F.col('uti') == 2) & (F.col('dt_interna').isNotNull()),
            F.col('n'))).astype('int').alias('hosp_clinc_SRAG'),

        F.count(F.when((F.col('uti') == 1) & (
                       (F.col('dt_entuti').isNotNull()) |
            (F.col('dt_saiduti').isNotNull())
        ),
            F.col('n'))).astype('int').alias('hosp_ICU_SRAG'),

        F.count(F.when((F.col('classi_fin') == 5) & (
                       (F.col('dt_entuti').isNotNull()) |
                       (F.col('dt_saiduti').isNotNull())), F.col('n'))
                ).astype('int').alias('ocup_ICU_SRAG'),

        F.count(F.when((F.col('classi_fin') == 5) & (
                       (F.col('dt_evoluca').isNotNull()) |
                       (F.col('dt_entuti').isNotNull()) |
                       (F.col('dt_saiduti').isNotNull())) , F.col('n'))
                ).astype('int').alias('ocup_clinc_SRAG'),

        F.count(F.when(
            (F.col('classi_fin') == 5) & (F.col('evolucao') == 2) & (
                (F.col('dt_evoluca').isNotNull()) |
                (F.col('dt_encerra').isNotNull())), F.col('n'))
                ).astype('int').alias('newDeath_SRAG'),

        F.count(F.when(
            (F.col('classi_fin') == 5) & (F.col('evolucao') == 1) & (
                (F.col('dt_evoluca').isNotNull()) |
                (F.col('dt_encerra').isNotNull())), F.col('n'))
                ).astype('int').alias('newRecovered_SRAG'),

        F.count(
            F.when(
                (F.col('classi_fin') == 5) & (F.col('evolucao') == 2) &
                (F.col('uti') == 1) & (
                    (F.col('dt_evoluca').isNotNull()) |
                     (F.col('dt_encerra').isNotNull())), F.col('n'))
        ).astype('int').alias('newDeath_ICU_SRAG'),

        F.count(
            F.when(
                (F.col('classi_fin') == 5) & (F.col('evolucao') == 1) &
                (F.col('uti') == 1) & (
                    (F.col('dt_evoluca').isNotNull() |
                     (F.col('dt_encerra').isNotNull()))), F.col('n'))
        ).astype('int').alias('newRecovered_ICU_SRAG'),

        F.count(
            F.when(
                (F.col('classi_fin') == 5) & (F.col('evolucao') == 2) &
                (F.col('uti') == 2) & (
                    (F.col('dt_evoluca').isNotNull() |
                     (F.col('dt_encerra').isNotNull()))), F.col('n'))
        ).astype('int').alias('newDeath_clinic_SRAG'),

        F.count(
            F.when(
                (F.col('classi_fin') == 5) & (F.col('evolucao') == 1) &
                (F.col('uti') == 2) & (
                    (F.col('dt_evoluca').isNotNull() |
                     (F.col('dt_encerra').isNotNull()))), F.col('n'))
        ).astype('int').alias('newRecovered_clinic_SRAG')

        ).orderBy('date').coalesce(1).write.mode('overwrite') \
            .csv(os.path.join(groupby, 'srag_mun'), header=True)
