#!/usr/bin/env python3

import os
# from datetime import datetime
from unidecode import unidecode
from pyspark.sql.functions import (
    add_months, col, count, concat_ws,
    first, isnan, lit, split, months_between, udf, when
)
from pyspark.sql.types import StringType, IntegerType

from config import spark


colunas_p_renomear = {
    'paciente_id': 'id',
    'paciente_idade': 'idade',
    'paciente_enumSexoBiologico': 'sexo',
    'paciente_racaCor_codigo': 'raca',
    'paciente_endereco_coIbgeMunicipio': 'mun_res',
    'paciente_endereco_nmMunicipio': 'nome_mun_res',
    'paciente_endereco_uf': 'uf_res',
    'vacina_dataAplicacao': 'data',
    'vacina_descricao_dose': 'dose',
    'vacina_nome': 'vacina',
}


@udf(returnType=StringType())
def unidecode_udf(coluna):
    try:
        return unidecode(coluna).lower()
    except AttributeError:
        return 999


HD = '/media/fabio/19940C2755DB566F'
DATALAKE = 'PAMepi/datalake/raw_data_covid19_version-2022-02-23'
rw = 'data-vacinacao_brasil'


dataset = os.path.join(HD, DATALAKE, rw)

# rw = spark.read.csv(dataset, header=True, sep=';')

# rw.select(
#     *[
#         count(
#             when(
#                 col(coluna).isNull() |
#                 isnan(coluna),
#                 coluna
#             )
#         ).alias(coluna)
#         for coluna in rw.columns
#     ]
# ).write.csv('vacina_total_nulls', mode='overwrite', header=True)


# rw.filter(
#     (rw.vacina_dataAplicacao < '2021-01-17') |
#     (rw.vacina_dataAplicacao > datetime.now().strftime('%Y-%m-%d'))
# ).groupby('vacina_descricao_dose').count() \
#     .write.csv('erro_data_min_max', mode='overwrite', header=True)


# rw.select('vacina_descricao_dose').distinct().coalesce(1) \
#     .write.csv('total_estagio_doses', mode='overwrite', header=True)


# for coluna_antiga, coluna_nova in colunas_p_renomear.items():
#     rw = rw.withColumnRenamed(coluna_antiga, coluna_nova)
#     rw = rw.withColumn(coluna_nova, unidecode_udf(coluna_nova))
# 
# colunas_de_interesse = list(colunas_p_renomear.values())
# 
# 
# rw.filter(rw.data <= '2021-11-15').select(colunas_de_interesse) \
#     .write.csv(
#         os.path.join(HD, DATALAKE, 'vacinacao_tmp_99'),
#         mode='overwrite',
#         header=True
#     )

# rw = spark.read.csv(os.path.join(HD, DATALAKE, 'vacinacao_tmp_99'),  header=True)
# 
# rw.groupby(colunas_de_interesse[:-3]) \
#     .pivot('dose') \
#     .agg(concat_ws(',', first('data'), first('vacina'))) \
#     .write.csv(os.path.join(HD, DATALAKE, 'vacinacao_pivot_tmp_99'),
#                mode='overwrite', header=True, compression='gzip')


rw = spark.read.csv(
    os.path.join(HD, DATALAKE, 'vacinacao_pivot_tmp_99'),
    header=True)


fx_etaria = udf(lambda idade: '0-17' if (idade >= 0 and idade < 18) else
                            '18-29' if (idade >= 18 and idade < 30) else
                            '30-39' if (idade >= 30 and idade < 40) else
                            '40-49' if (idade >= 40 and idade < 50) else
                            '50-64' if (idade >= 50 and idade < 65) else
                            '65-74' if (idade >= 65 and idade < 75) else
                            '75-84' if (idade >= 75 and idade < 85) else
                            '85+'
                )

rw = rw.withColumn('idade', col('idade').cast(IntegerType())) \
    .withColumn('fx_etaria', fx_etaria(col('idade')))


rw = rw.withColumn('dt_min', lit('2021-01-17'))

primeira_dose_ou_unica = []

for coluna in rw.columns:
    if coluna == '1a dose' or coluna == 'dose inicial' or coluna == 'unica':
        primeira_dose_ou_unica.append(coluna)


for dose in primeira_dose_ou_unica:
    rw = rw.withColumn(
        dose,
        when(
            (split(col(dose), ',').getItem(0) <
            '2021-01-17'),
            concat_ws(
                ',',
                col('dt_min'),
                split(col(dose), ',').getItem(1)
            )
        ).otherwise(col(dose))
    )


for dose in ['1a dose', 'dose inicial']:
    rw = rw.withColumn(
        '2a dose',
        when(
            (split(col('2a dose'), ',').getItem(0)) <
            (split(col(dose), ',').getItem(0)),
            concat_ws(
                ',',
                add_months(split(col(dose), ',').getItem(0), 3),
                split(col('2a dose'), ',').getItem(1)
            )
        ).otherwise(col('2a dose'))
    )


# for dose in ['1a dose', 'dose inicial']:
#     rw = rw.withColumn(
#         dose,
#         when(
#             (col(dose).isNull()) &
#             (col('2a dose').isNotNull()),
#             concat_ws(
#                 ',',
#                 date_sub(split(col('2a dose'), ',').getItem(0), 90),
#                 split(col('2a dose'), ',').getItem(1)
#             )
#         ).otherwise(col(dose))
#     )


# QUEBRANDO CODIGO AQUI
# for dose in ['1a dose', 'dose inicial']:
#     rw = rw.withColumn(
#         '2a dose',
#         when(
#             (col('2a dose').isNull()),
#             when(
#                 (col('3a dose').isNotNull()) &
#                 (col(dose).like('%janssen%')),
#                 lit(None)
#             ) \
#             .when(
#                 (col('3a dose').isNotNull()) &
#                 (~col(dose).like('%janssen%')),
#                 concat_ws(
#                     ',',
#                     add_months(split(col(dose), ',').getItem(0), 3),
#                     split(col(dose), ',').getItem(1))
#             )
#         ).otherwise(col('2a dose'))
#     )


rw = rw.withColumn(
    'unica',
    when(
        (col('1a dose').like('%janssen%')),
        col('1a dose')
    ).when(
        (col('dose inicial').like('%janssen%')),
        col('dose inicial')
    ).otherwise(col('unica'))
)

for dose in ['1a dose', 'dose inicial']:
    rw = rw.withColumn(
        # '1a dose',
        dose,
        when(
            # (col('1a dose').like('%janssen%')),
            (col(dose).like('%janssen%')),
            lit(None)
        ).otherwise(col(dose))
    )

# rw = rw.withColumn(
#     'dose inicial',
#     when(
#         (col('dose inicial').like('%janssen%')),
#         lit(None)
#     ).otherwise(col('dose inicial'))
# )

# rw = rw.withColumn(
#     'dose adicional',
#     when(
#         (col('dose adicional').isNotNull()) &
#         (col('2a dose').isNotNull()),
#         when(
#             split(col('dose adicional'), ',').getItem(0) <
#             split(col('2a dose'), ',').getItem(0),
#             concat_ws(
#                 ',',
#                 add_months(
#                     split(col('2a dose'), ',').getItem(0), 3
#                 ),
#                 split(col('dose adicional'), ',').getItem(1))
#         ).otherwise(col('dose adicional'))
#     ).when(
#         (col('unica').isNotNull()) &
#         (col('dose adicional').isNotNull()),
#         when(
#             split(col('dose adicional'), ',').getItem(0) <
#             split(col('unica'), ',').getItem(0),
#             concat_ws(
#                 ',',
#                 add_months(
#                     split(col('unica'), ',').getItem(0), 9),
#                 split(col('dose adicional'), ',').getItem(1))
#         ).otherwise(col('dose adicional'))
#     ).otherwise(col('dose adicional'))
# )

# rw = rw.withColumn(
#     '1o reforco',
#     when(
#         (col('2a dose').isNotNull()) &
#         (col('1o reforco').isNotNull()),
#         when(
#             split(col('1o reforco'), ',').getItem(0) <
#             split(col('2a dose'), ',').getItem(0),
#             concat_ws(
#                 ',',
#                 add_months(
#                     split(col('2a dose'), ',').getItem(0), 3),
#                 split(col('1o reforco'), ',').getItem(1))
#         ).otherwise(col('1o reforco'))
#     ).when(
#         (col('unica').isNotNull()) &
#         (col('1o reforco').isNotNull()),
#         when(
#             split(col('1o reforco'), ',').getItem(0) <
#             split(col('unica'), ',').getItem(0),
#             concat_ws(
#                 ',',
#                 add_months(
#                     split(col('unica'), ',').getItem(0), 9),
#                 split(col('1o reforco'), ',').getItem(1))
#         ).otherwise(col('1o reforco'))
#     ).otherwise(col('1o reforco'))
# )


# rw = rw.withColumn(
#     'reforco',
#     when(
#         (col('2a dose').isNotNull()) &
#         (col('reforco').isNotNull()),
#         when(
#             split(col('reforco'), ',').getItem(0) <
#             split(col('2a dose'), ',').getItem(0),
#             concat_ws(
#                 ',',
#                 add_months(
#                     split(col('2a dose'), ',').getItem(0), 3),
#                 split(col('reforco'), ',').getItem(1))
#         ).otherwise(col('reforco'))
#     ).when(
#         (col('unica').isNotNull()) &
#         (col('reforco').isNotNull()),
#         when(
#             split(col('reforco'), ',').getItem(0) <
#             split(col('unica'), ',').getItem(0),
#             concat_ws(
#                 ',',
#                 add_months(
#                     split(col('unica'), ',').getItem(0), 9),
#                 split(col('reforco'), ',').getItem(1))
#         ).otherwise(col('reforco'))
#     ).otherwise(col('reforco'))
# )


# rw = rw.withColumn(
#     '3a dose',
#     when(
#         (col('2a dose').isNotNull()) &
#         (col('3a dose').isNotNull()),
#         when(
#             split(col('3a dose'), ',').getItem(0) <
#             split(col('2a dose'), ',').getItem(0),
#             concat_ws(
#                 ',',
#                 add_months(
#                     split(col('2a dose'), ',').getItem(0), 3),
#                 split(col('3a dose'), ',').getItem(1))
#         ).otherwise(col('3a dose'))
#     ).when(
#         (col('unica').isNotNull()) &
#         (col('3a dose').isNotNull()),
#         when(
#             split(col('3a dose'), ',').getItem(0) <
#             split(col('unica'), ',').getItem(0),
#             concat_ws(
#                 ',',
#                 add_months(
#                     split(col('unica'), ',').getItem(0), 9),
#                 split(col('3a dose'), ',').getItem(1))
#         ).otherwise(col('3a dose'))
#     ).otherwise(col('3a dose'))
# )

rw = rw.withColumn(
    'apenas_primeira_dose',
    when(
        (col('1a dose').isNotNull()) & (col('2a dose').isNull()),
        when(
            col('1a dose').isNotNull(),
            col('1a dose')
        ).otherwise(lit('999'))
    ).when(
        (col('unica').isNotNull()) & (col('2a dose').isNull()),
        when(
            col('dose inicial').isNotNull(),
            col('dose inicial')
        ).otherwise(lit('999'))
    )
)

rw = rw.withColumn(
    'imunizados_sem_booster',
    when(
        (col('1a dose').isNotNull()) &
        (col('2a dose').isNotNull()) &
        (col('reforco').isNull()) &
        (col('1o reforco').isNull()) &
        (col('dose adicional').isNull()) &
        (col('3a dose').isNull()),
        when(
            col('2a dose').isNotNull(),
            col('2a dose')
        ).otherwise(lit('999'))
    ).when(
        (col('dose inicial').isNotNull()) &
        (col('2a dose').isNotNull()) &
        (col('reforco').isNull()) &
        (col('1o reforco').isNull()) &
        (col('dose adicional').isNull()) &
        (col('3a dose').isNull()),
        when(
            col('dose inicial').isNotNull(),
            col('2a dose')
        ).otherwise(lit('999'))
    ).when(
        (col('unica').isNotNull()) &
        (col('reforco').isNull()) &
        (col('1o reforco').isNull()) &
        (col('dose adicional').isNull()) &
        (col('3a dose').isNull()),
        when(
            col('unica').isNotNull(),
            col('unica')
        ).otherwise(lit('999'))
    )
)


rw = rw.withColumn(
    'imunizados_com_booster',
    when(
        (col('1a dose').isNotNull()) &
        (col('2a dose').isNotNull()) &
        (col('reforco').isNotNull()),
        when(
            col('reforco').isNotNull(),
            col('reforco')
        ).otherwise(lit('999'))
    ).when(
        (col('dose inicial').isNotNull()) &
        (col('2a dose').isNotNull()) &
        (col('reforco').isNotNull()),
        when(
            col('reforco').isNotNull(),
            col('reforco')
        ).otherwise(lit('999'))
    ).when(
        col('unica').isNotNull() &
        col('reforco').isNotNull(),
        when(
            col('reforco').isNotNull(),
            col('reforco')
        ).otherwise(lit('999'))
    ).when(
        (col('1a dose').isNotNull()) &
        (col('2a dose').isNotNull()) &
        (col('3a dose').isNotNull()),
        when(
            col('3a dose').isNotNull(),
            col('3a dose')
        ).otherwise(lit('999'))
    ).when(
        (col('dose inicial').isNotNull()) &
        (col('2a dose').isNotNull()) &
        (col('3a dose').isNotNull()),
        when(
            col('3a dose').isNotNull(),
            col('3a dose')
        ).otherwise(lit('999'))
    ).when(
        col('unica').isNotNull() &
        col('3a dose').isNotNull(),
        when(
            col('3a dose').isNotNull(),
            col('3a dose')
        ).otherwise(lit('999'))
    ).when(
        (col('1a dose').isNotNull()) &
        (col('2a dose').isNotNull()) &
        (col('1o reforco').isNotNull()),
        when(
            col('1o reforco').isNotNull(),
            col('1o reforco')
        ).otherwise(lit('999'))
    ).when(
        (col('dose inicial').isNotNull()) &
        (col('2a dose').isNotNull()) &
        (col('1o reforco').isNotNull()),
        when(
            col('1o reforco').isNotNull(),
            col('1o reforco')
        ).otherwise(lit('999'))
    ).when(
        col('unica').isNotNull() &
        col('1o reforco').isNotNull(),
        when(
            col('1o reforco').isNotNull(),
            col('1o reforco')
        ).otherwise(lit('999'))
    ).when(
        (col('1a dose').isNotNull()) &
        (col('2a dose').isNotNull()) &
        (col('dose adicional').isNotNull()),
        when(
            col('dose adicional').isNotNull(),
            col('dose adicional')
        ).otherwise(lit('999'))
    ).when(
        (col('dose inicial').isNotNull()) &
        (col('2a dose').isNotNull()) &
        (col('dose adicional').isNotNull()),
        when(
            col('dose adicional').isNotNull(),
            col('dose adicional')
        ).otherwise(lit('999'))
    ).when(
        col('unica').isNotNull() &
        col('dose adicional').isNotNull(),
        when(
            col('dose adicional').isNotNull(),
            col('dose adicional')
        ).otherwise(lit('999'))
    )
)


rw = rw.withColumn('dt_pattern', lit('2021-11-15'))

rw = rw.withColumn(
    'i >= 6',
    when(
        col('apenas_primeira_dose').isNotNull(),
        when(months_between(
            col('dt_pattern'),
            split(col('apenas_primeira_dose'), ',').getItem(0)
        ) > 6, 1)
    )
)

rw = rw.withColumn(
    'i >= 2 < 6',
    when(
        col('apenas_primeira_dose').isNotNull(),
        when(
            (months_between(
                col('dt_pattern'),
                split(col('apenas_primeira_dose'), ',').getItem(0)) >= 2
             ) &
            (months_between(
                col('dt_pattern'),
                split(col('apenas_primeira_dose'), ',').getItem(0)) < 6
             ), 1)
    )
)


rw = rw.withColumn(
    'i < 2',
    when(
        col('apenas_primeira_dose').isNotNull(),
        when(
            months_between(
                col('dt_pattern'),
                split(col('apenas_primeira_dose'), ',').getItem(0)
            ) < 2, 1
        )
    )
)


rw = rw.withColumn(
    'c >= 6',
    when(
        col('imunizados_sem_booster').isNotNull(),
        when(
            months_between(
                col('dt_pattern'),
                split(col('imunizados_sem_booster'), ',').getItem(0)
            ) >= 6, 1
        )
    )
)

rw = rw.withColumn(
    'c >= 2 < 6',
    when(
        col('imunizados_sem_booster').isNotNull(),
        when(
            (months_between(
                col('dt_pattern'),
                split(col('imunizados_sem_booster'), ',').getItem(0)) >= 2
             ) &
            (months_between(
                col('dt_pattern'),
                split(col('imunizados_sem_booster'), ',').getItem(0)) < 6
             ), 1
        )
    )
)


rw = rw.withColumn(
    'c < 2',
    when(
        col('imunizados_sem_booster').isNotNull(),
        when(
            months_between(
                col('dt_pattern'),
                split(col('imunizados_sem_booster'), ',').getItem(0)
            ) < 2, 1
        )
    )
)

rw = rw.withColumn(
    'c + b >= 6',
    when(
        col('imunizados_com_booster').isNotNull(),
        when(
            (months_between(
                col('dt_pattern'),
                split(col('imunizados_com_booster'), ',').getItem(0)
            ) >= 6
             ), 1
        )
    )
)

rw = rw.withColumn(
    'c + b >= 2 < 6',
    when(
        col('imunizados_com_booster').isNotNull(),
        when(
            (months_between(
                col('dt_pattern'),
                split(col('imunizados_com_booster'), ',').getItem(0)) >= 2
             ) &
            (months_between(
                col('dt_pattern'),
                split(col('imunizados_com_booster'), ',').getItem(0)) < 6
             ), 1
        )
    )
)

rw = rw.withColumn(
    'c + b < 2',
    when(
        col('imunizados_com_booster').isNotNull(),
        when(
            (months_between(
                col('dt_pattern'),
                split(col('imunizados_com_booster'), ',').getItem(0)
            ) < 2), 1
        )
    )
)


# rw.groupby('sexo').count().show(truncate=False)
# rw.groupby('raca').count().show(truncate=False)
# rw.groupby('fx_etaria').count().show(truncate=False)
# 
# 
# rw.groupby('sexo') \
#     .agg(
#         count(col('apenas_primeira_dose')) \
#         .astype('int').alias('apenas_primeira_dose'),
# 
#         count(col('imunizados_sem_booster')) \
#         .astype('int').alias('imunizados_sem_booster'),
# 
#         count(col('imunizados_com_booster')) \
#         .astype('int').alias('imunizados_com_booster')
# 
#     ).show(truncate=False)
# 
# 
# rw.groupby('raca') \
#     .agg(
#         count(col('apenas_primeira_dose')) \
#         .astype('int').alias('apenas_primeira_dose'),
# 
#         count(col('imunizados_sem_booster')) \
#         .astype('int').alias('imunizados_sem_booster'),
# 
#         count(col('imunizados_com_booster')) \
#         .astype('int').alias('imunizados_com_booster')
# 
#     ).show(truncate=False)
# 
# 
# rw.groupby('fx_etaria') \
#     .agg(
#         count(col('apenas_primeira_dose')) \
#         .astype('int').alias('apenas_primeira_dose'),
# 
#         count(col('imunizados_sem_booster')) \
#         .astype('int').alias('imunizados_sem_booster'),
# 
#         count(col('imunizados_com_booster')) \
#         .astype('int').alias('imunizados_com_booster')
# 
#     ).show(truncate=False)


rw = rw.withColumn(
    'apenas_primeira_dose_vacina',
    when(
        col('apenas_primeira_dose').isNotNull(),
        split(col('apenas_primeira_dose'), ',').getItem(1)
    ).otherwise(lit('999'))

).withColumn(
   'imunizados_sem_booster_vacina',
    when(
        col('imunizados_sem_booster').isNotNull(),
        split(col('imunizados_sem_booster'), ',').getItem(1)
    ).otherwise(lit('999'))

).withColumn(
  'imunizados_com_booster_vacina',
    when(
        col('imunizados_com_booster').isNotNull(),
        split(col('imunizados_com_booster'), ',').getItem(1)
    ).otherwise(lit('999'))

)


# rw.agg(
#     count('i >= 6').astype('int').alias('i >= 6'),
#     count('i >= 2 < 6').astype('int').alias('i >= 2 < 6'),
#     count('i < 2').astype('int').alias('i < 2'),
# 
#     count('c >= 6').astype('int').alias('c >= 6'),
#     count('c >= 2 < 6').astype('int').alias('c >= 2 < 6'),
#     count('c < 2').astype('int').alias('c < 2'),
# 
#     count('c + b >= 6').astype('int').alias('c + b >= 6'),
#     count('c + b >= 2 < 6').astype('int').alias('c + b >= 2 < 6'),
#     count('c + b < 2').astype('int').alias('c + b < 2'),
# ).show(truncate=False)


# rw.groupby('apenas_primeira_dose_vacina').count().show(truncate=False)
rw.groupby('imunizados_sem_booster_vacina').count().show(truncate=False)
rw.groupby('imunizados_com_booster_vacina').count().show(truncate=False)
