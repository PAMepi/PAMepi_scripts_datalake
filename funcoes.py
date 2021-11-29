#!/usr/bin/env python3

import pyspark.sql.functions as F
import pyspark.sql.types as T


def converte_data(dataframe, coluna):
    if isinstance(coluna, list):
        for col in coluna:
            dataframe = dataframe.withColumn(
                col, F.when(F.to_date(F.col(col), 'dd/MM/yyyy').isNotNull(),
                               F.to_date(F.col(col), 'dd/MM/yyyy')) \
                .when(F.to_date(F.col(col), 'dd-MM-yyyy').isNotNull(),
                              F.to_date(F.col(col), 'dd-MM-yyyy')) \
                                .otherwise(F.col(col))
            ).withColumn(col, F.col(col).cast(T.DateType()))

    elif isinstance(coluna, str):
        dataframe = dataframe.withColumn(
            coluna, F.when(F.to_date(F.col(coluna), 'dd/MM/yyyy') \
                           .isNotNull(), F.to_date(F.col(coluna),
                                                   'dd/MM/yyyy')) \
            .when(F.to_date(F.col(coluna), 'dd-MM-yyyy').isNotNull(),
                  F.to_date(F.col(coluna), 'dd-MM-yyyy')
        ).otherwise(F.col(coluna))) \
            .withColumn(coluna, F.col(coluna).cast(T.DateType()))

    return dataframe


def padroniza_texto(dataframe, coluna):
    @F.udf(returnType=T.StringType())
    def _(texto):
        try:
            return texto.lower().replace(u'\xa0', ' ').replace('á', 'a') \
                .replace('à', 'a').replace('ã', 'a').replace('â', 'a') \
                .replace('é', 'e').replace('è', 'e').replace('ẽ', 'e') \
                .replace('ê', 'e').replace('í', 'i').replace('ì', 'i') \
                .replace('ĩ', 'i').replace('î', 'i').replace('ó', 'o') \
                .replace('ò', 'o').replace('õ', 'o').replace('ô', 'o') \
                .replace('ú', 'u').replace('ù', 'u').replace('ũ', 'u') \
                .replace('û', 'u').replace('ç', 'c').lstrip(' ').rstrip(' ')
        except AttributeError:
            return None

    if isinstance(coluna, str):
        dataframe = dataframe.withColumn(coluna, _(coluna))

    elif isinstance(coluna, list):
        for col in coluna:
            dataframe = dataframe.withColumn(col, _(col))

    return dataframe


def cria_faixa_etaria(dataframe, coluna):

    IDADE = {
        '0': 4, '1': 4, '2': 4, '3': 4, '4': 4, '5': 509, '6': 509,
        '7': 509, '8': 509, '9': 509, '10': 1014, '11': 1014, '12': 1014,
        '13': 1014, '14': 1014, '15': 1519, '16': 1519, '17': 1519,
        '18': 1519, '19': 1519, '20': 2024, '21': 2024, '22': 2024,
        '23': 2024, '24': 2024, '25': 2529, '26': 2529, '27': 2529,
        '28': 2529, '29': 2529, '30': 3034, '31': 3034, '32': 3034,
        '33': 3034, '34': 3034, '35': 3539, '36': 3539, '37': 3539,
        '38': 3539, '39': 3539, '40': 4044, '41': 4044, '42': 4044,
        '43': 4044, '44': 4044, '45': 4549, '46': 4549, '47': 4549,
        '48': 4549, '49': 4549, '50': 5054, '51': 5054, '52': 5054,
        '53': 5054, '54': 5054, '55': 5559, '56': 5559, '57': 5559,
        '58': 5559, '59': 5559, '60': 6064, '61': 6064, '62': 6064,
        '63': 6064, '64': 6064, '65': 6569, '66': 6569, '67': 6569,
        '68': 6569, '69': 6569, '70': 7074, '71': 7074, '72': 7074,
        '73': 7074, '74': 7074, '75': 8099, '76': 8099, '77': 8099,
        '78': 8099, '79': 8099, '80': 8099, '81': 8099, '82': 8099,
        '83': 8099, '84': 8099, '85': 8099, '86': 8099, '87': 8099,
        '88': 8099, '89': 8099, '90': 8099, '91': 8099, '92': 8099,
        '93': 8099, '94': 8099, '95': 8099, '96': 8099, '97': 8099,
        '98': 8099, '99': 8099, '100': 8099,
    }

    @F.udf(returnType=T.IntegerType())
    def _(coluna):
        if coluna in IDADE.keys():
            return IDADE[coluna]
        else:
            return 99

    dataframe = dataframe.withColumn('age_group', _(F.col(coluna)))

    return dataframe


def correcao_de_idade(dataframe, coluna_idade, coluna_data_nascimento):
    dataframe = dataframe.withColumn(
        coluna_idade,
        F.when(F.col(coluna_idade).isNull(),
               F.floor(F.datediff(F.current_date(),
                                  coluna_data_nascimento)/365.25)) \
    .otherwise(F.col(coluna_idade)))

    return dataframe


def converte_sexo(dataframe, coluna_sexo):
    dataframe = dataframe.withColumn(
        coluna_sexo,
        F.when(F.col(coluna_sexo).startswith("f"), 1) \
        .when(F.col(coluna_sexo).startswith("m"), 2) \
        .when((F.col(coluna_sexo).startswith("i")) |
              (F.col(coluna_sexo).startswith("u")), 99))

    return dataframe


def rename_columns(dataframe, coluna):
    for col in dataframe.columns:
        dataframe = dataframe.withColumnRenamed(col, col.lower())

    for old, new in coluna.items():
        dataframe = dataframe.withColumnRenamed(old, new)
    return dataframe


def union_all(dfs):
    cols = set()

    for df in dfs:
        for col in df.columns:
            cols.add(col)

    cols = sorted(cols)

    for col in cols:
        for i in range(len(dfs)):
            if col not in dfs[i].columns:
                dfs[i] = dfs[i].withColumn(col, F.lit(''))

    df = reduce(lambda df1, df2: df1.union(df2), dfs)

    return df
