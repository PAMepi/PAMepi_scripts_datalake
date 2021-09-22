# carregando bibliotecas
import os
import re
import findspark # caso o pyspark ja esteja setado nas variaveis de ambiente
findspark.init() # localiza e adiciona o pyspark ao editor/interpretador

from pyspark.sql import Window, SparkSession
import pyspark.sql.types as T
import pyspark.sql.functions as F

from funcoes import union_all


# criando sessao 
spark = SparkSession.builder.appName('sg').getOrCreate()

# setando local dos arquivos
sg_folder = os.path.join(os.path.dirname(__name__), 'sindrome_gripal_bruto')

# carregando como lista todos os arquivos
dfs = [
    spark.read.csv(os.path.join(sg_folder, x),
                   header=True, sep=';')
    for x in os.listdir(sg_folder)
]

# concatenando a lista e preenchendo dados faltantes de colunas
df = union_all(dfs)

# dicionario para renomear as colunas de interesse
rnm_columns = {
    'classificacaoFinal': 'class_fin',
    'condicoes': 'cond',
    'dataEncerramento': 'dt_enc',
    'dataInicioSintomas': 'dt_in_sint',
    'dataNascimento': 'dt_nasc',
    'dataNotificacao': 'dt_ntf',
    'dataTeste': 'dt_teste',
    'estado': 'est',
    'estadoIBGE': 'est_ibge',
    'estadoNotificacao': 'est_ntf',
    'estadoNotificacaoIBGE': 'est_ntf_ibge',
    'estadoTeste': 'est_teste',
    'evolucaoCaso': 'ev_caso',
    'municipio': 'mun',
    'municipioIBGE': 'mun_ibge',
    'municipioNotificacao': 'mun_ntf',
    'municipioNotificacaoIBGE': 'mun_ntf_ibge',
    'resultadoTeste': 'res_teste',
    'sintomas': 'sint',
    'tipoTeste': 'tp_teste',
    'profissionalSaude': 'prof_saude'
}

# renomeando colunas
for old, new in rnm_columns.items():
    df = df.withColumnRenamed(old, new)

# convertado variavies de data em tipo proprio para data
# passando tipo que nao sao data para caixa baixa
for col in df.columns:
    if col.startswith('dt'):
        df = df.withColumn(col, df[col].cast(T.DateType()))
    else:
        df = df.withColumn(col, F.lower(col))

# segue algumas conversoes
df = df.withColumn(
    'sexo', F.when(df['sexo'].startswith('f'), 1) \
    .when(df['sexo'].startswith('m'), 2) \
    .when((df['sexo'] == 'indefinido') | (df['sexo'] == 'undefined'), 9) \
    .otherwise(df['sexo'])
)

df = df.withColumn(
    'tp_teste', F.when(df['tp_teste'] == 'rt-pcr', 1) \
    .when((df['tp_teste'].startswith('quimioluminesc')) |
          (df['tp_teste'].startswith('imunoensaio por eletroquimioluminesc')),
          2) \
    .when(df['tp_teste'] == 'teste rápido - antígeno', 3) \
    .when(df['tp_teste'] == 'teste rápido - anticorpo', 4) \
    .when((df['tp_teste'] == 'concluido') |
          (df['tp_teste'] == 'concluído'), 5) \
    .when(df['tp_teste'].startswith('enzimaimunoensaio'), 6) \
    .when(df['tp_teste'] == 'null', None) \
    .when((df['tp_teste'] == 'undefined') |
          (df['tp_teste'] == 'indefinido'), 9) \
    .otherwise(df['tp_teste'])
)

df = df.withColumn(
    'res_teste', F.when(df['res_teste'] == 'null', None) \
    .when(df['res_teste'] == 'negativo', 2) \
    .when(df['res_teste'] == 'positivo', 1) \
    .when((df['res_teste'] == 'undefined') |
          (df['res_teste'] == 'indeterminado') |
          (df['res_teste'] == 'inconclusivo ou indeterminado') |
          (df['res_teste'] == 'inconclusivo'), 9) \
    .when(df['res_teste'] == 'null', None) \
    .otherwise(df['res_teste'])
)

df = df.withColumn(
    'class_fin', F.when(df['class_fin'] == 'confirmado', 1) \
    .when(df['class_fin'] == 'descartado', 2) \
    .when((df['class_fin'] == 'confirmado laboratorial') |
          (df['class_fin'] == 'confirmação laboratorial'), 3) \
    .when((df['class_fin'] == 'confirmado clínico-imagem') |
          (df['class_fin'] == 'confirmado clinico-imagem'), 4) \
    .when((df['class_fin'] == 'confirmação clínico epidemiológico') |
          (df['class_fin'] == 'confirmado clinico-epidemiologico') |
          (df['class_fin'] == 'confirmado clínico-epidemiológico'), 5) \
    .when((df['class_fin'] == 'sindrome gripal nao especificada') |
          (df['class_fin'] == 'síndrome gripal não especificada'), 6) \
    .when(df['class_fin'] == 'confirmado por critério clínico', 7) \
    .when(df['class_fin'] == 'null', None) \
    .otherwise(df['class_fin'])
)

df = df.withColumn(
    'ev_caso', F.when((df['ev_caso'] == 'óbito') |
                      (df['ev_caso'] == 'obito'), 1) \

    .when(df['ev_caso'] == 'cura', 2) \

    .when(df['ev_caso'] == 'internado em uti', 3) \

    .when(df['ev_caso'] == 'cancelado', 4) \

    .when(df['ev_caso'] == 'em tratamento domiciliar', 5) \

    .when(df['ev_caso'] == 'internado', 5) \

    .when(df['ev_caso'] == 'ignorado', 9) \

    .otherwise(df['ev_caso'])
)

df = df.withColumn(
    'est_teste', F.when(df['est_teste'] == 'solicitado', 1) \
    .when(df['est_teste'] == 'coletado', 2) \
    .when((df['est_teste'] == 'concluido') |
          (df['est_teste'] == 'concluído'), 3) \
    .when((df['est_teste'] == 'exame nao solicitado') |
          (df['est_teste'] == 'exame não solicitado'), 4) \
    .when((df['est_teste'] == 'undefined') |
          (df['est_teste'] == 'indefinido'), 9) \
    .when(df['est_teste'] == 'null', None) \
    .otherwise(df['est_teste'])
)

df = df.withColumn(
    'idade',
    F.when(((df['idade'].isNull()) |
            (F.isnan(df['idade'])) |
            (df['idade'] > 110) |
            (df['idade'] < 0)) &
           ((F.year(df['dt_nasc']) > 1911) &
            (F.year(df['dt_nasc']) < 2021)),
           F.year(F.current_date() - F.year(df['dt_nasc']))) \
    .otherwise(df['idade'])
)

df = df.withColumn(
    'dt_nasc_qc',
    F.when(
        ((df['dt_nasc'].isNull())  | (F.year(df['dt_nasc']) < 1920)) &
            (df['idade'] > 0) & (df['idade'] < 110),
            F.lit(2021 - df['idade'])) \
    .otherwise(F.year(df['dt_nasc']))
)

# dicionario para faixa etaria
var_fx = {
    '0': 4, '1': 4, '2': 4, '3': 4, '4': 4, '5': 509, '6': 509, '7': 509,
    '8': 509, '9': 509, '10': 1014, '11': 1014, '12': 1014, '13': 1014,
    '14': 1014, '15': 1519, '16': 1519, '17': 1519, '18': 1519, '19': 1519,
    '20': 2024, '21': 2024, '22': 2024, '23': 2024, '24': 2024, '25': 2529,
    '26': 2529, '27': 2529, '28': 2529, '29': 2529, '30': 3034, '31': 3034,
    '32': 3034, '33': 3034, '34': 3034, '35': 3539, '36': 3539, '37': 3539,
    '38': 3539, '39': 3539, '40': 4044, '41': 4044, '42': 4044, '43': 4044,
    '44': 4044, '45': 4549, '46': 4549, '47': 4549, '48': 4549, '49': 4549,
    '50': 5054, '51': 5054, '52': 5054, '53': 5054, '54': 5054, '55': 5559,
    '56': 5559, '57': 5559, '58': 5559, '59': 5559, '60': 6064, '61': 6064,
    '62': 6064, '63': 6064, '64': 6064, '65': 6569, '66': 6569, '67': 6569,
    '68': 6569, '69': 6569, '70': 7074, '71': 7074, '72': 7074, '73': 7074,
    '74': 7074, '75': 8099, '76': 8099, '77': 8099, '78': 8099, '79': 8099,
    '80': 8099, '81': 8099, '82': 8099, '83': 8099, '84': 8099, '85': 8099,
    '86': 8099, '87': 8099, '88': 8099, '89': 8099, '90': 8099, '91': 8099,
    '92': 8099, '93': 8099, '94': 8099, '95': 8099, '96': 8099, '97': 8099,
    '98': 8099, '99': 8099, '100': 8099, '101': 8099, '102': 8099, '103': 8099,
    '104': 8099, '105': 8099, '106': 8099, '107': 8099, '108': 8099,
    '109': 8099, '110': 8099
}

# funcao que retorna a faixa se o valor da coluna utilizada estiver dentro do
# dicionario para faixa etaria
def return_fx(age):
    try:
        return var_fx[age]
    except KeyError:
        return None

return_age_udf = F.udf(return_fx, T.IntegerType())

df = df.withColumn('fx_et', return_age_udf(df['idade']))

# tentar salvar em parquet pode ser inviavel dependendo da maquina
# mas seria o melhor formato para fazer consultadas posteriormente
# neste caso foi usado csv devido as configuracoes da minha maquina
df.write.csv('new_data_sg', header=True, sep=',')
