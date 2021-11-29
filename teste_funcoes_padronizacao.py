#!/usr/bin/env python3

import datetime
from unittest import TestCase, main
from pyspark.sql import SparkSession
import pyspark.sql.types as T

from funcoes import converte_data, padroniza_texto


class TestaFuncoesPAMepi(TestCase):
    spark = SparkSession.builder.appName('test').getOrCreate()

    valores = [
        ('04/07/1994', 'Fábio', '1ª Dose Revacinação'),
        ('25-08-1991', 'Iuri', u'\xa02ª Dose'),
        ('02/02/2020', 'Çeleste', u'3ª\xa0Dose'),
        ('2020-01-18', 'Çristiano', 'dose 1'),
        (None, None, None)
    ]

    colunas = ['dataNascimento', 'Nome', 'Dose']

    dataset = spark.createDataFrame(valores).toDF(*colunas)

    def testa_se_encontra_string_no_campo_data_e_converte_no_tipo_data(self):
        resultado_esperado = [
            T.Row(dataNascimento = datetime.date(1994, 7, 4)),
            T.Row(dataNascimento = datetime.date(1991, 8, 25)),
            T.Row(dataNascimento = datetime.date(2020, 2, 2)),
            T.Row(dataNascimento = datetime.date(2020, 1, 18)),
            T.Row(dataNascimento = None),
        ]

        resultado_teste = converte_data(
            self.dataset.select('dataNascimento'), 'dataNascimento'
        )

        resultado_coletado = resultado_teste.collect()

        self.assertEquals(resultado_esperado, resultado_coletado)

    def testa_remocao_de_caracteres_no_texto(self):
        resultado_esperado = [
            T.Row(Nome = 'fabio'),
            T.Row(Nome = 'iuri'),
            T.Row(Nome = 'celeste'),
            T.Row(Nome = 'cristiano'),
            T.Row(Nome = None),
        ]

        resultado_teste = padroniza_texto(
            self.dataset.select('Nome'), 'Nome'
        )

        resultado_coletado = resultado_teste.collect()

        self.assertEquals(resultado_esperado, resultado_coletado)

    def testa_padronizacao_das_doses(self):
        resultado_esperado = [
            T.Row(Dose = '1ª dose revacinacao'),
            T.Row(Dose = '2ª dose'),
            T.Row(Dose = '3ª dose'),
            T.Row(Dose = 'dose 1'),
            T.Row(Dose = None),
        ]

        resultado_teste = padroniza_texto(
            self.dataset.select('Dose'), 'Dose'
        )

        resultado_coletado = resultado_teste.collect()

        self.assertEquals(resultado_esperado, resultado_coletado)



if __name__ == '__main__':
    main()
