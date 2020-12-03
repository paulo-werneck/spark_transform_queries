from pyspark.sql.window import Window
import pyspark.sql.functions as F
from datetime import date
from pyspark.sql import Window


dfa1 = spark.read.table('rw_staging_de_para_cliente_criptografia')
dfb1 = spark.read.table('rw_staging_de_para_cliente_criptografia_complemento')

dfa2 = dfa1.select(
    'COD_CLIENTE',
    'COD_CPF_CNPJ_GUID',
    F.row_number().over(
        Window.partitionBy('COD_CLIENTE', 'COD_CPF_CNPJ_GUID'
                           ).orderBy(dfa1['DAT_EXPORTACAO'].desc())
    ).alias('NUM')
).filter('NUM = 1')

dfb2 = dfb1.select(
    'COD_CLIENTE',
    'COD_CPF_CNPJ_GUID',
    F.row_number().over(
        Window.partitionBy('COD_CLIENTE', 'COD_CPF_CNPJ_GUID'
                           ).orderBy(dfb1['DAT_EXPORTACAO'].desc())
    ).alias('NUM')
).filter('NUM = 1')

df2 = dfa2.union(dfb2)

df3 = df2.select(
    'COD_CLIENTE',
    'COD_CPF_CNPJ_GUID'
).distinct()
