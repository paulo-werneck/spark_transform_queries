from pyspark.sql.window import Window
import pyspark.sql.functions as F
from datetime import date
from pyspark.sql import Window


spark.catalog.setCurrentDatabase('db')

df1 = spark.read.table('rw_tscclibol')

df2 = df1.withColumn(
    'DAT_EXPORTACAO_DATA',
    F.date_trunc('day', 'DAT_EXPORTACAO')
).withColumn(
    'NUM',
    F.row_number().over(
        Window.partitionBy('COD_CLIENTE', F.date_trunc('day', 'DAT_ATUALIZ')
                           ).orderBy('DAT_ATUALIZ')
    )
).filter('NUM = 1')

df3 = df2.withColumn(
    'LAG_DSC_CHAVE_REGISTRO',
    F.lag(df2['DSC_CHAVE_REGISTRO']).over(
        Window.orderBy('COD_CLIENTE', 'DAT_EXPORTACAO')
    )
)

df4 = df3.withColumn(
    'DAT_INICIO_VIGENCIA',
    F.when(
        F.lag(F.date_trunc('day', df3['DAT_CRIACAO'])).over(
            Window.partitionBy('COD_CLIENTE').orderBy('DAT_EXPORTACAO_DATA')
        ).isNull(),
        F.date_trunc('day', df3['DAT_CRIACAO'])
    ).otherwise(
        F.date_trunc('day', df3['DAT_ATUALIZ'])
    )
)

df5 = df4.withColumn(
    'DAT_FIM_VIGENCIA',
    F.when(
        F.lead(F.date_trunc('day', df4['DAT_ATUALIZ'])).over(
            Window.partitionBy('COD_CLIENTE').orderBy('DAT_ATUALIZ')
        ).isNull(),
        F.lit(date(2199, 1, 1))
    ).otherwise(
        F.lead(F.date_trunc('day', df4['DAT_ATUALIZ'])).over(
            Window.partitionBy('COD_CLIENTE').orderBy('DAT_ATUALIZ')
        )
    )
)

df6 = df5.filter(
    'DSC_CHAVE_REGISTRO != LAG_DSC_CHAVE_REGISTRO OR LAG_DSC_CHAVE_REGISTRO IS NULL'
).select(
  'COD_CLIENTE',
  'IND_DV_CLIENTE',
  'ANO_NASCIMENTO',
  'MES_NASCIMENTO',
  'IND_SITUACAO',
  'DAT_CRIACAO',
  'IND_CONTA_INV',
  'DAT_INATIVACAO',
  'COD_ASSESSOR',
  'DAT_EXPORTACAO_DATA',
  'DAT_INICIO_VIGENCIA',
  'DAT_FIM_VIGENCIA'
)