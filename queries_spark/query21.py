from datetime import date
from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark.sql import Window


df1 = spark.read.table('rw_virtual_opemissor')

df2 = df1.withColumn(
    'DAT_EXPORTACAO_DATA',
    F.date_trunc('day', 'DAT_EXPORTACAO')
).withColumn(
    'NUM',
    F.row_number().over(

        Window.partitionBy('FC_CODEMISSOR', F.date_trunc('day', 'DAT_EXPORTACAO')
                           ).orderBy(df1['DAT_EXPORTACAO'].desc())
    )
).filter('NUM = 1')

df3 = df2.withColumn(
    'LAG_DSC_CHAVE_REGISTRO',
    F.lag(df2['DSC_CHAVE_REGISTRO']).over(
        Window.orderBy('FC_CODEMISSOR', 'DAT_EXPORTACAO')
    )
)

df4 = df3.withColumn(
    'DAT_INICIO_VIGENCIA',
    F.when(
        F.lag(df3['DAT_EXPORTACAO_DATA']).over(
            Window.partitionBy('FC_CODEMISSOR').orderBy('DAT_EXPORTACAO_DATA')
        ).isNull(),
        F.lit(date(1900, 1, 1))
    ).otherwise(
        F.col('DAT_EXPORTACAO_DATA')
    )
)

df5 = df4.withColumn(
    'DAT_FIM_VIGENCIA',
    F.when(
        F.lead(df4['DAT_EXPORTACAO_DATA']).over(
            Window.partitionBy('FC_CODEMISSOR').orderBy('DAT_EXPORTACAO_DATA')
        ).isNull(),
        F.lit(date(2199, 1, 1))
    ).otherwise(
        F.lead(df4['DAT_EXPORTACAO_DATA']).over(
            Window.partitionBy('FC_CODEMISSOR').orderBy('DAT_EXPORTACAO_DATA')
        )
    )
)

df6 = df5.filter(
    'DSC_CHAVE_REGISTRO != LAG_DSC_CHAVE_REGISTRO or LAG_DSC_CHAVE_REGISTRO IS NULL'
)
