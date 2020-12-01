from pyspark.sql.window import Window
import pyspark.sql.functions as F
from datetime import date
from pyspark.sql import Window


df1 = spark.read.table('rw_carteira_administrada_carteira')

df2 = df1.withColumn(
    'DAT_EXPORTACAO_DATA',
    F.date_trunc('day', 'DAT_EXPORTACAO')
).withColumn(
    'NUM',
    F.row_number().over(
        Window.partitionBy('COD_CLIENTE', F.date_trunc('day', 'DAT_EXPORTACAO')
                           ).orderBy(df1['DAT_EXPORTACAO'].desc())
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
        F.lag(df3['DAT_INC']).over(
            Window.partitionBy('COD_CLIENTE').orderBy('DAT_INC')
        ).isNull(),
        F.date_trunc('day', df3['DAT_INC']).cast('date')
    ).otherwise(
        F.date_trunc('day', df3['DAT_ALT']).cast('date')
    )
)

df5 = df4.withColumn(
    'DAT_FIM_VIGENCIA',
    F.when(
        F.lead(F.date_trunc('day', df4['DAT_ALT'])).over(
            Window.partitionBy('COD_CLIENTE').orderBy('DAT_ALT')
        ).isNull(),
        F.lit(date(2199, 1, 1))
    ).otherwise(
        F.date_trunc('day', F.lead(df4['DAT_ALT']).over(
            Window.partitionBy('COD_CLIENTE').orderBy('DAT_ALT')
        ).cast('date'))
    )
)

df6 = df5.filter(
    'DSC_CHAVE_REGISTRO != LAG_DSC_CHAVE_REGISTRO or LAG_DSC_CHAVE_REGISTRO IS NULL'
)
