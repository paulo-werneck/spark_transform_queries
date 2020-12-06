from pyspark.sql.window import Window
import pyspark.sql.functions as F
from datetime import date
from pyspark.sql import Window


df1 = spark.read.table('tscsfp')

df2 = df1.groupBy(
    'DAT_TIMESTAMP', 
    'COD_CLIENTE',
    'COD_SFP_GRUPO'
).agg(F.sum(df1['VAL_BEN']).alias('VAL_BEN'))

df3 = df2.withColumn(
    'DAT_INICIO_VIGENCIA',
    F.when(
        F.lag(df2['DAT_TIMESTAMP']).over(
            Window.partitionBy('COD_CLIENTE', 'COD_SFP_GRUPO').orderBy('DAT_TIMESTAMP')
        ).isNull(),
        F.lit(date(1900, 1, 1))
    ).otherwise(
        F.col('DAT_TIMESTAMP')
    )
)

df4 = df3.withColumn(
    'DAT_FIM_VIGENCIA',
    F.when(
        F.lead(df3['DAT_TIMESTAMP']).over(
            Window.partitionBy('COD_CLIENTE', 'COD_SFP_GRUPO').orderBy('DAT_TIMESTAMP')
        ).isNull(),
        F.lit(date(2199, 1, 1))
    ).otherwise(
        F.lead(df3['DAT_TIMESTAMP']).over(
            Window.partitionBy('COD_CLIENTE', 'COD_SFP_GRUPO').orderBy('DAT_TIMESTAMP')
        )
    )
)