from datetime import date
from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark.sql import Window


df1a = spark.read.table('rw_sql_alphatools_xpa_funds_basefund')
df1b = spark.read.table('rw_sql_alphatools_funds_fundgroupmembership')

df2 = df1a.join(
    df1b, df1a.ID == df1b.FUND_ID
).filter(
    "GROUP_ID = 20"
).drop(df1b["DAT_EXPORTACAO"])

df3 = df2.select(
    'ID',
    'NAME',
    'CLASSIFICATION',
    'LEGAL_ID',
    'LEGAL_NAME',
    'IS_INTERNAL',
    'START_DATE',
    F.date_trunc('day', 'DAT_EXPORTACAO').alias('DAT_EXPORTACAO_DATA'),
    F.row_number().over(
        Window.partitionBy(F.date_trunc('day', 'DAT_EXPORTACAO'), 'ID',
                           ).orderBy(df2['DAT_EXPORTACAO'].desc())
    ).alias('NUM')
).filter('NUM = 1')

df4 = df3.withColumn(
    'DAT_INICIO_VIGENCIA',
    F.when(
        F.lag(df3['DAT_EXPORTACAO_DATA']).over(
            Window.partitionBy('ID').orderBy('DAT_EXPORTACAO_DATA')
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
            Window.partitionBy('ID').orderBy('DAT_EXPORTACAO_DATA')
        ).isNull(),
        F.lit(date(2199, 1, 1))
    ).otherwise(
        F.lead(df4['DAT_EXPORTACAO_DATA']).over(
            Window.partitionBy('ID').orderBy('DAT_EXPORTACAO_DATA')
        )
    )
)