from datetime import date
from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark.sql import Window


df1 = spark.read.table('rw_sql_sysphera_planning_dt_fundo')

df2 = df1.select(
    'IDENTIFICADOR',
    F.row_number().over(
        Window.partitionBy('CNPJ').orderBy(df1['DAT_UPDATE'].desc())
    ).alias('NUM')
).filter(
    'NUM = 1'
)

df3 = df1.join(
    df2,
    on="IDENTIFICADOR",
    how="leftsemi"
)

df4 = df3.withColumn(
    'DAT_EXPORTACAO_DATA',
    F.date_trunc('day', 'DAT_EXPORTACAO')
).withColumn(
    'NUM',
    F.row_number().over(
        Window.partitionBy('CNPJ', F.date_trunc('day', 'DAT_EXPORTACAO')
                           ).orderBy('DAT_EXPORTACAO')
    )
).filter(F.col("CNPJ").isNotNull())


df4 = df4.withColumn(
    'DAT_INICIO_VIGENCIA',
    F.when(
        F.lag(df4['DAT_EXPORTACAO_DATA']).over(
            Window.partitionBy('CNPJ').orderBy('DAT_EXPORTACAO_DATA')
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
            Window.partitionBy('CNPJ').orderBy('DAT_EXPORTACAO_DATA')
        ).isNull(),
        F.lit(date(2199, 1, 1))
    ).otherwise(
        F.lead(df4['DAT_EXPORTACAO_DATA']).over(
            Window.partitionBy('CNPJ').orderBy('DAT_EXPORTACAO_DATA')
        )
    )
)

df6 = df5.filter("NUM = 1")