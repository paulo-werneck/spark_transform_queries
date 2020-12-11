from datetime import date
from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark.sql import Window


df1 = spark.read.table('rw_sql_msvc_fundos_cadastro_tb_fundo')

df2 = df1.withColumn(
    'DAT_EXPORTACAO_DATA',
    F.date_trunc('day', 'DAT_EXPORTACAO')
).withColumn(
    'NUM',
    F.row_number().over(
        Window.partitionBy('NR_CNPJ', F.date_trunc('day', 'DH_DATA_INSERCAO')
                           ).orderBy(df1['DH_DATA_INSERCAO'].desc())
    )
).filter('NUM = 1')

df3 = df2.withColumn(
    'DAT_INICIO_VIGENCIA',
    F.when(
        F.lag(df2['DAT_EXPORTACAO_DATA']).over(
            Window.partitionBy('NR_CNPJ').orderBy('DH_DATA_INSERCAO')
        ).isNull(),
        F.lit(date(1900, 1, 1))
    ).otherwise(
        F.col('DAT_EXPORTACAO_DATA')
    )
)

df4 = df3.withColumn(
    'DAT_FIM_VIGENCIA',
    F.when(
        F.lead(df3['DAT_EXPORTACAO_DATA']).over(
            Window.partitionBy('NR_CNPJ').orderBy('DH_DATA_INSERCAO')
        ).isNull(),
        F.lit(date(2199, 1, 1))
    ).otherwise(
        F.lead(df3['DAT_EXPORTACAO_DATA']).over(
            Window.partitionBy('NR_CNPJ').orderBy('DH_DATA_INSERCAO')
        )
    )
)
