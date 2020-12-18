from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark.sql import Window


df1 = spark.read.table('rw_sql_msvc_fundos_cadastro_tb_categoria_previdencia')

df2 = df1.withColumn(
    'DATA_INI',
    F.when(
        F.lag(df1['DAT_EXPORTACAO']).over(
            Window.partitionBy('ID_CATEGORIA_PREVIDENCIA').orderBy('ID_CATEGORIA_PREVIDENCIA', 'DAT_EXPORTACAO')
        ).isNull(),
        F.lit('1900-01-01')
    ).otherwise(
        F.lag(df1['DAT_EXPORTACAO']).over(
            Window.partitionBy('ID_CATEGORIA_PREVIDENCIA').orderBy('ID_CATEGORIA_PREVIDENCIA', 'DAT_EXPORTACAO')
        )
    )
)

df3 = df2.withColumn(
    'DATA_FIM',
    F.when(
        F.lead(df2['DAT_EXPORTACAO']).over(
            Window.partitionBy('ID_CATEGORIA_PREVIDENCIA').orderBy('ID_CATEGORIA_PREVIDENCIA', 'DAT_EXPORTACAO')
        ).isNull(),
        F.lit('9999-01-01')
    ).otherwise(
        F.lead(df2['DAT_EXPORTACAO']).over(
            Window.partitionBy('ID_CATEGORIA_PREVIDENCIA').orderBy('ID_CATEGORIA_PREVIDENCIA', 'DAT_EXPORTACAO')
        )
    )
)

df4 = df3.select(
    F.col("ID_CATEGORIA_PREVIDENCIA"),
    F.col("NM_CATEGORIA_PREVIDENCIA"),
    F.col("DAT_EXPORTACAO"),
    F.when(
        F.col("DATA_INI") == '1900-01-01',
        F.col("DATA_INI")
    ).otherwise(
        F.col("DAT_EXPORTACAO")
    ).alias("DAT_INICIO_VIGENCIA"),
    F.col("DATA_FIM").alias("DAT_FIM_VIGENCIA")
)