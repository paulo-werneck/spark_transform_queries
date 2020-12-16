from datetime import date
from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark.sql import Window


df1 = spark.read.table('rw_corrwin_tscestciv')

df2 = df1.select(
    'COD_ESTADO_CIVIL',
    'DSC_ESTADO_CIVIL',
    'DAT_EXPORTACAO',
    F.date_trunc('day', 'DAT_EXPORTACAO').alias('DAT_EXPORTACAO_DATA'),
    F.row_number().over(
        Window.partitionBy('COD_ESTADO_CIVIL', F.date_trunc('day', 'DAT_EXPORTACAO')
                           ).orderBy(df1['DAT_EXPORTACAO'].desc())
    ).alias('NUM')
).filter('NUM = 1')

df3 = df2.select(
    'COD_ESTADO_CIVIL',
    'DSC_ESTADO_CIVIL',
    'DAT_EXPORTACAO_DATA',
    F.lag(df2['COD_ESTADO_CIVIL']).over(
        Window.orderBy('COD_ESTADO_CIVIL', 'DSC_ESTADO_CIVIL', 'DAT_EXPORTACAO')
    ).alias('LAG_COD_ESTADO_CIVIL'),
    F.lag(df2['DSC_ESTADO_CIVIL']).over(
        Window.orderBy('COD_ESTADO_CIVIL', 'DSC_ESTADO_CIVIL', 'DAT_EXPORTACAO')
    ).alias('LAG_DSC_ESTADO_CIVIL')
)

df4 = df3.withColumn(
    'DAT_INICIO_VIGENCIA',
    F.when(
        F.lag(df3['DAT_EXPORTACAO_DATA']).over(
            Window.partitionBy('COD_ESTADO_CIVIL').orderBy('DAT_EXPORTACAO_DATA')
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
            Window.partitionBy('COD_ESTADO_CIVIL').orderBy('DAT_EXPORTACAO_DATA')
        ).isNull(),
        F.lit(date(2199, 1, 1))
    ).otherwise(
        F.lead(df4['DAT_EXPORTACAO_DATA']).over(
            Window.partitionBy('COD_ESTADO_CIVIL').orderBy('DAT_EXPORTACAO_DATA')
        )
    )
)

df6 = df5.filter(
    """
    (COD_ESTADO_CIVIL != LAG_COD_ESTADO_CIVIL OR LAG_COD_ESTADO_CIVIL IS NULL) or
    (DSC_ESTADO_CIVIL != LAG_DSC_ESTADO_CIVIL OR LAG_DSC_ESTADO_CIVIL IS NULL)
    """
)
