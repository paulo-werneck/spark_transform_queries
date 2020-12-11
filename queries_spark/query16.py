from datetime import date
from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark.sql import Window


df_classe_anbima = spark.read.table('rw_sql_msvc_fundos_cadastro_tb_classe_anbima')

df_classe_anbima2 = df_classe_anbima.withColumn(
    'DAT_EXPORTACAO_DATA',
    F.date_trunc('day', 'DAT_EXPORTACAO')
).withColumn(
    'NUM',
    F.row_number().over(
        Window.partitionBy('ID_CLASSE_ANBIMA', F.date_trunc('day', 'DAT_EXPORTACAO')
                           ).orderBy(df1['DAT_EXPORTACAO'].desc())
    )
).filter('NUM = 1')

df_classe_anbima3 = df_classe_anbima2.withColumn(
    'DAT_INICIO_VIGENCIA',
    F.when(
        F.lag(df_classe_anbima2['DAT_EXPORTACAO_DATA']).over(
            Window.partitionBy('ID_CLASSE_ANBIMA').orderBy('DAT_EXPORTACAO_DATA')
        ).isNull(),
        F.lit(date(1900, 1, 1))
    ).otherwise(
        F.col('DAT_EXPORTACAO_DATA')
    )
)

df_classe_anbima4 = df_classe_anbima3.withColumn(
    'DAT_FIM_VIGENCIA',
    F.when(
        F.lead(df_classe_anbima3['DAT_EXPORTACAO_DATA']).over(
            Window.partitionBy('ID_CLASSE_ANBIMA').orderBy('DAT_EXPORTACAO_DATA')
        ).isNull(),
        F.lit(date(2199, 1, 1))
    ).otherwise(
        F.lead(df_classe_anbima3['DAT_EXPORTACAO_DATA']).over(
            Window.partitionBy('ID_CLASSE_ANBIMA').orderBy('DAT_EXPORTACAO_DATA')
        )
    )
)

df_classe_anbima_final = df_classe_anbima4.withColumnRenamed("DAT_EXPORTACAO_DATA", "DT3")drop('NUM')

#################################################


df_categoria_anbima = spark.read.table('rw_sql_msvc_fundos_cadastro_tb_categoria_anbima')

df_categoria_anbima2 = df_categoria_anbima.withColumn(
    'DAT_EXPORTACAO_DATA',
    F.date_trunc('day', 'DAT_EXPORTACAO')
).withColumn(
    'NUM',
    F.row_number().over(
        Window.partitionBy('ID_CATEGORIA_ANBIMA', F.date_trunc('day', 'DAT_EXPORTACAO')
                           ).orderBy(df1['DAT_EXPORTACAO'].desc())
    )
).filter('NUM = 1')

df_categoria_anbima3 = df_categoria_anbima2.withColumn(
    'DAT_INICIO_VIGENCIA',
    F.when(
        F.lag(df_categoria_anbima2['DAT_EXPORTACAO_DATA']).over(
            Window.partitionBy('ID_CATEGORIA_ANBIMA').orderBy('DAT_EXPORTACAO_DATA')
        ).isNull(),
        F.lit(date(1900, 1, 1))
    ).otherwise(
        F.col('DAT_EXPORTACAO_DATA')
    )
)

df_categoria_anbima4 = df_categoria_anbima3.withColumn(
    'DAT_FIM_VIGENCIA',
    F.when(
        F.lead(df_categoria_anbima3['DAT_EXPORTACAO_DATA']).over(
            Window.partitionBy('ID_CATEGORIA_ANBIMA').orderBy('DAT_EXPORTACAO_DATA')
        ).isNull(),
        F.lit(date(2199, 1, 1))
    ).otherwise(
        F.lead(df_categoria_anbima3['DAT_EXPORTACAO_DATA']).over(
            Window.partitionBy('ID_CATEGORIA_ANBIMA').orderBy('DAT_EXPORTACAO_DATA')
        )
    )
)

df_categoria_anbima_final = df_categoria_anbima4.withColumnRenamed("DAT_EXPORTACAO_DATA", "DT2").drop('NUM')

#################################################


df_subcategoria_anbima = spark.read.table('rw_sql_msvc_fundos_cadastro_tb_subcategoria_anbima')

df_subcategoria_anbima2 = df_subcategoria_anbima.withColumn(
    'DAT_EXPORTACAO_DATA',
    F.date_trunc('day', 'DAT_EXPORTACAO')
).withColumn(
    'NUM',
    F.row_number().over(
        Window.partitionBy('ID_SUBCATEGORIA_ANBIMA', F.date_trunc('day', 'DAT_EXPORTACAO')
                           ).orderBy(df1['DAT_EXPORTACAO'].desc())
    )
).filter('NUM = 1')

df_subcategoria_anbima3 = df_subcategoria_anbima2.withColumn(
    'DAT_INICIO_VIGENCIA',
    F.when(
        F.lag(df_subcategoria_anbima2['DAT_EXPORTACAO_DATA']).over(
            Window.partitionBy('ID_SUBCATEGORIA_ANBIMA').orderBy('DAT_EXPORTACAO_DATA')
        ).isNull(),
        F.lit(date(1900, 1, 1))
    ).otherwise(
        F.col('DAT_EXPORTACAO_DATA')
    )
)

df_subcategoria_anbima4 = df_subcategoria_anbima3.withColumn(
    'DAT_FIM_VIGENCIA',
    F.when(
        F.lead(df_subcategoria_anbima3['DAT_EXPORTACAO_DATA']).over(
            Window.partitionBy('ID_SUBCATEGORIA_ANBIMA').orderBy('DAT_EXPORTACAO_DATA')
        ).isNull(),
        F.lit(date(2199, 1, 1))
    ).otherwise(
        F.lead(df_subcategoria_anbima3['DAT_EXPORTACAO_DATA']).over(
            Window.partitionBy('ID_SUBCATEGORIA_ANBIMA').orderBy('DAT_EXPORTACAO_DATA')
        )
    )
)

df_subcategoria_anbima_final = df_subcategoria_anbima4.drop('NUM')

#################################################


datas_df = df_subcategoria_anbima_final.join(
    df_categoria_anbima_final,
    df_subcategoria_anbima_final.CD_CATEGORIA_ANBIMA == df_categoria_anbima_final.ID_CATEGORIA_ANBIMA,
    how="left"
).join(
    df_classe_anbima_final,
    df_categoria_anbima_final.CD_CLASSE_ANBIMA == df_classe_anbima_final.ID_CLASSE_ANBIMA,
    how="left"
).select(
    'ID_SUBCATEGORIA_ANBIMA',
    'ID_CATEGORIA_ANBIMA',
    'ID_CLASSE_ANBIMA', 
    'DAT_EXPORTACAO_DATA',
    'DT2',
    'DT3'
)

datas_df1 = datas_df.select('ID_SUBCATEGORIA_ANBIMA','ID_CATEGORIA_ANBIMA','ID_CLASSE_ANBIMA','DAT_EXPORTACAO_DATA').distinct()
datas_df2 = datas_df.select('ID_SUBCATEGORIA_ANBIMA','ID_CATEGORIA_ANBIMA','ID_CLASSE_ANBIMA','DT2').distinct()
datas_df3 = datas_df.select('ID_SUBCATEGORIA_ANBIMA','ID_CATEGORIA_ANBIMA','ID_CLASSE_ANBIMA','DT3').distinct()

datas_df = datas_df1.union(datas_df2).union(datas_df3)
datas_df = datas_df.filter(
    F.col('DAT_EXPORTACAO_DATA').isNotNull()
).selectExpr(
    'ID_SUBCATEGORIA_ANBIMA as ID_SUBCATEGORIA_ANBIMA_2',
    'ID_CATEGORIA_ANBIMA as ID_CATEGORIA_ANBIMA_2',
    'ID_CLASSE_ANBIMA as ID_CLASSE_ANBIMA_2',
    'DAT_EXPORTACAO_DATA as DAT_EXPORTACAO_DATA_2'
).distinct()

#################################################


df_final1 = datas_df.join(
    df_subcategoria_anbima_final,
    [
        df_subcategoria_anbima_final.ID_SUBCATEGORIA_ANBIMA == datas_df.ID_SUBCATEGORIA_ANBIMA_2,
        df_subcategoria_anbima_final.DAT_INICIO_VIGENCIA <= datas_df.DAT_EXPORTACAO_DATA_2,
        datas_df.DAT_EXPORTACAO_DATA_2 < df_subcategoria_anbima_final.DAT_FIM_VIGENCIA
    ]
).join(
    df_categoria_anbima_final,
    [
        df_categoria_anbima_final.ID_CATEGORIA_ANBIMA == datas_df.ID_CATEGORIA_ANBIMA_2,
        df_categoria_anbima_final.DAT_INICIO_VIGENCIA <= datas_df.DAT_EXPORTACAO_DATA_2,
        datas_df.DAT_EXPORTACAO_DATA_2 < df_categoria_anbima_final.DAT_FIM_VIGENCIA
    ]
).join(
    df_classe_anbima_final,
    [
        df_classe_anbima_final.ID_CATEGORIA_ANBIMA == datas_df.ID_CATEGORIA_ANBIMA_2,
        df_classe_anbima_final.DAT_INICIO_VIGENCIA <= datas_df.DAT_EXPORTACAO_DATA_2,
        datas_df.DAT_EXPORTACAO_DATA_2 < df_classe_anbima_final.DAT_FIM_VIGENCIA
    ]
)

df_final2 = df_final1.selectExpr(
    'NM_CLASSE_ANBIMA',
    'NM_CATEGORIA_ANBIMA',
    'NM_SUBCATEGORIA_ANBIMA',
    'ID_SUBCATEGORIA_ANBIMA_2 as ID_SUBCATEGORIA_ANBIMA',
    'DAT_EXPORTACAO_DATA_2 as DAT_EXPORTACAO_DATA'
).distinct()

df4 = df4.withColumn(
    'DAT_INICIO_VIGENCIA',
    F.when(
        F.lag(df4['DAT_EXPORTACAO_DATA']).over(
            Window.partitionBy('ID_SUBCATEGORIA_ANBIMA').orderBy('DAT_EXPORTACAO_DATA')
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
            Window.partitionBy('ID_SUBCATEGORIA_ANBIMA').orderBy('DAT_EXPORTACAO_DATA')
        ).isNull(),
        F.lit(date(2199, 1, 1))
    ).otherwise(
        F.lead(df4['DAT_EXPORTACAO_DATA']).over(
            Window.partitionBy('ID_SUBCATEGORIA_ANBIMA').orderBy('DAT_EXPORTACAO_DATA')
        )
    )
)




