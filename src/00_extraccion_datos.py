import pandas as pd
# Llamamos funciones AA
from lakeanalytics import (
    read_parquet_gcp,
    write_parquet_gcp,
    list_files_gcp,
    generar_engine_aa
)

# Listar objetos/folderes en GCP
list_files_gcp("main-aanalytics/lakehouse/silver/censos-encuestas/")

# Lectura simple de tabla
path_file = "main-aanalytics/lakehouse/silver/censos-encuestas/enaho/mod_07_gastos_alimentos_bebidas_modulo.meta_columns.parquet"
df = read_parquet_gcp(path_file)

# Escritura simple de tabla
path_file = "main-aanalytics/lakehouse/learning/keith.parquet"
write_parquet_gcp(df.head(),path_file)


# Consulta a PostgreSQL en GCP
engine_apoyo = generar_engine_aa()

sql_query = """
select * from viva_leads.tb_celulares
limit 10
"""
consulta_sql = pd.read_sql(sql_query,con=engine_apoyo)
