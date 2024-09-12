import pandas as pd
# Llamamos funciones AA
from lakeanalytics import (
    read_parquet_gcp,
    write_parquet_gcp,
    list_files_gcp
)

# Listar objetos/folderes en GCP
list_files_gcp("main-aanalytics/lakehouse/silver/censos-encuestas/")

# Lectura simple de tabla
path_file = "main-aanalytics/lakehouse/silver/censos-encuestas/enaho/mod_07_gastos_alimentos_bebidas_modulo.meta_columns.parquet"
df = read_parquet_gcp(path_file)

# Escritura simple de tabla
path_file = "main-aanalytics/lakehouse/learning/keith.parquet"
write_parquet_gcp(df.head(),path_file)


