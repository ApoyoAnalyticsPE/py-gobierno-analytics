from google.cloud import storage
import pandas as pd
import os
from io import BytesIO
import unicodedata
import re

os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = "C:/proyectos/creds/credenciales_lakehouse.json"


def read_parquet_gcp(gcp_path:str)->pd.DataFrame:
    elements = gcp_path.split("/")
    bucket_name = elements[0]
    prefix = '/'.join(gcp_path.split("/")[1:])
    cli = storage.Client()
    bucket = cli.bucket(bucket_name)
    blob_file = bucket.get_blob(prefix).download_as_bytes()
    res = pd.read_parquet(BytesIO(blob_file))
    return res

def list_files_gcp(gcp_path:str,pattern=None) -> list:
    elements = gcp_path.split("/")
    bucket_name = elements[0]
    prefix = '/'.join(gcp_path.split("/")[1:])
    client = storage.Client()
    bucket = client.get_bucket(bucket_name)
    blobs = bucket.list_blobs(prefix=prefix)
    result = []
    for blob in blobs:
        result.append(blob.name)
    return result

def write_parquet_gcp(file,gcp_path):
    elements = gcp_path.split("/")
    bucket_name = elements[0]
    prefix = '/'.join(gcp_path.split("/")[1:])
    print(bucket_name)
    print(prefix)
    cli = storage.Client()
    bucket = cli.bucket(bucket_name)
    tabla_buffer =  BytesIO()
    file.to_parquet(tabla_buffer,engine='pyarrow')
    tabla_buffer.seek(0)
    bucket.blob(prefix).upload_from_file(tabla_buffer)


def list_remote_files(kwd,gcp_path):
    elements = gcp_path.split("/")
    bucket_name = elements[0]
    cli = storage.Client()
    bucket = cli.bucket(bucket_name) 
    prefix = '/'.join(gcp_path.split("/")[1:])
    res = list(bucket.list_blobs(prefix =prefix))
    return [path.name for path in res if kwd in path.name]


def clean_string(txt):
    """
    Normalizamos los textos
    """
    txt = str(txt).strip()
    txt = unicodedata.normalize('NFKD', txt).encode('ASCII', 'ignore').decode()

    txt = re.sub('[^0-9a-zA-Z&_.]+', ' ', txt)
    txt = re.sub(' +', ' ',txt)
    txt = txt.strip().lower().replace(" ",'_')
    return txt

import pandas as pd

def clean_string(txt):
    """
    Normalizamos los textos
    """
    txt = str(txt).strip()
    txt = unicodedata.normalize('NFKD', txt).encode('ASCII', 'ignore').decode()

    txt = re.sub('[^0-9a-zA-Z&_.]+', ' ', txt)
    txt = re.sub(' +', ' ',txt)
    txt = txt.strip().lower().replace(" ",'_')
    return txt

def estandarizacion_metadatos(df, meta):
    nuevos_nombres = []
    metadata_variables = []
    metadata_etiquetas = []
    for col in meta.column_names:
        medida = meta.variable_measure[col]
        description = meta.column_names_to_labels[col]
        name = clean_string(col)
        nuevos_nombres.append(name)
        tipo_dato = meta.readstat_variable_types[col]
        metadata = {
            'name': name,
            'description': description,
            'measure': medida,
            'name_origin': col,
            'data_type': tipo_dato
        }
        try:
            labels = meta.variable_value_labels[col]
            metadata_etiquetas.append({
                'name':name,
                'label_value':list(labels.keys()),
                'label_meaning':list(labels.values())
                })
        except:
            pass
        metadata_variables.append(metadata)

    df.columns = nuevos_nombres
    df_meta_variables = pd.DataFrame(metadata_variables)
    df_meta_etiquetas =  pd.DataFrame(metadata_etiquetas).explode(['label_value','label_meaning'])
    return df,df_meta_variables,df_meta_etiquetas