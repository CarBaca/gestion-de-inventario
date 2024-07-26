# Código de función para actualización de las tablas en GCP

import functions_framework
from google.cloud import bigquery
from google.cloud import storage

@functions_framework.cloud_event
def actualizar_tablas(cloud_event):
    # Inicializar clientes de BigQuery y Cloud Storage
    client_bq = bigquery.Client()
    client_gcs = storage.Client()

    # Configurar nombres de bucket y dataset
    bucket_name = 'inventario-bd-bucket'
    folder_name = 'Bases de datos en bak'
    dataset_id = 'InventarioBD'

    # Lista de tablas y archivos CSV correspondientes
    tablas_y_archivos = {
        'Tabla_InventarioInicial': 'Tabla_InventarioInicial.csv',
        'Tabla_InventarioFinal': 'Tabla_InventarioFinal.csv',
        'Tabla_Producto': 'Tabla_Producto.csv',
        'Tabla_VentasFinal': 'Tabla_VentasFinal.csv',
        'Tabla_Compras': 'Tabla_Compras.csv',
        'Tabla_DetalleCompras': 'Tabla_DetalleCompras.csv'
    }

    # Procesar cada tabla y su archivo CSV correspondiente
    for tabla, archivo_csv in tablas_y_archivos.items():
        file_name = f'{folder_name}/{archivo_csv}'
        uri = f'gs://{bucket_name}/{file_name}'
        table_id = f'{client_bq.project}.{dataset_id}.{tabla}'

        # Configurar trabajo de carga
        job_config = bigquery.LoadJobConfig(
            source_format=bigquery.SourceFormat.CSV,
            skip_leading_rows=1,  # Ajusta esto según tu CSV
            write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,  # Reemplaza la tabla
            autodetect=True  # Autodetecta el esquema
        )

        # Cargar datos del CSV a la tabla
        load_job = client_bq.load_table_from_uri(
            uri,
            table_id,
            job_config=job_config
        )

        # Esperar a que el trabajo de carga termine
        load_job.result()
        print(f'Tabla {tabla} actualizada con éxito.')

    return 'Tablas actualizadas correctamente.'


@functions_framework.cloud_event
def actualizar_tablas(cloud_event):
    # Inicializar clientes de BigQuery y Cloud Storage
    client_bq = bigquery.Client()
    client_gcs = storage.Client()

    # Configurar nombres de bucket y dataset
    bucket_name = 'inventario-bd-bucket'
    folder_name = 'Bases de datos en bak'
    dataset_id = 'InventarioBD'

    # Lista de tablas y archivos CSV correspondientes
    tablas_y_archivos = {
        'Tabla_InventarioInicial': 'Tabla_InventarioInicial.csv',
        'Tabla_InventarioFinal': 'Tabla_InventarioFinal.csv',
        'Tabla_Producto': 'Tabla_Producto.csv',
        'Tabla_VentasFinal': 'Tabla_VentasFinal.csv',
        'Tabla_Compras': 'Tabla_Compras.csv',
        'Tabla_DetalleCompras': 'Tabla_DetalleCompras.csv'
    }

    # Procesar cada tabla y su archivo CSV correspondiente
    for tabla, archivo_csv in tablas_y_archivos.items():
        file_name = f'{folder_name}/{archivo_csv}'
        uri = f'gs://{bucket_name}/{file_name}'
        table_id = f'{client_bq.project}.{dataset_id}.{tabla}'

        # Configurar trabajo de carga
        job_config = bigquery.LoadJobConfig(
            source_format=bigquery.SourceFormat.CSV,
            skip_leading_rows=1,  # Ajusta esto según tu CSV
            write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,  # Reemplaza la tabla
            autodetect=True  # Autodetecta el esquema
        )

        # Cargar datos del CSV a la tabla
        load_job = client_bq.load_table_from_uri(
            uri,
            table_id,
            job_config=job_config
        )

        # Esperar a que el trabajo de carga termine
        load_job.result()
        print(f'Tabla {tabla} actualizada con éxito.')

    return 'Tablas actualizadas correctamente.'


# Triggered from a message on a Cloud Pub/Sub topic.
@functions_framework.cloud_event
def hello_pubsub(cloud_event):
    # Print out the data from Pub/Sub, to prove that it worked
    print(base64.b64decode(cloud_event.data["message"]["data"]))


# Requirements
#google-cloud-bigquery
#google-cloud-storage
#functions-framework
