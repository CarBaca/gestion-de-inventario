
---
## Traslado de la base de datos a Google Cloud Platform (GCP) 💭

---
El primer paso fue la creación del proyecto **Data Solutions**, dentro del mismo se creó la instancia **data-solutions**. Luego, se creó el bucket **inventario-bd-bucket** en Cloud Storage para que funcione como contenedor; en éste se cargaron los 6 archivos CSV en la carpeta ‘Bases de datos en bak’. Posteriormente, se creó el dataset llamado **InventarioBD** mediante el espacio de trabajo de SQL en BigQuery. En este dataset se crearon las 6 tablas pertenecientes a cada uno de los archivos CSV cargados previamente en el bucket.

## Automatización y programación de la actualización de los datos de las tablas en el dataset 🕙

1. **Crearción de un Topic en Pub/Sub**: Configuración del topic o tema para recibir mensajes que desencadenen la actualización de las tablas.
    - Se procedió a la creación del tema **InventarioBD-actualizar-tablas** para ser usado como trigger o activador de la función que permite la actualización de los datos en la tabla, permitiendo también su relación con el programador de Google para que la función actualice las tablas semanalmente, los sábados a las 22 hs.
   
2. **Creación de una función en Google Cloud Functions**: Configuración de la función para suscribirse al topic y realizar la actualización de las tablas en BigQuery.
    - Este código automatiza la actualización de tablas en BigQuery utilizando archivos CSV almacenados en un bucket de Google Cloud Storage (GCS). Se configura y ejecuta como una Cloud Function en GCP, respondiendo a eventos de Cloud Pub/Sub para iniciar el proceso de carga de datos. Esta automatización es útil para mantener actualizadas las tablas en BigQuery con datos dinámicos almacenados en GCS.

3. **Configurar un Job en Cloud Scheduler**: Programación de un job para publicar un mensaje en el topic a la hora y día correspondiente.
    - Se procedió a la creación de un nuevo trabajo llamado **programar-actualizacion-tablas** en Cloud Scheduler. Este trabajo fue programado con frecuencia semanal, los sábados a las 22 hs., y fue relacionado directamente con el tema de Pub/Sub mencionado anteriormente.

---
### Explicación del Código ☄️

Este código es una función desarrollada en Python 3.9 que se ejecuta en Google Cloud Platform (GCP) utilizando Cloud Functions. La función está diseñada para actualizar tablas en BigQuery a partir de archivos CSV almacenados en Google Cloud Storage (GCS). Utiliza varias librerías de Google Cloud y funciones específicas de GCP para manejar eventos y procesar datos.

La función `actualizar_tablas_inventario` se ejecuta en respuesta a un evento de Cloud Pub/Sub. La función inicializa clientes para BigQuery y Cloud Storage, configura los nombres del bucket de GCS y el dataset de BigQuery, define un diccionario que mapea los nombres de las tablas de BigQuery a los archivos CSV correspondientes en GCS. Para cada par de tabla y archivo CSV, construye la URI del archivo en GCS y el ID de la tabla en BigQuery. Configura un trabajo de carga en BigQuery para usar el formato CSV, saltar la primera fila del archivo CSV, reemplazar la tabla existente y autodetectar el esquema del archivo CSV. Luego, carga los datos del CSV en la tabla de BigQuery y espera a que el trabajo de carga termine antes de continuar con el siguiente archivo CSV. Una vez que todos los archivos han sido procesados y cargados en las tablas correspondientes, la función devuelve un mensaje indicando que las tablas han sido actualizadas correctamente.

Librerías que intervienen en este código:

- **functions_framework**: Permite la creación de funciones que pueden ser desplegadas y ejecutadas en GCP.
- **google.cloud.bigquery**: Proporciona herramientas para interactuar con BigQuery, un servicio de almacenamiento y análisis de datos a gran escala.
- **google.cloud.storage**: Permite la interacción con Google Cloud Storage, un servicio de almacenamiento de objetos.

La función comienza inicializando clientes para BigQuery y Cloud Storage.

```python
@functions_framework.cloud_event
def actualizar_tablas(cloud_event):
    # Inicializar clientes de BigQuery y Cloud Storage
    client_bq = bigquery.Client()
    client_gcs = storage.Client()
```

Luego, configura los nombres del bucket de GCS y el dataset de BigQuery donde se almacenarán los datos.

```python
 # Configurar nombres de bucket y dataset
    bucket_name = 'inventario-bd-bucket'
    folder_name = 'Bases de datos en bak'
    dataset_id = 'InventarioBD'
```

El script define un diccionario `tablas_y_archivos` que mapea los nombres de las tablas de BigQuery a los archivos CSV correspondientes en GCS. Para cada par de tabla y archivo CSV, se construye la URI del archivo en GCS y el ID de la tabla en BigQuery.

```python
    # Lista de tablas y archivos CSV correspondientes
    tablas_y_archivos = {
        'Tabla_InventarioInicial': 'Tabla_InventarioInicial.csv',
        'Tabla_InventarioFinal': 'Tabla_InventarioFinal.csv',
        'Tabla_Producto': 'Tabla_Producto.csv',
        'Tabla_VentasFinal': 'Tabla_VentasFinal.csv',
        'Tabla_Compras': 'Tabla_Compras.csv',
        'Tabla_DetalleCompras': 'Tabla_DetalleCompras.csv'
    }
```

Para cada archivo CSV, se configura un trabajo de carga (load job) en BigQuery utilizando `bigquery.LoadJobConfig`. Este trabajo de carga está configurado para:
- Usar el formato CSV.
- Saltar la primera fila del archivo CSV (que usualmente contiene encabezados).
- Reemplazar la tabla existente (write disposition).
- Autodetectar el esquema del archivo CSV.

```python
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
```

Luego, el script carga los datos del CSV en la tabla de BigQuery utilizando `load_job`. El trabajo de carga se ejecuta y se espera a que termine antes de continuar con el siguiente archivo CSV. Una vez que todos los archivos han sido procesados y cargados en las tablas correspondientes, la función devuelve un mensaje indicando que las tablas han sido actualizadas correctamente.

```python
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
```

---
## Conclusión

El traslado de la base de datos del proyecto a GCP fue creado para facilitar el acceso de nuestro cliente, **Drink's**, a datos actualizados y precisos. La automatización implementada en GCP permite la actualización regular y eficiente de las tablas en BigQuery a partir de archivos CSV almacenados en Google Cloud Storage, asegurando que la información esté siempre actualizada. Esta automatización es útil para mantener actualizadas las tablas en BigQuery con datos dinámicos almacenados en GCS ya que no solo mejora la accesibilidad y la precisión de los datos para Drink's, sino que también proporciona una plataforma sólida y flexible para futuras expansiones y mejoras.

---
### Beneficios de tener el proyecto en GCP

1. **Escalabilidad**: GCP permite escalar el almacenamiento y el procesamiento de datos fácilmente a medida que las necesidades de la empresa crecen.
2. **Seguridad**: GCP ofrece robustas medidas de seguridad para proteger los datos almacenados y procesados.
3. **Automatización**: La automatización de tareas repetitivas reduce errores y libera tiempo para que el personal se enfoque en tareas más estratégicas.
4. **Acceso global**: Los datos y las aplicaciones están disponibles desde cualquier lugar, lo que facilita la colaboración y el acceso remoto.
5. **Integración**: GCP ofrece una amplia gama de servicios que se integran fácilmente entre sí, permitiendo construir soluciones completas y eficientes.
6. **Costo-eficiencia**: Pagas solo por los recursos que utilizas, lo que puede reducir costos comparado con soluciones on-premises.

