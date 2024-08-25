
---

## Database Migration to Google Cloud Platform (GCP) 💭

---

The first step was creating the **Data Solutions** project, within which the **data-solutions** instance was created. Then, the **inventario-bd-bucket** bucket was created in Cloud Storage to act as a container; six CSV files were uploaded into the folder ‘Bases de datos en bak’. Subsequently, a dataset called **InventarioBD** was created using the SQL workspace in BigQuery. Within this dataset, six tables were created corresponding to each of the CSV files previously uploaded to the bucket.

## Automation and Scheduling of Table Data Updates in the Dataset 🕙

1. **Creating a Topic in Pub/Sub**: Setting up the topic to receive messages triggering the table updates.
    - The **InventarioBD-actualizar-tablas** topic was created to be used as a trigger for the function that allows the table data to be updated. This topic is also linked with Google Scheduler so that the function updates the tables weekly on Saturdays at 10:00 PM.
   
2. **Creating a Function in Google Cloud Functions**: Configuring the function to subscribe to the topic and update the tables in BigQuery.
    - This code automates the table updates in BigQuery using CSV files stored in a Google Cloud Storage (GCS) bucket. It is configured and executed as a Cloud Function in GCP, triggered by Cloud Pub/Sub events to initiate the data loading process. This automation is useful for keeping the BigQuery tables updated with dynamic data stored in GCS.

3. **Configuring a Job in Cloud Scheduler**: Scheduling a job to publish a message to the topic at the corresponding time and day.
    - A new job called **programar-actualizacion-tablas** was created in Cloud Scheduler. This job is scheduled to run weekly on Saturdays at 10:00 PM and is directly linked to the Pub/Sub topic mentioned above.

---

### Code Explanation ☄️

This code is a function developed in Python 3.9, running on Google Cloud Platform (GCP) using Cloud Functions. The function is designed to update tables in BigQuery from CSV files stored in Google Cloud Storage (GCS). It uses several Google Cloud libraries and GCP-specific functions to handle events and process data.

The `actualizar_tablas_inventario` function is triggered by a Cloud Pub/Sub event. The function initializes clients for BigQuery and Cloud Storage, sets up the GCS bucket and BigQuery dataset names, and defines a dictionary mapping BigQuery table names to their corresponding CSV files in GCS. For each table and CSV file pair, it builds the file URI in GCS and the table ID in BigQuery. It configures a load job in BigQuery to use the CSV format, skip the first row of the CSV file, replace the existing table, and autodetect the CSV file schema. Then, it loads the CSV data into the BigQuery table and waits for the load job to complete before moving on to the next CSV file. Once all files have been processed and loaded into the corresponding tables, the function returns a message indicating that the tables have been successfully updated.

Libraries used in this code:

- **functions_framework**: Allows the creation of functions that can be deployed and run on GCP.
- **google.cloud.bigquery**: Provides tools for interacting with BigQuery, a large-scale data storage and analysis service.
- **google.cloud.storage**: Enables interaction with Google Cloud Storage, an object storage service.

The function starts by initializing clients for BigQuery and Cloud Storage.

```python
@functions_framework.cloud_event
def actualizar_tablas(cloud_event):
    # Initialize BigQuery and Cloud Storage clients
    client_bq = bigquery.Client()
    client_gcs = storage.Client()
```

It then configures the GCS bucket and BigQuery dataset names where the data will be stored.

```python
# Set bucket and dataset names
bucket_name = 'inventario-bd-bucket'
folder_name = 'Bases de datos en bak'
dataset_id = 'InventarioBD'
```

The script defines a dictionary `tablas_y_archivos` that maps BigQuery table names to their corresponding CSV files in GCS. For each table and CSV file pair, the file URI in GCS and the table ID in BigQuery are constructed.

```python
# List of tables and their corresponding CSV files
tablas_y_archivos = {
    'Tabla_InventarioInicial': 'Tabla_InventarioInicial.csv',
    'Tabla_InventarioFinal': 'Tabla_InventarioFinal.csv',
    'Tabla_Producto': 'Tabla_Producto.csv',
    'Tabla_VentasFinal': 'Tabla_VentasFinal.csv',
    'Tabla_Compras': 'Tabla_Compras.csv',
    'Tabla_DetalleCompras': 'Tabla_DetalleCompras.csv'
}
```

For each CSV file, a load job is configured in BigQuery using `bigquery.LoadJobConfig`. This load job is set to:
- Use the CSV format.
- Skip the first row of the CSV file (which usually contains headers).
- Replace the existing table (write disposition).
- Autodetect the CSV file schema.

```python
# Process each table and its corresponding CSV file
for tabla, archivo_csv in tablas_y_archivos.items():
    file_name = f'{folder_name}/{archivo_csv}'
    uri = f'gs://{bucket_name}/{file_name}'
    table_id = f'{client_bq.project}.{dataset_id}.{tabla}'

    # Configure load job
    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.CSV,
        skip_leading_rows=1,  # Adjust this according to your CSV
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,  # Replace the table
        autodetect=True  # Autodetect the schema
    )
```

Next, the script loads the CSV data into the BigQuery table using `load_job`. The load job is executed and waits to finish before continuing with the next CSV file. Once all files have been processed and loaded into the corresponding tables, the function returns a message indicating that the tables have been successfully updated.

```python
    # Load data from CSV into the table
    load_job = client_bq.load_table_from_uri(
        uri,
        table_id,
        job_config=job_config
    )

    # Wait for the load job to finish
    load_job.result()
    print(f'Table {tabla} successfully updated.')

return 'Tables updated successfully.'
```

---

## Conclusion

The migration of the project’s database to GCP was created to facilitate our client **Drink's** access to updated and accurate data. The automation implemented in GCP allows for regular and efficient updates of the tables in BigQuery from CSV files stored in Google Cloud Storage, ensuring the information is always up to date. This automation is useful for maintaining the tables in BigQuery with dynamic data stored in GCS, as it not only improves data accessibility and accuracy for Drink's but also provides a solid and flexible platform for future expansions and improvements.

---

### Benefits of Hosting the Project in GCP

1. **Scalability**: GCP allows for easy scaling of data storage and processing as the business needs grow.
2. **Security**: GCP offers robust security measures to protect stored and processed data.
3. **Automation**: Automating repetitive tasks reduces errors and frees up time for staff to focus on more strategic activities.
4. **Global Access**: Data and applications are available from anywhere, facilitating collaboration and remote access.
5. **Integration**: GCP offers a wide range of services that easily integrate, enabling the building of complete and efficient solutions.
6. **Cost-Efficiency**: You only pay for the resources you use, which can reduce costs compared to on-premises solutions. 

---


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

