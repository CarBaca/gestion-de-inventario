
---

### Código de automatización y carga incremental de los datos mediante librería Watchdog  🤖

El siguiente código es un script en Python diseñado para monitorear una carpeta específica en el sistema de archivos. Cuando se detectan archivos CSV nuevos o modificados, los datos de estos archivos se procesan y se insertan en la base de datos `InventarioBD` en SQL Server. Se utilizan las siguientes librerías de Python:

1. **watchdog**: Para monitorear el sistema de archivos y detectar cambios.
2. **sqlalchemy**: Para interactuar con la base de datos SQL Server.
3. **pandas**: Para manipulación y análisis de datos.
4. **os**: Para interactuar con el sistema operativo.
5. **time**: Para manejar retrasos y pausas en la ejecución del programa.

#### Descripción de las Secciones del Código 🐕

1. **Configuración de la Base de Datos**:
   - Se define la configuración de la base de datos, incluyendo el servidor y el nombre de la base de datos.
   - Se crea una cadena de conexión utilizando SQLAlchemy para conectarse a la base de datos SQL Server.

   ```python
   database_config = {
       'server': r'CARLA',
       'database': 'InventarioBD',
   }
   connection_string = f"mssql+pyodbc://@{database_config['server']}/{database_config['database']}?driver=ODBC+Driver+17+for+SQL+Server&trusted_connection=yes"
   engine = create_engine(connection_string)
   ```

2. **Definición de la Clase Manejadora de Eventos**:
   - Se define la clase `DataHandler` que hereda de `FileSystemEventHandler` de la librería `watchdog`.
   - La clase tiene métodos para manejar eventos de creación y modificación de archivos (`on_created` y `on_modified`).
   - El método `process_new_file` se encarga de leer el archivo CSV, procesar los datos y actualizar la base de datos.

   ```python
   class DataHandler(FileSystemEventHandler):
       def __init__(self, engine):
           self.engine = engine

       def on_created(self, event):
           if event.is_directory or not event.src_path.endswith(".csv"):
               return
           self.process_new_file(event.src_path)

       def on_modified(self, event):
           if event.is_directory or not event.src_path.endswith(".csv"):
               return
           self.process_new_file(event.src_path)

       def process_new_file(self, file_path):
           try:
               new_data = pd.read_csv(file_path)
               new_data = new_data.drop(columns=['Unnamed: 0'], errors='ignore')

               # Convertir columnas que contengan la palabra 'date' a formato datetime
               for column in new_data.columns:
                   if 'date' in column.lower():
                       new_data[column] = pd.to_datetime(new_data[column], errors='coerce')

               table_name = os.path.splitext(os.path.basename(file_path))[0]

               # Obtener el nombre de la columna ID específica
               id_column = self.get_id_column(table_name)

               # Verificar que la columna ID exista en el DataFrame
               if id_column not in new_data.columns:
                   print(f"Error: La columna '{id_column}' no existe en el archivo {file_path}")
                   return

               # Cargar los datos existentes de la base de datos
               existing_data = pd.read_sql(f"SELECT * FROM {table_name}", self.engine)

               # Identificar filas nuevas (basado en id_column)
               new_rows = new_data[~new_data[id_column].isin(existing_data[id_column])]

               # Verificar integridad referencial para 'Inventario_inicialID' si es necesario
               if table_name == 'Tabla_VentasFinal':
                   inventario_inicial_ids = pd.read_sql("SELECT Inventario_inicialID FROM Tabla_InventarioInicial", self.engine)
                   valid_ids = inventario_inicial_ids['Inventario_inicialID']
                   new_rows = new_rows[new_rows['Inventario_inicialID'].isin(valid_ids)]

               # Insertar datos nuevos en la base de datos
               if not new_rows.empty:
                   new_rows.to_sql(table_name, self.engine, if_exists='append', index=False)
                   print(f"Se han insertado {len(new_rows)} filas nuevas en la tabla {table_name}")
               else:
                   print(f"No hay filas nuevas para insertar en la tabla {table_name}")

           except Exception as e:
               print(f"Error al procesar el archivo {file_path}: {e}")

       def get_id_column(self, table_name):
           id_columns = {
               'Tabla_VentasFinal': 'VentasID',
               'Tabla_Detallecompras': 'Detalle_compraID',
               'Tabla_InventarioFinal': 'Inventario_FinalID',
               'Tabla_InventarioInicial': 'Inventario_inicialID',
               'Tabla_Producto': 'MarcaID',
               'Tabla_Compras': 'CompraID',
           }
           return id_columns.get(table_name, 'id')
   ```

3. **Función para Procesar Archivos Existentes**:
   - La función `process_existing_files` recorre todos los archivos CSV existentes en la carpeta especificada y los procesa.

   ```python
   def process_existing_files(path, handler):
       for filename in os.listdir(path):
           if filename.endswith(".csv"):
               file_path = os.path.join(path, filename)
               handler.process_new_file(file_path)
   ```

4. **Punto de Entrada Principal**:
   - Se define el directorio a observar.
   - Se crea una instancia de `DataHandler`.
   - Se procesan los archivos existentes en la carpeta.
   - Se configura el observador de cambios (`Observer`) para monitorear el directorio especificado.
   - Se inicia el observador y se mantiene en ejecución hasta que se interrumpe manualmente.

   ```python
   if __name__ == '__main__':
       path_to_watch = 'C:\\Users\\Carla Baca\\Desktop\\Inventory_Prueba\\CSV_Finales'
       event_handler = DataHandler(engine)

       # Procesar archivos existentes en la carpeta
       process_existing_files(path_to_watch, event_handler)

       observer = Observer()
       observer.schedule(event_handler, path=path_to_watch, recursive=True)
       observer.start()
       print(f"Observando cambios en: {path_to_watch}")

       try:
           while True:
               time.sleep(1)
       except KeyboardInterrupt:
           observer.stop()
       observer.join()
       print("El observador ha sido detenido.")
   ```

### Descripción General 👀

Este código automatiza la actualización de una base de datos SQL Server con datos de archivos CSV. Monitorea una carpeta específica para detectar nuevos archivos o modificaciones en archivos existentes y actualiza la base de datos en consecuencia, asegurando que solo se inserten filas nuevas y verificando la integridad referencial donde sea necesario. Esto es útil para mantener una base de datos actualizada con datos dinámicos provenientes de archivos CSV sin intervención manual constante.

Una vez detectados los cambios o modificaciones en los archivos, los nuevos datos son insertados en la base de datos `InventariosBD` en SQL Server. Para lograr esto, se utilizan varias librerías de Python, como **watchdog**, **sqlalchemy**, **pandas**, **os** y **time**. La librería `watchdog` permite monitorear el sistema de archivos y detectar cambios en tiempo real. `SQLAlchemy` proporciona herramientas para trabajar con bases de datos SQL de manera eficiente y permite la conexión a la base de datos SQL Server. `Pandas` es fundamental para la manipulación y análisis de datos, permitiendo leer, transformar y cargar datos de archivos CSV. La librería `os` permite interactuar con el sistema operativo, por ejemplo, para manejar rutas de archivos. Por último, la librería `time` permite pausar la ejecución del programa, lo que es útil para mantener el monitoreo activo.

En cuanto al funcionamiento del código, primero se define la configuración de la base de datos, incluyendo el servidor y el nombre de la base de datos, y se crea una cadena de conexión utilizando SQLAlchemy para conectarse a la base de datos SQL Server. Luego, se define una clase `DataHandler` que hereda de `FileSystemEventHandler` de la librería `watchdog`. Esta clase tiene métodos para manejar eventos de creación y modificación de archivos en la carpeta monitoreada. Cuando se detecta un nuevo archivo o una modificación en un archivo existente, se lee el archivo CSV, se procesan los datos y se actualiza la base de datos.

Además, se define una función para procesar los archivos CSV que ya existen en la carpeta al inicio del monitoreo. Esta función recorre todos los archivos CSV en la carpeta especificada y los procesa utilizando la clase `DataHandler`. Finalmente, se define el punto de entrada principal del script. Se especifica el directorio que se va a monitorear, se crea una instancia de `DataHandler`, se procesan los archivos existentes en la carpeta y se configura y se inicia un observador de cambios (`Observer`) para monitorear la carpeta especificada de manera recursiva. El observador se mantiene en ejecución hasta que se interrumpe manualmente.

En resumen, este código automatiza la actualización de una base de datos SQL Server con datos de archivos CSV. Monitorea una carpeta específica para detectar nuevos archivos o modificaciones en archivos existentes y actualiza la base de datos en consecuencia, asegurando que solo se inserten filas nuevas y verificando la integridad referencial donde sea necesario. Esto es útil para mantener una base de datos actualizada con datos dinámicos provenientes de archivos CSV sin intervención manual constante.

## Tecnologías 
![VisualStudio Code](https://img.shields.io/badge/Visual%20Studio%20Code-007ACC?style=for-the-badge&logo=visual-studio-code&logoColor=white) ![Python](https://img.shields.io/badge/Python-3776AB?style=for-the-badge&logo=python&logoColor=white) ![Microsoft SQL Server](https://img.shields.io/badge/Microsoft%20SQL%20Server-CC2927?style=for-the-badge&logo=microsoft-sql-server&logoColor=white) 

---