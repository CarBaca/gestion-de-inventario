
---

### Automation and Incremental Data Load Code using Watchdog Library 🤖

The following Python script is designed to monitor a specific folder in the file system. When new or modified CSV files are detected, the data from these files is processed and inserted into the `InventarioBD` database in SQL Server. The following Python libraries are used:

1. **watchdog**: To monitor the file system and detect changes.
2. **sqlalchemy**: To interact with the SQL Server database.
3. **pandas**: For data manipulation and analysis.
4. **os**: To interact with the operating system.
5. **time**: To handle delays and pauses in program execution.

#### Code Sections Description 🐕

1. **Database Configuration**:
   - The database configuration is defined, including the server and database name.
   - A connection string is created using SQLAlchemy to connect to the SQL Server database.

   ```python
   database_config = {
       'server': r'CARLA',
       'database': 'InventarioBD',
   }
   connection_string = f"mssql+pyodbc://@{database_config['server']}/{database_config['database']}?driver=ODBC+Driver+17+for+SQL+Server&trusted_connection=yes"
   engine = create_engine(connection_string)
   ```

2. **Event Handler Class Definition**:
   - The `DataHandler` class is defined, inheriting from `FileSystemEventHandler` from the `watchdog` library.
   - The class has methods to handle file creation and modification events (`on_created` and `on_modified`).
   - The `process_new_file` method is responsible for reading the CSV file, processing the data, and updating the database.

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

               # Convert columns containing the word 'date' to datetime format
               for column in new_data.columns:
                   if 'date' in column.lower():
                       new_data[column] = pd.to_datetime(new_data[column], errors='coerce')

               table_name = os.path.splitext(os.path.basename(file_path))[0]

               # Get the specific ID column name
               id_column = self.get_id_column(table_name)

               # Ensure the ID column exists in the DataFrame
               if id_column not in new_data.columns:
                   print(f"Error: The column '{id_column}' does not exist in the file {file_path}")
                   return

               # Load existing data from the database
               existing_data = pd.read_sql(f"SELECT * FROM {table_name}", self.engine)

               # Identify new rows (based on id_column)
               new_rows = new_data[~new_data[id_column].isin(existing_data[id_column])]

               # Check referential integrity for 'Inventario_inicialID' if needed
               if table_name == 'Tabla_VentasFinal':
                   inventario_inicial_ids = pd.read_sql("SELECT Inventario_inicialID FROM Tabla_InventarioInicial", self.engine)
                   valid_ids = inventario_inicial_ids['Inventario_inicialID']
                   new_rows = new_rows[new_rows['Inventario_inicialID'].isin(valid_ids)]

               # Insert new rows into the database
               if not new_rows.empty:
                   new_rows.to_sql(table_name, self.engine, if_exists='append', index=False)
                   print(f"{len(new_rows)} new rows have been inserted into the {table_name} table")
               else:
                   print(f"No new rows to insert into the {table_name} table")

           except Exception as e:
               print(f"Error processing the file {file_path}: {e}")

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

3. **Function to Process Existing Files**:
   - The `process_existing_files` function loops through all existing CSV files in the specified folder and processes them.

   ```python
   def process_existing_files(path, handler):
       for filename in os.listdir(path):
           if filename.endswith(".csv"):
               file_path = os.path.join(path, filename)
               handler.process_new_file(file_path)
   ```

4. **Main Entry Point**:
   - The directory to be monitored is defined.
   - An instance of `DataHandler` is created.
   - Existing files in the folder are processed.
   - The change observer (`Observer`) is set up to monitor the specified directory.
   - The observer is started and kept running until it is manually interrupted.

   ```python
   if __name__ == '__main__':
       path_to_watch = 'C:\\Users\\Carla Baca\\Desktop\\Inventory_Prueba\\CSV_Finales'
       event_handler = DataHandler(engine)

       # Process existing files in the folder
       process_existing_files(path_to_watch, event_handler)

       observer = Observer()
       observer.schedule(event_handler, path=path_to_watch, recursive=True)
       observer.start()
       print(f"Watching for changes in: {path_to_watch}")

       try:
           while True:
               time.sleep(1)
       except KeyboardInterrupt:
           observer.stop()
       observer.join()
       print("The observer has been stopped.")
   ```

### Overview 👀

This code automates the update of a SQL Server database with data from CSV files. It monitors a specific folder for new files or modifications in existing files and updates the database accordingly, ensuring that only new rows are inserted and verifying referential integrity where necessary. This is useful for keeping a database up-to-date with dynamic data from CSV files without constant manual intervention.

Once changes or modifications to the files are detected, the new data is inserted into the `InventarioBD` database in SQL Server. To achieve this, several Python libraries are used, such as **watchdog**, **sqlalchemy**, **pandas**, **os**, and **time**. The `watchdog` library allows monitoring of the file system and detecting changes in real-time. `SQLAlchemy` provides tools for working efficiently with SQL databases and enables connection to the SQL Server database. `Pandas` is key for data manipulation and analysis, allowing reading, transforming, and loading data from CSV files. The `os` library enables interaction with the operating system, for instance, to manage file paths. Lastly, the `time` library allows for pausing the program execution, which is useful for keeping the monitoring active.

Regarding the code functionality, it first defines the database configuration, including the server and database name, and creates a connection string using SQLAlchemy to connect to the SQL Server database. Then, a `DataHandler` class is defined, inheriting from `FileSystemEventHandler` of the `watchdog` library. This class has methods to handle file creation and modification events in the monitored folder. When a new file or a modification in an existing file is detected, the CSV file is read, the data is processed, and the database is updated.

Additionally, a function is defined to process the CSV files already present in the folder at the start of monitoring. This function loops through all the CSV files in the specified folder and processes them using the `DataHandler` class. Finally, the main entry point of the script is defined. The directory to be monitored is specified, a `DataHandler` instance is created, the existing files in the folder are processed, and an observer (`Observer`) is set up and started to monitor the specified folder recursively. The observer keeps running until manually interrupted.

In summary, this code automates the update of a SQL Server database with data from CSV files. It monitors a specific folder for new files or modifications in existing files and updates the database accordingly, ensuring that only new rows are inserted and verifying referential integrity where necessary. This is useful for keeping a database up-to-date with dynamic data from CSV files without constant manual intervention.

## Technologies 
![VisualStudio Code](https://img.shields.io/badge/Visual%20Studio%20Code-007ACC?style=for-the-badge&logo=visual-studio-code&logoColor=white) ![Python](https://img.shields.io/badge/Python-3776AB?style=for-the-badge&logo=python&logoColor=white) ![Microsoft SQL Server](https://img.shields.io/badge/Microsoft%20SQL%20Server-CC2927?style=for-the-badge&logo=microsoft-sql-server&logoColor=white) 

---


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
