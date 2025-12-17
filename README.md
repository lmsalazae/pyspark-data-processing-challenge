# Pipeline de Procesamiento de Datos PySpark
Este desarrollo de PySpark implementa un pipeline ETL (Extract, Transform, Load) para procesar datos, utilizando un archivo de configuracion en formato YAML y el paquete OmegaConf para gestionar parámetros de entorno, ejecucion, entrada/salida y reglas de negocio.

## Estructura y Elementos del Archivo de Configuracion (config.yaml) 
El archivo config.yaml es el corazon del pipeline, ya que define todos los parámetros y reglas que guían el procesamiento de los datos.

### CONFIGURACION DEL ENTORNO (environment)
Define el entorno de ejecucion del job.

	* name: Nombre del entorno de ejecucion (ej. "DEV", "QA", "PROD").

	* master: Configuracion del maestro de Spark (ej. "local[\*]" para ejecucion local).

### PARAMETROS DE EJECUCION DEL JOB (run_parameters)
Configura los parámetros clave para el ciclo de vida del job.

	* start_date / end_date: Rango de fechas para el filtrado de los datos (Formato YYYY-MM-DD).

	* output_base_path: Ruta base donde se escribirán los datos procesados.

	* date_filter_column: Columna utilizada para el filtro de rango de fechas (ej. "fecha_proceso").

	* country_filter_column: Columna utilizada para el filtro por país (ej. "pais").

	* country_filter_value: Valor específico del país a filtrar (ej. "TODOS" para no aplicar filtro).

	* partition_columns: Columnas a usar para particionar los datos al escribir la salida.

### CONFIGURACION DE DATOS DE ENTRADA (input_data)
Especifica como leer los datos de origen.

	* file_path: Directorio donde se encuentran los archivos de datos de entrada.

	* file_format: Formato de los archivos de entrada (ej. "csv").

	* options: Opciones de lectura específicas del formato (ej. header: True, inferSchema: True).

	* schema: Configuracion del esquema a ser usado al momento de la lectura.

### PARAMETROS DE CALIDAD DE DATOS (data_quality)
Define las reglas de calidad de datos para la entrada y la salida.

	* input: Reglas para los datos de entrada.

	* min_expected_rows: Mínimo de filas esperadas para aprobar la validacion de conteo.

	* required_columns: Lista de columnas que deben estar presentes.

	* output: Reglas para los datos de salida.

	* not_nulls: Lista de columnas que no deben contener valores nulos después del procesamiento.

### PARAMETROS DE COLUMNAS DERIVADAS (derived_cols)
Reglas para crear nuevas columnas binarias (indicadoras).

	* col1 / col2: Cada bloque define una nueva columna.

	* source: Columna de origen para aplicar la condicion.

	* name: Nombre de la nueva columna derivada.

	* conditions: Lista de valores en la columna de origen que activan el valor 1 en la nueva columna.

### PARAMETROS DE TRATAMIENTO DE NULOS (data_filling)
Define los valores para rellenar los datos nulos.

	* text: Configuracion para rellenar columnas de tipo texto.

	* columns: Lista de columnas de texto a rellenar.

	* value: Valor de relleno para columnas de texto (ej. "NOT INFO").

	* number: Configuracion para rellenar columnas de tipo numérico.

	* columns: Lista de columnas numéricas a rellenar.

	* value: Valor de relleno para columnas numéricas (ej. 0).

### PARAMETROS DE CONVERSION A UNIDADES (unit_conversion)
Reglas para la estandarizacion de unidades (conversion).

	* quantity / price / unit: Bloques para configurar la conversion de campos relacionados.

	* name: Nombre de la columna original.

	* new_name: Nombre de la nueva columna estandarizada.

	* unit (para conversion):

	* value: Unidad original a convertir (ej. "CS").

	* new_value: Nueva unidad estandarizada (ej. "ST").

	* factor: Factor de conversion aplicado a cantidad y precio.

### PARAMETROS DE NUEVAS COLUMNAS PROPUESTAS (additional_fields)
Define los nombres de columnas adicionales que se proponen como datos utiles.
El nombre de archivo sirve para auditoria y linaje de datos.
El total precalcula una operacion bastante util con los campos calculados previamente.

	* total: Nombre para la columna de cálculo de total (ej. "total_estandar").

	* file: Nombre para la columna que capturará el nombre del archivo de origen (ej. "filename").

### PARAMETROS DE ORDENAMIENTO DE COLUMNAS (columns_config)
Define como deben quedar las columnas en la salida.
Se proponen nuevos nombres para algunas columnas, de tal manera que todo el dataset se puede entender mejor.
Se propone tambien un nuevo orden en las columnas del dataset con el mismo objetivo.

	* columns_order: Lista que especifica el orden final de las columnas.

	* columns_rename: Mapeo de columnas para renombrar (campos utilizados para preservar el valor original antes de la estandarizacion).

### PARAMETROS DE CONFIGURACION DE LOGGING (logging)
Define la comfiguracion para el logging centralizado.

	* log_file: Ruta y nombre del archivo donde se registrarán los eventos.

	* log_level: Nivel mínimo de mensajes a registrar (ej. "INFO", "ERROR").

## Descripcion de Funciones del Script PySpark (data_process.py)
El script está modularizado en funciones, cada una con una responsabilidad específica dentro del pipeline.

### 1. setup_logging(conf: OmegaConf) -> logging.Logger
Tarea: Inicializa y configura el sistema de logging de Python antes de iniciar la sesion de Spark.

#### Acciones Clave:

	* Lee los parámetros log_file y log_level del config.yaml.

	* Crea el directorio del log si no existe, previniendo errores de FileNotFoundError al intentar configurar el manejador de archivo.

	* Configura dos handlers: uno para escribir en el archivo de log (modo append) y otro para mostrar la salida en la consola (sys.stdout).

	* Devuelve una instancia configurada del logging.Logger.

### 2. setup_environment(conf: OmegaConf) -> SparkSession
Tarea: Inicializa la sesion de Spark utilizando los parámetros definidos en la seccion environment del archivo de configuracion.

#### Acciones Clave:

	* Crea el nombre de la aplicacion a partir del nombre del entorno.

	* Establece la configuracion del maestro (ej. local[\*]).

	* Establece el nivel de log de Spark a "ERROR" para evitar verbosidad excesiva.

### 3. read_data(spark: SparkSession, conf: OmegaConf) -> DataFrame
Tarea: Carga los datos de entrada desde la ruta especificada.

#### Acciones Clave:

	* Implementa la contruccion del esquema (StructType) a partir de la configuración para la lectura de los datos.

	* Lee los datos basándose en la configuracion de input_data (ruta, formato, opciones).

	* Añade una columna (cuyo nombre se define en additional_fields.file) para capturar el nombre del archivo de origen (input_file_name()), extrayendo solo el nombre del archivo de la ruta completa usando regex.

### 4. data_quality_input(df: DataFrame, conf: OmegaConf) -> bool
Tarea: Ejecuta las validaciones de calidad de datos para la entrada.

#### Acciones Clave:

	* Verifica que el conteo de filas (df.count()) sea mayor o igual a min_expected_rows.

	* Verifica que todas las required_columns estén presentes en el DataFrame.

	* Retorna True si todas las validaciones son exitosas, False en caso contrario.

### 5. transform_data(df: DataFrame, conf: OmegaConf) -> DataFrame
Tarea: Orquesta y aplica la secuencia completa de transformaciones y reglas de negocio.

#### Acciones Clave:

	* Elimina registros duplicados (dropDuplicates).

	* Formatea la columna de fecha a tipo Date (to_date).

	* Aplica la secuencia de filtros:

		- date_filter (Rango de fechas).

		- country_filter (Filtro por país, si no es "TODOS").

		- delivery_filter (Filtro por valores específicos en tipo_entrega).

	* Crea las columnas derivadas (derived_cols).

	* Trata los valores nulos (fix_nulls).

	* Aplica la conversion de unidades (treatment_units).

	* Calcula la columna total estándar (total_estandar) como cantidad_estandar * precio_estandar.

	* Renombra y ordena las columnas (rename_and_order_cols).

#### Funciones de Filtrado (Subprocesos de transform_data)
	* date_filter(df: DataFrame, conf: OmegaConf) -> DataFrame: Filtra las filas dentro del rango start_date y end_date.

	* country_filter(df: DataFrame, conf: OmegaConf) -> DataFrame: Filtra por el country_filter_value si este no es "TODOS".

	* delivery_filter(df: DataFrame, conf: OmegaConf) -> DataFrame: Filtra la data uniendo las filas que cumplen las condiciones de derived_cols.col1 o derived_cols.col2.

#### Funciones de Transformacion (Subprocesos de transform_data)
	* derived_cols(df: DataFrame, conf: OmegaConf) -> DataFrame: Crea las columnas binarias de indicador (entrega_rutina, entrega_bonificada) usando la funcion when de Spark.

	* fix_nulls(df: DataFrame, conf: OmegaConf) -> DataFrame: Rellena los valores nulos (na.fill) según la configuracion de data_filling para columnas de texto y numéricas.

	* treatment_units(df: DataFrame, conf: OmegaConf) -> DataFrame: Aplica la logica de estandarizacion de unidades:

		- Si unidad es igual al value de conversion (ej. "CS"), aplica el factor:

			* cantidad_estandar = cantidad_origen * factor.

			* precio_estandar = precio_origen / (cantidad_origen * factor) o cantidad_estandar.

		- Establece la unidad_estandar al new_value (ej. "ST").

	* rename_and_order_cols(df: DataFrame, conf: OmegaConf) -> DataFrame: Renombra las columnas originales a nombres de respaldo (ej. precio a precio_origen) y luego selecciona las columnas en el orden final definido en columns_order.

### 6. data_quality_output(df: DataFrame, conf: OmegaConf) -> bool
Tarea: Ejecuta las validaciones de calidad de datos para la salida.

#### Acciones Clave:

	* Verifica que las columnas listadas en data_quality.output.not_nulls no contengan ningún valor nulo después de las transformaciones.

	* Retorna True si todas las validaciones son exitosas, False en caso contrario.

### 7. write_data(df: DataFrame, conf: OmegaConf) -> None
Tarea: Escribe el DataFrame procesado en la ubicacion de salida.

#### Acciones Clave:

	* Escribe los datos en modo "overwrite".

	* Utiliza el formato Parquet que es un formato nativo para pyspark y además es eficiente para la lectura.

	* Particiona los datos según las columnas definidas en partition_columns.

	* La ruta de salida es una combinacion de output_base_path y el nombre del entorno.

### 8. main() -> None
Tarea: Funcion principal que gestiona el flujo de ejecucion.

#### Acciones Clave:

	* Carga la configuracion (OmegaConf.load).

	* Configura el logging.

	* Sigue el flujo secuencial: setup_environment -> read_data.

	* Aplica data_quality_input y solo procede a la transformacion si se aprueba.

	* Si se aprueba, aplica transform_data.

	* Aplica data_quality_output y solo procede a la escritura si se aprueba.

	* Si se aprueba, aplica write_data.

	* Detiene la sesion de Spark al finalizar o en caso de error (finally).