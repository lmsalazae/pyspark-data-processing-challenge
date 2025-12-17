# Pipeline de Procesamiento de Datos PySpark
Este desarrollo de PySpark implementa un pipeline ETL (Extract, Transform, Load) para procesar datos, utilizando un archivo de configuración en formato YAML para gestionar parámetros de entorno, ejecución, entrada/salida y reglas de negocio.

## Estructura y Elementos del Archivo de Configuración (config.yaml) (H2)
El archivo config.yaml es el corazón del pipeline, ya que define todos los parámetros y reglas que guían el procesamiento de los datos.

### CONFIGURACION DEL ENTORNO (environment)
Define el entorno de ejecución del job.

	* name: Nombre del entorno de ejecución (ej. "DEV", "QA", "PROD").

	* master: Configuración del maestro de Spark (ej. "local[\*]" para ejecución local).

### PARAMETROS DE EJECUCION DEL JOB (run_parameters)
Configura los parámetros clave para el ciclo de vida del job.

	* start_date / end_date: Rango de fechas para el filtrado de los datos (Formato YYYY-MM-DD).

	* output_base_path: Ruta base donde se escribirán los datos procesados.

	* date_filter_column: Columna utilizada para el filtro de rango de fechas (ej. "fecha_proceso").

	* country_filter_column: Columna utilizada para el filtro por país (ej. "pais").

	* country_filter_value: Valor específico del país a filtrar (ej. "TODOS" para no aplicar filtro).

	* partition_columns: Columnas a usar para particionar los datos al escribir la salida.

### CONFIGURACION DE DATOS DE ENTRADA (input_data)
Especifica cómo leer los datos de origen.

	* file_path: Directorio donde se encuentran los archivos de datos de entrada.

	* file_format: Formato de los archivos de entrada (ej. "csv").

	* options: Opciones de lectura específicas del formato (ej. header: True, inferSchema: True).

### PARAMETROS DE CALIDAD DE DATOS (data_quality)
Define las reglas de calidad de datos para la entrada y la salida.

	* input: Reglas para los datos de entrada.

	* min_expected_rows: Mínimo de filas esperadas para aprobar la validación de conteo.

	* required_columns: Lista de columnas que deben estar presentes.

	* output: Reglas para los datos de salida.

	* not_nulls: Lista de columnas que no deben contener valores nulos después del procesamiento.

### PARAMETROS DE COLUMNAS DERIVADAS (derived_cols)
Reglas para crear nuevas columnas binarias (indicadoras).

	* col1 / col2: Cada bloque define una nueva columna.

	* source: Columna de origen para aplicar la condición.

	* name: Nombre de la nueva columna derivada.

	* conditions: Lista de valores en la columna de origen que activan el valor 1 en la nueva columna.

### PARAMETROS DE TRATAMIENTO DE NULOS (data_filling)
Define los valores para rellenar los datos nulos.

	* text: Configuración para rellenar columnas de tipo texto.

	* columns: Lista de columnas de texto a rellenar.

	* value: Valor de relleno para columnas de texto (ej. "NOT INFO").

	* number: Configuración para rellenar columnas de tipo numérico.

	* columns: Lista de columnas numéricas a rellenar.

	* value: Valor de relleno para columnas numéricas (ej. 0).

### PARAMETROS DE CONVERSION A UNIDADES (unit_conversion)
Reglas para la estandarización de unidades (conversión).

	* quantity / price / unit: Bloques para configurar la conversión de campos relacionados.

	* name: Nombre de la columna original.

	* new_name: Nombre de la nueva columna estandarizada.

	* unit (para conversión):

	* value: Unidad original a convertir (ej. "CS").

	* new_value: Nueva unidad estandarizada (ej. "ST").

	* factor: Factor de conversión aplicado a cantidad (multiplicación) y precio (división).

### PARAMETROS DE NUEVAS COLUMNAS PROPUESTAS (additional_fields)
Define los nombres de columnas adicionales que se añadirán.

	* total: Nombre para la columna de cálculo de total (ej. "total_estandar").

	* file: Nombre para la columna que capturará el nombre del archivo de origen (ej. "filename").

### PARAMETROS DE ORDENAMIENTO DE COLUMNAS (columns_config)
Define cómo deben quedar las columnas en la salida.

	* columns_order: Lista que especifica el orden final de las columnas.

	* columns_rename: Mapeo de columnas para renombrar (utilizado para preservar el valor original antes de la estandarización).

## Descripción de Funciones del Script PySpark (data_process.py)
El script está modularizado en funciones, cada una con una responsabilidad específica dentro del pipeline.

### 1. setup_environment(conf: OmegaConf) -> SparkSession
Tarea: Inicializa la sesión de Spark utilizando los parámetros definidos en la sección environment del archivo de configuración.

#### Acciones Clave: (H4)

	* Crea el nombre de la aplicación a partir del nombre del entorno.

	* Establece la configuración del maestro (ej. local[\*]).

	* Establece el nivel de log de Spark a "ERROR" para evitar verbosidad excesiva.

### 2. read_data(spark: SparkSession, conf: OmegaConf) -> DataFrame
Tarea: Carga los datos de entrada desde la ruta especificada.

#### Acciones Clave: (H4)

	* Lee los datos basándose en la configuración de input_data (ruta, formato, opciones).

	* Añade una columna (cuyo nombre se define en additional_fields.file) para capturar el nombre del archivo de origen (input_file_name()), extrayendo solo el nombre del archivo de la ruta completa mediante regex.

### 3. data_quality_input(df: DataFrame, conf: OmegaConf) -> bool
Tarea: Ejecuta las validaciones de calidad de datos para la entrada.

#### Acciones Clave: (H4)

	* Verifica que el conteo de filas (df.count()) sea mayor o igual a min_expected_rows.

	* Verifica que todas las required_columns estén presentes en el DataFrame.

	* Retorna True si todas las validaciones son exitosas, False en caso contrario.

### 4. transform_data(df: DataFrame, conf: OmegaConf) -> DataFrame
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

	* Aplica la conversión de unidades (treatment_units).

	* Calcula la columna total estándar (total_estandar) como cantidad_estandar * precio_estandar.

	* Renombra y ordena las columnas (rename_and_order_cols).

#### Funciones de Filtrado (Subprocesos de transform_data)
	* date_filter(df: DataFrame, conf: OmegaConf) -> DataFrame: Filtra las filas dentro del rango start_date y end_date.

	* country_filter(df: DataFrame, conf: OmegaConf) -> DataFrame: Filtra por el country_filter_value si este no es "TODOS".

	* delivery_filter(df: DataFrame, conf: OmegaConf) -> DataFrame: Filtra la data uniendo las filas que cumplen las condiciones de derived_cols.col1 o derived_cols.col2.

#### Funciones de Transformación (Subprocesos de transform_data)
	* derived_cols(df: DataFrame, conf: OmegaConf) -> DataFrame: Crea las columnas binarias de indicador (entrega_rutina, entrega_bonificada) usando la función when de Spark.

	* fix_nulls(df: DataFrame, conf: OmegaConf) -> DataFrame: Rellena los valores nulos (na.fill) según la configuración de data_filling para columnas de texto y numéricas.

	* treatment_units(df: DataFrame, conf: OmegaConf) -> DataFrame: Aplica la lógica de estandarización de unidades:

		- Si unidad es igual al value de conversión (ej. "CS"), aplica el factor:

			* cantidad_estandar = cantidad_origen * factor.

			* precio_estandar = precio_origen / factor.

		- Establece la unidad_estandar al new_value (ej. "ST").

	* rename_and_order_cols(df: DataFrame, conf: OmegaConf) -> DataFrame: Renombra las columnas originales a nombres de respaldo (ej. precio a precio_origen) y luego selecciona las columnas en el orden final definido en columns_order.

### 5. data_quality_output(df: DataFrame, conf: OmegaConf) -> bool
Tarea: Ejecuta las validaciones de calidad de datos para la salida.

#### Acciones Clave: (H4)

	* Verifica que las columnas listadas en data_quality.output.not_nulls no contengan ningún valor nulo después de las transformaciones.

	* Retorna True si todas las validaciones son exitosas, False en caso contrario.

### 6. write_data(df: DataFrame, conf: OmegaConf) -> None
Tarea: Escribe el DataFrame procesado en la ubicación de salida.

#### Acciones Clave:

	* Escribe los datos en modo "overwrite".

	* Utiliza el formato Parquet.

	* Particiona los datos según las columnas definidas en partition_columns.

	* La ruta de salida es una combinación de output_base_path y el nombre del entorno.

### 7. main() -> None
Tarea: Función principal que gestiona el flujo de ejecución.

#### Acciones Clave:

	* Carga la configuración (OmegaConf.load).

	* Sigue el flujo secuencial: setup_environment -> read_data.

	* Aplica data_quality_input y solo procede a la transformación si se aprueba.

	* Si se aprueba, aplica transform_data.

	* Aplica data_quality_output y solo procede a la escritura si se aprueba.

	* Si se aprueba, aplica write_data.

	* Detiene la sesión de Spark al finalizar o en caso de error (finally).