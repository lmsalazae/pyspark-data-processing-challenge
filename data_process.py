from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, to_date, lit, when, upper, count, round, input_file_name, regexp_extract
from omegaconf import OmegaConf
import sys
import os

def setup_environment(conf: OmegaConf) -> SparkSession:
    """
    Crea la sesion Spark con los parametros de la configuracion.
    """
    app_name = f"DataProcess_{conf.environment.name}"
    spark = SparkSession.builder \
        .appName(app_name) \
        .master(conf.environment.master) \
        .getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    print(f"--- Sesión Spark iniciada en entorno: {conf.environment.name} ---")
    return spark

def read_data(spark: SparkSession, conf: OmegaConf) -> DataFrame:
    """
    Lee datos del directorio configurado.
    """
    data_config = conf.input_data
    file_path = data_config.file_path
    add_field_file = conf.additional_fields.file
    try:
        df = spark.read \
            .format(data_config.file_format) \
            .options(**data_config.options) \
            .load(file_path)
        # Captura de nombre de archivo
        df = df.withColumn(add_field_file, input_file_name())
        df = df.withColumn(add_field_file, regexp_extract(col(add_field_file), r'[^/]+$', 0))
        print(f"Datos cargados exitosamente desde: {file_path}")
        return df
    except Exception as e:
        print(f"ERROR: No se pudo cargar datos: {file_path}. Deteniendo Spark. {e}")
        spark.stop()
        sys.exit(1)

def date_filter(df: DataFrame, conf: OmegaConf) -> DataFrame:
    """
    Filtra los datos entre dos fechas dadas
    """
    date_col = conf.run_parameters.date_filter_column
    start_date = conf.run_parameters.start_date
    end_date = conf.run_parameters.end_date
    df_filtered = df.filter(
        ((col(date_col) >= lit(start_date).cast("date")) & 
        (col(date_col) <= lit(end_date).cast("date")))
    )
    print('Filtro fecha >>> OK')
    return df_filtered

def country_filter(df: DataFrame, conf: OmegaConf) -> DataFrame:
    """
    Filtra los datos por el pais
    """
    country_col = conf.run_parameters.country_filter_column
    country_val = conf.run_parameters.country_filter_value
    if country_val.upper() == "TODOS":
        df_filtered = df
    else:
        df_filtered = df.filter(col(country_col) == lit(country_val))
    print('Filtro pais >>> OK')
    return df_filtered

def delivery_filter(df: DataFrame, conf: OmegaConf) -> DataFrame:
    """
    Filtra los datos por condiciones especificas del campo tipo_entrega
    """
    # Configuracion de parametros para filtros
    delivery_col1 = conf.derived_cols.col1.source
    condition_filter_col1 = list(set(conf.derived_cols.col1.conditions))
    delivery_col2 = conf.derived_cols.col2.source
    condition_filter_col2 = list(set(conf.derived_cols.col2.conditions))
    # Aplicando los filtros independientemente y uniendo los resultados
    df_filtered1 = df.filter(
            upper(col(delivery_col1)).isin(condition_filter_col1)
    )
    df_filtered2 = df.filter(
            upper(col(delivery_col2)).isin(condition_filter_col2)
    )
    df_filtered = df_filtered1.unionByName(df_filtered2)
    print('Filtro tipo entrega >>> OK')
    # df_filtered.show()
    return df_filtered

def derived_cols(df: DataFrame, conf: OmegaConf) -> DataFrame:
    """
    Crea dos columnas nuevas desde una columna existente y unas condiciones
    """
    # Configuracion de parametros para filtros
    delivery_col1 = conf.derived_cols.col1.source
    condition_filter_col1 = list(set(conf.derived_cols.col1.conditions))
    new_col1_name = conf.derived_cols.col1.name
    delivery_col2 = conf.derived_cols.col2.source
    condition_filter_col2 = list(set(conf.derived_cols.col2.conditions))
    new_col2_name = conf.derived_cols.col2.name
    # Generacion de nuevas columnas
    df_rules = df.withColumn(
        new_col1_name, 
        when(upper(col(delivery_col1)).isin(condition_filter_col1) , lit(1))
        .otherwise(lit(0))
    )
    df_rules = df_rules.withColumn(
        new_col2_name, 
        when(upper(col(delivery_col2)).isin(condition_filter_col2) , lit(1))
        .otherwise(lit(0))
    )
    print("Columnas derivadas de tipo entrega >>> OK")
    # df_rules.show(100)
    return df_rules

def fix_nulls(df: DataFrame, conf: OmegaConf) -> DataFrame:
    """
    Rellena los valores nulos con valores dados desde la configuracion.
    """
    text_cols = list(set(conf.data_filling.text.columns))
    replace_text_value = conf.data_filling.text.value
    df_fill_text = df.na.fill(
        value=replace_text_value, 
        subset=text_cols
    )
    num_cols = list(set(conf.data_filling.number.columns))
    replace_num_value = conf.data_filling.number.value
    df_fill = df_fill_text.na.fill(
        value=replace_num_value, 
        subset=num_cols
    )
    print("Rellenado valores nulos >>> OK")
    return df_fill

def treatment_units(df: DataFrame, conf: OmegaConf) -> DataFrame:
    """
    Crea nuevas columnas aplicando reglas de negocio.
    """
    # Configuracion de parametros
    quantity_new_name = conf.unit_conversion.quantity.new_name
    quantity_name = conf.unit_conversion.quantity.name
    price_new_name = conf.unit_conversion.price.new_name
    price_name = conf.unit_conversion.price.name
    unit_new_name = conf.unit_conversion.unit.new_name
    unit_new_value = conf.unit_conversion.unit.new_value
    unit_name = conf.unit_conversion.unit.name
    unit_value = conf.unit_conversion.unit.value
    unit_factor = conf.unit_conversion.unit.factor
    # Generacion de nuevas columnas
    df_fix = df.withColumn(
        quantity_new_name, 
        when(upper(col(unit_name)) == unit_value , col(quantity_name)*unit_factor)
        .otherwise(col(quantity_name))
    )
    df_fix = df_fix.withColumn(
        price_new_name, 
        when(upper(col(unit_name)) == unit_value , round(col(price_name)/unit_factor,2))
        .otherwise(col(price_name))
    )
    df_fix = df_fix.withColumn(
        unit_new_name, 
        lit(unit_new_value)
    )
    print("Unidad, cantidad y precio ajustadas >>> OK")
    return df_fix

def rename_and_order_cols(df: DataFrame, conf: OmegaConf) -> DataFrame:
    """
    Renombra y ordena las columnas del dataframe
    """
    # Renombrado de columnas
    columns_rename = dict(conf.columns_config.columns_rename)
    select_expressions = []
    for column in df.columns:
        if column in columns_rename:
            new_name = columns_rename[column]
            select_expressions.append(col(column).alias(new_name))
            print(f"Renombrado: '{column}' a '{new_name}'")
        else:
            select_expressions.append(col(column))
    df_rename = df.select(*select_expressions)
    # Ordenamiento de las columnas
    columns_order = list(conf.columns_config.columns_order)
    df_ordered = df_rename.select(*columns_order)
    print("Renombrado y ordenamiento de columnas >>> OK")
    return df_ordered

def transform_data(df: DataFrame, conf: OmegaConf) -> DataFrame:
    """
    Agrupa la aplicacion de filtros, las reglas de negocio y otras transformaciones sobre los datos
    """
    date_col = conf.run_parameters.date_filter_column
    # Eliminando registros duplicados
    print("Eliminando registros duplicados...")
    df_dedup = df.dropDuplicates()
    # Conversión de fecha (asumiendo formato yyyyMMdd)
    df_formated = df_dedup.withColumn(date_col, to_date(col(date_col), "yyyyMMdd"))
    print("Aplicando Filtros...")
    # Filtro rango de fechas
    df_date = date_filter(df_formated, conf)
    # Filtro país
    df_country = country_filter(df_date, conf)
    # Filtro tipo_entrega
    df_delivery = delivery_filter(df_country, conf)
    print(f"Registros originales: {df.count()} | Registros filtrados: {df_delivery.count()}")
    # Creacion nuevas columnas
    new_df = derived_cols(df_delivery, conf)
    # Rellenado de valores nulos
    df_fix = fix_nulls(new_df, conf)
    # Conversion de unidades
    df_units = treatment_units(df_fix, conf)
    quantity_new_name = conf.unit_conversion.quantity.new_name
    price_new_name = conf.unit_conversion.price.new_name
    add_field_total = conf.additional_fields.total
    df_units = df_units.withColumn(add_field_total, col(quantity_new_name)*col(price_new_name))
    df_ordered = rename_and_order_cols(df_units, conf)
    df_ordered.show(10, truncate=False)
    return df_ordered

def data_quality_input(df: DataFrame, conf: OmegaConf) -> bool:
    """
    Ejecuta las validaciones de DQ sobre los datos de entrada.
    """
    dq_config = conf.data_quality.input
    # Cantidad minima de registros esperados
    actual_rows = df.count()
    min_expected = dq_config.min_expected_rows
    print("Ejecutando Comprobación de Calidad de Datos...")
    result_dq = []
    if actual_rows < min_expected:
        print(f"[DQ INPUT FALLIDA] Conteo bajo: {actual_rows} filas. Se esperaban al menos {min_expected}")
        result_dq.append(False)
    else:
        print(f"[DQ INPUT APROBADA] Conteo de filas: {actual_rows}")
        result_dq.append(True)
    # Columnas requeridas
    actual_columns = set(df.columns)
    required_columns = set(dq_config.required_columns)
    missing_columns = required_columns - actual_columns
    if missing_columns:
        print(f"[DQ INPUT FALLIDA] Columnas faltantes: {missing_columns}")
        result_dq.append(False)
    else:
        print("[DQ INPUT APROBADA] Todas las columnas requeridas están presentes")
        result_dq.append(True)
    result = False in result_dq
    return not result

def data_quality_output(df: DataFrame, conf: OmegaConf) -> bool:
    """
    Ejecuta las validaciones de DQ sobre los datos de salida
    """
    dq_config = conf.data_quality.output
    not_nulls_cols = set(dq_config.not_nulls)
    result_dq = []
    # Validando columnas sin nulos
    for col_name in not_nulls_cols:
        null_count = df.filter(col(col_name).isNull()).count()
        if null_count > 0:
            print(f"[DQ OUTPUT FALLIDA]: La columna '{col_name}' contiene {null_count} valores nulos.")
            result_dq.append(False)
        else:
            print(f"[DQ OUTPUT APROBADA]: La columna '{col_name}' no contiene valores nulos.")
            result_dq.append(True)
    result = False in result_dq
    return not result

def write_data(df: DataFrame, conf: OmegaConf) -> None:
    """
    Escribe el DataFrame resultante en la ruta configurada.
    """
    partition_cols = conf.run_parameters.partition_columns
    output_path = os.path.join(conf.run_parameters.output_base_path,conf.environment.name)
    print(f"Escribiendo datos en: {output_path}")
    try:
        df.write \
            .mode("overwrite") \
            .partitionBy(*partition_cols) \
            .parquet(output_path)
        print(f"Escritura exitosa.")
    except Exception as e:
        print(f"ERROR: No se pudo escribir el resultado: {e}")
        raise 

def main() -> None:
    """
    Funcion principal que orquesta el pipeline de datos.
    """
    spark = None
    try:
        # Cargar la configuración
        conf = OmegaConf.load("config.yaml")
        # Inicializacion
        spark = setup_environment(conf)
        # Lectura
        df_raw = read_data(spark, conf)
        # Calidad de Datos de entrada
        if data_quality_input(df_raw, conf):
            # Transformación y Filtrado
            df_processed = transform_data(df_raw, conf)
            # Calidad de Datos de salida
            if data_quality_output(df_processed, conf):
                # Escritura (Solo si DQ es APROBADA)
                write_data(df_processed, conf)
            else:
                print("ERROR: Se encontró una falla en la Calidad de Datos de salida")
        else:
            print("ERROR: Se encontró una falla en la Calidad de Datos de entrada")
    except Exception as e:
        print(f"ERROR FATAL en el pipeline: {e}")
        sys.exit(1)
    finally:
        if spark:
            spark.stop()
            print("Sesion Spark detenida")

if __name__ == "__main__":
    main()
