
import os
import re
from urllib.parse import urlencode

from pyspark.sql import SparkSession, functions as F


# --------------------------------------------------
# 1. Configuración de Snowflake
# --------------------------------------------------

def required_env(name):
    value = os.environ.get(name)

    if not value:
        raise ValueError(f"No se encontró la variable de entorno {name}")

    return value


def validate_identifier(value, name):
    value = value.strip().upper()

    if not re.fullmatch(r"[A-Z_][A-Z0-9_$]*", value):
        raise ValueError(f"Identificador Snowflake inválido: {name}")

    return value


ACCOUNT = required_env("SNOWFLAKE_ACCOUNT")
USER = required_env("SNOWFLAKE_USER")
PASSWORD = required_env("SNOWFLAKE_PASSWORD")
ROLE = required_env("SNOWFLAKE_ROLE")
WAREHOUSE = required_env("SNOWFLAKE_WAREHOUSE")
DATABASE = validate_identifier(
    required_env("SNOWFLAKE_DATABASE"),
    "SNOWFLAKE_DATABASE"
)

OBT_SCHEMA = "OBT"
TARGET_TABLE = f"{DATABASE}.{OBT_SCHEMA}.AAPL_DAILY_OBT"
STAGING_TABLE = f"{DATABASE}.{OBT_SCHEMA}.AAPL_DAILY_OBT_STG"

# Normalizar el identificador de cuenta
account_host = ACCOUNT.strip()
account_host = account_host.removeprefix("https://")
account_host = account_host.removeprefix("http://")
account_host = account_host.split("/")[0]

suffix = ".snowflakecomputing.com"

if account_host.lower().endswith(suffix):
    account_host = account_host[:-len(suffix)]

connection_params = urlencode({
    "warehouse": WAREHOUSE,
    "db": DATABASE,
    "schema": OBT_SCHEMA,
    "role": ROLE
})

JDBC_URL = (
    f"jdbc:snowflake://{account_host}.snowflakecomputing.com/"
    f"?{connection_params}"
)

JDBC_DRIVER = "net.snowflake.client.jdbc.SnowflakeDriver"

JDBC_PROPERTIES = {
    "user": USER,
    "password": PASSWORD,
    "driver": JDBC_DRIVER,
    "fetchsize": "1000"
}


# --------------------------------------------------
# 2. Funciones JDBC
# --------------------------------------------------


def execute_snowflake_sql(spark, sql):
    """Ejecuta SQL utilizando el registro JDBC de Spark."""

    jvm = spark.sparkContext._jvm

    connection = None
    statement = None

    try:
        # Obtener el objeto DriverRegistry de Spark
        registry_class = getattr(
            jvm.org.apache.spark.sql.execution.datasources.jdbc,
            "DriverRegistry$"
        )

        registry = getattr(registry_class, "MODULE$")

        # Registrar el driver utilizando el classloader de Spark
        registry.register(JDBC_DRIVER)

        # Obtener la instancia del driver registrada
        driver = registry.get(JDBC_DRIVER)

        # Preparar las propiedades de conexión
        properties = jvm.java.util.Properties()
        properties.setProperty("user", USER)
        properties.setProperty("password", PASSWORD)

        # Crear la conexión directamente mediante el driver
        connection = driver.connect(JDBC_URL, properties)

        if connection is None:
            raise RuntimeError(
                "El driver JDBC de Snowflake no pudo establecer "
                "la conexión."
            )

        statement = connection.createStatement()
        statement.execute(sql)

        print("Sentencia SQL ejecutada correctamente.")

    finally:
        if statement is not None:
            statement.close()

        if connection is not None:
            connection.close()



def read_snowflake_table(spark, table_name):
    print(f"Leyendo tabla: {table_name}")

    return (
        spark.read
        .format("jdbc")
        .option("url", JDBC_URL)
        .option("dbtable", table_name)
        .options(**JDBC_PROPERTIES)
        .load()
    )


# --------------------------------------------------
# 3. Preparación automática del destino
# --------------------------------------------------

def prepare_snowflake_destination(spark):

    print(f"Preparando esquema {DATABASE}.{OBT_SCHEMA}...")

    execute_snowflake_sql(
        spark,
        f"CREATE SCHEMA IF NOT EXISTS {DATABASE}.{OBT_SCHEMA}"
    )

    print("Preparando tabla de staging...")

    staging_ddl = f"""
    CREATE TRANSIENT TABLE IF NOT EXISTS {STAGING_TABLE} (
        DATE_KEY NUMBER(38,0),
        TRADE_DATE DATE,
        CALENDAR_YEAR NUMBER(38,0),
        CALENDAR_QUARTER NUMBER(38,0),
        MONTH_NUMBER NUMBER(38,0),
        MONTH_NAME VARCHAR(20),
        DAY_OF_MONTH NUMBER(38,0),
        DAY_OF_WEEK_ISO NUMBER(38,0),
        DAY_NAME VARCHAR(20),
        SYMBOL_KEY NUMBER(38,0),
        SYMBOL VARCHAR(10),
        CLOSE_PRICE FLOAT,
        SOURCE VARCHAR(100),
        INGESTED_AT TIMESTAMP_NTZ
    )
    """

    execute_snowflake_sql(spark, staging_ddl)

    # Limpiar datos de una ejecución anterior
    execute_snowflake_sql(
        spark,
        f"TRUNCATE TABLE {STAGING_TABLE}"
    )

    print("Destino preparado correctamente.")


# --------------------------------------------------
# 4. Construcción de la OBT con Spark
# --------------------------------------------------

def main():

    spark = (
        SparkSession.builder
        .appName("PSet2-AAPL-OBT")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )

    try:
        print("Iniciando construcción de la OBT...")

        # Leer las tres tablas de Gold
        fact = read_snowflake_table(
            spark,
            f"{DATABASE}.GOLD.FACT_AAPL_DAILY"
        )

        dim_date = read_snowflake_table(
            spark,
            f"{DATABASE}.GOLD.DIM_DATE"
        )

        dim_symbol = read_snowflake_table(
            spark,
            f"{DATABASE}.GOLD.DIM_SYMBOL"
        )

        total_fact = fact.count()

        print(f"Registros en Gold Fact: {total_fact}")

        # Construir la OBT mediante PySpark
        obt = (
            fact.alias("f")

            .join(
                dim_date.alias("d"),
                F.col("f.DATE_KEY") == F.col("d.DATE_KEY"),
                "inner"
            )

            .join(
                dim_symbol.alias("s"),
                F.col("f.SYMBOL_KEY") == F.col("s.SYMBOL_KEY"),
                "inner"
            )

            .select(
                F.col("f.DATE_KEY").alias("DATE_KEY"),
                F.col("d.FULL_DATE").alias("TRADE_DATE"),
                F.col("d.CALENDAR_YEAR").alias("CALENDAR_YEAR"),
                F.col("d.CALENDAR_QUARTER").alias("CALENDAR_QUARTER"),
                F.col("d.MONTH_NUMBER").alias("MONTH_NUMBER"),
                F.col("d.MONTH_NAME").alias("MONTH_NAME"),
                F.col("d.DAY_OF_MONTH").alias("DAY_OF_MONTH"),
                F.col("d.DAY_OF_WEEK_ISO").alias("DAY_OF_WEEK_ISO"),
                F.col("d.DAY_NAME").alias("DAY_NAME"),
                F.col("f.SYMBOL_KEY").alias("SYMBOL_KEY"),
                F.col("s.SYMBOL").alias("SYMBOL"),
                F.col("f.CLOSE_PRICE").alias("CLOSE_PRICE"),
                F.col("f.SOURCE").alias("SOURCE"),
                F.col("f.INGESTED_AT").alias("INGESTED_AT")
            )
        )

        # Validar grano antes de escribir
        total_obt = obt.count()

        duplicate_keys = (
            obt.groupBy("DATE_KEY", "SYMBOL_KEY")
            .count()
            .filter(F.col("count") > 1)
            .count()
        )

        print(f"Registros después de los joins: {total_obt}")
        print(f"Combinaciones duplicadas: {duplicate_keys}")

        if total_obt != total_fact:
            raise ValueError(
                "La cantidad de registros de la OBT no coincide "
                "con la tabla de hechos."
            )

        if duplicate_keys > 0:
            raise ValueError(
                "Se encontraron duplicados en el grano de la OBT."
            )

        # --------------------------------------------------
        # 5. Preparar staging y escribir los datos
        # --------------------------------------------------

        prepare_snowflake_destination(spark)

        print(f"Escribiendo datos en staging: {STAGING_TABLE}")

        (
            obt.coalesce(1)
            .write
            .format("jdbc")
            .option("url", JDBC_URL)
            .option("dbtable", STAGING_TABLE)
            .option("user", USER)
            .option("password", PASSWORD)
            .option("driver", JDBC_DRIVER)
            .option("batchsize", "1000")
            .mode("append")
            .save()
        )

        # Comprobar la carga antes de publicar
        staging = read_snowflake_table(spark, STAGING_TABLE)
        total_staging = staging.count()

        print(f"Registros en staging: {total_staging}")

        if total_staging != total_obt:
            raise ValueError(
                "La cantidad de registros en staging no coincide "
                "con la OBT construida."
            )

        # --------------------------------------------------
        # 6. Publicar la OBT definitiva
        # --------------------------------------------------

        print(f"Publicando tabla definitiva: {TARGET_TABLE}")

        execute_snowflake_sql(
            spark,
            f"""
            CREATE OR REPLACE TABLE {TARGET_TABLE}
            COPY GRANTS
            AS
            SELECT *
            FROM {STAGING_TABLE}
            """
        )

        # --------------------------------------------------
        # 7. Validación final
        # --------------------------------------------------

        result = read_snowflake_table(spark, TARGET_TABLE)

        total_written = result.count()

        published_duplicates = (
            result.groupBy("DATE_KEY", "SYMBOL_KEY")
            .count()
            .filter(F.col("count") > 1)
            .count()
        )

        print(f"Registros escritos en OBT: {total_written}")
        print(f"Duplicados en OBT: {published_duplicates}")

        if total_written != total_obt:
            raise ValueError(
                "La cantidad de registros publicados no coincide "
                "con la OBT construida."
            )

        if published_duplicates > 0:
            raise ValueError(
                "Se encontraron duplicados en la tabla definitiva."
            )

        # Limpiar staging únicamente después de validar
        execute_snowflake_sql(
            spark,
            f"DROP TABLE IF EXISTS {STAGING_TABLE}"
        )

        print("OBT construida y validada correctamente.")

    finally:
        spark.stop()


if __name__ == "__main__":
    main()
