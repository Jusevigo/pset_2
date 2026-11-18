
import os
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


ACCOUNT = required_env("SNOWFLAKE_ACCOUNT")
USER = required_env("SNOWFLAKE_USER")
PASSWORD = required_env("SNOWFLAKE_PASSWORD")
ROLE = required_env("SNOWFLAKE_ROLE")
WAREHOUSE = required_env("SNOWFLAKE_WAREHOUSE")
DATABASE = required_env("SNOWFLAKE_DATABASE")

# Normalizar el identificador de cuenta para construir la URL JDBC
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
    "schema": "OBT",
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
# 2. Función de lectura desde Snowflake
# --------------------------------------------------

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
# 3. Construcción de la OBT con Spark
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

        # Leer las tres tablas dimensionales de Gold
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

        # Contar los registros de la tabla de hechos
        total_fact = fact.count()

        print(f"Registros en Gold Fact: {total_fact}")

        # Unir hechos y dimensiones utilizando PySpark
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

        # Validaciones antes de cargar
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
        # 4. Escritura de la OBT en Snowflake
        # --------------------------------------------------

        target_table = f"{DATABASE}.OBT.AAPL_DAILY_OBT"

        print(f"Escribiendo tabla: {target_table}")

        (
            obt.coalesce(1)
            .write
            .format("jdbc")
            .option("url", JDBC_URL)
            .option("dbtable", target_table)
            .option("user", USER)
            .option("password", PASSWORD)
            .option("driver", JDBC_DRIVER)
            .option("batchsize", "1000")
            .mode("overwrite")
            .save()
        )

        # --------------------------------------------------
        # 5. Validación de la tabla escrita
        # --------------------------------------------------

        result = read_snowflake_table(
            spark,
            target_table
        )

        total_written = result.count()

        print(f"Registros escritos en OBT: {total_written}")

        if total_written != total_obt:
            raise ValueError(
                "La cantidad de registros escritos no coincide "
                "con la OBT construida."
            )

        print("OBT construida y validada correctamente.")

    finally:
        spark.stop()


if __name__ == "__main__":
    main()
