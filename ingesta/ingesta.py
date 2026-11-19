
import os
from pathlib import Path

import pandas as pd
import yfinance as yf
import snowflake.connector


# Configuración de la extracción
SYMBOL = "AAPL"
START_DATE = os.getenv("START_DATE", "2010-07-01")
END_DATE = os.getenv("END_DATE", "2026-10-01")

# Directorio raíz del proyecto
PROJECT_ROOT = Path(__file__).resolve().parents[1]
RAW_DIR = PROJECT_ROOT / "data" / "raw"
# Ruta opcional proporcionada por Kestra para almacenar el CSV
OUTPUT_CSV_PATH = os.getenv("OUTPUT_CSV_PATH")

def extract_data():
    """Descarga los precios históricos de cierre de Yahoo Finance."""

    print(f"Iniciando descarga de datos para {SYMBOL}...")

    data = yf.download(
        SYMBOL,
        start=START_DATE,
        end=END_DATE,
        auto_adjust=False,
        progress=False
    )

    if data.empty:
        raise ValueError("Yahoo Finance no devolvió datos.")

    close = data["Close"]

    # yfinance puede devolver columnas MultiIndex
    if isinstance(close, pd.DataFrame):
        close = close[SYMBOL]

    close = close.dropna().sort_index()

    result = close.rename("CLOSE").to_frame()
    result.index.name = "DATE"
    result = result.reset_index()

    result["SYMBOL"] = SYMBOL
    result["SOURCE"] = "Yahoo Finance"

    print(f"Registros descargados: {len(result)}")

    return result


def save_raw_csv(data):
    """Guarda los datos extraídos como archivo CSV."""

    if OUTPUT_CSV_PATH:
        # Ejecución orquestada por Kestra
        output_path = Path(OUTPUT_CSV_PATH)
    else:
        # Ejecución manual desde el proyecto local
        output_path = RAW_DIR / "aapl_daily.csv"

    output_path.parent.mkdir(parents=True, exist_ok=True)

    data.to_csv(output_path, index=False)

    print(f"Archivo CSV guardado en: {output_path}")

def load_to_snowflake(data):
    """Carga los registros en RAW utilizando staging y MERGE."""

    print("Conectando con Snowflake...")

    conn = snowflake.connector.connect(
        account=os.environ["SNOWFLAKE_ACCOUNT"],
        user=os.environ["SNOWFLAKE_USER"],
        password=os.environ["SNOWFLAKE_PASSWORD"],
        role=os.environ["SNOWFLAKE_ROLE"],
        warehouse=os.environ["SNOWFLAKE_WAREHOUSE"],
        database=os.environ["SNOWFLAKE_DATABASE"],
        schema=os.environ["SNOWFLAKE_SCHEMA"]
    )

    cursor = conn.cursor()

    try:
        # 1. Crear una tabla temporal con la misma estructura que RAW
        cursor.execute("""
            CREATE OR REPLACE TEMPORARY TABLE AAPL_DAILY_STAGE
            LIKE PSET2_DB.RAW.AAPL_DAILY
        """)

        # 2. Preparar los registros para la carga
        rows = [
            (
                row.SYMBOL,
                pd.Timestamp(row.DATE).date(),
                float(row.CLOSE),
                row.SOURCE
            )
            for row in data.itertuples(index=False)
        ]

        # 3. Insertar los registros en la tabla temporal
        cursor.executemany("""
            INSERT INTO AAPL_DAILY_STAGE
                (SYMBOL, DATE, CLOSE, SOURCE, INGESTED_AT)
            VALUES (%s, %s, %s, %s, CURRENT_TIMESTAMP())
        """, rows)

        print(f"Registros cargados en staging: {len(rows)}")

        # 4. Insertar o actualizar los registros en RAW
        cursor.execute("""
            MERGE INTO PSET2_DB.RAW.AAPL_DAILY AS target
            USING AAPL_DAILY_STAGE AS source
                ON target.SYMBOL = source.SYMBOL
                AND target.DATE = source.DATE

            WHEN MATCHED THEN UPDATE SET
                target.CLOSE = source.CLOSE,
                target.SOURCE = source.SOURCE,
                target.INGESTED_AT = source.INGESTED_AT

            WHEN NOT MATCHED THEN INSERT (
                SYMBOL,
                DATE,
                CLOSE,
                SOURCE,
                INGESTED_AT
            )
            VALUES (
                source.SYMBOL,
                source.DATE,
                source.CLOSE,
                source.SOURCE,
                source.INGESTED_AT
            )
        """)

        conn.commit()

        print("MERGE ejecutado correctamente.")

        # 5. Consultar el total de registros en RAW
        cursor.execute("""
            SELECT COUNT(*)
            FROM PSET2_DB.RAW.AAPL_DAILY
        """)

        total = cursor.fetchone()[0]

        print(f"Total de registros en Snowflake RAW: {total}")

    finally:
        cursor.close()
        conn.close()


def main():
    data = extract_data()
    save_raw_csv(data)
    load_to_snowflake(data)

    print("Proceso de ingesta finalizado correctamente.")


if __name__ == "__main__":
    main()
