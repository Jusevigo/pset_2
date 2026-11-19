# PSet 2 — Pipeline de Ingeniería de Datos para AAPL

## 1. Descripción

Este repositorio implementa un pipeline ELT para procesar datos históricos de precios diarios de Apple Inc. (AAPL), obtenidos de Yahoo Finance.

La ingesta se realiza con Python y Kestra. dbt construye las capas Bronze, Silver y Gold en Snowflake, y Apache Spark genera una One Big Table (OBT) a partir del modelo dimensional Gold.

El resultado final se almacena en Snowflake y queda preparado para las siguientes etapas de análisis y modelado.

## 2. Flujo del pipeline

```text
Yahoo Finance
      |
      v
    Kestra
      |
      v
Python / Ingesta
      |
      v
Snowflake RAW
      |
      v
    dbt Core
      |
      v
   Bronze
      |
      v
   Silver
      |
      v
   Gold
      |
      v
  Apache Spark
      |
      v
Snowflake OBT
```

Kestra controla la ejecución secuencial de cinco tareas:

1. `ingest_aapl`: descarga los datos y los carga en RAW.
2. `dbt_debug`: comprueba la conexión con Snowflake.
3. `dbt_run`: construye Bronze, Silver y Gold.
4. `dbt_test`: ejecuta las pruebas de calidad.
5. `spark_build_obt`: construye y publica la OBT.

## 3. Estructura del repositorio

```text
pset_2/
├── .devcontainer/
│   └── devcontainer.json
├── .dockerignore
├── Dockerfile
├── docker-compose.yml
├── requirements.txt
├── .env.example
├── scripts/
│   ├── prepare_env.py
│   └── initialize_snowflake.sql
├── ingesta/
│   └── ingesta.py
├── kestra/
│   └── flows/
│       └── aapl_ingestion.yml
├── dbt/
│   ├── Dockerfile
│   ├── dbt_project.yml
│   ├── profiles.yml
│   ├── macros/
│   ├── models/
│   └── tests/
├── spark/
│   ├── Dockerfile
│   └── jobs/
│       └── build_obt.py
└── README.md
```

## 4. Requisitos y configuración

### 4.1. Entorno

El proyecto está preparado para ejecutarse mediante GitHub Codespaces y Docker-in-Docker.

También puede ejecutarse localmente utilizando Docker Desktop y Docker Compose.

### 4.2. Configuración de secretos

Antes de crear un Codespace, configurar los siguientes Repository Secrets en:

GitHub → Repository → Settings → Secrets and variables → Codespaces.

| Secreto | Descripción |
|---|---|
| `SNOWFLAKE_ACCOUNT` | Identificador de la cuenta Snowflake. |
| `SNOWFLAKE_USER` | Usuario de Snowflake. |
| `SNOWFLAKE_PASSWORD` | Contraseña de Snowflake. |
| `POSTGRES_PASSWORD` | Contraseña de PostgreSQL. |
| `KESTRA_BASIC_AUTH_PASSWORD` | Contraseña de acceso a Kestra. |

El script `scripts/prepare_env.py` genera automáticamente los archivos `.env` y `.env_encoded` a partir de las variables disponibles en el entorno.

Ejecutar desde la raíz del repositorio:

```bash
python scripts/prepare_env.py
```

El archivo `.env.example` contiene los nombres y valores predeterminados de las variables no sensibles.

Los archivos `.env` y `.env_encoded` son sensibles y están excluidos de Git. Los valores Snowflake utilizados por Kestra se codifican en Base64; esta codificación no constituye cifrado.

## 5. Inicialización de Snowflake

Antes de ejecutar el pipeline, se deben preparar los objetos de Snowflake.

Abrir Snowflake Worksheets, seleccionar el rol `ACCOUNTADMIN` y ejecutar el contenido de:

```text
scripts/initialize_snowflake.sql
```

El script prepara:

- Rol `PSET2_ROLE`.
- Warehouse `COMPUTE_WH`.
- Base de datos `PSET2_DB`.
- Esquema `RAW`.
- Tabla `PSET2_DB.RAW.AAPL_DAILY`.
- Permisos de acceso necesarios.

Si el rol aún no está asignado al usuario de Snowflake, ejecutar adicionalmente:

```sql
GRANT ROLE PSET2_ROLE TO USER <SNOWFLAKE_USER>;
```

El script puede ejecutarse nuevamente sin reemplazar los objetos existentes, gracias a las cláusulas `IF NOT EXISTS`.

Los esquemas Bronze, Silver, Gold y OBT se preparan durante las transformaciones y el procesamiento posterior.

## 6. Construcción e inicio de la infraestructura

Desde la terminal, en la raíz del repositorio, generar primero los archivos de entorno:

```bash
python scripts/prepare_env.py
```

Construir la imagen de ingesta:

```bash
docker build -t pset2-ingesta:1.0 .
```

Construir las imágenes de dbt y Spark:

```bash
docker compose build dbt spark
```

Validar la configuración:

```bash
docker compose config --quiet
```

Iniciar los servicios:

```bash
docker compose up -d
```

La infraestructura incluye:

| Servicio | Función |
|---|---|
| PostgreSQL 18 | Almacenamiento interno de Kestra. |
| Kestra 2.0.7 | Orquestación del pipeline. |
| dbt Core 1.12.7 | Transformaciones SQL. |
| Apache Spark 4.3.0 | Construcción de la OBT. |

Comprobar el estado:

```bash
docker compose ps
```

La interfaz de Kestra está disponible en el puerto 8080:

```text
http://localhost:8080
```

Las imágenes personalizadas incorporan sus respectivos códigos y dependencias, por lo que las tareas de Kestra no dependen de volúmenes de código locales.

## 7. Configuración y ejecución de Kestra

El flujo principal se encuentra en:

```text
kestra/flows/aapl_ingestion.yml
```

Para registrarlo en Kestra:

1. Abrir la interfaz web.
2. Entrar en el namespace `pset2`.
3. Crear o importar el flujo `aapl_ingestion`.
4. Incorporar la definición del archivo YAML del repositorio.
5. Guardar el flujo.

La ejecución manual permite especificar las fechas de extracción.

Valores predeterminados:

| Parámetro | Valor |
|---|---|
| `start_date` | `2010-07-01` |
| `end_date` | `2026-10-01` |

La fecha final es exclusiva.

Para iniciar el procesamiento, utilizar el botón **Execute** en la interfaz de Kestra.

El flujo también cuenta con una programación automática de lunes a viernes a las 18:00, hora de Ecuador.

La política de reintentos exponenciales está configurada para las tareas de procesamiento que pueden experimentar fallos transitorios.

El archivo CSV generado durante la ingesta se conserva como archivo de salida de la tarea mediante `outputFiles` de Kestra.

## 8. Validación del resultado

### 8.1. Ejecución de Kestra

Una ejecución satisfactoria debe completar las siguientes tareas:

- `ingest_aapl`
- `dbt_debug`
- `dbt_run`
- `dbt_test`
- `spark_build_obt`

### 8.2. Datos en Snowflake

Los datos originales se almacenan en:

```text
PSET2_DB.RAW.AAPL_DAILY
```

Las capas de transformación se almacenan en:

```text
PSET2_DB.BRONZE
PSET2_DB.SILVER
PSET2_DB.GOLD
```

La tabla final se encuentra en:

```text
PSET2_DB.OBT.AAPL_DAILY_OBT
```

### 8.3. Consulta de validación de la OBT

Ejecutar en Snowflake:

```sql
SELECT
    COUNT(*) AS TOTAL_REGISTROS,
    COUNT(DISTINCT DATE_KEY) AS FECHAS_UNICAS,
    COUNT(DISTINCT SYMBOL_KEY) AS SIMBOLOS,
    MIN(TRADE_DATE) AS FECHA_MINIMA,
    MAX(TRADE_DATE) AS FECHA_MAXIMA,
    COUNT_IF(
        CLOSE_PRICE IS NULL OR CLOSE_PRICE <= 0
    ) AS PRECIOS_INVALIDOS
FROM PSET2_DB.OBT.AAPL_DAILY_OBT;
```

Resultados obtenidos para el período histórico validado:

| Indicador | Resultado |
|---|---:|
| Registros | 4.087 |
| Fechas únicas | 4.087 |
| Símbolos | 1 |
| Fecha mínima | 2010-07-01 |
| Fecha máxima | 2026-09-30 |
| Precios inválidos | 0 |
| Duplicados por fecha y símbolo | 0 |

Spark realiza una validación de la cantidad de registros y del grano de la OBT antes y después de publicar la tabla.

La tabla de staging se elimina después de una publicación satisfactoria.

## 9. Seguridad

- No versionar `.env` ni `.env_encoded`.
- No incluir contraseñas directamente en los scripts.
- Mantener los secretos de GitHub Codespaces y Snowflake fuera del repositorio.
- No publicar credenciales en capturas de pantalla o logs.
- Mantener excluidos los archivos temporales y las cachés generadas automáticamente.
- No eliminar los volúmenes persistentes de Docker sin verificar previamente su contenido.
