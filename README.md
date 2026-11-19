# PSet 2 — Pipeline ELT de datos financieros de AAPL

Proyecto de ingeniería de datos para extraer, cargar, transformar y consolidar datos históricos diarios de Apple Inc. (AAPL). La solución utiliza Yahoo Finance como fuente, Kestra para la orquestación, Snowflake como almacenamiento, dbt para las transformaciones y Apache Spark para construir la tabla final One Big Table (OBT).

## 1. Arquitectura

El flujo implementado es:

```text
Yahoo Finance
     |
     v
Python / yfinance
     |
     v
Kestra (orquestación)
     |
     v
Snowflake - RAW
     |
     v
dbt - BRONZE
     |
     v
dbt - SILVER
     |
     v
dbt - GOLD
     |
     v
Apache Spark
     |
     v
Snowflake - OBT
```

**Componentes principales**

| Componente | Responsabilidad |
|---|---|
| Yahoo Finance / yfinance | Fuente de precios históricos diarios |
| Python | Extracción y carga de datos |
| Kestra 2.0.7 | Orquestación de las tareas |
| PostgreSQL 18 | Base de datos de Kestra |
| Snowflake | Almacenamiento y procesamiento de datos |
| dbt Core 1.12.7 | Transformaciones SQL y pruebas de calidad |
| Apache Spark 4.3.0 | Integración de las tablas Gold y construcción de la OBT |
| Docker Compose | Administración del entorno de ejecución |

## 2. Estructura del repositorio

```text
.
├── .devcontainer/
│   └── devcontainer.json
├── data/
│   └── raw/
├── dbt/
│   ├── models/
│   │   ├── bronze/
│   │   ├── silver/
│   │   └── gold/
│   ├── tests/
│   ├── macros/
│   ├── dbt_project.yml
│   ├── profiles.yml
│   └── Dockerfile
├── docs/
├── ingesta/
│   └── ingesta.py
├── kestra/
│   └── flows/
│       └── aapl_ingestion.yml
├── scripts/
│   ├── initialize_snowflake.sql
│   └── prepare_env.py
├── spark/
│   ├── jobs/
│   │   └── build_obt.py
│   └── Dockerfile
├── .dockerignore
├── .env.example
├── .gitignore
├── docker-compose.yml
├── Dockerfile
└── requirements.txt
```

## 3. Requisitos previos

Antes de ejecutar el proyecto, se necesita:

- Git.
- Docker Desktop con Docker Compose.
- Python instalado para ejecutar el script de preparación del entorno.
- Una cuenta de Snowflake con permisos para configurar los recursos requeridos.
- Acceso a Internet para consultar Yahoo Finance y descargar las dependencias necesarias.

Se recomienda iniciar Docker Desktop antes de ejecutar los comandos siguientes.

## 4. Clonar el repositorio

```bash
git clone <URL_DEL_REPOSITORIO>
cd pset_2
```

## 5. Configurar las variables de entorno

El proyecto utiliza variables de entorno para evitar almacenar credenciales directamente en el código.

Primero, crear el archivo `.env` a partir de la plantilla:

**Windows PowerShell:**

```powershell
Copy-Item .env.example .env
```

**Linux / macOS:**

```bash
cp .env.example .env
```

Editar `.env` y completar las variables:

```dotenv
SNOWFLAKE_ACCOUNT=
SNOWFLAKE_USER=
SNOWFLAKE_PASSWORD=
SNOWFLAKE_ROLE=PSET2_ROLE
SNOWFLAKE_WAREHOUSE=COMPUTE_WH
SNOWFLAKE_DATABASE=PSET2_DB
SNOWFLAKE_SCHEMA=RAW

POSTGRES_PASSWORD=
KESTRA_BASIC_AUTH_USERNAME=admin@kestra.io
KESTRA_BASIC_AUTH_PASSWORD=
```

### Configuración de Snowflake

- `SNOWFLAKE_ACCOUNT`: identificador de cuenta utilizado para establecer la conexión. No debe incluir `https://` ni `.snowflakecomputing.com`.
- `SNOWFLAKE_USER`: usuario de inicio de sesión de Snowflake.
- `SNOWFLAKE_PASSWORD`: contraseña del usuario.
- `SNOWFLAKE_ROLE`: rol utilizado por el proyecto.
- `SNOWFLAKE_WAREHOUSE`: warehouse de procesamiento.
- `SNOWFLAKE_DATABASE`: base de datos del proyecto.
- `SNOWFLAKE_SCHEMA`: esquema de entrada de los datos.

**Importante:** el identificador de cuenta y el nombre de usuario son valores diferentes. Cada persona debe utilizar los correspondientes a su propia cuenta de Snowflake.

No compartir ni subir a GitHub los archivos `.env` o `.env_encoded`.

### Generar la configuración de secretos

Una vez completado `.env`, ejecutar:

```bash
python scripts/prepare_env.py
```

El script valida las variables requeridas, conserva los valores configurados y genera:

- `.env`: configuración local del proyecto.
- `.env_encoded`: variables codificadas en Base64 para su utilización como secretos en Kestra.

**Nota de seguridad:** Base64 es un mecanismo de codificación, no de cifrado. Los archivos generados contienen información sensible y deben permanecer fuera del repositorio.

## 6. Inicializar Snowflake

Antes de ejecutar el pipeline, deben existir los recursos necesarios en Snowflake.

1. Abrir una hoja de trabajo (*Worksheet*) en Snowflake.
2. Seleccionar el rol `ACCOUNTADMIN` o un rol administrativo con permisos equivalentes.
3. Ejecutar el contenido completo del archivo:

```text
scripts/initialize_snowflake.sql
```

El script crea, si no existen, los siguientes recursos:

| Recurso | Nombre |
|---|---|
| Rol | `PSET2_ROLE` |
| Warehouse | `COMPUTE_WH` |
| Base de datos | `PSET2_DB` |
| Esquema de entrada | `RAW` |
| Tabla de entrada | `AAPL_DAILY` |

También concede al rol del proyecto los permisos necesarios para utilizar el warehouse, acceder a la base de datos y operar sobre la tabla RAW.

### Asignar el rol al usuario de Snowflake

La asignación del rol al usuario debe realizarse por separado. En una hoja de trabajo con permisos administrativos, ejecutar:

```sql
GRANT ROLE PSET2_ROLE TO USER NOMBRE_DE_USUARIO;
```

Reemplazar `NOMBRE_DE_USUARIO` por el nombre exacto del usuario de inicio de sesión de Snowflake. No escribir literalmente el texto del ejemplo.

### Configurar las credenciales de conexión

En el archivo `.env` se deben configurar:

- `SNOWFLAKE_ACCOUNT`: identificador de cuenta de Snowflake.
- `SNOWFLAKE_USER`: usuario de inicio de sesión.
- `SNOWFLAKE_PASSWORD`: contraseña.
- `SNOWFLAKE_ROLE`: `PSET2_ROLE`.
- `SNOWFLAKE_WAREHOUSE`: `COMPUTE_WH`.
- `SNOWFLAKE_DATABASE`: `PSET2_DB`.
- `SNOWFLAKE_SCHEMA`: `RAW`.

El identificador de cuenta (`SNOWFLAKE_ACCOUNT`) no es el nombre de usuario (`SNOWFLAKE_USER`). Son valores independientes y deben corresponder a la cuenta y al usuario configurados en Snowflake.

Las transformaciones posteriores crean y utilizan los esquemas correspondientes a las capas Bronze, Silver, Gold y OBT.

## 7. Construir y levantar los servicios

Validar primero la configuración de Docker Compose:

```bash
docker compose config --quiet
```

Construir las imágenes:

```bash
docker compose build
```

Levantar los servicios:

```bash
docker compose up -d
```

Comprobar el estado de los contenedores:

```bash
docker compose ps
```

Los servicios principales son PostgreSQL, Kestra, dbt y Spark.

La interfaz web de Kestra está disponible en:

```text
http://localhost:8080
```

Ingresar con las credenciales definidas en `.env`:

- Usuario: `KESTRA_BASIC_AUTH_USERNAME`
- Contraseña: `KESTRA_BASIC_AUTH_PASSWORD`

## 8. Configurar y ejecutar el flujo en Kestra

El flujo se encuentra en:

```text
kestra/flows/aapl_ingestion.yml
```

Para registrarlo:

1. Abrir la interfaz web de Kestra.
2. Acceder a la sección de Flows.
3. Importar el archivo `aapl_ingestion.yml`.
4. Verificar el namespace `pset2` y el identificador `aapl_ingestion`.
5. Guardar el flujo.
6. Ejecutar manualmente el flujo o utilizar su programación configurada.

El flujo recibe dos parámetros:

| Parámetro | Valor predeterminado |
|---|---|
| `start_date` | `2010-07-01` |
| `end_date` | `2026-10-01` |

La fecha final es exclusiva.

### Etapas del flujo

| Tarea | Descripción |
|---|---|
| `ingest_aapl` | Descarga los datos de Yahoo Finance y realiza la carga idempotente en Snowflake RAW mediante MERGE. |
| `dbt_debug` | Verifica la conexión y configuración de dbt. |
| `dbt_run` | Ejecuta las transformaciones Bronze, Silver y Gold. |
| `dbt_test` | Ejecuta las pruebas de calidad de datos declaradas en dbt. |
| `spark_build_obt` | Construye, valida y publica la tabla OBT en Snowflake. |

El flujo está programado de lunes a viernes a las 18:00, con zona horaria `America/Guayaquil`.

## 9. Modelado y almacenamiento de datos

Las transformaciones se organizan en tres capas:

| Capa | Modelo | Función |
|---|---|---|
| Bronze | `BRONZE_AAPL_DAILY` | Selección de columnas y estandarización de tipos. |
| Silver | `SILVER_AAPL_DAILY` | Normalización de símbolos y filtrado de registros inválidos. |
| Gold | `DIM_DATE` | Dimensión temporal. |
| Gold | `DIM_SYMBOL` | Dimensión de activos. |
| Gold | `FACT_AAPL_DAILY` | Tabla de hechos con el precio de cierre y metadatos. |

dbt ejecuta las transformaciones directamente en Snowflake.

Posteriormente, Spark lee las tablas Gold, realiza las uniones por las claves de fecha y símbolo, y construye la tabla final:

```text
PSET2_DB.OBT.AAPL_DAILY_OBT
```

La OBT integra los atributos de calendario, la identificación del activo, el precio de cierre y los metadatos de procedencia.

## 10. Validación de resultados

El proyecto incorpora controles de calidad en dbt y Spark, entre ellos:

- Validación de valores nulos.
- Validación de unicidad.
- Validación de relaciones entre dimensiones y hechos.
- Validación de precios positivos.
- Validación del grano de la tabla de hechos.
- Comparación de conteos entre la tabla de hechos y la OBT.
- Comprobación de duplicados antes y después de publicar la tabla final.

Para consultar el resultado:

```sql
SELECT *
FROM PSET2_DB.OBT.AAPL_DAILY_OBT
ORDER BY DATE_KEY DESC
LIMIT 10;
```

Para verificar el volumen y período:

```sql
SELECT
    COUNT(*) AS TOTAL_REGISTROS,
    COUNT(DISTINCT DATE_KEY) AS FECHAS_UNICAS,
    COUNT(DISTINCT SYMBOL_KEY) AS SIMBOLOS,
    MIN(DATE_KEY) AS FECHA_INICIAL,
    MAX(DATE_KEY) AS FECHA_FINAL
FROM PSET2_DB.OBT.AAPL_DAILY_OBT;
```

## 11. Detener los servicios

Para detener los contenedores:

```bash
docker compose down
```

Este comando conserva los volúmenes persistentes de Docker. No utilizar `docker compose down -v` salvo que se desee eliminar también esos volúmenes y el estado local asociado.

## 12. Seguridad y consideraciones

- No subir archivos `.env` ni `.env_encoded` al repositorio.
- No incluir contraseñas ni secretos en el código fuente.
- Cada usuario debe configurar sus propias credenciales y recursos de Snowflake.
- La fuente de datos es Yahoo Finance mediante yfinance; su disponibilidad depende del proveedor.
- La ingesta utiliza `auto_adjust=False`, por lo que los precios de cierre no están ajustados por dividendos ni splits.
- Los registros con `Close` nulo se descartan durante la extracción.
- El alcance de esta entrega comprende la ingesta, transformación, validación y publicación de la OBT. No incluye el entrenamiento de un modelo predictivo.

---

**Proyecto académico — Maestría en Ciencia de Datos**  
**Universidad San Francisco de Quito (USFQ)**  
**PSet 2 — Ingeniería de Datos**
