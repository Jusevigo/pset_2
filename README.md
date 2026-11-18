# PSet 2 — Pipeline de Ingeniería de Datos para AAPL

## 1. Descripción del proyecto

Este proyecto implementa un pipeline de ingeniería de datos para la extracción, almacenamiento, transformación y procesamiento de datos históricos del activo financiero Apple Inc. (AAPL).

La fuente de información es Yahoo Finance. Los datos se extraen mediante Python, se almacenan inicialmente en Snowflake y posteriormente se transformarán utilizando dbt para construir las capas Bronze, Silver y Gold. Finalmente, Apache Spark se utilizará para construir una One Big Table (OBT).

La infraestructura local se administra mediante Docker Compose y Kestra se encarga de la ingesta y orquestación del pipeline.

## 2. Arquitectura

La arquitectura general del proyecto es:

```text
                 Yahoo Finance
                       |
                       v
                Python / yfinance
                       |
                       v
                  Kestra
              (Orquestación)
                       |
                       v
               Snowflake RAW
                       |
                       v
                 dbt Core
                       |
           +-----------+-----------+
           |           |           |
           v           v           v
         Bronze      Silver       Gold
           |           |           |
           +-----------+-----------+
                       |
                       v
                  Apache Spark
                       |
                       v
                  Snowflake
                       |
                       v
                      OBT
```

### Responsabilidades de los componentes

| Componente | Responsabilidad |
|---|---|
| Yahoo Finance | Fuente de datos históricos de AAPL. |
| Python | Extracción, preparación y carga de los datos. |
| Docker | Construcción y ejecución del contenedor de ingesta. |
| Kestra | Orquestación y ejecución del proceso de ingesta. |
| PostgreSQL | Almacenamiento interno de metadatos y cola de Kestra. |
| Snowflake | Almacenamiento central de los datos. |
| dbt Core | Transformaciones y construcción de las capas Bronze, Silver y Gold. |
| Apache Spark | Construcción de la One Big Table (OBT). |
| Docker Compose | Administración de los servicios de infraestructura local. |

**Nota:** PostgreSQL se utiliza exclusivamente para el funcionamiento interno de Kestra. No almacena los datos de negocio del proyecto.

## 3. Tecnologías utilizadas

| Tecnología | Versión / configuración |
|---|---|
| Python | 3.11 |
| yfinance | 0.2.66 |
| pandas | 2.3.3 |
| Snowflake Connector | 4.1.1 |
| python-dotenv | 1.1.1 |
| Docker | Docker Desktop |
| PostgreSQL | 18 |
| Kestra | 2.0.7 |
| Apache Spark | 4.3.0 (`spark:4.3.0-python3`) |
| Snowflake | Cuenta universitaria en GCP, región us-central1 |
| dbt Core | Pendiente de implementación |

## 4. Estructura del repositorio

La estructura del proyecto contempla los siguientes directorios y archivos:

```text
pset_2/
│
├── Dockerfile
├── requirements.txt
├── docker-compose.yml
├── .env
├── .env_encoded
├── .env.example
├── .gitignore
├── README.md
│
├── ingesta/
│   └── ingesta.py
│
├── data/
│   └── raw/
│       └── aapl_daily.csv
│
├── kestra/
│   └── flows/
│       └── aapl_ingestion.yml
│
├── dbt/
│   ├── dbt_project.yml
│   ├── models/
│   │   ├── bronze/
│   │   ├── silver/
│   │   └── gold/
│   └── tests/
│
├── spark/
│   └── jobs/
│       └── build_obt.py
│
└── docs/
    └── architecture/
```

Los directorios y archivos correspondientes a dbt, Spark y documentación adicional forman parte de la estructura prevista y se completarán durante las siguientes etapas del proyecto.

## 5. Configuración del entorno

### 5.1. Requisitos previos

Para ejecutar el proyecto se necesita:

- Docker Desktop.
- Docker Compose.
- Acceso a una cuenta de Snowflake.
- PowerShell en Windows o una terminal compatible.
- Git.

### 5.2. Variables de entorno

Las credenciales y parámetros de conexión a Snowflake se almacenan en un archivo `.env`, que contiene las siguientes variables:

```env
SNOWFLAKE_ACCOUNT=
SNOWFLAKE_USER=
SNOWFLAKE_PASSWORD=
SNOWFLAKE_ROLE=
SNOWFLAKE_WAREHOUSE=
SNOWFLAKE_DATABASE=
SNOWFLAKE_SCHEMA=
```

Los valores reales deben configurarse localmente. El archivo `.env` no debe subirse al repositorio.

Para utilizar las variables como secrets en Kestra se generó un archivo `.env_encoded`, que contiene los valores codificados en Base64 y utiliza el prefijo `SECRET_`.

**Importante:** Base64 es un mecanismo de codificación, no de cifrado. Por tanto, `.env_encoded` también debe tratarse como un archivo sensible y mantenerse fuera del repositorio.

El archivo `.env.example` sirve como plantilla para configurar el entorno sin compartir credenciales.

### 5.3. Docker Compose

Docker Compose administra los siguientes servicios:

- PostgreSQL para Kestra.
- Kestra para la orquestación.
- Spark para el procesamiento distribuido/local.

Para validar la configuración:

```powershell
docker compose config --quiet
```

Para iniciar PostgreSQL:

```powershell
docker compose up -d postgres
```

Para iniciar Kestra:

```powershell
docker compose up -d kestra
```

También se pueden iniciar ambos servicios juntos:

```powershell
docker compose up -d postgres kestra
```

Para iniciar Spark cuando se requiera:

```powershell
docker compose up -d spark
```

Para consultar el estado de los contenedores:

```powershell
docker compose ps
```

La interfaz web de Kestra se encuentra disponible en:

http://localhost:8080

Las credenciales de acceso local se configuran en `docker-compose.yml`. No deben reutilizarse contraseñas de desarrollo en entornos de producción.

## 6. Ingesta de datos

### 6.1. Fuente de información

Se utiliza Yahoo Finance para obtener los precios históricos diarios de cierre de Apple Inc. (AAPL).

La extracción se realiza mediante la biblioteca `yfinance`.

Parámetros utilizados:

| Parámetro | Valor |
|---|---|
| Símbolo | AAPL |
| Fecha inicial | 2010-07-01 |
| Fecha final exclusiva | 2026-10-01 |
| Variable extraída | Close |
| Fuente | Yahoo Finance |

La fecha final es exclusiva en la consulta de Yahoo Finance, por lo que el último registro corresponde al 30 de septiembre de 2026.

### 6.2. Preparación de los datos

El script `ingesta/ingesta.py` realiza las siguientes operaciones:

1. Descarga los datos históricos desde Yahoo Finance.
2. Extrae la columna `Close`.
3. Maneja las columnas MultiIndex que puede devolver yfinance.
4. Elimina valores nulos y ordena los registros cronológicamente.
5. Prepara las columnas necesarias para el almacenamiento.
6. Guarda los datos extraídos en un archivo CSV.
7. Carga los registros en Snowflake.

El conjunto de datos contiene las siguientes columnas:

| Columna | Descripción |
|---|---|
| SYMBOL | Símbolo del activo financiero. |
| DATE | Fecha del registro. |
| CLOSE | Precio de cierre. |
| SOURCE | Fuente de información. |

### 6.3. Almacenamiento local

Los datos extraídos se guardan en:

```text
data/raw/aapl_daily.csv
```

Este archivo se genera durante la ejecución de la ingesta y se conserva mediante un volumen compartido entre el contenedor y el equipo local.

### 6.4. Carga en Snowflake

Los datos se cargan en la tabla:

```text
PSET2_DB.RAW.AAPL_DAILY
```

La carga utiliza una tabla temporal de staging y una operación `MERGE`.

La correspondencia de registros se determina mediante las columnas:

- `SYMBOL`
- `DATE`

Si el registro ya existe, se actualizan sus valores. Si no existe, se inserta uno nuevo.

Este mecanismo permite ejecutar nuevamente la ingesta sin generar duplicados por símbolo y fecha.

La tabla incluye además:

- `INGESTED_AT`: fecha y hora de carga.
- `SOURCE`: fuente de los datos.

## 7. Configuración de Snowflake

Se creó la base de datos:

```text
PSET2_DB
```

Y se definieron los siguientes esquemas:

| Esquema | Propósito |
|---|---|
| RAW | Datos originales provenientes de la fuente. |
| BRONZE | Primera capa de transformación. |
| SILVER | Datos depurados y estandarizados. |
| GOLD | Datos preparados para análisis. |
| OBT | Almacenamiento de la One Big Table. |

La tabla inicial de ingesta se encuentra en:

```text
PSET2_DB.RAW.AAPL_DAILY
```

## 8. Orquestación con Kestra

Kestra se utiliza para ejecutar y coordinar el proceso de ingesta.

El flujo principal se identifica como:

```text
Namespace: pset2
Flow ID: aapl_ingestion
```

Su definición se encuentra versionada en:

```text
kestra/flows/aapl_ingestion.yml
```

El flujo realiza las siguientes operaciones:

1. Recupera las credenciales de Snowflake mediante secrets.
2. Ejecuta el contenedor `pset2-ingesta:1.0`.
3. Ejecuta el script `ingesta/ingesta.py`.
4. Conserva el CSV generado mediante un volumen compartido.
5. Finaliza cuando la extracción y carga en Snowflake terminan correctamente.

Los secrets utilizados son:

```text
SNOWFLAKE_ACCOUNT
SNOWFLAKE_USER
SNOWFLAKE_PASSWORD
SNOWFLAKE_ROLE
SNOWFLAKE_WAREHOUSE
SNOWFLAKE_DATABASE
SNOWFLAKE_SCHEMA
```

### 8.1. Consideración sobre el volumen compartido

La configuración actual del flujo utiliza una ruta absoluta correspondiente al equipo Windows donde se desarrolló el proyecto:

```text
/run/desktop/mnt/host/c/USFQ/Maestría/Ciencia_Datos/pset_2/data/raw
```

Esta ruta debe adaptarse si el proyecto se ejecuta desde otra ubicación o equipo.

El flujo se creó inicialmente desde la interfaz web de Kestra y posteriormente se guardó como archivo YAML local. La versión local se utilizó para actualizar el flujo en Kestra y se ejecutó nuevamente para verificar su funcionamiento.

### 8.2. Prueba de secrets

Se creó un flujo auxiliar de prueba en Kestra para verificar la disponibilidad de las variables de entorno.

La prueba confirmó la disponibilidad de las siete variables de Snowflake dentro del contenedor de ejecución.

## 9. Validación de la ingesta

Se ejecutó satisfactoriamente el flujo `aapl_ingestion` desde Kestra.

Resultados obtenidos:

| Validación | Resultado |
|---|---:|
| Registros descargados | 4087 |
| Registros cargados en staging | 4087 |
| Registros finales en RAW | 4087 |
| Fechas únicas | 4087 |
| Fecha mínima | 2010-07-01 |
| Fecha máxima | 2026-09-30 |
| Valores nulos en CLOSE | 0 |
| Duplicados por SYMBOL y DATE | 0 |
| Estado de ejecución en Kestra | SUCCESS |

La validación de duplicados se realizó mediante:

```sql
SELECT
    SYMBOL,
    DATE,
    COUNT(*) AS REPETICIONES
FROM PSET2_DB.RAW.AAPL_DAILY
GROUP BY SYMBOL, DATE
HAVING COUNT(*) > 1;
```

La consulta no devolvió registros, confirmando que no existen duplicados por símbolo y fecha.

## 10. Problemas encontrados y soluciones

Durante la implementación se identificaron y resolvieron los siguientes inconvenientes:

| Problema | Solución |
|---|---|
| La imagen Docker no ejecutaba el script automáticamente. | Se agregó un `CMD` al Dockerfile para ejecutar `ingesta.py`. |
| yfinance podía devolver columnas MultiIndex. | Se agregó una comprobación para extraer correctamente la columna `Close` de AAPL. |
| El contenedor de Kestra utilizaba un directorio de trabajo diferente al esperado. | Se utilizó la ruta absoluta `/app/ingesta/ingesta.py`. |
| Las variables de Snowflake no estaban disponibles inicialmente en el contenedor de ejecución. | Se configuró el archivo `.env_encoded` y se inyectaron los secrets mediante Kestra. |
| Era necesario conservar el CSV generado dentro del equipo local. | Se configuró un volumen compartido entre Docker y el directorio `data/raw`. |
| La ruta con el carácter `í` podía presentar problemas de codificación. | Se guardó el YAML local explícitamente en UTF-8 y se verificó la ejecución después de actualizar el flujo en Kestra. |
| PostgreSQL presentó mensajes de recuperación después de un apagado inesperado. | Se revisaron los logs y se comprobó que el servicio recuperó su estado operativo sin eliminar el volumen persistente. |

## 11. Estado actual y trabajo pendiente

### Implementado y probado

- [x] Configuración inicial de Snowflake.
- [x] Creación de la base de datos y los esquemas.
- [x] Desarrollo del script de ingesta en Python.
- [x] Construcción de la imagen Docker de ingesta.
- [x] Configuración de Docker Compose.
- [x] Configuración de PostgreSQL para Kestra.
- [x] Configuración de secrets de Snowflake.
- [x] Desarrollo y ejecución del flujo de ingesta en Kestra.
- [x] Persistencia del CSV mediante volumen compartido.
- [x] Carga de datos en Snowflake RAW.
- [x] Validación de registros y ausencia de duplicados.
- [x] Versionamiento local del flujo YAML.

### Pendiente

- [ ] Configurar dbt Core y su conexión con Snowflake.
- [ ] Desarrollar los modelos de la capa Bronze.
- [ ] Desarrollar los modelos de la capa Silver.
- [ ] Desarrollar los modelos de la capa Gold.
- [ ] Implementar pruebas de calidad de datos en dbt.
- [ ] Desarrollar el proceso de construcción de la OBT mediante Apache Spark.
- [ ] Almacenar y validar la OBT en Snowflake.
- [ ] Documentar y validar el pipeline completo.

## 12. Seguridad y buenas prácticas

- No almacenar contraseñas directamente en los scripts.
- Mantener `.env` y `.env_encoded` fuera del repositorio.
- Utilizar `.env.example` para documentar las variables requeridas.
- No publicar credenciales ni secretos en capturas de pantalla o archivos de configuración.
- Mantener versionados los scripts, configuraciones y flujos del proyecto.
- Evitar eliminar volúmenes persistentes de Docker sin verificar previamente su contenido.
- Adaptar las rutas locales cuando se ejecute el proyecto en otro equipo.

---

**Estado del proyecto:** la extracción, ingesta, orquestación y almacenamiento inicial en Snowflake RAW se encuentran implementados y probados. Las transformaciones con dbt y la construcción de la OBT con Spark corresponden a las siguientes etapas del desarrollo.