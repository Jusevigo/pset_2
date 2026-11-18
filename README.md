# PSet 2 — Pipeline de Ingeniería de Datos para AAPL

## 1. Descripción del proyecto

Este proyecto implementa un pipeline de ingeniería de datos para la extracción, almacenamiento, transformación y procesamiento de datos históricos del activo financiero Apple Inc. (AAPL).

La fuente de información es Yahoo Finance. Los datos se extraen mediante Python y se almacenan inicialmente en Snowflake. Posteriormente, dbt realiza las transformaciones necesarias para construir las capas Bronze, Silver y Gold. Finalmente, Apache Spark utiliza las tablas del modelo dimensional Gold para construir una One Big Table (OBT), que también se almacena en Snowflake.

La infraestructura local se administra mediante Docker Compose. Kestra se encarga de la ingesta y orquestación del pipeline, mientras que Snowflake funciona como plataforma central de almacenamiento y procesamiento.

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
                       Docker Ingesta
                             |
                             v
                       Snowflake RAW
                             |
                             v
                          dbt Core
                             |
                             v
                          BRONZE
                             |
                             v
                          SILVER
                             |
                             v
                            GOLD
                       (Modelo estrella)
                         /         \
                        /           \
                   DIM_DATE     DIM_SYMBOL
                        \           /
                         \         /
                       FACT_AAPL_DAILY
                             |
                             v
                       Apache Spark
                         (PySpark)
                             |
                      Lectura vía JDBC
                             |
                             v
                    Construcción de OBT
                             |
                             v
                       Snowflake OBT
                  AAPL_DAILY_OBT
```

### Responsabilidades de los componentes

| Componente | Responsabilidad |
|---|---|
| Yahoo Finance | Fuente de datos históricos de AAPL. |
| Python | Extracción, preparación y carga de los datos. |
| Docker | Construcción y ejecución del contenedor de ingesta. |
| Kestra | Orquestación, programación y ejecución del proceso de ingesta. |
| PostgreSQL | Almacenamiento interno de metadatos y cola de Kestra. |
| Snowflake | Almacenamiento central de los datos RAW, Bronze, Silver, Gold y OBT. |
| dbt Core | Transformaciones y construcción de las capas Bronze, Silver y Gold. |
| Apache Spark | Construcción de la One Big Table mediante PySpark. |
| JDBC | Comunicación entre Spark y Snowflake. |
| Docker Compose | Administración de los servicios de infraestructura local. |

**Nota:** PostgreSQL se utiliza exclusivamente para el funcionamiento interno de Kestra. No almacena los datos de negocio del proyecto.

## 3. Tecnologías utilizadas

| Tecnología | Versión / configuración |
|---|---|
| Python (ingesta) | 3.11 |
| yfinance | 0.2.66 |
| pandas | 2.3.3 |
| Snowflake Connector | 4.1.1 |
| python-dotenv | 1.1.1 |
| Docker | Docker Desktop |
| PostgreSQL | 18 |
| Kestra | 2.0.7 |
| dbt Core | 1.12.7 |
| dbt-snowflake | 1.12.5 |
| Apache Spark | 4.3.0 (`spark:4.3.0-python3`) |
| Java | OpenJDK 21 |
| Snowflake JDBC Driver | 4.4.1 |
| Snowflake | Cuenta universitaria en GCP, región `us-central1` |

## 4. Estructura del repositorio

La estructura del proyecto es la siguiente:

```text
pset_2/
│
├── Dockerfile
├── requirements.txt
├── docker-compose.yml
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
│   ├── profiles.yml
│   │
│   ├── macros/
│   │   └── generate_schema_name.sql
│   │
│   ├── models/
│   │   ├── sources.yml
│   │   ├── bronze/
│   │   │   └── bronze_aapl_daily.sql
│   │   │
│   │   ├── silver/
│   │   │   └── silver_aapl_daily.sql
│   │   │
│   │   └── gold/
│   │       ├── dimensions/
│   │       │   ├── dim_date.sql
│   │       │   └── dim_symbol.sql
│   │       │
│   │       ├── facts/
│   │       │   └── fact_aapl_daily.sql
│   │       │
│   │       └── schema.yml
│   │
│   └── tests/
│       ├── assert_fact_close_price_positive.sql
│       └── assert_fact_grain_unique.sql
│
├── spark/
│   └── jobs/
│       └── build_obt.py
│
└── docs/
```

Los archivos `.env` y `.env_encoded` se utilizan localmente para gestionar las variables de entorno y los secretos, pero están excluidos del repositorio mediante `.gitignore`. El archivo `.env.example` proporciona una referencia de las variables necesarias sin incluir credenciales reales.

Los directorios generados automáticamente por dbt, como `target/`, `logs/` y `dbt_packages/`, también están excluidos del versionamiento.

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

## 10. Transformaciones y modelado con dbt

Las transformaciones se implementaron con dbt Core 1.12.7 y el adaptador `dbt-snowflake` 1.12.5, utilizando Snowflake como plataforma de almacenamiento y procesamiento.

La configuración del proyecto se encuentra en la carpeta `dbt/`, que contiene `dbt_project.yml`, `profiles.yml`, los modelos SQL, las pruebas y las macros.

Las credenciales se gestionan mediante variables de entorno (`env_var()`), evitando incluir contraseñas directamente en los archivos versionados.

### 10.1. Configuración de conexión

El perfil `pset2_aapl` utiliza las siguientes variables:

- `SNOWFLAKE_ACCOUNT`
- `SNOWFLAKE_USER`
- `SNOWFLAKE_PASSWORD`
- `SNOWFLAKE_ROLE`
- `SNOWFLAKE_WAREHOUSE`
- `SNOWFLAKE_DATABASE`

El esquema predeterminado del perfil es `BRONZE`. Los modelos de cada capa tienen configuraciones independientes en `dbt_project.yml`.

Se implementó la macro `generate_schema_name.sql` para utilizar directamente los esquemas personalizados definidos en el proyecto (`BRONZE`, `SILVER` y `GOLD`), sin que dbt concatene el esquema predeterminado con el personalizado.

Para verificar la conexión, se utilizó:

```powershell
dbt debug --project-dir .\dbt --profiles-dir .\dbt
```

### 10.2. Capa Bronze

El modelo `bronze_aapl_daily.sql` obtiene los registros mediante `source('raw', 'aapl_daily')`, cuya definición apunta a:

`PSET2_DB.RAW.AAPL_DAILY`

En esta capa se realiza la selección explícita de las columnas y la conversión de sus tipos de datos. No se aplican filtros de limpieza, de manera que se conserva la información disponible en RAW.

El modelo se materializa como una vista en:

`PSET2_DB.BRONZE.BRONZE_AAPL_DAILY`

La validación confirmó:

| Indicador | Resultado |
|---|---:|
| Registros | 4.087 |
| Fechas únicas | 4.087 |
| Fecha mínima | 2010-07-01 |
| Fecha máxima | 2026-09-30 |
| Cierres nulos | 0 |

### 10.3. Capa Silver

El modelo `silver_aapl_daily.sql` utiliza `ref('bronze_aapl_daily')` para acceder a Bronze.

En esta capa se aplican las siguientes reglas:

- Normalización del símbolo bursátil a mayúsculas y eliminación de espacios.
- Eliminación de registros sin símbolo, fecha, precio, fuente o fecha de ingesta.
- Exclusión de precios nulos o menores o iguales a cero.
- Estandarización de los tipos de datos.

Silver se materializa como una vista en:

`PSET2_DB.SILVER.SILVER_AAPL_DAILY`

La validación posterior a la limpieza produjo los siguientes resultados:

| Indicador | Resultado |
|---|---:|
| Registros | 4.087 |
| Fechas únicas | 4.087 |
| Símbolos inválidos | 0 |
| Fechas nulas | 0 |
| Cierres inválidos | 0 |
| Fuentes inválidas | 0 |
| Fechas de ingesta nulas | 0 |

### 10.4. Capa Gold: modelo dimensional

Se implementó un modelo dimensional tipo estrella. El grano de la tabla de hechos es **una observación por activo y fecha de negociación**, identificada por la combinación de `DATE_KEY` y `SYMBOL_KEY`.

El modelo contiene tres tablas:

**Dimensiones**

- `DIM_DATE`: dimensión de fechas con clave numérica en formato `YYYYMMDD` y atributos de año, trimestre, mes y día.
- `DIM_SYMBOL`: dimensión de símbolos bursátiles con una clave sustituta generada mediante `ROW_NUMBER()`.

**Tabla de hechos**

- `FACT_AAPL_DAILY`: contiene las claves de las dimensiones, el precio de cierre (`CLOSE_PRICE`) y los metadatos de procedencia (`SOURCE`, `INGESTED_AT`).

Las relaciones se construyen mediante `ref()` y las uniones entre las dos dimensiones y la tabla de hechos.

Los tres modelos se materializan como tablas en `PSET2_DB.GOLD`.

Actualmente, la dimensión de símbolos contiene un único registro (`AAPL`), dado que ese es el único activo incluido en el alcance del proyecto.

### 10.5. Pruebas de calidad en dbt

Se implementaron pruebas declarativas y personalizadas.

Las pruebas declarativas incluyen:

- `not_null`: verifica que los campos obligatorios no contengan valores nulos.
- `unique`: verifica la unicidad de las claves y atributos definidos en las dimensiones.
- `relationships`: verifica la integridad referencial entre la tabla de hechos y las dimensiones.
- `accepted_values`: restringe los símbolos a los valores definidos en el alcance actual (`AAPL`).

También se implementaron dos pruebas SQL personalizadas:

- `assert_fact_grain_unique.sql`: comprueba que no existan registros duplicados para una combinación de fecha y símbolo.
- `assert_fact_close_price_positive.sql`: comprueba que los precios de cierre sean estrictamente positivos.

Todas las pruebas se ejecutaron correctamente mediante:

```powershell
dbt test --project-dir .\dbt --profiles-dir .\dbt
```

### 10.6. Ejecución de dbt

Para reproducir las transformaciones desde la raíz del repositorio, se deben tener disponibles las variables de entorno de Snowflake y ejecutar:

```powershell
dbt debug --project-dir .\dbt --profiles-dir .\dbt

dbt run --project-dir .\dbt --profiles-dir .\dbt

dbt test --project-dir .\dbt --profiles-dir .\dbt
```

Los modelos de Bronze y Silver se materializan como vistas, mientras que los modelos de Gold se materializan como tablas.

---

## 11. Construcción de la One Big Table (OBT) con Apache Spark

La construcción de la One Big Table se implementó mediante Apache Spark 4.3.0 y PySpark, utilizando el driver JDBC de Snowflake (`net.snowflake:snowflake-jdbc:4.4.1`).

El contenedor Spark está definido en `docker-compose.yml` y utiliza la imagen `spark:4.3.0-python3`.

Se configuró un volumen compartido entre la carpeta local `spark/jobs` y `/opt/spark/jobs` dentro del contenedor. También se incorporó `.env` mediante `env_file` para proporcionar las credenciales necesarias sin almacenarlas directamente en el código.

### 11.1. Conexión con Snowflake

La conexión se realiza mediante JDBC. El driver se incorpora durante la ejecución de Spark utilizando Maven, por lo que no es necesario almacenar manualmente el archivo JAR en el repositorio.

Se utiliza `/tmp/spark-ivy` como directorio de caché de dependencias para evitar problemas de permisos con el directorio predeterminado de Ivy en el contenedor.

Para iniciar el servicio:

```powershell
docker compose up -d spark
```

Para comprobar la versión:

```powershell
docker exec pset2-spark /opt/spark/bin/spark-submit --version
```

### 11.2. Construcción de la OBT

El proceso se encuentra implementado en:

`spark/jobs/build_obt.py`

El programa realiza las siguientes operaciones:

1. Obtiene las credenciales de Snowflake desde las variables de entorno.
2. Establece una conexión JDBC con Snowflake.
3. Lee `FACT_AAPL_DAILY`, `DIM_DATE` y `DIM_SYMBOL` desde la capa Gold.
4. Ejecuta las uniones mediante PySpark utilizando las claves de las dimensiones.
5. Selecciona los atributos de fecha, símbolo, precio de cierre y metadatos.
6. Comprueba que el número de registros resultante coincida con el de la tabla de hechos.
7. Verifica que no existan duplicados en la combinación `DATE_KEY` y `SYMBOL_KEY`.
8. Escribe la OBT en Snowflake y vuelve a leerla para comprobar el resultado.

El destino es:

`PSET2_DB.OBT.AAPL_DAILY_OBT`

La tabla de destino debe crearse previamente en Snowflake. El proceso utiliza el modo `overwrite` para reconstruirla en cada ejecución, evitando acumular registros de ejecuciones anteriores.

### 11.3. Ejecución del proceso

Desde PowerShell se ejecuta:

```powershell
docker exec pset2-spark /opt/spark/bin/spark-submit --conf spark.jars.ivy=/tmp/spark-ivy --packages net.snowflake:snowflake-jdbc:4.4.1 /opt/spark/jobs/build_obt.py
```

### 11.4. Validación de la OBT

La ejecución de Spark confirmó que se escribieron 4.087 registros y que la validación posterior fue satisfactoria.

También se realizó una comprobación independiente directamente en Snowflake:

| Indicador | Resultado |
|---|---:|
| Registros | 4.087 |
| Fechas únicas | 4.087 |
| Símbolos | 1 |
| Fecha mínima | 2010-07-01 |
| Fecha máxima | 2026-09-30 |
| Precios inválidos | 0 |

Los resultados confirman que la OBT conserva la granularidad de la tabla de hechos y los atributos de ambas dimensiones.

---

## 12. Problemas encontrados y soluciones

Durante la implementación se identificaron y resolvieron los siguientes inconvenientes:

| Problema | Solución |
|---|---|
| La imagen Docker no ejecutaba el script automáticamente. | Se agregó un `CMD` al Dockerfile para ejecutar `ingesta.py`. |
| yfinance podía devolver columnas MultiIndex. | Se agregó una comprobación para extraer correctamente la columna `Close` de AAPL. |
| El contenedor de Kestra utilizaba un directorio de trabajo diferente al esperado. | Se utilizó la ruta absoluta `/app/ingesta/ingesta.py`. |
| Las variables de Snowflake no estaban disponibles inicialmente en el contenedor de ejecución. | Se configuró `.env_encoded` y se inyectaron los secrets mediante Kestra. |
| Era necesario conservar el CSV generado dentro del equipo local. | Se configuró un volumen compartido entre Docker y el directorio `data/raw`. |
| La ruta con caracteres especiales podía presentar problemas de codificación. | Se guardó el YAML local explícitamente en UTF-8 y se verificó la ejecución después de actualizar el flujo en Kestra. |
| PostgreSQL presentó mensajes de recuperación después de un apagado inesperado. | Se revisaron los logs y se comprobó que el servicio recuperó su estado operativo sin eliminar el volumen persistente. |
| dbt no encontraba inicialmente el archivo de perfil. | Se corrigió el nombre de `profile.yml` a `profiles.yml`. |
| dbt no encontraba inicialmente el modelo durante una ejecución. | Se identificó que algunos archivos no estaban guardados en el editor. Se guardaron y se repitió la ejecución correctamente. |
| dbt podía generar nombres de esquemas concatenados. | Se implementó `generate_schema_name.sql` para utilizar directamente los esquemas personalizados. |
| Los archivos generados por dbt podían incorporarse accidentalmente al repositorio. | Se añadieron exclusiones para `dbt/target/`, `dbt/logs/`, `dbt/dbt_packages/` y `dbt/.user.yml`. |
| `spark-submit` no estaba disponible directamente en el PATH del contenedor. | Se utilizó la ruta absoluta `/opt/spark/bin/spark-submit`. |
| Spark no podía guardar inicialmente la caché de dependencias de Ivy. | Se configuró `spark.jars.ivy=/tmp/spark-ivy`. |
| La escritura JDBC no encontraba inicialmente la tabla de destino en Snowflake. | Se creó previamente `PSET2_DB.OBT.AAPL_DAILY_OBT` y se repitió la ejecución satisfactoriamente. |
| El driver JDBC intentó consultar endpoints de metadatos de proveedores cloud que no estaban disponibles desde el entorno local. | Se comprobó que estos mensajes no impidieron la lectura, escritura ni validación final de la OBT. |

---

## 13. Estado actual y trabajo pendiente

### Implementado y probado

- [x] Configuración inicial de Snowflake.
- [x] Creación de la base de datos y los esquemas.
- [x] Desarrollo del script de ingesta en Python.
- [x] Construcción de la imagen Docker de ingesta.
- [x] Configuración de Docker Compose.
- [x] Configuración de PostgreSQL para Kestra.
- [x] Configuración de secrets de Snowflake.
- [x] Desarrollo y ejecución del flujo de ingesta en Kestra.
- [x] Configuración de la ejecución programada entre semana.
- [x] Configuración de reintentos exponenciales en Kestra.
- [x] Parametrización de fechas para ejecuciones manuales y backfill.
- [x] Persistencia del CSV mediante volumen compartido.
- [x] Carga de datos en Snowflake RAW mediante `MERGE`.
- [x] Validación estructural de RAW.
- [x] Implementación y validación de modelos dbt Bronze, Silver y Gold.
- [x] Implementación del modelo dimensional estrella.
- [x] Implementación y ejecución satisfactoria de las pruebas de calidad en dbt.
- [x] Implementación del job de construcción de la OBT mediante PySpark.
- [x] Lectura de las tablas Gold mediante JDBC.
- [x] Construcción y escritura de la OBT en Snowflake.
- [x] Validación de los 4.087 registros de la OBT.
- [x] Versionamiento de scripts, modelos, configuraciones y flujos mediante Git.

### Pendiente

- [ ] Verificar una ejecución automática del schedule de Kestra.
- [ ] Comprobar experimentalmente la política de reintentos mediante un fallo controlado.
- [ ] Completar la documentación técnica final y sus anexos.
- [ ] Documentar la ejecución completa en un entorno limpio, por ejemplo, un Codespace nuevo.

**Consideración sobre calidad de datos:** el script de ingesta aplica actualmente `dropna()` al precio de cierre antes de cargar los registros en RAW. Por ello, el conteo de cierres nulos igual a cero en Snowflake no permite determinar si Yahoo Finance entregó observaciones faltantes que fueron descartadas durante la extracción. La precisión de las cotizaciones tampoco se ha contrastado contra una fuente independiente.

---

## 14. Seguridad y buenas prácticas

- No almacenar contraseñas directamente en los scripts.
- Mantener `.env` y `.env_encoded` fuera del repositorio.
- Utilizar `.env.example` para documentar las variables requeridas.
- Mantener `profiles.yml` sin credenciales literales, utilizando referencias a variables de entorno.
- No publicar credenciales ni secretos en capturas de pantalla o archivos de configuración.
- Mantener versionados los scripts, modelos, configuraciones y flujos del proyecto.
- Excluir archivos temporales, logs, cachés y resultados generados automáticamente.
- Evitar eliminar volúmenes persistentes de Docker sin verificar previamente su contenido.
- Adaptar las rutas locales cuando se ejecute el proyecto en otro equipo.

---

**Estado general del proyecto:** la ingesta, orquestación, almacenamiento RAW, transformaciones Bronze/Silver/Gold, pruebas de calidad y construcción de la One Big Table mediante Spark se encuentran implementados y ejecutados satisfactoriamente. Quedan pendientes algunas verificaciones operativas y la documentación técnica final.