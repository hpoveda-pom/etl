# Suite ETL - Procesamiento de Datos

Suite completa de herramientas ETL para procesar datos desde múltiples fuentes (SQL Server, Excel) hacia diferentes destinos (CSV, ClickHouse, Snowflake).

---

## Información del Proyecto

- **Autor**: Herbert Poveda
- **Empresa**: POM Cobranzas
- **Departamento**: Business Intelligence (BI)
- **Fecha**: 19 de enero de 2026
- **Versión**: 1.0.0

---

## Tabla de Contenidos

- [Descripción General](#descripción-general)
- [Requisitos](#requisitos)
- [Instalación](#instalación)
- [Configuración con archivo .env](#configuración-con-archivo-env)
- [Estructura de Carpetas](#estructura-de-carpetas)
- [Scripts ETL](#scripts-etl)
  - [sqlserver_to_csv.py](#1-sqlserver_to_csvpy)
  - [excel_to_csv.py](#2-excel_to_csvpy)
  - [compress_csv_to_gz.py](#3-compress_csv_to_gzpy)
  - [csv_to_snowflake.py](#4-csv_to_snowflakepy)
  - [csv_to_clickhouse.py](#5-csv_to_clickhousepy)
  - [ingest_all_excels_to_stage.py](#6-ingest_all_excels_to_stagepy)
  - [snowflake_csv_to_tables.py](#7-snowflake_csv_to_tablespy)
  - [sqlserver_to_snowflake_streaming.py](#8-sqlserver_to_snowflake_streamingpy)
  - [sqlserver_to_clickhouse_streaming.py](#9-sqlserver_to_clickhouse_streamingpy)
  - [clickhouse_csv_to_tables.py](#10-clickhouse_csv_to_tablespy)
  - [snowflake_drop_tables.py](#11-snowflake_drop_tablespy)
  - [clickhouse_drop_tables.py](#12-clickhouse_drop_tablespy)
- [Flujos de Trabajo Comunes](#flujos-de-trabajo-comunes)
- [Troubleshooting](#troubleshooting)
- [Changelog](#changelog)

---

## Descripción General

Esta suite ETL proporciona herramientas para:

- **Extracción**: Exportar datos desde SQL Server y Excel
- **Transformación**: Convertir formatos, comprimir archivos, filtrar datos
- **Carga**: Importar datos a ClickHouse y Snowflake

### Características Principales

- Exportación masiva de tablas SQL Server a CSV
- Conversión de archivos Excel a CSV
- Compresión de CSV a formato GZ
- Carga automática a Snowflake y ClickHouse
- **Streaming directo SQL Server -> ClickHouse/Snowflake (sin CSV intermedio)**
- **Carga incremental inteligente** (por ID, timestamp o hash de fila)
- **Deduplicación automática** con ReplacingMergeTree
- **Manejo de updates y deletes** con lookback window
- Filtrado flexible por tablas, carpetas y archivos
- Manejo robusto de errores con reintentos
- Logging detallado de operaciones
- Sanitización automática de nombres

---

## Requisitos

### Requisitos Generales

- **Python 3.7+**
- **Sistema Operativo**: Windows (principalmente), Linux/Mac compatible

### Dependencias Python

```bash
pip install pandas pyodbc openpyxl clickhouse-connect snowflake-connector-python python-dotenv
```

O instalar todas de una vez:

```bash
pip install -r requirements.txt
```

**Nota**: `python-dotenv` es opcional pero recomendado para usar archivos `.env`. Si no está instalado, los scripts usarán variables de entorno del sistema.

### Requisitos Adicionales por Script

| Script | Requisitos Adicionales |
|--------|----------------------|
| `sqlserver_to_csv.py` | Driver ODBC para SQL Server |
| `excel_to_csv.py` | openpyxl (incluido en dependencias) |
| `csv_to_snowflake.py` | Cuenta de Snowflake |
| `csv_to_clickhouse.py` | Cuenta de ClickHouse Cloud |
| `ingest_all_excels_to_stage.py` | Cuenta de Snowflake |
| `sqlserver_to_snowflake_streaming.py` | Driver ODBC, Cuenta de Snowflake, pandas |
| `sqlserver_to_clickhouse_streaming.py` | Driver ODBC, Cuenta de ClickHouse, clickhouse-connect |
| `snowflake_csv_to_tables.py` | Cuenta de Snowflake |
| `clickhouse_csv_to_tables.py` | Cuenta de ClickHouse Cloud |
| `snowflake_drop_tables.py` | Cuenta de Snowflake |
| `clickhouse_drop_tables.py` | Cuenta de ClickHouse Cloud |

### Drivers ODBC para SQL Server

Necesitas uno de los siguientes drivers instalados:
- ODBC Driver 17 for SQL Server (recomendado)
- ODBC Driver 13 for SQL Server
- SQL Server Native Client 11.0
- SQL Server

---

## Instalación

1. **Clonar o descargar el repositorio**

2. **Instalar dependencias**:
```bash
pip install pandas pyodbc openpyxl clickhouse-connect snowflake-connector-python python-dotenv
```

3. **Configurar archivo `.env`** (ver sección [Configuración con archivo .env](#configuración-con-archivo-env))

4. **Crear estructura de carpetas** (se crean automáticamente, pero puedes crearlas manualmente):
```
UPLOADS/
└── POM_DROP/
    ├── inbox/          # Archivos Excel a procesar
    ├── processed/      # Archivos procesados exitosamente
    ├── error/          # Archivos con errores
    ├── csv_staging/    # CSV generados (intermedio)
    ├── csv_processed/  # CSV procesados exitosamente
    └── csv_error/      # CSV con errores
```

---

## Configuración con archivo .env

**Recomendado**: Usar un archivo `.env` para gestionar todas las credenciales y configuraciones de forma segura.

### Paso 1: Crear archivo .env

Copia el archivo de ejemplo y personalízalo:

```bash
# Windows (PowerShell)
copy .env.example .env

# Linux/Mac
cp .env.example .env
```

### Paso 2: Editar .env con tus credenciales

Abre el archivo `.env` y configura tus credenciales:

```env
# ============================================
# Configuración SQL Server
# ============================================
SQL_SERVER=SRV-DESA\SQLEXPRESS
SQL_DATABASE=MiBaseDeDatos
SQL_USER=tu_usuario         # Requerido por defecto (autenticación SQL Server)
SQL_PASSWORD=tu_password    # Requerido por defecto (autenticación SQL Server)
SQL_USE_WINDOWS_AUTH=false  # Opcional: true para usar autenticación Windows
SQL_DRIVER=ODBC Driver 17 for SQL Server

# ============================================
# Configuración ClickHouse Cloud
# ============================================
CH_HOST=f4rf85ygzj.eastus2.azure.clickhouse.cloud
CH_PORT=8443
CH_USER=default
CH_PASSWORD=tu_password_aqui  # [WARN] OBLIGATORIO
CH_DATABASE=default

# ============================================
# Configuración Snowflake
# ============================================
SF_ACCOUNT=fkwugeu-qic97823
SF_USER=HPOVEDAPOMCR
SF_PASSWORD=tu_password_aqui  # [WARN] OBLIGATORIO
SF_ROLE=ACCOUNTADMIN
SF_WAREHOUSE=COMPUTE_WH
SF_DATABASE=POM_TEST01
SF_SCHEMA=RAW

# ============================================
# Configuración de Streaming
# ============================================
STREAMING_CHUNK_SIZE=10000
TARGET_TABLE_PREFIX=          # Vacío por defecto (sin prefijo)
TABLES_FILTER=                # Opcional: Tabla1,Tabla2
LOOKBACK_DAYS=7               # Días hacia atrás para detectar updates
USE_REPLACING_MERGE_TREE=true # Usar ReplacingMergeTree en ClickHouse
CH_TIMEZONE=UTC               # Zona horaria para DateTime64

# ============================================
# Configuración de Carpetas
# ============================================
INBOX_DIR=UPLOADS\POM_DROP\inbox
PROCESSED_DIR=UPLOADS\POM_DROP\processed
ERROR_DIR=UPLOADS\POM_DROP\error
CSV_STAGING_DIR=UPLOADS\POM_DROP\csv_staging
CSV_PROCESSED_DIR=UPLOADS\POM_DROP\csv_processed
CSV_ERROR_DIR=UPLOADS\POM_DROP\csv_error
```

### Paso 3: Verificar que funciona

Al ejecutar cualquier script, verás un mensaje si el `.env` se cargó correctamente:

```
[OK] Archivo .env cargado desde: C:\xampp\htdocs\etl\.env
```

### Variables de Entorno Alternativas

Si prefieres no usar `.env`, puedes configurar variables de entorno del sistema:

**Windows (PowerShell)**:
```powershell
$env:CH_PASSWORD="tu_password"
$env:SQL_SERVER="SRV-DESA\SQLEXPRESS"
```

**Windows (CMD)**:
```cmd
set CH_PASSWORD=tu_password
set SQL_SERVER=SRV-DESA\SQLEXPRESS
```

**Linux/Mac**:
```bash
export CH_PASSWORD="tu_password"
export SQL_SERVER="SRV-DESA\\SQLEXPRESS"
```

### Seguridad del archivo .env

[WARN] **IMPORTANTE**: 
- **NUNCA** subas el archivo `.env` a Git
- Agrega `.env` a tu `.gitignore`
- El archivo `.env.example` puede estar en Git (sin credenciales)
- Mantén permisos restrictivos en el archivo `.env`

---

## Estructura de Carpetas

El sistema utiliza la siguiente estructura de carpetas (configurable mediante variables de entorno o `.env`):

```
UPLOADS/POM_DROP/
├── inbox/              # Archivos Excel (.xlsx) a procesar
├── processed/          # Archivos Excel procesados exitosamente
├── error/              # Archivos Excel con errores
├── csv_staging/        # CSV generados (punto intermedio)
│   ├── SQLSERVER_*     # Carpetas con CSV de SQL Server
│   └── [nombre_excel] # Carpetas con CSV de Excel
├── csv_processed/      # Carpetas de CSV procesadas exitosamente
└── csv_error/          # Carpetas de CSV con errores
```

### Flujo de Archivos

1. **Excel -> CSV**: `inbox/` -> `csv_staging/` -> `processed/` o `error/`
2. **SQL Server -> CSV**: `csv_staging/SQLSERVER_*/` -> (procesamiento) -> `csv_processed/` o `csv_error/`
3. **CSV -> Cloud**: `csv_staging/` -> (carga) -> `csv_processed/` o `csv_error/`

---

## Scripts ETL

### 1. sqlserver_to_csv.py

**Versión**: 1.0.0  
**Descripción**: Exporta tablas de SQL Server a archivos CSV.

**Requisitos**:
- Driver ODBC para SQL Server
- Acceso a SQL Server (Windows Auth o SQL Auth)

**Variables de Entorno** (o `.env`):
- `SQL_SERVER`, `SQL_DATABASE` (requeridos)
- `SQL_USER`, `SQL_PASSWORD` (requeridos por defecto, ver nota abajo)
- `SQL_USE_WINDOWS_AUTH` (opcional, default: `false` - si es `true`, no requiere SQL_USER/SQL_PASSWORD)
- `SQL_DRIVER` (opcional, default: "ODBC Driver 17 for SQL Server")
- `CSV_STAGING_DIR`
- `TABLES_FILTER` (opcional)

**Uso**:

```bash
# Exportar todas las tablas de una base de datos
python sqlserver_to_csv.py MiBaseDeDatos

# Exportar tablas específicas
python sqlserver_to_csv.py MiBaseDeDatos Tabla1,Tabla2,Tabla3
```

**Características**:
- **Autenticación SQL Server por defecto** (requiere SQL_USER y SQL_PASSWORD)
- Para usar autenticación Windows, define `SQL_USE_WINDOWS_AUTH=true` en `.env`
- Detección automática de drivers ODBC
- Exclusión de tablas por prefijos
- Filtrado de tablas específicas
- Reintentos automáticos en caso de pérdida de conexión
- Sanitización automática de nombres de archivos

**Salida**: Archivos CSV en `CSV_STAGING_DIR/SQLSERVER_[nombre_base_datos]/`

---

### 2. excel_to_csv.py

**Versión**: 1.0.0  
**Descripción**: Convierte archivos Excel (.xlsx) a CSV. Cada hoja del Excel se convierte en un archivo CSV separado.

**Requisitos**:
- `openpyxl` (incluido en dependencias)

**Variables de Entorno** (o `.env`):
- `INBOX_DIR`, `PROCESSED_DIR`, `ERROR_DIR`
- `CSV_STAGING_DIR`
- `SHEETS_ALLOWLIST` (opcional)

**Uso**:

```bash
# Procesar todos los Excel en inbox
python excel_to_csv.py
```

**Características**:
- Procesa todos los archivos `.xlsx` en `INBOX_DIR`
- Cada hoja del Excel se convierte en un CSV separado
- Crea una carpeta con el nombre del Excel (sanitizado)
- Mueve archivos a `processed/` o `error/` según resultado
- Manejo de archivos bloqueados con reintentos

**Salida**: 
- Archivos CSV en `CSV_STAGING_DIR/[nombre_excel]/[hoja].csv`
- Excel original movido a `processed/` o `error/`

---

### 3. compress_csv_to_gz.py

**Versión**: 1.0.0  
**Descripción**: Comprime archivos CSV a formato `.csv.gz` para reducir el tamaño.

**Requisitos**: Ninguno adicional

**Variables de Entorno** (o `.env`):
- `CSV_STAGING_DIR`
- `FOLDERS_FILTER` (opcional)
- `CSV_FILTER` (opcional)
- `DELETE_ORIGINALS` (opcional, `true`/`false`)

**Uso**:

```bash
# Comprimir todos los CSV
python compress_csv_to_gz.py

# Comprimir CSV de una carpeta específica
python compress_csv_to_gz.py SQLSERVER_POM_Aplicaciones

# Comprimir CSV específicos
python compress_csv_to_gz.py SQLSERVER_POM_Aplicaciones ResutadoNotificar,Bitacora
```

**Características**:
- Comprime CSV a formato GZ (compatible con Snowflake y ClickHouse)
- Muestra ratio de compresión
- Opción para eliminar CSV originales después de comprimir exitosamente
- Filtrado por carpetas y archivos
- Omite archivos que ya tienen su versión `.csv.gz`

**Salida**: Archivos `.csv.gz` en las mismas carpetas

---

### 4. csv_to_snowflake.py

**Versión**: 1.0.0  
**Descripción**: Carga archivos CSV (o CSV.gz) desde `csv_staging` a Snowflake.

**Requisitos**:
- Cuenta de Snowflake
- `snowflake-connector-python`

**Variables de Entorno** (o `.env`):
- `SF_ACCOUNT`, `SF_USER`, `SF_PASSWORD`, `SF_ROLE`, `SF_WAREHOUSE`
- `SF_DATABASE`, `SF_SCHEMA`
- `CSV_STAGING_DIR`, `CSV_PROCESSED_DIR`, `CSV_ERROR_DIR`
- `FOLDERS_FILTER`, `CSV_FILTER` (opcional)

**Uso**:

```bash
# Cargar todos los CSV a Snowflake
python csv_to_snowflake.py

# Especificar base de datos y schema
python csv_to_snowflake.py POM_TEST01 RAW

# Filtrar por carpeta
python csv_to_snowflake.py POM_TEST01 RAW SQLSERVER_POM_Aplicaciones
```

**Características**:
- Crea automáticamente stage `RAW_STAGE` si no existe
- Crea tablas `INGEST_LOG` e `INGEST_GENERIC_RAW` en schema RAW
- Comprime CSV a `.gz` automáticamente antes de subir
- Registra operaciones en `INGEST_LOG`
- Mueve carpetas procesadas a `csv_processed/` o `csv_error/`

**Salida**: 
- Datos cargados en Snowflake
- Carpetas movidas a `csv_processed/` o `csv_error/`

---

### 5. csv_to_clickhouse.py

**Versión**: 1.0.0  
**Descripción**: Carga archivos CSV (o CSV.gz) desde `csv_staging` a ClickHouse Cloud.

**Requisitos**:
- Cuenta de ClickHouse Cloud
- `clickhouse-connect`

**Variables de Entorno** (o `.env`):
- `CH_HOST`, `CH_PORT`, `CH_USER`, `CH_PASSWORD`, `CH_DATABASE`
- `CH_TABLE` (opcional)
- `CSV_STAGING_DIR`, `CSV_PROCESSED_DIR`, `CSV_ERROR_DIR`
- `FOLDERS_FILTER`, `CSV_FILTER` (opcional)

**Uso**:

```bash
# Cargar todos los CSV a ClickHouse
python csv_to_clickhouse.py

# Especificar base de datos
python csv_to_clickhouse.py default

# Filtrar por carpeta
python csv_to_clickhouse.py default SQLSERVER_POM_Aplicaciones
```

**Características**:
- Crea tablas automáticamente con estructura del CSV
- Detecta delimitador automáticamente (coma, punto y coma, tab, pipe)
- Maneja columnas duplicadas renombrándolas
- Soporta CSV comprimidos (.csv.gz)
- Sanitiza nombres de columnas y tablas
- Mueve carpetas procesadas a `csv_processed/` o `csv_error/`

**Salida**: 
- Tablas creadas en ClickHouse con los datos cargados
- Carpetas movidas a `csv_processed/` o `csv_error/`

---

### 6. ingest_all_excels_to_stage.py

**Versión**: 1.0.0  
**Descripción**: Procesa archivos Excel directamente desde `inbox` y los carga a Snowflake en un solo paso.

**Requisitos**:
- Cuenta de Snowflake
- `snowflake-connector-python`
- `openpyxl`

**Variables de Entorno** (o `.env`):
- `SF_ACCOUNT`, `SF_USER`, `SF_PASSWORD`, `SF_ROLE`, `SF_WAREHOUSE`
- `SF_DATABASE`, `SF_SCHEMA`
- `INBOX_DIR`, `PROCESSED_DIR`, `ERROR_DIR`
- `SHEETS_ALLOWLIST` (opcional)

**Uso**:

```bash
# Procesar todos los Excel en inbox
python ingest_all_excels_to_stage.py
```

**Características**:
- Lee Excel desde `inbox/`
- Convierte cada hoja a CSV comprimido (.csv.gz)
- Sube directamente al stage de Snowflake
- Carga datos a `INGEST_GENERIC_RAW`
- Registra operaciones en `INGEST_LOG`
- Mueve Excel a `processed/` o `error/` según resultado

**Salida**: 
- Datos cargados en Snowflake
- Excel movido a `processed/` o `error/`

---

### 7. snowflake_csv_to_tables.py

**Versión**: 1.0.0  
**Descripción**: Crea tablas individuales en Snowflake desde archivos CSV que ya están en el stage.

**Requisitos**:
- Cuenta de Snowflake
- Archivos CSV ya cargados en el stage `RAW_STAGE`

**Variables de Entorno** (o `.env`):
- `SF_ACCOUNT`, `SF_USER`, `SF_PASSWORD`, `SF_ROLE`, `SF_WAREHOUSE`
- `SF_DATABASE`, `SF_SCHEMA`

**Uso**:

```bash
# Crear tablas desde todos los CSV en el stage
python snowflake_csv_to_tables.py

# Especificar base de datos y schema
python snowflake_csv_to_tables.py POM_TEST01 RAW
```

**Características**:
- Lee archivos CSV desde el stage de Snowflake
- Crea una tabla por cada archivo CSV
- El nombre de la tabla es el nombre del archivo (sin extensión)
- Omite tablas que ya existen
- Detecta headers automáticamente
- Carga datos desde el stage a la tabla

**Salida**: Tablas creadas en Snowflake con los datos de los CSV

---

### 8. sqlserver_to_snowflake_streaming.py

**Versión**: 1.0.0  
**Descripción**: Exporta tablas de SQL Server directamente a Snowflake usando streaming (sin pasar por CSV intermedio).

**Requisitos**:
- Driver ODBC para SQL Server
- Cuenta de Snowflake
- `snowflake-connector-python`
- `pandas`

**Variables de Entorno** (o `.env`):
- `SQL_SERVER`, `SQL_DATABASE` (requeridos)
- `SQL_USER`, `SQL_PASSWORD` (requeridos por defecto, ver nota abajo)
- `SQL_USE_WINDOWS_AUTH` (opcional, default: `false` - si es `true`, no requiere SQL_USER/SQL_PASSWORD)
- `SQL_DRIVER` (opcional, default: "ODBC Driver 17 for SQL Server")
- `SF_ACCOUNT`, `SF_USER`, `SF_PASSWORD`, `SF_ROLE`, `SF_WAREHOUSE`
- `SF_DATABASE`, `SF_SCHEMA`
- `STREAMING_CHUNK_SIZE` (opcional, default: 10000)
- `TARGET_TABLE_PREFIX` (opcional, default: "SQLSERVER_")
- `TABLES_FILTER` (opcional)

**Uso**:

```bash
# Exportar todas las tablas usando variables de entorno
python sqlserver_to_snowflake_streaming.py

# Especificar base de datos SQL Server y Snowflake
python sqlserver_to_snowflake_streaming.py POM_DBS POM_TEST01 RAW

# Exportar tablas específicas
python sqlserver_to_snowflake_streaming.py POM_DBS POM_TEST01 RAW "Tabla1,Tabla2"
```

**Características**:
- Streaming directo desde SQL Server a Snowflake (sin archivos intermedios)
- Crea tablas automáticamente basándose en la estructura de SQL Server
- Mapea tipos de datos de SQL Server a Snowflake
- Procesa datos en chunks para manejar grandes volúmenes
- Agrega columna `ingested_at` con timestamp automático
- Excluye tablas por prefijos (configurable)

**Salida**: Tablas creadas en Snowflake con datos cargados directamente desde SQL Server

---

### 9. sqlserver_to_clickhouse_streaming.py 

**Versión**: 2.0.0  
**Descripción**: Exporta tablas de SQL Server directamente a ClickHouse usando streaming con carga incremental inteligente.

**Requisitos**:
- Driver ODBC para SQL Server
- Cuenta de ClickHouse Cloud
- `clickhouse-connect`
- `python-dotenv` (recomendado para `.env`)

**Variables de Entorno** (o `.env`):
- `SQL_SERVER`, `SQL_DATABASE` (requeridos)
- `SQL_USER`, `SQL_PASSWORD` (requeridos por defecto, ver nota abajo)
- `SQL_USE_WINDOWS_AUTH` (opcional, default: `false` - si es `true`, no requiere SQL_USER/SQL_PASSWORD)
- `SQL_DRIVER` (opcional, default: "ODBC Driver 17 for SQL Server")
- `CH_HOST`, `CH_PORT`, `CH_USER`, `CH_PASSWORD`, `CH_DATABASE` [WARN] **OBLIGATORIO**
- `STREAMING_CHUNK_SIZE` (opcional, default: 10000)
- `TARGET_TABLE_PREFIX` (opcional, default: "" - sin prefijo)
- `TABLES_FILTER` (opcional)
- `LOOKBACK_DAYS` (opcional, default: 7 - días para detectar updates)
- `USE_REPLACING_MERGE_TREE` (opcional, default: true)
- `CH_TIMEZONE` (opcional, default: UTC)

**Uso**:

```bash
# Exportar tabla específica con límite de registros
python sqlserver_to_clickhouse_streaming.py POM_Aplicaciones POM_Aplicaciones_test "PC_Gestiones" 5

# Exportar todas las tablas (modo incremental automático)
python sqlserver_to_clickhouse_streaming.py POM_Aplicaciones POM_Aplicaciones_test

# Exportar tabla específica (sin límite)
python sqlserver_to_clickhouse_streaming.py POM_Aplicaciones POM_Aplicaciones_test "PC_Gestiones"
```

**Características Avanzadas**:

####  Carga Incremental Inteligente
- **Detección automática** de columna ID o timestamp
- **Modo ID**: Procesa solo registros nuevos basado en ID incremental
- **Modo Timestamp**: Procesa solo registros nuevos basado en fecha/hora
- **Modo Hash**: Para tablas sin ID ni fecha, usa hash MD5 de la fila completa
- **Lookback Window**: Detecta updates/deletes en los últimos N días

#### 🚀 Optimizaciones de Rendimiento
- **Streaming por chunks**: Procesa datos en lotes configurables
- **Verificación de hashes por chunk**: Escalable, no carga todos los hashes
- **Sin pandas**: Trabajo directo con listas para mejor rendimiento
- **Medición de tiempo**: Muestra duración y velocidad de cada chunk

#### 🔒 Deduplicación Automática
- **ReplacingMergeTree**: Engine de ClickHouse para deduplicación automática
- **ORDER BY correcto**: Por `row_hash` (modo hash), `id` (modo ID) o `timestamp` (modo fecha)
- **Deduplicación antes de insertar**: Filtra duplicados en memoria antes de cargar

####  Monitoreo y Logging
- Muestra tiempo de procesamiento por chunk
- Velocidad de procesamiento (filas/segundo)
- Resumen final con estadísticas
- Detección de updates vs nuevos registros

**Ejemplo de Salida**:
```
[OK] Archivo .env cargado desde: C:\xampp\htdocs\etl\.env
[OK] Conectado a SQL Server: SRV-DESA\SQLEXPRESS/POM_Aplicaciones
[OK] Conectado a ClickHouse: f4rf85ygzj.eastus2.azure.clickhouse.cloud:8443/POM_Aplicaciones_test
Modo: INCREMENTAL (columna ID: Id)
-> Exportando: dbo.PC_Gestiones -> PC_Gestiones
Usando ReplacingMergeTree con versión: ingested_at
 ORDER BY: Id (para deduplicación por ID)
[OK] Tabla creada: PC_Gestiones (9 columnas)
Lookback window (7 días): 10 IDs en rango (para detectar updates)
Modo incremental (ID): último valor procesado = 10
 Iniciando streaming (chunk size: 10000)...
[OK] Chunk 1: 10000 filas insertadas (total: 10000) [1.23s] 8,130 filas/s
[OK] Chunk 2: 10000 filas insertadas (total: 20000) [1.18s] 8,475 filas/s
  Tiempo total: 2m 15.3s | Tiempo promedio por chunk: 1.22s | Velocidad: 8,197 filas/s
[OK] Exportación completada: 1 tablas exportadas
```

**Salida**: Tablas creadas en ClickHouse con datos cargados directamente desde SQL Server, con deduplicación automática y carga incremental

---

### 10. clickhouse_csv_to_tables.py

**Versión**: 1.0.0  
**Descripción**: Crea tablas individuales en ClickHouse desde archivos CSV locales.

**Requisitos**:
- Cuenta de ClickHouse Cloud
- `clickhouse-connect`
- Archivos CSV en `CSV_STAGING_DIR`

**Variables de Entorno** (o `.env`):
- `CH_HOST`, `CH_PORT`, `CH_USER`, `CH_PASSWORD`, `CH_DATABASE`
- `CSV_STAGING_DIR`

**Uso**:

```bash
# Crear tablas desde todos los CSV en staging
python clickhouse_csv_to_tables.py

# Especificar base de datos
python clickhouse_csv_to_tables.py default

# Filtrar por carpeta
python clickhouse_csv_to_tables.py default CIERRE_PROPIAS___7084110
```

**Características**:
- Lee archivos CSV desde el directorio local
- Crea una tabla por cada archivo CSV
- El nombre de la tabla es el nombre del archivo (sin extensión)
- Detecta delimitador automáticamente
- Maneja columnas duplicadas renombrándolas
- Soporta CSV comprimidos (.csv.gz) y sin comprimir
- Omite tablas que ya existen (o las reemplaza si tienen estructura diferente)

**Salida**: Tablas creadas en ClickHouse con los datos de los CSV

---

### 11. snowflake_drop_tables.py

**Versión**: 1.0.0  
**Descripción**: Elimina tablas en Snowflake de forma segura con confirmación.

**Requisitos**:
- Cuenta de Snowflake
- `snowflake-connector-python`

**Variables de Entorno** (o `.env`):
- `SF_ACCOUNT`, `SF_USER`, `SF_PASSWORD`, `SF_ROLE`, `SF_WAREHOUSE`
- `SF_DATABASE`, `SF_SCHEMA`
- `REQUIRE_CONFIRMATION` (opcional, default: "true")

**Uso**:

```bash
# Eliminar tablas específicas (requiere confirmación)
python snowflake_drop_tables.py POM_TEST01 RAW Tabla1,Tabla2,Tabla3

# Eliminar tablas por patrón
python snowflake_drop_tables.py POM_TEST01 RAW "PC_%"
```

**Características**:
- Confirmación de seguridad por defecto
- Soporta filtros por patrón (LIKE)
- Soporta lista específica de tablas
- Opción para eliminar todas las tablas del schema
- Opción para omitir confirmación (útil para scripts automatizados)
- Muestra lista de tablas a eliminar antes de confirmar

**Salida**: Tablas eliminadas en Snowflake

---

### 12. clickhouse_drop_tables.py

**Versión**: 1.0.0  
**Descripción**: Elimina tablas en ClickHouse de forma segura con confirmación.

**Requisitos**:
- Cuenta de ClickHouse Cloud
- `clickhouse-connect`

**Variables de Entorno** (o `.env`):
- `CH_HOST`, `CH_PORT`, `CH_USER`, `CH_PASSWORD`, `CH_DATABASE`
- `REQUIRE_CONFIRMATION` (opcional, default: "true")

**Uso**:

```bash
# Eliminar tablas específicas (requiere confirmación)
python clickhouse_drop_tables.py default Tabla1,Tabla2,Tabla3

# Eliminar tablas por patrón
python clickhouse_drop_tables.py default "PC_%"
```

**Características**:
- Confirmación de seguridad por defecto
- Soporta filtros por patrón (LIKE)
- Soporta lista específica de tablas
- Opción para eliminar todas las tablas de la base de datos
- Opción para omitir confirmación (útil para scripts automatizados)
- Muestra lista de tablas a eliminar antes de confirmar

**Salida**: Tablas eliminadas en ClickHouse

---

## Flujos de Trabajo Comunes

### Flujo 1: SQL Server -> CSV -> Snowflake

```bash
# 1. Exportar tablas de SQL Server a CSV
python sqlserver_to_csv.py MiBaseDeDatos

# 2. (Opcional) Comprimir CSV
python compress_csv_to_gz.py

# 3. Cargar CSV a Snowflake
python csv_to_snowflake.py POM_TEST01 RAW

# 4. (Opcional) Crear tablas individuales desde el stage
python snowflake_csv_to_tables.py POM_TEST01 RAW
```

### Flujo 2: Excel -> CSV -> ClickHouse

```bash
# 1. Convertir Excel a CSV
python excel_to_csv.py

# 2. (Opcional) Comprimir CSV
python compress_csv_to_gz.py

# 3. Cargar CSV a ClickHouse
python csv_to_clickhouse.py default
```

### Flujo 3: Excel -> Snowflake (Directo)

```bash
# Procesar Excel directamente a Snowflake
python ingest_all_excels_to_stage.py
```

### Flujo 4: SQL Server -> Snowflake (Streaming Directo)

```bash
# Exportar directamente desde SQL Server a Snowflake (sin CSV intermedio)
python sqlserver_to_snowflake_streaming.py POM_DBS POM_TEST01 RAW
```

### Flujo 5: SQL Server -> ClickHouse (Streaming Directo con Incremental) 

```bash
# Primera ejecución: carga inicial
python sqlserver_to_clickhouse_streaming.py POM_Aplicaciones POM_Aplicaciones_test "PC_Gestiones"

# Ejecuciones posteriores: solo carga registros nuevos/actualizados
python sqlserver_to_clickhouse_streaming.py POM_Aplicaciones POM_Aplicaciones_test "PC_Gestiones"
```

**Ventajas del modo incremental**:
- Solo procesa registros nuevos (más rápido)
- Detecta updates automáticamente (lookback window)
- Deduplicación automática con ReplacingMergeTree
- Funciona incluso sin ID ni fecha (modo hash)

### Flujo 6: CSV -> ClickHouse (Crear Tablas Individuales)

```bash
# Crear tablas individuales desde CSV locales
python clickhouse_csv_to_tables.py default
```

### Flujo 7: Procesamiento Completo Automatizado

```bash
# Script batch (ejemplo para Windows)
@echo off
echo Exportando SQL Server...
python sqlserver_to_csv.py MiBaseDeDatos
echo Comprimiendo CSV...
python compress_csv_to_gz.py
echo Cargando a Snowflake...
python csv_to_snowflake.py POM_TEST01 RAW
echo Creando tablas individuales...
python snowflake_csv_to_tables.py POM_TEST01 RAW
echo Proceso completado!
```

### Flujo 8: Limpieza de Tablas

```bash
# Eliminar tablas específicas en Snowflake
python snowflake_drop_tables.py POM_TEST01 RAW Tabla1,Tabla2

# Eliminar tablas por patrón en ClickHouse
python clickhouse_drop_tables.py default "TMP_%"
```

---

## Troubleshooting

### Error: "CH_PASSWORD es obligatorio"

**Solución**: 
- Crea un archivo `.env` con `CH_PASSWORD=tu_password`
- O define la variable de entorno: `set CH_PASSWORD=tu_password` (Windows) o `export CH_PASSWORD=tu_password` (Linux/Mac)
- Instala `python-dotenv`: `pip install python-dotenv`

### Error: "No se encontró un driver ODBC compatible"

**Solución**: Instala uno de los drivers ODBC para SQL Server:
- [ODBC Driver 17 for SQL Server](https://docs.microsoft.com/en-us/sql/connect/odbc/download-odbc-driver-for-sql-server)
- [SQL Server Native Client](https://docs.microsoft.com/en-us/sql/relational-databases/native-client/applications/installing-sql-server-native-client)

### Error: "Error de autenticación" en SQL Server

**Solución**: 
- **Por defecto se requiere autenticación SQL Server**: Define `SQL_USER` y `SQL_PASSWORD` en `.env`
- Si quieres usar autenticación Windows, define `SQL_USE_WINDOWS_AUTH=true` en `.env`
- Verifica que tengas permisos en SQL Server
- Si usas autenticación SQL, verifica que `SQL_USER` y `SQL_PASSWORD` sean correctos
- Si usas autenticación Windows, verifica que tu usuario tenga acceso a SQL Server

### Error: "La base de datos no existe" en Snowflake

**Solución**:
- Verifica el nombre exacto de la base de datos (case-sensitive)
- Verifica que tengas permisos para acceder a la base de datos
- El script intentará crear la base de datos si no existe (requiere permisos)

### Error: "No se pudieron leer los headers" en ClickHouse

**Solución**:
- Verifica que el CSV tenga headers en la primera fila
- Verifica que el delimitador sea correcto (coma, punto y coma, etc.)
- Verifica que el archivo no esté corrupto

### Error: "PermissionError" al mover archivos

**Solución**:
- Verifica que el archivo no esté abierto en otro programa
- Verifica permisos de escritura en las carpetas destino
- El script intenta automáticamente con reintentos

### Archivos no se procesan

**Solución**:
- Verifica que los archivos estén en las carpetas correctas (`inbox/`, `csv_staging/`)
- Verifica filtros (`FOLDERS_FILTER`, `CSV_FILTER`, `SHEETS_ALLOWLIST`)
- Verifica que los archivos tengan las extensiones correctas (`.xlsx`, `.csv`, `.csv.gz`)

### Error: "Sorting key contains nullable columns" en ClickHouse

**Solución**:
- Este error ocurre cuando una columna nullable se usa en ORDER BY
- El script ahora maneja esto automáticamente usando `ingested_at` como fallback
- Si persiste, verifica que la columna ID no sea nullable en SQL Server

---

## Notas Adicionales

### Sanitización de Nombres

Todos los scripts sanitizan automáticamente los nombres de:
- Archivos CSV
- Nombres de tablas
- Nombres de columnas
- Nombres de carpetas

Caracteres especiales se reemplazan por guiones bajos (`_`).

### Encoding

Todos los archivos CSV se generan con encoding **UTF-8** para soportar caracteres especiales.

### Manejo de NULL/NaN

Los valores NULL/NaN se manejan correctamente:
- En CSV: se convierten a cadenas vacías o se mantienen como NULL según el destino
- En Snowflake: se manejan como NULL
- En ClickHouse: se manejan como cadenas vacías o NULL según el tipo de columna

### Logging

Los scripts proporcionan información detallada:
- Operaciones exitosas
- Advertencias
- Errores
- Estadísticas (filas, columnas, tamaño de archivos)
- Tiempo de procesamiento y velocidad (en scripts de streaming)

---

## Changelog

### Versión 2.1.0 - 19 de enero de 2026

**Cambios en autenticación SQL Server (todos los scripts)**:
- [OK] **Autenticación SQL Server por defecto**: Ahora requiere `SQL_USER` y `SQL_PASSWORD` por defecto
- [OK] **Variable `SQL_USE_WINDOWS_AUTH`**: Define `SQL_USE_WINDOWS_AUTH=true` para usar autenticación Windows
- [OK] **Aplicado a todos los ETLs**: `sqlserver_to_csv.py`, `sqlserver_to_json.py`, `sqlserver_to_clickhouse_streaming.py`, `sqlserver_to_snowflake_streaming.py`
- [OK] **Script de test actualizado**: `sqlserver_test_connection.py` ahora también usa SQL Auth por defecto
- [OK] **Mensajes de error mejorados**: Indican claramente qué tipo de autenticación se está usando

### Versión 2.0.0 - 19 de enero de 2026

**Mejoras en `sqlserver_to_clickhouse_streaming.py`**:
- [OK] Carga incremental inteligente (ID, timestamp, hash)
- [OK] Deduplicación automática con ReplacingMergeTree
- [OK] Lookback window para detectar updates/deletes
- [OK] Verificación de hashes por chunk (escalable)
- [OK] Normalización explícita de valores para hashing
- [OK] ORDER BY correcto según modo incremental
- [OK] Eliminado pandas innecesario (mejor rendimiento)
- [OK] Medición de tiempo y velocidad por chunk
- [OK] Soporte para archivo `.env` con `python-dotenv`
- [OK] Validación de credenciales obligatorias
- [OK] Manejo correcto de DateTime64 con timezone
- [OK] Manejo de columnas nullable en ORDER BY

### Versión 1.0.0 - 19 de enero de 2026

**Scripts incluidos**:
- `sqlserver_to_csv.py` v1.0.0
- `excel_to_csv.py` v1.0.0
- `compress_csv_to_gz.py` v1.0.0
- `csv_to_snowflake.py` v1.0.0
- `csv_to_clickhouse.py` v1.0.0
- `ingest_all_excels_to_stage.py` v1.0.0
- `snowflake_csv_to_tables.py` v1.0.0
- `sqlserver_to_snowflake_streaming.py` v1.0.0
- `sqlserver_to_clickhouse_streaming.py` v1.0.0
- `clickhouse_csv_to_tables.py` v1.0.0
- `snowflake_drop_tables.py` v1.0.0
- `clickhouse_drop_tables.py` v1.0.0

**Características iniciales**:
- Exportación de tablas SQL Server a CSV
- Conversión de Excel a CSV
- Compresión de CSV a formato GZ
- Carga automática a Snowflake y ClickHouse
- Streaming directo SQL Server -> Snowflake/ClickHouse
- Creación de tablas individuales desde CSV
- Eliminación segura de tablas con confirmación
- Filtrado flexible por tablas, carpetas y archivos
- Manejo robusto de errores con reintentos
- Logging detallado de operaciones
- Sanitización automática de nombres
- Exclusión de tablas por prefijos en SQL Server

---

## Contribuciones

Las contribuciones son bienvenidas. Por favor:

1. Fork el proyecto
2. Crea una rama para tu feature (`git checkout -b feature/AmazingFeature`)
3. Commit tus cambios (`git commit -m 'Add some AmazingFeature'`)
4. Push a la rama (`git push origin feature/AmazingFeature`)
5. Abre un Pull Request

---

## Licencia

Este proyecto está bajo la Licencia MIT - ver el archivo LICENSE para más detalles.

---

## Contacto y Soporte

**Autor**: Herbert Poveda  
**Empresa**: POM Cobranzas  
**Departamento**: Business Intelligence (BI)  
**Fecha**: 19 de enero de 2026

Para preguntas o problemas:
- Abre un issue en el repositorio
- Revisa la sección [Troubleshooting](#troubleshooting)
- Verifica las variables de entorno y configuración en `.env`

---

**Desarrollado por Business Intelligence (BI) - POM Cobranzas**
