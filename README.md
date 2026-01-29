# ETL Scripts - Documentación

Scripts ETL para migración de datos entre SQL Server, ClickHouse y otros sistemas.

---

## Estructura de Carpetas

Los scripts están organizados en las siguientes carpetas:

- **`silver/`** - Scripts de migración a formato Silver (tipos de datos reales)
  - `sqlserver_to_clickhouse_silver.py` - Migración SQL Server → ClickHouse (Silver)
  - `clickhouse_raw_to_table.py` - Convierte tablas RAW a tablas Silver

- **`gold/`** - Scripts de transformación a formato Gold (reservado para futuras transformaciones avanzadas)

- **`streaming/`** - Scripts de streaming continuo e incremental
  - `sqlserver_to_clickhouse_streamingv4.py` - Streaming continuo SQL Server → ClickHouse (recomendado)
  - `sqlserver_to_snowflake_streaming.py` - Streaming SQL Server → Snowflake

- **`tools/`** - Herramientas y utilidades
  - **Extracción y conversión:**
    - `sqlserver_to_csv.py` - Exporta tablas de SQL Server a CSV
    - `sqlserver_to_json.py` - Exporta tablas de SQL Server a JSON
    - `excel_to_csv.py` - Convierte archivos Excel a CSV
    - `compress_csv_to_gz.py` - Comprime archivos CSV a formato .gz
  - **Carga de datos:**
    - `csv_to_clickhouse.py` - Carga CSV a ClickHouse
    - `csv_to_mysql.py` - Carga CSV a MySQL
    - `csv_to_snowflake.py` - Carga CSV a Snowflake
    - `clickhouse_csv_to_tables.py` - Crea tablas desde CSV en ClickHouse
    - `snowflake_csv_to_tables.py` - Crea tablas desde CSV en Snowflake
  - **Administración:**
    - `clickhouse_drop.py` - Elimina bases de datos o tablas en ClickHouse
    - `clickhouse_drop_tables.py` - Elimina múltiples tablas en ClickHouse
    - `clickhouse_truncate.py` - Vacía tablas en ClickHouse
    - `snowflake_drop_tables.py` - Elimina tablas en Snowflake
    - `clone_clickhouse_database.py` - Clona bases de datos en ClickHouse
    - `enable_cdc_sqlserver.py` - Habilita Change Data Capture en SQL Server
  - **Verificación:**
    - `check_all_connections.py` - Verifica conexiones a todos los sistemas
    - `check_clickhouse_databases.py` - Verifica bases de datos en ClickHouse
    - `check_sqlserver_databases.py` - Verifica bases de datos en SQL Server
    - `sqlserver_test_connection.py` - Prueba conexión a SQL Server

- **`services/`** - Scripts de servicios y automatización
  - `run_streaming_allv4.sh` - Gestiona servicios streaming v4 continuos (recomendado)
  - `run_streaming_allv2.sh` - Ejecuta streaming v2 batch (para cron)
  - `run_streaming_all.sh` - Versión anterior del runner

- **`archive/`** - Scripts antiguos archivados (versiones anteriores)

---

## Índice

### Instalación y Configuración
- [Instalación](#instalación)
- [Configuración](#configuración)

### Scripts Principales

#### SQL Server → ClickHouse
- [1. SQL Server → ClickHouse (Silver)](#1-sql-server--clickhouse-silver)
- [2. SQL Server → ClickHouse (Raw)](#2-sql-server--clickhouse-raw)
- [3. SQL Server → ClickHouse (Streaming Incremental)](#3-sql-server--clickhouse-streaming-incremental)
- [4. ClickHouse Raw → Tabla](#4-clickhouse-raw--tabla)

#### ClickHouse - Administración
- [5. ClickHouse - DROP](#5-clickhouse---drop)
- [6. ClickHouse - TRUNCATE](#6-clickhouse---truncate)
- [14. ClickHouse - DROP Tables](#14-clickhouse---drop-tables)

#### SQL Server → CSV/Excel
- [7. SQL Server → CSV](#7-sql-server--csv)
- [9. Excel → CSV](#9-excel--csv)
- [10. Comprimir CSV](#10-comprimir-csv)

#### CSV → ClickHouse/Snowflake
- [8. CSV → ClickHouse](#8-csv--clickhouse)
- [12. CSV → Snowflake](#12-csv--snowflake)

#### SQL Server → Snowflake
- [11. SQL Server → Snowflake (Streaming)](#11-sql-server--snowflake-streaming)
- [13. Snowflake - DROP Tables](#13-snowflake---drop-tables)

#### Utilidades
- [15. Verificar Conexiones](#15-verificar-conexiones)
- [16. Verificar Bases de Datos - ClickHouse](#16-verificar-bases-de-datos---clickhouse)
- [17. Verificar Bases de Datos - SQL Server](#17-verificar-bases-de-datos---sql-server)
- [18. Clonar Base de Datos - ClickHouse](#18-clonar-base-de-datos---clickhouse)

### Guías
- [Flujos Comunes](#flujos-comunes)
  - [Migración Completa SQL Server → ClickHouse](#migración-completa-sql-server--clickhouse)
  - [Migración Incremental (Solo Nuevos)](#migración-incremental-solo-nuevos)
  - [Limpiar y Re-migrar](#limpiar-y-re-migrar)
  - [Vaciar Tabla (Mantener Estructura)](#vaciar-tabla-mantener-estructura)
- [Logs y Monitoreo](#logs-y-monitoreo)
- [Troubleshooting](#troubleshooting)
- [Notas](#notas)

---

## Instalación

### Dependencias Python

```bash
pip install pandas pyodbc openpyxl clickhouse-connect snowflake-connector-python python-dotenv
```

### Driver ODBC SQL Server

Instalar uno de estos drivers:
- ODBC Driver 17 for SQL Server (recomendado)
- ODBC Driver 13 for SQL Server
- SQL Server Native Client 11.0

---

## Configuración

Crear archivo `.env` en el directorio `etl/`:

```env
# SQL Server (Desarrollo)
SQL_SERVER=SRV-DESA\SQLEXPRESS
SQL_USER=tu_usuario
SQL_PASSWORD=tu_password
SQL_DRIVER=ODBC Driver 17 for SQL Server

# SQL Server Producción (opcional, para sqlserver_to_clickhouse_streaming.py --prod)
SQL_SERVER_PROD=SRV-PROD\SQLEXPRESS
SQL_USER_PROD=usuario_prod
SQL_PASSWORD_PROD=password_prod

# ClickHouse
CH_HOST=f4rf85ygzj.eastus2.azure.clickhouse.cloud
CH_PORT=8443
CH_USER=default
CH_PASSWORD=tu_password
CH_DATABASE=default

# Snowflake (opcional)
SF_ACCOUNT=tu_account
SF_USER=tu_usuario
SF_PASSWORD=tu_password
SF_ROLE=ACCOUNTADMIN
SF_WAREHOUSE=COMPUTE_WH
SF_DATABASE=POM_TEST01
SF_SCHEMA=RAW
```

---

## Scripts Principales

### 1. SQL Server → ClickHouse (Silver)

Migra tablas de SQL Server a ClickHouse con tipos de datos reales.

**Uso:**
```bash
python silver/sqlserver_to_clickhouse_silver.py ORIG_DB DEST_DB [tablas] [limit] [reset]
```

**Ejemplos:**
```bash
# Migrar todas las tablas de POM_Aplicaciones
python silver/sqlserver_to_clickhouse_silver.py POM_Aplicaciones POM_Aplicaciones

# Migrar tablas específicas (PC_Gestiones y Casos)
python silver/sqlserver_to_clickhouse_silver.py POM_Aplicaciones POM_Aplicaciones "dbo.PC_Gestiones,dbo.Casos"

# Migrar con límite de registros (útil para pruebas)
python silver/sqlserver_to_clickhouse_silver.py POM_Aplicaciones POM_Aplicaciones dbo.PC_Gestiones 1000

# Migrar todas las tablas y resetear (DROP + CREATE)
python silver/sqlserver_to_clickhouse_silver.py POM_Aplicaciones POM_Aplicaciones * 0 reset
```

**Características:**
- Mapea tipos de datos SQL Server → ClickHouse
- Crea tablas con estructura real (no String genérico)
- Soporta tablas específicas o todas (`*`)
- Opción `reset` para eliminar y recrear tablas

---

### 2. SQL Server → ClickHouse (Raw)

Migra tablas a ClickHouse con todas las columnas como String (raw).

**Uso:**
```bash
python archive/sqlserver_to_clickhouse.py ORIG_DB DEST_DB [tablas] [limit]
```

**Ejemplos:**
```bash
# Migrar todas las tablas
python archive/sqlserver_to_clickhouse.py POM_Aplicaciones POM_Aplicaciones

# Migrar tablas específicas
python archive/sqlserver_to_clickhouse.py POM_Aplicaciones POM_Aplicaciones "dbo.PC_Gestiones,dbo.Casos"

# Migrar con límite
python archive/sqlserver_to_clickhouse.py POM_Aplicaciones POM_Aplicaciones dbo.PC_Gestiones 5000
```

**Nota:** Este script está en `archive/` porque se recomienda usar `sqlserver_to_clickhouse_silver.py` en su lugar.

---

### 3. SQL Server → ClickHouse (Streaming Incremental v2)

Streaming incremental liviano que solo inserta registros nuevos. **Requiere que las tablas ya existan** (usar `sqlserver_to_clickhouse_silver.py` primero para crear las tablas).

**Uso:**
```bash
python archive/sqlserver_to_clickhouse_streamingv2.py ORIG_DB DEST_DB [tablas] [limit] [--prod]
```

**Ejemplos:**
```bash
# Streaming incremental de todas las tablas (desarrollo)
python archive/sqlserver_to_clickhouse_streamingv2.py POM_Aplicaciones POM_Aplicaciones

# Streaming de una tabla específica (desarrollo)
python archive/sqlserver_to_clickhouse_streamingv2.py POM_Aplicaciones POM_Aplicaciones dbo.PC_Gestiones

# Streaming desde SQL Server PRODUCCIÓN (usar --prod)
python archive/sqlserver_to_clickhouse_streamingv2.py POM_Aplicaciones POM_Aplicaciones --prod
```

**Nota:** Este script está en `archive/`. Se recomienda usar `sqlserver_to_clickhouse_streamingv4.py` en su lugar.

**Características:**
- **Liviano y rápido**: asume tablas existentes
- **Detección automática**: detecta columna incremental (IDENTITY > Id > ID > última int)
- **Solo nuevos registros**: consulta el último valor en ClickHouse y solo inserta filas nuevas
- **Ejecución batch**: diseñado para ejecutarse en cron
- **Soporte producción**: usa `--prod` para conectar a SQL Server de producción

**Requisito previo:**
Las tablas deben existir en ClickHouse. Si no existen, el script las omitirá con un mensaje indicando que uses `sqlserver_to_clickhouse_silver.py` primero.

---

### 3.1. SQL Server → ClickHouse (Streaming Continuo v4) ⭐ RECOMENDADO

Streaming **continuo y seguro** que funciona como servicio. Implementa estrategias adaptativas para detectar cambios de forma no invasiva.

**✅ Características principales:**
- **No invasivo**: Solo queries SELECT, sin modificar la base de datos
- **Lock file local**: Previene ejecuciones duplicadas en el mismo servidor
- **Estrategias adaptativas**: ROWVERSION > ID > Timestamp+PK
- **Servicio continuo**: No requiere cron, corre indefinidamente
- **Seguro para producción**: No requiere permisos especiales

**⚠️ IMPORTANTE - Limitaciones:**
- **Solo UNA instancia a la vez**: El lock file previene duplicados en el mismo servidor, pero NO previene race conditions entre múltiples servidores
- **Estado en ClickHouse**: Depende de merges (ReplacingMergeTree). Si hay out-of-order updates, podrían perderse temporalmente hasta el merge
- **ROWVERSION como UInt64**: La columna ROWVERSION en ClickHouse debe ser tipo `UInt64` (no String)

**Uso:**
```bash
python streaming/sqlserver_to_clickhouse_streamingv4.py ORIG_DB DEST_DB [tablas] [--prod] [--poll-interval SECONDS]
```

**Ejemplos:**
```bash
# Streaming de todas las tablas (desarrollo)
python streaming/sqlserver_to_clickhouse_streamingv4.py POM_Aplicaciones POM_Aplicaciones

# Streaming de tablas específicas (producción)
python streaming/sqlserver_to_clickhouse_streamingv4.py POM_Aplicaciones POM_Aplicaciones "PC_Gestiones,Casos" --prod

# Con intervalo de polling personalizado (10 segundos)
python streaming/sqlserver_to_clickhouse_streamingv4.py POM_Aplicaciones POM_Aplicaciones --prod --poll-interval 10
```

**Estrategias de detección (prioridad):**

1. **ROWVERSION** (Mejor opción)
   - Nativo de SQL Server, se actualiza automáticamente
   - No depende de zonas horarias
   - Detecta INSERTs y UPDATEs
   - Más seguro y eficiente

2. **ID Incremental**
   - Usa columna ID (identity o numérica)
   - Detecta solo INSERTs nuevos
   - Simple y eficiente

3. **Timestamp + PK** (Watermark doble)
   - Solo si hay columna de modificación Y PK/ID
   - Watermark doble evita pérdida de datos por empates
   - Detecta INSERTs y UPDATEs

**Variables de entorno:**
```env
# Streaming v4
POLL_INTERVAL=10  # Intervalo de polling en segundos (default: 10)
INSERT_BATCH_ROWS=2000  # Tamaño de batch para inserts
```

**Ejecución como servicio:**

Usar el script `run_streaming_allv4.sh`:

```bash
# Iniciar todos los servicios
./run_streaming_allv4.sh start

# Ver estado de servicios
./run_streaming_allv4.sh status

# Detener todos los servicios
./run_streaming_allv4.sh stop

# Reiniciar todos los servicios
./run_streaming_allv4.sh restart
```

**IMPORTANTE - Manejo de UPDATEs:**

Para tablas con ROWVERSION o Timestamp (que detectan UPDATEs):
- ClickHouse debe usar **ReplacingMergeTree** con `ORDER BY (Id)`
- Los updates se insertan como nuevas filas (event sourcing)
- ClickHouse elimina duplicados automáticamente usando ReplacingMergeTree

**Ejemplo de tabla en ClickHouse:**
```sql
CREATE TABLE POM_Aplicaciones.PC_Gestiones
(
    Id Int64,
    -- otras columnas --
    RowVersion UInt64  -- ROWVERSION como UInt64 (convertido desde bytes)
)
ENGINE = ReplacingMergeTree(RowVersion)
ORDER BY Id;
```

**⚠️ NOTA:** La columna ROWVERSION debe ser `UInt64` en ClickHouse, no `String`. El script convierte automáticamente los bytes de SQL Server a UInt64.

**Ventajas vs v2:**
- ✅ Servicio continuo (no requiere cron)
- ✅ Estado en ClickHouse (consulta directa, no archivos locales)
- ✅ Detecta UPDATEs (con ROWVERSION o Timestamp)
- ✅ Más seguro (watermark doble para timestamps - último registro real ordenado)
- ✅ ROWVERSION como UInt64 (orden numérico correcto, no lexicográfico)
- ✅ Lock file local previene ejecuciones duplicadas en el mismo servidor

**Requisitos:**
- Las tablas deben existir en ClickHouse
- Tablas deben tener ROWVERSION, ID o Timestamp+PK
- Para UPDATEs: usar ReplacingMergeTree en ClickHouse

---

### 4. ClickHouse Raw → Tabla

Convierte tablas RAW (String) a tablas Silver (tipos reales).

**Uso:**
```bash
python silver/clickhouse_raw_to_table.py DEST_DB [TABLE] [LIMIT]
```

**Ejemplos:**
```bash
# Convertir todas las tablas RAW
python silver/clickhouse_raw_to_table.py POM_Aplicaciones

# Convertir tabla específica
python silver/clickhouse_raw_to_table.py POM_Aplicaciones PC_Gestiones

# Convertir con límite
python silver/clickhouse_raw_to_table.py POM_Aplicaciones PC_Gestiones 10000
```

---

### 5. ClickHouse - DROP

Elimina base de datos o tabla en ClickHouse.

**Uso:**
```bash
# Eliminar base de datos
python tools/clickhouse_drop.py DATABASE nombre_base_datos

# Eliminar tabla
python tools/clickhouse_drop.py TABLE nombre_base_datos nombre_tabla
```

**Ejemplos:**
```bash
# Eliminar base de datos (requiere confirmación)
python tools/clickhouse_drop.py DATABASE POM_Aplicaciones

# Eliminar tabla
python tools/clickhouse_drop.py TABLE POM_Aplicaciones PC_Gestiones
python tools/clickhouse_drop.py TABLE POM_Aplicaciones Casos
```

---

### 6. ClickHouse - TRUNCATE

Vacía una tabla (elimina datos, mantiene estructura).

**Uso:**
```bash
python tools/clickhouse_truncate.py nombre_base_datos nombre_tabla
```

**Ejemplos:**
```bash
# Vaciar tabla
python tools/clickhouse_truncate.py POM_Aplicaciones PC_Gestiones
python tools/clickhouse_truncate.py POM_Aplicaciones Casos
```

---

### 7. SQL Server → CSV

Exporta tablas de SQL Server a archivos CSV.

**Uso:**
```bash
python tools/sqlserver_to_csv.py DATABASE [tablas]
```

**Ejemplos:**
```bash
# Exportar todas las tablas
python tools/sqlserver_to_csv.py POM_Aplicaciones

# Exportar tablas específicas
python tools/sqlserver_to_csv.py POM_Aplicaciones "PC_Gestiones,Casos"
```

---

### 8. CSV → ClickHouse

Carga archivos CSV a ClickHouse.

**Uso:**
```bash
python tools/csv_to_clickhouse.py [DEST_DB] [folder_filter]
```

**Ejemplos:**
```bash
# Cargar todos los CSV
python tools/csv_to_clickhouse.py POM_Aplicaciones

# Cargar CSV de carpeta específica
python tools/csv_to_clickhouse.py POM_Aplicaciones SQLSERVER_POM_Aplicaciones
```

---

### 9. Excel → CSV

Convierte archivos Excel (.xlsx) a CSV.

**Uso:**
```bash
python tools/excel_to_csv.py
```

**Características:**
- Procesa archivos en `UPLOADS/POM_DROP/inbox/`
- Cada hoja del Excel → 1 archivo CSV
- Mueve Excel procesado a `processed/` o `error/`

---

### 10. Comprimir CSV

Comprime archivos CSV a formato .gz.

**Uso:**
```bash
python tools/compress_csv_to_gz.py [folder_filter] [csv_filter]
```

**Ejemplos:**
```bash
# Comprimir todos los CSV
python tools/compress_csv_to_gz.py

# Comprimir CSV de carpeta específica
python tools/compress_csv_to_gz.py SQLSERVER_POM_Aplicaciones

# Comprimir CSV específicos
python tools/compress_csv_to_gz.py SQLSERVER_POM_Aplicaciones "PC_Gestiones,Casos"
```

---

### 11. SQL Server → Snowflake (Streaming)

Migración directa SQL Server → Snowflake.

**Uso:**
```bash
python streaming/sqlserver_to_snowflake_streaming.py ORIG_DB DEST_DB SCHEMA [tablas]
```

**Ejemplos:**
```bash
# Migrar todas las tablas
python streaming/sqlserver_to_snowflake_streaming.py POM_Aplicaciones POM_TEST01 RAW

# Migrar tablas específicas
python streaming/sqlserver_to_snowflake_streaming.py POM_Aplicaciones POM_TEST01 RAW "PC_Gestiones,Casos"
```

---

### 12. CSV → Snowflake

Carga archivos CSV a Snowflake.

**Uso:**
```bash
python tools/csv_to_snowflake.py [DEST_DB] [SCHEMA] [folder_filter]
```

**Ejemplos:**
```bash
# Cargar todos los CSV
python tools/csv_to_snowflake.py POM_TEST01 RAW

# Cargar CSV de carpeta específica
python tools/csv_to_snowflake.py POM_TEST01 RAW SQLSERVER_POM_Aplicaciones
```

---

### 13. Snowflake - DROP Tables

Elimina tablas en Snowflake.

**Uso:**
```bash
python tools/snowflake_drop_tables.py DEST_DB SCHEMA [tablas|pattern]
```

**Ejemplos:**
```bash
# Eliminar tablas específicas
python tools/snowflake_drop_tables.py POM_TEST01 RAW "PC_Gestiones,Casos"

# Eliminar por patrón
python tools/snowflake_drop_tables.py POM_TEST01 RAW "PC_%"
```

---

### 14. ClickHouse - DROP Tables

Elimina tablas en ClickHouse.

**Uso:**
```bash
python tools/clickhouse_drop_tables.py DEST_DB [tablas|pattern]
```

**Ejemplos:**
```bash
# Eliminar tablas específicas
python tools/clickhouse_drop_tables.py POM_Aplicaciones "PC_Gestiones,Casos"

# Eliminar por patrón
python tools/clickhouse_drop_tables.py POM_Aplicaciones "PC_%"
```

---

### 15. Verificar Conexiones

Verifica conexiones a SQL Server, ClickHouse y Snowflake.

**Uso:**
```bash
python tools/check_all_connections.py
```

---

### 16. Verificar Bases de Datos - ClickHouse

Muestra información detallada de todas las bases de datos en ClickHouse: cantidad de tablas, vistas, funciones y tamaño en KB.

**Uso:**
```bash
python tools/check_clickhouse_databases.py
```

**Salida:**
```
================================================================================
INFORMACIÓN DE BASES DE DATOS - CLICKHOUSE
================================================================================
Servidor: 192.168.100.114:8123
Usuario: default

[OK] Conexión a ClickHouse establecida

Base de Datos                    Tablas     Vistas     SP/Func    Tamaño (KB)        
--------------------------------------------------------------------------------
POM_Aplicaciones                 45         2          0          1.234.567,89       
POM_Reportes                     12         0          0          456.789,12           
--------------------------------------------------------------------------------
TOTAL                            57         2          0          1.691.357,01       
================================================================================
```

**Características:**
- Lista todas las bases de datos (excluyendo las del sistema)
- Muestra cantidad de tablas, vistas y funciones personalizadas
- Muestra tamaño en KB con separadores de miles (formato: 1.234.567,89)
- Muestra totales al final

---

### 17. Verificar Bases de Datos - SQL Server

Muestra información detallada de todas las bases de datos en SQL Server: cantidad de tablas, vistas, stored procedures y tamaño en KB.

**Uso:**
```bash
# Desarrollo (por defecto)
python tools/check_sqlserver_databases.py
python tools/check_sqlserver_databases.py --dev

# Producción
python tools/check_sqlserver_databases.py --prod
python tools/check_sqlserver_databases.py prod
```

**Salida:**
```
================================================================================
INFORMACIÓN DE BASES DE DATOS - SQL SERVER
================================================================================
Entorno: DESARROLLO
Servidor: SRV-DESA\SQLEXPRESS
Usuario: tu_usuario

[OK] Conexión a SQL Server (DESARROLLO) establecida. Conectado a: master
[INFO] Bases de datos excluidas (blacklist): Archive_Reporteria, POMRestricted, Testing

Base de Datos                    Tablas     Vistas     SP         Tamaño (KB)        
--------------------------------------------------------------------------------
POM_Aplicaciones                 45         8          12         2.345.678,90       
POM_Reportes                     12         3          5          567.890,12         
--------------------------------------------------------------------------------
TOTAL                            57         11         17         2.913.569,02       
================================================================================
```

**Características:**
- Lista todas las bases de datos (excluyendo las del sistema: master, tempdb, model, msdb)
- **Blacklist automática**: Excluye bases de datos sin acceso (Archive_Reporteria, POMRestricted, Testing, etc.)
- Soporte para entornos: `--dev` (desarrollo, por defecto) o `--prod` (producción)
- Muestra cantidad de tablas, vistas y stored procedures
- Muestra tamaño en KB con separadores de miles (formato: 1.234.567,89)
- Muestra totales al final
- Maneja errores por base de datos individualmente

**Configuración para Producción:**
Agregar al archivo `.env`:
```env
# SQL Server Producción
SQL_SERVER_PROD=SRV-PROD\SQLEXPRESS
SQL_USER_PROD=usuario_prod
SQL_PASSWORD_PROD=password_prod
```

**Blacklist de Bases de Datos:**
Las bases de datos que dan error de acceso se excluyen automáticamente. Para agregar más bases de datos a la blacklist, editar la lista `EXCLUDED_DATABASES` en el script.

---

### 18. Clonar Base de Datos - ClickHouse

Clona una base de datos completa de ClickHouse (estructura y opcionalmente datos) a otra base de datos.

**Uso:**
```bash
# Clonar solo estructura (sin datos)
python tools/clone_clickhouse_database.py ORIG_DB DEST_DB

# Clonar estructura + datos
python tools/clone_clickhouse_database.py ORIG_DB DEST_DB --data

# Clonar eliminando tablas existentes en destino
python tools/clone_clickhouse_database.py ORIG_DB DEST_DB --data --drop-existing
```

**Ejemplos:**
```bash
# Crear backup de estructura
python tools/clone_clickhouse_database.py POM_Aplicaciones POM_Aplicaciones_backup

# Crear backup completo (estructura + datos)
python tools/clone_clickhouse_database.py POM_Aplicaciones POM_Aplicaciones_backup --data

# Clonar a nueva base de datos eliminando existentes
python tools/clone_clickhouse_database.py POM_Reportes POM_Reportes_test --data --drop-existing
```

**Características:**
- Clona todas las tablas y vistas de la base de datos origen
- Opción para clonar solo estructura o estructura + datos
- Opción para eliminar tablas existentes en destino antes de clonar
- Preserva la estructura completa (columnas, tipos, índices, ENGINE, etc.)
- Muestra progreso y resumen al final

**Parámetros:**
- `ORIG_DB`: Base de datos origen (debe existir)
- `DEST_DB`: Base de datos destino (se crea si no existe)
- `--data`: Clonar también los datos (sin este parámetro solo clona estructura)
- `--drop-existing`: Eliminar tablas/vistas existentes en destino antes de clonar

**Notas:**
- Si una tabla/vista ya existe en destino y no se usa `--drop-existing`, se omite con un mensaje
- El clonado de datos puede tardar según el tamaño de las tablas
- Las vistas se clonan siempre sin datos (solo estructura)

---

## Flujos Comunes

### Migración Completa SQL Server → ClickHouse

```bash
# 1. Migrar todas las tablas (Silver)
python silver/sqlserver_to_clickhouse_silver.py POM_Aplicaciones POM_Aplicaciones * 0 reset

# 2. Verificar datos
python tools/check_all_connections.py
```

### Migración Incremental (Solo Nuevos)

**Opción 1: Streaming v2 (Batch - Cron)**
```bash
# 1. Primera vez: crear tablas y carga inicial (Silver)
python silver/sqlserver_to_clickhouse_silver.py POM_Aplicaciones POM_Aplicaciones "dbo.PC_Gestiones,dbo.Casos"

# 2. Automatizar: ejecutar streaming v2 periódicamente (cron)
# Configurar en crontab: */5 * * * * /bin/bash /home/hpoveda/etl/services/run_streaming_allv2.sh
```

**Opción 2: Streaming v4 (Servicio Continuo) ⭐ RECOMENDADO**
```bash
# 1. Primera vez: crear tablas y carga inicial (Silver)
python silver/sqlserver_to_clickhouse_silver.py POM_Aplicaciones POM_Aplicaciones "dbo.PC_Gestiones,dbo.Casos"

# 2. Iniciar servicio streaming v4 (corre indefinidamente)
./services/run_streaming_allv4.sh start

# 3. Verificar estado
./services/run_streaming_allv4.sh status
```

**Nota:** 
- v2: Detecta automáticamente el último ID procesado y solo inserta registros nuevos. Diseñado para cron.
- v4: Servicio continuo que detecta cambios usando ROWVERSION, ID o Timestamp+PK. Estado en ClickHouse. **IMPORTANTE:** Solo debe ejecutarse UNA instancia a la vez (lock file local previene duplicados en el mismo servidor, pero NO entre múltiples servidores).

### Limpiar y Re-migrar

```bash
# 1. Eliminar tabla
python tools/clickhouse_drop.py TABLE POM_Aplicaciones PC_Gestiones

# 2. Re-migrar
python silver/sqlserver_to_clickhouse_silver.py POM_Aplicaciones POM_Aplicaciones dbo.PC_Gestiones
```

### Vaciar Tabla (Mantener Estructura)

```bash
# Vaciar datos
python tools/clickhouse_truncate.py POM_Aplicaciones PC_Gestiones

# Re-cargar datos
python silver/sqlserver_to_clickhouse_silver.py POM_Aplicaciones POM_Aplicaciones dbo.PC_Gestiones
```

---

## Logs y Monitoreo

### Scripts de Streaming Automatizado

#### run_streaming_allv2.sh (Batch - Cron)

El script `run_streaming_allv2.sh` ejecuta streaming incremental v2 (batch) de múltiples bases de datos. Diseñado para ejecutarse en cron.

#### run_streaming_allv4.sh (Servicio Continuo) ⭐ RECOMENDADO

El script `run_streaming_allv4.sh` gestiona servicios streaming v4 continuos. Cada base de datos corre como servicio independiente en background.

**Ubicación de logs:**

**v2 (Batch):**
```
etl/logs/
├── runner.log              # Log del script runner v2
├── POM_Aplicaciones.log    # Log de streaming v2 de POM_Aplicaciones
└── ...
```

**v4 (Servicio Continuo):**
```
etl/logs/
├── runner_v4.log           # Log del script runner v4
├── POM_Aplicaciones_v4.log # Log de servicio streaming v4 de POM_Aplicaciones
└── ...

/tmp/streaming_v4_pids/
├── POM_Aplicaciones.pid   # PID del servicio
└── ...
```

**Formato del runner.log:**
El archivo `runner.log` incluye información detallada de cada ejecución:
- Fecha y hora de inicio y fin
- Qué base de datos se está procesando
- Tiempo de duración de cada streaming (en minutos y segundos)
- Estado de cada ejecución (✓ éxito o ✗ error)
- Tiempo total de ejecución completa

**Ejemplo de salida en runner.log:**
```
[2026-01-26 23:45:00] ==========================================
[2026-01-26 23:45:00] INICIO DE EJECUCIÓN COMPLETA
[2026-01-26 23:45:00] Fecha/Hora: 2026-01-26 23:45:00
[2026-01-26 23:45:00] ==========================================
[2026-01-26 23:45:00] Iniciando streaming: POM_Aplicaciones
[2026-01-26 23:47:32] ✓ Completado: POM_Aplicaciones | Duración: 2m 32s
[2026-01-26 23:47:32] Iniciando streaming: POM_Reportes
[2026-01-26 23:48:15] ✓ Completado: POM_Reportes | Duración: 0m 43s
[2026-01-26 23:48:15] Iniciando streaming: Reporteria
[2026-01-26 23:49:02] ✓ Completado: Reporteria | Duración: 0m 47s
[2026-01-26 23:49:02] Iniciando streaming: POM_PJ
[2026-01-26 23:49:18] ✓ Completado: POM_PJ | Duración: 0m 16s
[2026-01-26 23:49:18] Iniciando streaming: POM_Buro
[2026-01-26 23:49:35] ✓ Completado: POM_Buro | Duración: 0m 17s
[2026-01-26 23:49:35] Iniciando streaming: POM_Historico
[2026-01-26 23:49:42] ✓ Completado: POM_Historico | Duración: 0m 7s
[2026-01-26 23:49:42] ==========================================
[2026-01-26 23:49:42] FIN DE EJECUCIÓN COMPLETA
[2026-01-26 23:49:42] Fecha/Hora: 2026-01-26 23:49:42
[2026-01-26 23:49:42] Tiempo total: 4m 42s
[2026-01-26 23:49:42] ==========================================
```

**Uso del script v2 (batch):**
```bash
# Ejecutar manualmente
cd /home/hpoveda/etl
bash services/run_streaming_allv2.sh

# O hacer ejecutable y ejecutar
chmod +x services/run_streaming_allv2.sh
./services/run_streaming_allv2.sh
```

**Uso del script v4 (servicio continuo):**
```bash
# Hacer ejecutable
chmod +x services/run_streaming_allv4.sh

# Iniciar todos los servicios
./services/run_streaming_allv4.sh start

# Ver estado
./services/run_streaming_allv4.sh status

# Detener todos
./services/run_streaming_allv4.sh stop

# Reiniciar todos
./services/run_streaming_allv4.sh restart
```

**Configuración en cron (v2 - batch):**

Para v2, configurar en crontab:
```bash
# Ejecutar cada 5 minutos
*/5 * * * * /bin/bash /home/hpoveda/etl/services/run_streaming_allv2.sh >> /var/log/etl/runner.log 2>&1
```

**Configuración como servicio systemd (v4 - recomendado):**

Para v4, es mejor usar systemd en lugar de cron:

```bash
# Crear servicio systemd
sudo nano /etc/systemd/system/streaming-v4.service
```

```ini
[Unit]
Description=SQL Server to ClickHouse Streaming v4 Services
After=network.target

[Service]
Type=oneshot
User=hpoveda
WorkingDirectory=/home/hpoveda/etl
ExecStart=/bin/bash /home/hpoveda/etl/services/run_streaming_allv4.sh start
RemainAfterExit=yes

[Install]
WantedBy=multi-user.target
```

O ejecutar directamente como servicios continuos (sin systemd):
```bash
# Iniciar servicios manualmente
./services/run_streaming_allv4.sh start

# Los servicios corren en background indefinidamente
# Para detener: ./services/run_streaming_allv4.sh stop
```

**Verificar crontab activo:**
```bash
# Ver crontab actual
crontab -l

# Ver logs del sistema cron
grep CRON /var/log/syslog | tail -20
```

**Nota sobre logs:**
- El script guarda logs individuales en `etl/logs/` (uno por cada base de datos)
- El crontab redirige la salida completa a `/var/log/etl/runner.log` (log del sistema)
- Ambos logs son útiles: los individuales para debugging específico, el del sistema para ver ejecuciones completas

**Ver logs en tiempo real:**
```bash
# Ver log de una base de datos específica (logs individuales)
tail -f /home/hpoveda/etl/logs/POM_Aplicaciones.log

# Ver log del runner (script interno)
tail -f /home/hpoveda/etl/logs/runner.log

# Ver log del sistema cron (salida completa del crontab)
tail -f /var/log/etl/runner.log

# Ver todos los logs recientes (individuales)
tail -f /home/hpoveda/etl/logs/*.log
```

**Buscar errores en logs:**
```bash
# Buscar errores en todos los logs
grep -i "error" /home/hpoveda/etl/logs/*.log

# Buscar errores en un log específico
grep -i "error" /home/hpoveda/etl/logs/POM_Aplicaciones.log

# Ver últimas 50 líneas con errores
grep -i "error" /home/hpoveda/etl/logs/*.log | tail -50
```

**Rotación de logs (opcional):**
Para evitar que los logs crezcan indefinidamente, puedes configurar rotación de logs:

```bash
# Crear script de rotación (logs_rotate.sh)
#!/bin/bash
LOGDIR="/home/hpoveda/etl/logs"
DATE=$(date +%Y%m%d)

# Comprimir logs antiguos
find "$LOGDIR" -name "*.log" -mtime +7 -exec gzip {} \;

# Eliminar logs comprimidos muy antiguos (más de 30 días)
find "$LOGDIR" -name "*.log.gz" -mtime +30 -delete
```

**Notas:**
- Los logs se crean automáticamente en `etl/logs/` si no existen
- El script usa un lockfile para evitar ejecuciones simultáneas
- Cada base de datos tiene su propio archivo de log
- El archivo `runner.log` registra el inicio y fin de cada ejecución completa

---

## Troubleshooting

### Error: "CH_PASSWORD es obligatorio"
- Verifica que el archivo `.env` tenga `CH_PASSWORD=tu_password`
- O define variable de entorno: `set CH_PASSWORD=tu_password` (Windows)

### Error: "No se encontró driver ODBC"
- Instala ODBC Driver 17 for SQL Server
- Verifica `SQL_DRIVER` en `.env`

### Error: "Invalid argument" en fechas
- Ya está corregido en `sqlserver_to_clickhouse_silver.py`
- Fechas inválidas se convierten a NULL automáticamente

### Error: "Permission denied"
- Verifica credenciales en `.env`
- Verifica permisos en SQL Server / ClickHouse / Snowflake

---

## Notas

- Todos los scripts usan el archivo `.env` para configuración
- Los nombres de tablas se sanitizan automáticamente
- Los scripts muestran progreso y estadísticas en tiempo real
- Usa `*` para procesar todas las tablas
- Usa `reset` para eliminar y recrear tablas (solo en scripts que lo soporten)
