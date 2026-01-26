# ETL Scripts - Documentación

Scripts ETL para migración de datos entre SQL Server, ClickHouse y otros sistemas.

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

### Guías
- [Flujos Comunes](#flujos-comunes)
  - [Migración Completa SQL Server → ClickHouse](#migración-completa-sql-server--clickhouse)
  - [Migración Incremental (Solo Nuevos)](#migración-incremental-solo-nuevos)
  - [Limpiar y Re-migrar](#limpiar-y-re-migrar)
  - [Vaciar Tabla (Mantener Estructura)](#vaciar-tabla-mantener-estructura)
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
python sqlserver_to_clickhouse_silver.py ORIG_DB DEST_DB [tablas] [limit] [reset]
```

**Ejemplos:**
```bash
# Migrar todas las tablas de POM_Aplicaciones
python sqlserver_to_clickhouse_silver.py POM_Aplicaciones POM_Aplicaciones

# Migrar tablas específicas (PC_Gestiones y Casos)
python sqlserver_to_clickhouse_silver.py POM_Aplicaciones POM_Aplicaciones "dbo.PC_Gestiones,dbo.Casos"

# Migrar con límite de registros (útil para pruebas)
python sqlserver_to_clickhouse_silver.py POM_Aplicaciones POM_Aplicaciones dbo.PC_Gestiones 1000

# Migrar todas las tablas y resetear (DROP + CREATE)
python sqlserver_to_clickhouse_silver.py POM_Aplicaciones POM_Aplicaciones * 0 reset
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
python sqlserver_to_clickhouse.py ORIG_DB DEST_DB [tablas] [limit]
```

**Ejemplos:**
```bash
# Migrar todas las tablas
python sqlserver_to_clickhouse.py POM_Aplicaciones POM_Aplicaciones

# Migrar tablas específicas
python sqlserver_to_clickhouse.py POM_Aplicaciones POM_Aplicaciones "dbo.PC_Gestiones,dbo.Casos"

# Migrar con límite
python sqlserver_to_clickhouse.py POM_Aplicaciones POM_Aplicaciones dbo.PC_Gestiones 5000
```

---

### 3. SQL Server → ClickHouse (Streaming Incremental)

Streaming incremental liviano que solo inserta registros nuevos. **Requiere que las tablas ya existan** (usar `sqlserver_to_clickhouse_silver.py` primero para crear las tablas).

**Uso:**
```bash
python sqlserver_to_clickhouse_streaming.py ORIG_DB DEST_DB [tablas] [limit] [--prod]
```

**Ejemplos:**
```bash
# Streaming incremental de todas las tablas (desarrollo)
python sqlserver_to_clickhouse_streaming.py POM_Aplicaciones POM_Aplicaciones

# Streaming de una tabla específica (desarrollo)
python sqlserver_to_clickhouse_streaming.py POM_Aplicaciones POM_Aplicaciones dbo.PC_Gestiones

# Streaming de múltiples tablas específicas (separadas por comas)
python sqlserver_to_clickhouse_streaming.py POM_Aplicaciones POM_Aplicaciones "dbo.PC_Gestiones,dbo.Casos,dbo.PG_TC"

# Streaming desde SQL Server PRODUCCIÓN (usar --prod)
python sqlserver_to_clickhouse_streaming.py POM_Aplicaciones POM_Aplicaciones --prod
python sqlserver_to_clickhouse_streaming.py POM_Aplicaciones POM_Aplicaciones dbo.PC_Gestiones --prod
```

**Características:**
- **Liviano y rápido**: ~400 líneas, asume tablas existentes
- **Detección automática**: detecta columna incremental (IDENTITY > Id > ID > última int)
- **Solo nuevos registros**: consulta el último valor en ClickHouse y solo inserta filas nuevas
- **Chunk size dinámico**: ajusta automáticamente según número de columnas
- **Validación de fechas**: maneja fechas inválidas correctamente
- **Streaming directo**: sin CSV intermedio
- **Soporte producción**: usa `--prod` para conectar a SQL Server de producción

**Configuración para Producción:**
Agregar al archivo `.env`:
```env
# SQL Server Producción (opcional)
SQL_SERVER_PROD=SRV-PROD\SQLEXPRESS
SQL_USER_PROD=usuario_prod
SQL_PASSWORD_PROD=password_prod
```

**Requisito previo:**
Las tablas deben existir en ClickHouse. Si no existen, el script las omitirá con un mensaje indicando que uses `sqlserver_to_clickhouse_silver.py` primero.

---

### 4. ClickHouse Raw → Tabla

Convierte tablas RAW (String) a tablas Silver (tipos reales).

**Uso:**
```bash
python clickhouse_raw_to_table.py DEST_DB [TABLE] [LIMIT]
```

**Ejemplos:**
```bash
# Convertir todas las tablas RAW
python clickhouse_raw_to_table.py POM_Aplicaciones

# Convertir tabla específica
python clickhouse_raw_to_table.py POM_Aplicaciones PC_Gestiones

# Convertir con límite
python clickhouse_raw_to_table.py POM_Aplicaciones PC_Gestiones 10000
```

---

### 5. ClickHouse - DROP

Elimina base de datos o tabla en ClickHouse.

**Uso:**
```bash
# Eliminar base de datos
python clickhouse_drop.py DATABASE nombre_base_datos

# Eliminar tabla
python clickhouse_drop.py TABLE nombre_base_datos nombre_tabla
```

**Ejemplos:**
```bash
# Eliminar base de datos (requiere confirmación)
python clickhouse_drop.py DATABASE POM_Aplicaciones

# Eliminar tabla
python clickhouse_drop.py TABLE POM_Aplicaciones PC_Gestiones
python clickhouse_drop.py TABLE POM_Aplicaciones Casos
```

---

### 6. ClickHouse - TRUNCATE

Vacía una tabla (elimina datos, mantiene estructura).

**Uso:**
```bash
python clickhouse_truncate.py nombre_base_datos nombre_tabla
```

**Ejemplos:**
```bash
# Vaciar tabla
python clickhouse_truncate.py POM_Aplicaciones PC_Gestiones
python clickhouse_truncate.py POM_Aplicaciones Casos
```

---

### 7. SQL Server → CSV

Exporta tablas de SQL Server a archivos CSV.

**Uso:**
```bash
python sqlserver_to_csv.py DATABASE [tablas]
```

**Ejemplos:**
```bash
# Exportar todas las tablas
python sqlserver_to_csv.py POM_Aplicaciones

# Exportar tablas específicas
python sqlserver_to_csv.py POM_Aplicaciones "PC_Gestiones,Casos"
```

---

### 8. CSV → ClickHouse

Carga archivos CSV a ClickHouse.

**Uso:**
```bash
python csv_to_clickhouse.py [DEST_DB] [folder_filter]
```

**Ejemplos:**
```bash
# Cargar todos los CSV
python csv_to_clickhouse.py POM_Aplicaciones

# Cargar CSV de carpeta específica
python csv_to_clickhouse.py POM_Aplicaciones SQLSERVER_POM_Aplicaciones
```

---

### 9. Excel → CSV

Convierte archivos Excel (.xlsx) a CSV.

**Uso:**
```bash
python excel_to_csv.py
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
python compress_csv_to_gz.py [folder_filter] [csv_filter]
```

**Ejemplos:**
```bash
# Comprimir todos los CSV
python compress_csv_to_gz.py

# Comprimir CSV de carpeta específica
python compress_csv_to_gz.py SQLSERVER_POM_Aplicaciones

# Comprimir CSV específicos
python compress_csv_to_gz.py SQLSERVER_POM_Aplicaciones "PC_Gestiones,Casos"
```

---

### 11. SQL Server → Snowflake (Streaming)

Migración directa SQL Server → Snowflake.

**Uso:**
```bash
python sqlserver_to_snowflake_streaming.py ORIG_DB DEST_DB SCHEMA [tablas]
```

**Ejemplos:**
```bash
# Migrar todas las tablas
python sqlserver_to_snowflake_streaming.py POM_Aplicaciones POM_TEST01 RAW

# Migrar tablas específicas
python sqlserver_to_snowflake_streaming.py POM_Aplicaciones POM_TEST01 RAW "PC_Gestiones,Casos"
```

---

### 12. CSV → Snowflake

Carga archivos CSV a Snowflake.

**Uso:**
```bash
python csv_to_snowflake.py [DEST_DB] [SCHEMA] [folder_filter]
```

**Ejemplos:**
```bash
# Cargar todos los CSV
python csv_to_snowflake.py POM_TEST01 RAW

# Cargar CSV de carpeta específica
python csv_to_snowflake.py POM_TEST01 RAW SQLSERVER_POM_Aplicaciones
```

---

### 13. Snowflake - DROP Tables

Elimina tablas en Snowflake.

**Uso:**
```bash
python snowflake_drop_tables.py DEST_DB SCHEMA [tablas|pattern]
```

**Ejemplos:**
```bash
# Eliminar tablas específicas
python snowflake_drop_tables.py POM_TEST01 RAW "PC_Gestiones,Casos"

# Eliminar por patrón
python snowflake_drop_tables.py POM_TEST01 RAW "PC_%"
```

---

### 14. ClickHouse - DROP Tables

Elimina tablas en ClickHouse.

**Uso:**
```bash
python clickhouse_drop_tables.py DEST_DB [tablas|pattern]
```

**Ejemplos:**
```bash
# Eliminar tablas específicas
python clickhouse_drop_tables.py POM_Aplicaciones "PC_Gestiones,Casos"

# Eliminar por patrón
python clickhouse_drop_tables.py POM_Aplicaciones "PC_%"
```

---

### 15. Verificar Conexiones

Verifica conexiones a SQL Server, ClickHouse y Snowflake.

**Uso:**
```bash
python check_all_connections.py
```

---

## Flujos Comunes

### Migración Completa SQL Server → ClickHouse

```bash
# 1. Migrar todas las tablas (Silver)
python sqlserver_to_clickhouse_silver.py POM_Aplicaciones POM_Aplicaciones * 0 reset

# 2. Verificar datos
python check_all_connections.py
```

### Migración Incremental (Solo Nuevos)

```bash
# 1. Primera vez: crear tablas y carga inicial (Silver)
python sqlserver_to_clickhouse_silver.py POM_Aplicaciones POM_Aplicaciones "dbo.PC_Gestiones,dbo.Casos"

# 2. Siguientes veces: solo nuevos registros (Streaming)
python sqlserver_to_clickhouse_streaming.py POM_Aplicaciones POM_Aplicaciones "dbo.PC_Gestiones,dbo.Casos"

# 3. Automatizar: ejecutar streaming periódicamente (cron, scheduler, etc.)
python sqlserver_to_clickhouse_streaming.py POM_Aplicaciones POM_Aplicaciones
```

**Nota:** El script streaming detecta automáticamente el último ID procesado y solo inserta registros nuevos. Si la tabla no existe, la omite indicando que uses `sqlserver_to_clickhouse_silver.py` primero.

### Limpiar y Re-migrar

```bash
# 1. Eliminar tabla
python clickhouse_drop.py TABLE POM_Aplicaciones PC_Gestiones

# 2. Re-migrar
python sqlserver_to_clickhouse_silver.py POM_Aplicaciones POM_Aplicaciones dbo.PC_Gestiones
```

### Vaciar Tabla (Mantener Estructura)

```bash
# Vaciar datos
python clickhouse_truncate.py POM_Aplicaciones PC_Gestiones

# Re-cargar datos
python sqlserver_to_clickhouse_silver.py POM_Aplicaciones POM_Aplicaciones dbo.PC_Gestiones
```

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
