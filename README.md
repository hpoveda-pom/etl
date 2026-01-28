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

### 16. Verificar Bases de Datos - ClickHouse

Muestra información detallada de todas las bases de datos en ClickHouse: cantidad de tablas, vistas, funciones y tamaño en KB.

**Uso:**
```bash
python check_clickhouse_databases.py
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
python check_sqlserver_databases.py
python check_sqlserver_databases.py --dev

# Producción
python check_sqlserver_databases.py --prod
python check_sqlserver_databases.py prod
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
python clone_clickhouse_database.py ORIG_DB DEST_DB

# Clonar estructura + datos
python clone_clickhouse_database.py ORIG_DB DEST_DB --data

# Clonar eliminando tablas existentes en destino
python clone_clickhouse_database.py ORIG_DB DEST_DB --data --drop-existing
```

**Ejemplos:**
```bash
# Crear backup de estructura
python clone_clickhouse_database.py POM_Aplicaciones POM_Aplicaciones_backup

# Crear backup completo (estructura + datos)
python clone_clickhouse_database.py POM_Aplicaciones POM_Aplicaciones_backup --data

# Clonar a nueva base de datos eliminando existentes
python clone_clickhouse_database.py POM_Reportes POM_Reportes_test --data --drop-existing
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

## Logs y Monitoreo

### Script de Streaming Automatizado

El script `run_streaming_all.sh` ejecuta streaming incremental de múltiples bases de datos y guarda logs en el directorio `etl/logs/`.

**Ubicación de logs:**
```
etl/logs/
├── runner.log              # Log del script runner (con timestamps, duración y estado)
├── POM_Aplicaciones.log    # Log de streaming de POM_Aplicaciones
├── POM_Reportes.log        # Log de streaming de POM_Reportes
├── Reporteria.log          # Log de streaming de Reporteria
├── POM_PJ.log             # Log de streaming de POM_PJ
├── POM_Buro.log            # Log de streaming de POM_Buro
└── POM_Historico.log       # Log de streaming de POM_Historico
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

**Uso del script:**
```bash
# Ejecutar manualmente
cd /home/hpoveda/etl
bash run_streaming_all.sh

# O hacer ejecutable y ejecutar
chmod +x run_streaming_all.sh
./run_streaming_all.sh
```

**Configuración en cron (ejecución automática):**

**En servidor cronworker:**
```bash
# Conectar al servidor
ssh hpoveda@cronworker

# Editar crontab
crontab -e

# Configuración actual (ejecuta cada 5 minutos)
*/5 * * * * /bin/bash /home/hpoveda/etl/run_streaming_all.sh >> /var/log/etl/runner.log 2>&1
```

**Otras opciones de frecuencia:**
```bash
# Ejecutar cada hora
0 * * * * /bin/bash /home/hpoveda/etl/run_streaming_all.sh >> /var/log/etl/runner.log 2>&1

# Ejecutar cada 6 horas
0 */6 * * * /bin/bash /home/hpoveda/etl/run_streaming_all.sh >> /var/log/etl/runner.log 2>&1

# Ejecutar diariamente a las 2 AM
0 2 * * * /bin/bash /home/hpoveda/etl/run_streaming_all.sh >> /var/log/etl/runner.log 2>&1

# Ejecutar cada 10 minutos
*/10 * * * * /bin/bash /home/hpoveda/etl/run_streaming_all.sh >> /var/log/etl/runner.log 2>&1
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
