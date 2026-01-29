# ETL Scripts - Documentación

Scripts ETL para migración de datos entre SQL Server, ClickHouse y otros sistemas.

---

## Estructura de Carpetas

Los scripts están organizados en las siguientes carpetas:

- **`silver/`** - Scripts de migración a formato Silver (tipos de datos reales)
  - `sqlserver_to_clickhouse_silver.py` - Migración SQL Server → ClickHouse (Silver)
  - `clickhouse_raw_to_table.py` - Convierte tablas RAW a tablas Silver

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
- [1. SQL Server → ClickHouse (Silver)](#1-sql-server--clickhouse-silver)
- [2. SQL Server → ClickHouse (Streaming v4)](#2-sql-server--clickhouse-streaming-v4)
- [3. ClickHouse Raw → Silver](#3-clickhouse-raw--silver)
- [4. SQL Server → Snowflake (Streaming)](#4-sql-server--snowflake-streaming)
- [5. Administración ClickHouse](#5-administración-clickhouse)
- [6. Administración Snowflake](#6-administración-snowflake)
- [7. Verificación y Utilidades](#7-verificación-y-utilidades)

### Guías
- [Flujos Comunes](#flujos-comunes)
  - [Migración Completa SQL Server → ClickHouse](#migración-completa-sql-server--clickhouse)
  - [Migración Incremental (Solo Nuevos)](#migración-incremental-solo-nuevos)
  - [Limpiar y Re-migrar](#limpiar-y-re-migrar)
  - [Vaciar Tabla (Mantener Estructura)](#vaciar-tabla-mantener-estructura)
- [Servicios y Automatización](#servicios-y-automatización)
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

### 2. SQL Server → ClickHouse (Streaming v4) ⭐ RECOMENDADO

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

### 3. ClickHouse Raw → Silver

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

### 4. SQL Server → Snowflake (Streaming)

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

### 5. Administración ClickHouse

**Eliminar base de datos o tabla:**
```bash
python tools/clickhouse_drop.py DATABASE nombre_base_datos
python tools/clickhouse_drop.py TABLE nombre_base_datos nombre_tabla
python tools/clickhouse_drop_tables.py DEST_DB [tablas|pattern]
```

**Vaciar tabla (mantener estructura):**
```bash
python tools/clickhouse_truncate.py nombre_base_datos nombre_tabla
```

**Clonar base de datos:**

```bash
python tools/clone_clickhouse_database.py ORIG_DB DEST_DB [--data] [--drop-existing]
```

---

### 6. Administración Snowflake

**Eliminar tablas:**
```bash
python tools/snowflake_drop_tables.py DEST_DB SCHEMA [tablas|pattern]
```

---

### 7. Verificación y Utilidades

**Verificar conexiones:**
```bash
python tools/check_all_connections.py
```

**Verificar bases de datos:**
```bash
python tools/check_clickhouse_databases.py
python tools/check_sqlserver_databases.py [--dev|--prod]
```

**Herramientas adicionales:**
- `tools/sqlserver_to_csv.py` - Exportar SQL Server a CSV
- `tools/csv_to_clickhouse.py` - Cargar CSV a ClickHouse
- `tools/csv_to_snowflake.py` - Cargar CSV a Snowflake
- `tools/excel_to_csv.py` - Convertir Excel a CSV
- `tools/compress_csv_to_gz.py` - Comprimir CSV

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

**Streaming v4 (Servicio Continuo) ⭐ RECOMENDADO**
```bash
# 1. Primera vez: crear tablas y carga inicial (Silver)
python silver/sqlserver_to_clickhouse_silver.py POM_Aplicaciones POM_Aplicaciones "dbo.PC_Gestiones,dbo.Casos"

# 2. Iniciar servicio streaming v4 (corre indefinidamente)
./services/run_streaming_allv4.sh start

# 3. Verificar estado
./services/run_streaming_allv4.sh status
```

**Nota:** Servicio continuo que detecta cambios usando ROWVERSION, ID o Timestamp+PK. Estado en ClickHouse. **IMPORTANTE:** Solo debe ejecutarse UNA instancia a la vez (lock file local previene duplicados en el mismo servidor, pero NO entre múltiples servidores).

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

## Servicios y Automatización

### Gestión de Servicios Streaming v4

El script `services/run_streaming_allv4.sh` gestiona múltiples servicios streaming v4 que corren en background. Cada base de datos se ejecuta como un proceso independiente.

**⚠️ IMPORTANTE - Configuración inicial:**

Antes de usar el script, **debes editar** las rutas en `services/run_streaming_allv4.sh`:

1. **Abrir el script:**
   ```bash
   nano ~/etl/services/run_streaming_allv4.sh
   ```

2. **Cambiar las siguientes líneas (al inicio del archivo):**
   ```bash
   BASE="/home/hpoveda/etl"     # ← Cambia por tu ruta completa del proyecto
   PY="/usr/bin/python3"        # ← Verifica la ruta de Python 3 (usa: which python3)
   ```

3. **Ejemplo de configuración:**
   ```bash
   # Si tu proyecto está en /opt/etl
   BASE="/opt/etl"
   
   # Si Python está en otra ubicación
   PY="/usr/local/bin/python3"
   # O verifica con:
   # which python3
   ```

4. **Guardar y hacer ejecutable:**
   ```bash
   chmod +x ~/etl/services/run_streaming_allv4.sh
   ```

**Uso básico:**

```bash
# 1. Ir al directorio del proyecto
cd ~/etl

# 2. Hacer el script ejecutable (solo primera vez)
chmod +x services/run_streaming_allv4.sh

# 3. Iniciar todos los servicios
./services/run_streaming_allv4.sh start

# 4. Verificar estado de los servicios
./services/run_streaming_allv4.sh status

# 5. Detener todos los servicios
./services/run_streaming_allv4.sh stop

# 6. Reiniciar todos los servicios
./services/run_streaming_allv4.sh restart
```

**Ejemplo de salida al iniciar:**
```bash
$ ./services/run_streaming_allv4.sh start
[2026-01-29 12:00:00] ==========================================
[2026-01-29 12:00:00] INICIO DE SERVICIOS STREAMING V4
[2026-01-29 12:00:00] Fecha/Hora: 2026-01-29 12:00:00
[2026-01-29 12:00:00] ==========================================
[2026-01-29 12:00:05] ✓ Servicio iniciado: POM_Aplicaciones (PID: 12345)
[2026-01-29 12:00:10] ✓ Servicio iniciado: POM_Reportes (PID: 12346)
...
```

**Ejemplo de salida al verificar estado:**
```bash
$ ./services/run_streaming_allv4.sh status
[2026-01-29 12:05:00] 📊 Estado de servicios:
[2026-01-29 12:05:00]   ✓ POM_Aplicaciones: CORRIENDO (PID: 12345)
[2026-01-29 12:05:00]   ✓ POM_Reportes: CORRIENDO (PID: 12346)
[2026-01-29 12:05:00]   ○ Reporteria: NO INICIADO
```

**Personalizar bases de datos:**

Para cambiar qué bases de datos se ejecutan, edita el script `services/run_streaming_allv4.sh`:

1. **Cambiar lista de bases de datos:**
   Busca la función `check_services_status()` y la sección `start)` para modificar la lista:
   ```bash
   for db_name in "POM_Aplicaciones" "POM_Reportes" "Reporteria" "POM_PJ" "POM_Buro" "POM_Historico"; do
   ```
   Cambia por tus bases de datos:
   ```bash
   for db_name in "MiBase1" "MiBase2"; do
   ```

2. **Cambiar parámetros de ejecución:**
   En la función `start_streaming_service()`, línea 47, puedes modificar:
   ```bash
   nohup $PY "$SCRIPT" "$db_name" "$db_name" --prod --poll-interval 10 >> "$log_file" 2>&1 &
   ```
   - Quita `--prod` si quieres usar desarrollo
   - Cambia `--poll-interval 10` por otro valor (segundos)

**Bases de datos por defecto:**
- `POM_Aplicaciones`
- `POM_Reportes`
- `Reporteria`
- `POM_PJ`
- `POM_Buro`
- `POM_Historico`

**Ubicación de archivos:**
```
etl/logs/
├── runner_v4.log              # Log del script gestor
├── POM_Aplicaciones_v4.log    # Log del servicio POM_Aplicaciones
├── POM_Reportes_v4.log        # Log del servicio POM_Reportes
└── ...

/tmp/streaming_v4_pids/
├── POM_Aplicaciones.pid       # PID del proceso
├── POM_Reportes.pid
└── ...
```

---

### Configuración como Servicio del Sistema (systemd)

Para que los servicios se inicien automáticamente al arrancar el sistema:

**1. Crear archivo de servicio systemd:**

```bash
sudo nano /etc/systemd/system/etl-streaming-v4.service
```

**2. Agregar el siguiente contenido:**

**⚠️ IMPORTANTE:** Ajusta las siguientes rutas según tu instalación:
- `User=hpoveda` → Cambia por tu usuario
- `Group=hpoveda` → Cambia por tu grupo
- `/home/hpoveda/etl` → Cambia por la ruta real de tu proyecto

```ini
[Unit]
Description=ETL Streaming v4 Services - SQL Server to ClickHouse
After=network.target

[Service]
Type=forking
User=hpoveda
Group=hpoveda
WorkingDirectory=/home/hpoveda/etl
ExecStart=/bin/bash /home/hpoveda/etl/services/run_streaming_allv4.sh start
ExecStop=/bin/bash /home/hpoveda/etl/services/run_streaming_allv4.sh stop
ExecReload=/bin/bash /home/hpoveda/etl/services/run_streaming_allv4.sh restart
Restart=on-failure
RestartSec=10

[Install]
WantedBy=multi-user.target
```

**Ejemplo con rutas diferentes:**
```ini
[Unit]
Description=ETL Streaming v4 Services - SQL Server to ClickHouse
After=network.target

[Service]
Type=forking
User=usuario_etl
Group=usuario_etl
WorkingDirectory=/opt/etl
ExecStart=/bin/bash /opt/etl/services/run_streaming_allv4.sh start
ExecStop=/bin/bash /opt/etl/services/run_streaming_allv4.sh stop
ExecReload=/bin/bash /opt/etl/services/run_streaming_allv4.sh restart
Restart=on-failure
RestartSec=10

[Install]
WantedBy=multi-user.target
```

**3. Recargar systemd y habilitar el servicio:**

```bash
# Recargar configuración de systemd
sudo systemctl daemon-reload

# Habilitar servicio para que inicie al arrancar
sudo systemctl enable etl-streaming-v4.service

# Iniciar el servicio ahora
sudo systemctl start etl-streaming-v4.service

# Verificar estado
sudo systemctl status etl-streaming-v4.service
```

**4. Comandos útiles de systemd:**

```bash
# Ver estado del servicio
sudo systemctl status etl-streaming-v4.service

# Iniciar servicio
sudo systemctl start etl-streaming-v4.service

# Detener servicio
sudo systemctl stop etl-streaming-v4.service

# Reiniciar servicio
sudo systemctl restart etl-streaming-v4.service

# Ver logs del servicio
sudo journalctl -u etl-streaming-v4.service -f

# Deshabilitar inicio automático
sudo systemctl disable etl-streaming-v4.service
```

---

### Monitoreo y Logs

**Ver logs en tiempo real:**

```bash
# Log de una base de datos específica
tail -f ~/etl/logs/POM_Aplicaciones_v4.log

# Log del gestor de servicios
tail -f ~/etl/logs/runner_v4.log

# Ver todos los logs recientes
tail -f ~/etl/logs/*.log
```

**Buscar errores:**

```bash
# Buscar errores en todos los logs
grep -i "error" ~/etl/logs/*.log

# Buscar errores en un log específico
grep -i "error" ~/etl/logs/POM_Aplicaciones_v4.log

# Ver últimas 50 líneas con errores
grep -i "error" ~/etl/logs/*.log | tail -50
```

**Verificar procesos en ejecución:**

```bash
# Ver procesos Python de streaming
ps aux | grep sqlserver_to_clickhouse_streamingv4

# Ver PIDs guardados
ls -la /tmp/streaming_v4_pids/

# Verificar si un proceso específico está corriendo
cat /tmp/streaming_v4_pids/POM_Aplicaciones.pid
ps -p $(cat /tmp/streaming_v4_pids/POM_Aplicaciones.pid)
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
