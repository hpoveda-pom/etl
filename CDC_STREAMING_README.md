# SQL Server to ClickHouse Streaming v3 - CDC Streaming

## Descripción

El script `sqlserver_to_clickhouse_streamingv3.py` implementa un **verdadero streaming** usando Change Data Capture (CDC) de SQL Server. A diferencia de la versión v2 que hace queries incrementales periódicas, esta versión:

- ✅ Captura **solo los cambios** (INSERT, UPDATE, DELETE) en tiempo real
- ✅ Funciona como un **servicio continuo** (daemon), no necesita cron
- ✅ Usa **CDC de SQL Server** para detectar cambios automáticamente
- ✅ Procesa cambios con **mínima latencia** (configurable)

## Diferencias con v2

| Característica | v2 (Incremental) | v3 (CDC Streaming) |
|----------------|------------------|---------------------|
| Método | Queries periódicas con WHERE col > last_value | CDC (Change Data Capture) |
| Ejecución | Cron job (batch) | Servicio continuo |
| Cambios detectados | Solo INSERTs nuevos | INSERT, UPDATE, DELETE |
| Latencia | Depende del intervalo del cron | Configurable (segundos) |
| Overhead | Queries completas periódicas | Solo cambios reales |
| Requisitos | Columna incremental | CDC habilitado en DB y tablas |

## Requisitos Previos

### 1. Habilitar CDC en SQL Server

CDC debe estar habilitado en:
- La base de datos
- Las tablas que quieres monitorear

#### Paso 1: Habilitar CDC en la base de datos

```sql
USE [TU_DATABASE];
EXEC sys.sp_cdc_enable_db;
```

O usando el script auxiliar:

```bash
python enable_cdc_sqlserver.py POM_Aplicaciones --enable-db --prod
```

#### Paso 2: Habilitar CDC en las tablas

```sql
USE [TU_DATABASE];
EXEC sys.sp_cdc_enable_table
    @source_schema = N'dbo',
    @source_name = N'MiTabla',
    @role_name = NULL;
```

O usando el script auxiliar:

```bash
python enable_cdc_sqlserver.py POM_Aplicaciones "Tabla1,Tabla2,Tabla3" --prod
```

### 2. Permisos necesarios

El usuario de SQL Server necesita:
- `db_owner` o permisos para habilitar CDC
- Permisos de lectura en las tablas CDC (`cdc.*`)

## Uso

### Ejecución básica

```bash
python sqlserver_to_clickhouse_streamingv3.py ORIG_DB DEST_DB [tablas] [--prod] [--poll-interval SECONDS]
```

### Ejemplos

```bash
# Streaming de todas las tablas con CDC habilitado (desarrollo)
python sqlserver_to_clickhouse_streamingv3.py POM_Aplicaciones POM_Aplicaciones

# Streaming de tablas específicas (producción)
python sqlserver_to_clickhouse_streamingv3.py POM_Aplicaciones POM_Aplicaciones "Tabla1,Tabla2" --prod

# Con intervalo de polling personalizado (5 segundos)
python sqlserver_to_clickhouse_streamingv3.py POM_Aplicaciones POM_Aplicaciones --prod --poll-interval 5

# Intervalo más agresivo (1 segundo) para baja latencia
python sqlserver_to_clickhouse_streamingv3.py POM_Aplicaciones POM_Aplicaciones --prod --poll-interval 1
```

### Parámetros

- `ORIG_DB`: Base de datos origen en SQL Server
- `DEST_DB`: Base de datos destino en ClickHouse
- `tablas`: (Opcional) Lista de tablas separadas por coma. Si no se especifica, procesa todas las tablas con CDC habilitado
- `--prod`: Usar credenciales de producción
- `--poll-interval N`: Intervalo de polling en segundos (default: 5)

## Ejecución como Servicio

### Linux (systemd)

Crear archivo `/etc/systemd/system/cdc-streaming.service`:

```ini
[Unit]
Description=SQL Server to ClickHouse CDC Streaming Service
After=network.target

[Service]
Type=simple
User=tu_usuario
WorkingDirectory=/ruta/a/etl
ExecStart=/usr/bin/python3 /ruta/a/etl/sqlserver_to_clickhouse_streamingv3.py POM_Aplicaciones POM_Aplicaciones --prod --poll-interval 5
Restart=always
RestartSec=10
StandardOutput=append:/ruta/a/etl/logs/cdc-streaming.log
StandardError=append:/ruta/a/etl/logs/cdc-streaming.error.log

[Install]
WantedBy=multi-user.target
```

Activar y iniciar:

```bash
sudo systemctl daemon-reload
sudo systemctl enable cdc-streaming.service
sudo systemctl start cdc-streaming.service
sudo systemctl status cdc-streaming.service
```

### Windows (NSSM o Task Scheduler)

Usar NSSM (Non-Sucking Service Manager) o configurar como tarea programada que se ejecute al inicio.

## Variables de Entorno

Agregar al `.env`:

```env
# SQL Server
SQL_SERVER=tu_servidor
SQL_USER=tu_usuario
SQL_PASSWORD=tu_password
SQL_DRIVER=ODBC Driver 17 for SQL Server

# SQL Server Producción (opcional)
SQL_SERVER_PROD=servidor_prod
SQL_USER_PROD=usuario_prod
SQL_PASSWORD_PROD=password_prod

# ClickHouse
CH_HOST=localhost
CH_PORT=8123
CH_USER=default
CH_PASSWORD=
CH_DATABASE=default

# CDC Streaming
CDC_POLL_INTERVAL=5  # Intervalo por defecto (segundos)
INSERT_BATCH_ROWS=1000  # Tamaño de batch para inserts
```

## Manejo de Cambios

### INSERTs
Se insertan directamente en ClickHouse.

### UPDATEs
ClickHouse no tiene UPDATE nativo. Opciones:
1. **ReplacingMergeTree**: Insertar el registro actualizado y dejar que ClickHouse elimine duplicados
2. **ALTER TABLE ... UPDATE**: Requiere versión reciente de ClickHouse
3. **Soft delete + re-insert**: Marcar como eliminado e insertar nuevo

El script actual usa la opción 1 (asumiendo ReplacingMergeTree).

### DELETEs
ClickHouse no tiene DELETE nativo. Opciones:
1. **ALTER TABLE ... DELETE**: Requiere versión reciente de ClickHouse
2. **Soft delete**: Agregar columna `_deleted` y marcar registros
3. **ReplacingMergeTree con versión**: Usar columna de versión para eliminar

El script actual solo loguea los deletes. Para implementar deletes reales, necesitas:
- ClickHouse 20.8+ con `ALTER TABLE ... DELETE`
- O implementar soft delete

## Monitoreo

El script muestra en cada iteración:
- Número de cambios detectados (inserts, updates, deletes)
- Tiempo de procesamiento por ciclo
- Totales acumulados

Ejemplo de salida:

```
[ITERATION 1] 2024-01-15 10:30:00
  dbo.Tabla1: +150 inserts, ~25 updates, -5 deletes
  dbo.Tabla2: +200 inserts, ~0 updates, -0 deletes
  Ciclo completado en 2.34s | Total: +350 ~25 -5
```

## Troubleshooting

### Error: "CDC no está habilitado en la base de datos"
Solución: Ejecutar `python enable_cdc_sqlserver.py DATABASE --enable-db`

### Error: "No se encontraron tablas con CDC habilitado"
Solución: Habilitar CDC en las tablas con `enable_cdc_sqlserver.py`

### Error: "Error obteniendo cambios CDC"
Posibles causas:
- Permisos insuficientes
- CDC deshabilitado en la tabla
- Problemas de conexión

### Cambios no se detectan
- Verificar que CDC está habilitado: `SELECT * FROM cdc.change_tables`
- Verificar que hay cambios recientes: `SELECT sys.fn_cdc_get_max_lsn()`
- Reducir `--poll-interval` para mayor frecuencia

## Consideraciones de Rendimiento

- **Poll Interval**: Intervalos muy cortos (< 1s) pueden aumentar carga en SQL Server
- **Batch Size**: Ajustar `INSERT_BATCH_ROWS` según volumen de cambios
- **Múltiples Tablas**: El script procesa todas las tablas en cada ciclo

## Comparación de Versiones

| Aspecto | v2 (Incremental) | v3 (CDC) |
|---------|------------------|----------|
| **Cuándo usar** | Carga inicial, tablas sin CDC | Streaming continuo, cambios en tiempo real |
| **Latencia** | Depende del cron (minutos/horas) | Segundos |
| **Overhead** | Queries completas | Solo cambios |
| **UPDATEs** | No detecta | Sí detecta |
| **DELETEs** | No detecta | Sí detecta |
| **Complejidad** | Baja | Media (requiere CDC) |

## Recomendación

- **v2**: Para cargas iniciales o cuando no puedes habilitar CDC
- **v3**: Para streaming continuo en producción con requisitos de baja latencia
