# SQL Server to ClickHouse Streaming v4 - No Invasivo

## Descripción

El script `sqlserver_to_clickhouse_streamingv4.py` implementa streaming **completamente NO INVASIVO** usando solo queries SELECT. 

**✅ NO requiere:**
- CDC (Change Data Capture)
- Triggers
- Modificaciones en la base de datos
- Permisos de sysadmin
- Cambios en tablas de producción

**✅ Solo usa:**
- Queries SELECT (solo lectura)
- Detección automática de estrategias
- Comparaciones inteligentes

## Estrategias de Detección de Cambios

El script detecta automáticamente la mejor estrategia para cada tabla:

### 1. Estrategia TIMESTAMP (Mejor)
**Cuándo se usa:** Si la tabla tiene una columna de tipo `datetime` con nombres como:
- `ModifiedDate`, `UpdatedAt`, `FechaModificacion`, etc.

**Qué detecta:**
- ✅ INSERTs nuevos
- ✅ UPDATEs (registros modificados)
- ✅ DELETEs (comparando IDs)

**Cómo funciona:**
- Consulta: `WHERE ModifiedDate > last_timestamp`
- Solo trae registros nuevos o modificados
- Muy eficiente, baja latencia

### 2. Estrategia INCREMENTAL (Buena)
**Cuándo se usa:** Si la tabla tiene una columna ID incremental (identity o numérica)

**Qué detecta:**
- ✅ INSERTs nuevos
- ✅ DELETEs (comparando IDs)

**Limitaciones:**
- ❌ No detecta UPDATEs (solo nuevos registros)

**Cómo funciona:**
- Consulta: `WHERE Id > last_id`
- Similar a v2, pero como servicio continuo

### 3. Estrategia COUNT (Fallback)
**Cuándo se usa:** Si no hay timestamp ni ID incremental

**Qué detecta:**
- ✅ Cualquier cambio (pero requiere sync completo)

**Limitaciones:**
- ❌ Menos eficiente (sincronización completa periódica)
- ❌ No detecta qué cambió específicamente

**Cómo funciona:**
- Compara `COUNT(*)` entre SQL Server y ClickHouse
- Si hay diferencia, hace sincronización completa
- Solo cada X horas (configurable, default: 1 hora)

## Uso

### Ejecución básica

```bash
python sqlserver_to_clickhouse_streamingv4.py ORIG_DB DEST_DB [tablas] [--prod] [--poll-interval SECONDS]
```

### Ejemplos

```bash
# Streaming de todas las tablas (desarrollo)
python sqlserver_to_clickhouse_streamingv4.py POM_Aplicaciones POM_Aplicaciones

# Streaming de tablas específicas (producción)
python sqlserver_to_clickhouse_streamingv4.py POM_Aplicaciones POM_Aplicaciones "Tabla1,Tabla2" --prod

# Con intervalo de polling personalizado (10 segundos)
python sqlserver_to_clickhouse_streamingv4.py POM_Aplicaciones POM_Aplicaciones --prod --poll-interval 10

# Intervalo más agresivo (5 segundos) para baja latencia
python sqlserver_to_clickhouse_streamingv4.py POM_Aplicaciones POM_Aplicaciones --prod --poll-interval 5
```

### Parámetros

- `ORIG_DB`: Base de datos origen en SQL Server
- `DEST_DB`: Base de datos destino en ClickHouse
- `tablas`: (Opcional) Lista de tablas separadas por coma. Si no se especifica, procesa todas las tablas
- `--prod`: Usar credenciales de producción
- `--poll-interval N`: Intervalo de polling en segundos (default: 10)

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

# Streaming v4
CDC_POLL_INTERVAL=10  # Intervalo de polling (segundos)
INSERT_BATCH_ROWS=2000  # Tamaño de batch para inserts
FULL_SYNC_INTERVAL=3600  # Intervalo para sync completo cuando no hay timestamps (segundos)
```

## Ejecución como Servicio

### Linux (systemd)

Crear archivo `/etc/systemd/system/streaming-v4.service`:

```ini
[Unit]
Description=SQL Server to ClickHouse Streaming v4 (No Invasivo)
After=network.target

[Service]
Type=simple
User=tu_usuario
WorkingDirectory=/ruta/a/etl
ExecStart=/usr/bin/python3 /ruta/a/etl/sqlserver_to_clickhouse_streamingv4.py POM_Aplicaciones POM_Aplicaciones --prod --poll-interval 10
Restart=always
RestartSec=10
StandardOutput=append:/ruta/a/etl/logs/streaming-v4.log
StandardError=append:/ruta/a/etl/logs/streaming-v4.error.log

[Install]
WantedBy=multi-user.target
```

Activar y iniciar:

```bash
sudo systemctl daemon-reload
sudo systemctl enable streaming-v4.service
sudo systemctl start streaming-v4.service
sudo systemctl status streaming-v4.service
```

## Manejo de Cambios

### INSERTs
Se insertan directamente en ClickHouse usando la estrategia detectada.

### UPDATEs
- **Con TIMESTAMP:** Se detectan automáticamente (registros con `ModifiedDate > last_timestamp`)
- **Sin TIMESTAMP:** No se detectan automáticamente (requiere sync completo)

**Nota:** ClickHouse no tiene UPDATE nativo. El script inserta los registros actualizados (asumiendo ReplacingMergeTree o similar).

### DELETEs
- Se detectan comparando IDs entre SQL Server y ClickHouse
- **No se eliminan automáticamente** de ClickHouse (requiere estrategia especial)

**Opciones para manejar DELETEs:**
1. **ClickHouse 20.8+**: Usar `ALTER TABLE ... DELETE WHERE id IN (...)` 
2. **Soft delete**: Agregar columna `_deleted` y marcar registros
3. **ReplacingMergeTree**: Usar columna de versión para eliminar

El script actual solo **detecta** los deletes pero no los elimina automáticamente por seguridad.

## Comparación de Versiones

| Característica | v2 (Incremental) | v3 (CDC) | v4 (No Invasivo) |
|----------------|------------------|---------|------------------|
| **Invasivo** | ❌ No | ✅ Sí (requiere CDC) | ❌ No |
| **Permisos** | Lectura | Sysadmin | Lectura |
| **Modifica BD** | ❌ No | ✅ Sí | ❌ No |
| **Detecta INSERTs** | ✅ Sí | ✅ Sí | ✅ Sí |
| **Detecta UPDATEs** | ❌ No | ✅ Sí | ✅ Sí (con timestamp) |
| **Detecta DELETEs** | ❌ No | ✅ Sí | ✅ Sí (comparando IDs) |
| **Ejecución** | Cron | Servicio | Servicio |
| **Latencia** | Depende del cron | Segundos | Segundos |
| **Overhead** | Queries periódicas | Solo cambios | Queries inteligentes |

## Ventajas de v4

1. **✅ Completamente no invasivo**: Solo SELECT, sin modificar nada
2. **✅ Seguro para producción**: No requiere permisos especiales
3. **✅ Detección automática**: Elige la mejor estrategia por tabla
4. **✅ Eficiente**: Usa timestamps cuando están disponibles
5. **✅ Servicio continuo**: No necesita cron
6. **✅ Baja latencia**: Polling configurable

## Limitaciones

1. **UPDATEs sin timestamp**: No se detectan automáticamente (requiere sync completo)
2. **DELETEs**: Se detectan pero no se eliminan automáticamente
3. **Sin CDC**: No tiene la precisión de CDC, pero es suficiente para la mayoría de casos

## Recomendaciones

### Para mejor rendimiento:
1. **Agregar columnas de timestamp** a las tablas importantes:
   ```sql
   ALTER TABLE MiTabla ADD ModifiedDate DATETIME DEFAULT GETDATE();
   ```
   (Esto requiere modificar la tabla, pero mejora mucho la detección)

2. **Usar ReplacingMergeTree** en ClickHouse para manejar UPDATEs:
   ```sql
   CREATE TABLE ... ENGINE = ReplacingMergeTree(ModifiedDate) ORDER BY Id;
   ```

3. **Ajustar poll interval** según volumen:
   - Alto volumen: 10-30 segundos
   - Bajo volumen: 5-10 segundos

## Troubleshooting

### "No se detectan UPDATEs"
- Verificar si la tabla tiene columna de timestamp
- Si no tiene, considerar agregarla o usar sync completo periódico

### "DELETEs no se eliminan"
- Es normal, el script solo los detecta
- Implementar estrategia de eliminación según tu caso

### "Sincronización completa muy frecuente"
- Ajustar `FULL_SYNC_INTERVAL` en el `.env`
- Considerar agregar timestamps a las tablas

### "Alto uso de CPU/recursos"
- Aumentar `--poll-interval`
- Reducir número de tablas monitoreadas
- Verificar que las queries usen índices

## Monitoreo

El script muestra en cada iteración:
- Estrategia usada por tabla
- Número de cambios detectados (inserts, updates, deletes)
- Tiempo de procesamiento por ciclo
- Totales acumulados

Ejemplo de salida:

```
[INFO] dbo.Tabla1 -> Estrategia: TIMESTAMP (ModifiedDate)
[INFO] dbo.Tabla2 -> Estrategia: INCREMENTAL (Id)
[INFO] dbo.Tabla3 -> Estrategia: COUNT (sync completo cada 3600s)

[ITERATION 1] 2024-01-15 10:30:00
  dbo.Tabla1: +150 inserts, ~25 updates, -5 deletes
  dbo.Tabla2: +200 inserts, ~0 updates, -0 deletes
  Ciclo completado en 2.34s | Total: +350 ~25 -5
```

## Conclusión

**v4 es ideal cuando:**
- ✅ No puedes modificar la base de datos de producción
- ✅ No tienes permisos de sysadmin
- ✅ Quieres streaming continuo sin invasión
- ✅ Las tablas tienen timestamps o IDs incrementales

**Considera v3 (CDC) si:**
- ✅ Puedes habilitar CDC
- ✅ Necesitas detección precisa de todos los cambios
- ✅ Tienes permisos de sysadmin
