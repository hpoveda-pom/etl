#!/usr/bin/env python3
"""
SQL Server to ClickHouse Streaming v4 - No Invasivo (CORREGIDO)
================================================================

Este script implementa streaming NO INVASIVO usando solo queries SELECT.
Estrategias seguras y robustas:
1. ROWVERSION (prioridad) - Nativo, seguro, no depende de zona horaria
2. ID Incremental - Seguro y eficiente
3. Timestamp + PK (watermark doble) - Solo si hay PK disponible

IMPORTANTE: Para tablas con ROWVERSION o TIMESTAMP (que detectan UPDATEs):
- ClickHouse debe usar ReplacingMergeTree con ORDER BY (Id)
- O implementar deduplicación periódica
- Los updates se insertan como nuevas filas (event sourcing)

Uso:
    python sqlserver_to_clickhouse_streamingv4.py ORIG_DB DEST_DB [tablas] [--prod] [--poll-interval SECONDS]

Ejemplo:
    python sqlserver_to_clickhouse_streamingv4.py POM_Aplicaciones POM_Aplicaciones --prod --poll-interval 10
"""

import os
import sys
import time
import signal
import datetime
import pyodbc
import clickhouse_connect
from decimal import Decimal
from dotenv import load_dotenv
from typing import Optional, List, Tuple, Dict, Any

# File locking multiplataforma
try:
    import fcntl  # Linux/Unix
    import errno
    HAS_FCNTL = True
except ImportError:
    HAS_FCNTL = False
    try:
        import msvcrt  # Windows
        HAS_MSVCRT = True
    except ImportError:
        HAS_MSVCRT = False

load_dotenv()

# =========================
# ENV CONFIG
# =========================
SQL_SERVER = os.getenv("SQL_SERVER")
SQL_USER = os.getenv("SQL_USER")
SQL_PASSWORD = os.getenv("SQL_PASSWORD")
SQL_DRIVER = os.getenv("SQL_DRIVER", "ODBC Driver 17 for SQL Server")

SQL_SERVER_PROD = os.getenv("SQL_SERVER_PROD")
SQL_USER_PROD = os.getenv("SQL_USER_PROD")
SQL_PASSWORD_PROD = os.getenv("SQL_PASSWORD_PROD")

CH_HOST = os.getenv("CH_HOST", "localhost")
CH_PORT = int(os.getenv("CH_PORT", "8123"))
CH_USER = os.getenv("CH_USER", "default")
CH_PASSWORD = os.getenv("CH_PASSWORD", "")
CH_DATABASE = os.getenv("CH_DATABASE", "default")

# Intervalo de polling por defecto (segundos)
DEFAULT_POLL_INTERVAL = int(os.getenv("POLL_INTERVAL", "10"))

# Tamaño de batch para insertar en ClickHouse
INSERT_BATCH_ROWS = int(os.getenv("INSERT_BATCH_ROWS", "2000"))

# Flag global para manejar señales de terminación
running = True

# Lock file para evitar ejecuciones duplicadas en el mismo servidor
LOCK_FILE_DIR = os.getenv("LOCK_FILE_DIR", "/tmp")

# =========================
# HELPERS
# =========================
def signal_handler(signum, frame):
    """Maneja señales para terminar el servicio gracefully"""
    global running
    print(f"\n[INFO] Señal recibida ({signum}). Terminando servicio...")
    running = False

def usage():
    print("Uso:")
    print("  python sqlserver_to_clickhouse_streamingv4.py ORIG_DB DEST_DB [tablas] [--prod] [--poll-interval SECONDS]")
    print("\nOpciones:")
    print("  --prod              Usar credenciales de producción")
    print("  --poll-interval N    Intervalo de polling en segundos (default: 10)")
    print("\nEjemplo:")
    print("  python sqlserver_to_clickhouse_streamingv4.py POM_Aplicaciones POM_Aplicaciones --prod --poll-interval 10")
    sys.exit(1)

def parse_args():
    if len(sys.argv) < 3:
        usage()

    orig_db = sys.argv[1].strip()
    dest_db = sys.argv[2].strip()

    tables_arg = "*"
    use_prod = False
    poll_interval = DEFAULT_POLL_INTERVAL

    args_list = sys.argv[3:]
    
    if "--prod" in args_list:
        use_prod = True
        args_list = [a for a in args_list if a != "--prod"]
    
    if "--poll-interval" in args_list:
        idx = args_list.index("--poll-interval")
        if idx + 1 < len(args_list):
            try:
                poll_interval = int(args_list[idx + 1])
                if poll_interval < 1:
                    poll_interval = 1
            except ValueError:
                print("[ERROR] --poll-interval debe ser un número entero")
                sys.exit(1)
            args_list = [a for i, a in enumerate(args_list) if i != idx and i != idx + 1]

    if len(args_list) >= 1:
        tables_arg = args_list[0].strip() or "*"

    if tables_arg == "*" or tables_arg.lower() == "all":
        tables = None
    else:
        tables = [x.strip() for x in tables_arg.split(",") if x.strip()]
        if not tables:
            raise Exception("Lista de tablas vacía.")

    return orig_db, dest_db, tables, use_prod, poll_interval

def build_sqlserver_conn_str(database_name: str, use_prod: bool = False):
    if use_prod and SQL_SERVER_PROD and SQL_USER_PROD and SQL_PASSWORD_PROD:
        server = SQL_SERVER_PROD
        user = SQL_USER_PROD
        password = SQL_PASSWORD_PROD
    else:
        if not SQL_SERVER or not SQL_USER or SQL_PASSWORD is None:
            raise Exception("Faltan SQL_SERVER / SQL_USER / SQL_PASSWORD en el .env")
        server = SQL_SERVER
        user = SQL_USER
        password = SQL_PASSWORD

    return (
        f"DRIVER={{{SQL_DRIVER}}};"
        f"SERVER={server};"
        f"DATABASE={database_name};"
        f"UID={user};"
        f"PWD={password};"
        f"TrustServerCertificate=yes;"
    )

def sql_conn(database_name: str, use_prod: bool = False):
    return pyodbc.connect(build_sqlserver_conn_str(database_name, use_prod))

def ch_client():
    secure = (CH_PORT == 8443)
    return clickhouse_connect.get_client(
        host=CH_HOST,
        port=CH_PORT,
        username=CH_USER,
        password=CH_PASSWORD,
        database=CH_DATABASE,
        secure=secure,
        verify=False,
    )

def ensure_database(ch, dest_db: str):
    ch.command(f"CREATE DATABASE IF NOT EXISTS `{dest_db}`")

def get_tables(cursor, requested_tables=None):
    """Obtiene lista de tablas"""
    if requested_tables is None:
        q = """
        SELECT TABLE_SCHEMA, TABLE_NAME
        FROM INFORMATION_SCHEMA.TABLES
        WHERE TABLE_TYPE='BASE TABLE'
          AND TABLE_NAME NOT LIKE 'TMP\\_%' ESCAPE '\\'
        ORDER BY TABLE_SCHEMA, TABLE_NAME
        """
        cursor.execute(q)
        return cursor.fetchall()
    else:
        normalized = []
        for t in requested_tables:
            if "." in t:
                schema, table = t.split(".", 1)
                normalized.append((schema.strip(), table.strip()))
            else:
                normalized.append(("dbo", t.strip()))
        return normalized

def get_columns(cursor, schema, table):
    q = """
    SELECT COLUMN_NAME, DATA_TYPE, NUMERIC_PRECISION, NUMERIC_SCALE, IS_NULLABLE
    FROM INFORMATION_SCHEMA.COLUMNS
    WHERE TABLE_SCHEMA = ?
      AND TABLE_NAME = ?
    ORDER BY ORDINAL_POSITION
    """
    cursor.execute(q, (schema, table))
    return cursor.fetchall()

def detect_rowversion_column(cursor, schema, table):
    """Detecta columna ROWVERSION (TIMESTAMP binario) de SQL Server"""
    # En SQL Server, ROWVERSION es simplemente una columna de tipo 'timestamp'
    # No hay campo is_rowversion en sys.columns, solo se detecta por el tipo
    q = """
    SELECT COLUMN_NAME
    FROM INFORMATION_SCHEMA.COLUMNS
    WHERE TABLE_SCHEMA = ?
      AND TABLE_NAME = ?
      AND DATA_TYPE = 'timestamp'
    ORDER BY ORDINAL_POSITION
    """
    cursor.execute(q, (schema, table))
    timestamp_cols = cursor.fetchall()
    if timestamp_cols:
        return timestamp_cols[0][0]
    
    return None

def detect_incremental_column(cursor, schema, table):
    """Detecta columna incremental (ID) - Detección estricta"""
    # Primero buscar identity columns
    q = """
    SELECT c.COLUMN_NAME
    FROM INFORMATION_SCHEMA.COLUMNS c
    INNER JOIN sys.columns sc ON sc.object_id = OBJECT_ID(QUOTENAME(?) + '.' + QUOTENAME(?))
        AND sc.name = c.COLUMN_NAME
    WHERE c.TABLE_SCHEMA = ?
      AND c.TABLE_NAME = ?
      AND sc.is_identity = 1
    ORDER BY sc.column_id
    """
    cursor.execute(q, (schema, table, schema, table))
    identity_cols = cursor.fetchall()
    if identity_cols:
        return identity_cols[0][0]

    # Buscar columnas Id, ID, id (case-insensitive, nombres exactos)
    q = """
    SELECT COLUMN_NAME, DATA_TYPE
    FROM INFORMATION_SCHEMA.COLUMNS
    WHERE TABLE_SCHEMA = ?
      AND TABLE_NAME = ?
      AND UPPER(COLUMN_NAME) IN ('ID', 'ID_', 'IDNUM', 'IDNUMERO')
      AND DATA_TYPE IN ('int', 'bigint', 'smallint', 'tinyint')
    ORDER BY CASE UPPER(COLUMN_NAME) WHEN 'ID' THEN 1 WHEN 'ID_' THEN 2 ELSE 3 END
    """
    cursor.execute(q, (schema, table))
    id_cols = cursor.fetchall()
    if id_cols:
        return id_cols[0][0]
    
    # Buscar columnas que terminen en 'Id' o 'ID' (sufijo común)
    q = """
    SELECT COLUMN_NAME, DATA_TYPE
    FROM INFORMATION_SCHEMA.COLUMNS
    WHERE TABLE_SCHEMA = ?
      AND TABLE_NAME = ?
      AND (COLUMN_NAME LIKE '%Id' OR COLUMN_NAME LIKE '%ID' OR COLUMN_NAME LIKE '%_Id' OR COLUMN_NAME LIKE '%_ID')
      AND DATA_TYPE IN ('int', 'bigint', 'smallint', 'tinyint')
    ORDER BY ORDINAL_POSITION
    """
    cursor.execute(q, (schema, table))
    id_like_cols = cursor.fetchall()
    if id_like_cols:
        return id_like_cols[0][0]

    # No usar fallback de "primer numeric" - detección estricta
    return None

def detect_timestamp_column(cursor, schema, table):
    """Detecta columna de timestamp/modified date (excluye columnas de creación)"""
    modification_patterns = [
        'ModifiedDate', 'Modified_Date', 'modified_date',
        'UpdatedAt', 'Updated_At', 'updated_at',
        'UpdateDate', 'Update_Date', 'update_date',
        'FechaModificacion', 'Fecha_Modificacion', 'fecha_modificacion',
        'LastModified', 'Last_Modified', 'last_modified',
        'ChangedDate', 'Changed_Date', 'changed_date',
        'ModifyDate', 'Modify_Date', 'modify_date',
        'Updated', 'updated', 'Modified', 'modified'
    ]
    
    creation_patterns = [
        'F_Ingreso', 'FechaCreacion', 'CreatedAt', 'CreateDate', 'F_Creacion', 'Ingreso', 'Created'
    ]
    
    q = """
    SELECT COLUMN_NAME, DATA_TYPE
    FROM INFORMATION_SCHEMA.COLUMNS
    WHERE TABLE_SCHEMA = ?
      AND TABLE_NAME = ?
      AND DATA_TYPE IN ('datetime', 'datetime2', 'smalldatetime', 'timestamp')
    ORDER BY ORDINAL_POSITION
    """
    cursor.execute(q, (schema, table))
    timestamp_cols = cursor.fetchall()
    
    # Priorizar columnas de modificación
    for pattern in modification_patterns:
        for col_name, data_type in timestamp_cols:
            if pattern.lower() in col_name.lower():
                return col_name, data_type
    
    # Excluir columnas de creación
    for col_name, data_type in timestamp_cols:
        is_creation = False
        for creation_pattern in creation_patterns:
            if creation_pattern.lower() in col_name.lower():
                is_creation = True
                break
        if not is_creation:
            return col_name, data_type
    
    return None, None

def get_max_value_from_clickhouse(ch, dest_db, table, column):
    """Obtiene el valor máximo de la columna desde ClickHouse"""
    try:
        full_table = f"`{dest_db}`.`{table}`"
        exists_result = ch.query(f"EXISTS TABLE {full_table}")
        if not exists_result.result_rows or exists_result.result_rows[0][0] == 0:
            return None
        
        try:
            desc_result = ch.query(f"DESCRIBE TABLE {full_table}")
            existing_columns = [row[0].lower() for row in desc_result.result_rows]
            if column.lower() not in existing_columns:
                return None
        except Exception:
            pass
        
        query = f"SELECT max(`{column}`) FROM {full_table}"
        result = ch.query(query)
        
        if result.result_rows and len(result.result_rows) > 0:
            max_value = result.result_rows[0][0]
            if max_value is not None:
                return max_value
        
        return None
    except Exception:
        return None

def get_max_rowversion_from_clickhouse(ch, dest_db, table, column):
    """Obtiene el ROWVERSION máximo desde ClickHouse (binario como hex string)"""
    try:
        full_table = f"`{dest_db}`.`{table}`"
        exists_result = ch.query(f"EXISTS TABLE {full_table}")
        if not exists_result.result_rows or exists_result.result_rows[0][0] == 0:
            return None
        
        try:
            desc_result = ch.query(f"DESCRIBE TABLE {full_table}")
            existing_columns = [row[0].lower() for row in desc_result.result_rows]
            if column.lower() not in existing_columns:
                return None
        except Exception:
            pass
        
        # Obtener el último ROWVERSION (ordenado desc)
        query = f"SELECT `{column}` FROM {full_table} ORDER BY `{column}` DESC LIMIT 1"
        result = ch.query(query)
        
        if result.result_rows and len(result.result_rows) > 0:
            max_value = result.result_rows[0][0]
            if max_value is not None:
                return max_value
        
        return None
    except Exception:
        return None

def compare_rowversion_bytes(rv1_hex: str, rv2_hex: str) -> bool:
    """Compara dos ROWVERSION como bytes, no como strings"""
    if rv1_hex is None:
        return False
    if rv2_hex is None:
        return True
    try:
        return bytes.fromhex(rv1_hex) > bytes.fromhex(rv2_hex)
    except:
        # Fallback a comparación string si no es hex válido
        return rv1_hex > rv2_hex

def normalize_py_value(v):
    """Normaliza valores de Python para ClickHouse"""
    if v is None:
        return None
    if isinstance(v, Decimal):
        return float(v)
    if isinstance(v, (datetime.datetime, datetime.date)):
        try:
            if isinstance(v, datetime.date) and not isinstance(v, datetime.datetime):
                v = datetime.datetime.combine(v, datetime.time.min)
            min_date = datetime.datetime(1970, 1, 1, 0, 0, 0)
            max_date = datetime.datetime(2106, 2, 7, 6, 28, 15)
            if v < min_date or v > max_date:
                return None
            v.timestamp()
            return v
        except (ValueError, OSError, OverflowError):
            return None
    if isinstance(v, datetime.time):
        return v.isoformat()
    if isinstance(v, (bytes, bytearray)):
        return v.hex()
    return v

def fetch_rows_by_rowversion(cursor, schema, table, colnames, rowversion_col, last_rowversion, chunk_size):
    """Obtiene filas nuevas/modificadas usando ROWVERSION (binario)"""
    cols = ", ".join([f"[{c}]" for c in colnames])
    rowversion_idx = colnames.index(rowversion_col)
    
    if last_rowversion is not None:
        # ROWVERSION se compara como binario en SQL Server
        # Si viene como string hex desde ClickHouse, convertir a bytes
        if isinstance(last_rowversion, str):
            try:
                last_rowversion_bytes = bytes.fromhex(last_rowversion)
            except:
                last_rowversion_bytes = last_rowversion.encode('latin-1')
        else:
            last_rowversion_bytes = last_rowversion
        
        query = f"""
        SELECT {cols}
        FROM [{schema}].[{table}]
        WHERE [{rowversion_col}] > ?
        ORDER BY [{rowversion_col}] ASC
        """
        cursor.execute(query, (last_rowversion_bytes,))
    else:
        query = f"""
        SELECT {cols}
        FROM [{schema}].[{table}]
        ORDER BY [{rowversion_col}] ASC
        """
        cursor.execute(query)
    
    while True:
        rows = cursor.fetchmany(chunk_size)
        if not rows:
            break
        
        out = []
        for r in rows:
            normalized_row = []
            for i, val in enumerate(r):
                if i == rowversion_idx and isinstance(val, bytes):
                    normalized_row.append(val.hex())
                else:
                    normalized_row.append(normalize_py_value(val))
            out.append(normalized_row)
        yield out

def fetch_rows_by_id(cursor, schema, table, colnames, id_col, last_id, chunk_size):
    """Obtiene filas nuevas usando ID incremental"""
    cols = ", ".join([f"[{c}]" for c in colnames])
    
    if last_id is not None:
        query = f"""
        SELECT {cols}
        FROM [{schema}].[{table}]
        WHERE [{id_col}] > ?
        ORDER BY [{id_col}] ASC
        """
        cursor.execute(query, (last_id,))
    else:
        query = f"""
        SELECT {cols}
        FROM [{schema}].[{table}]
        ORDER BY [{id_col}] ASC
        """
        cursor.execute(query)
    
    while True:
        rows = cursor.fetchmany(chunk_size)
        if not rows:
            break
        
        out = []
        for r in rows:
            out.append([normalize_py_value(x) for x in r])
        yield out

def fetch_rows_by_timestamp_with_pk(cursor, schema, table, colnames, timestamp_col, pk_col, last_timestamp, last_pk, chunk_size):
    """Obtiene filas usando timestamp + PK (watermark doble para evitar empates)"""
    cols = ", ".join([f"[{c}]" for c in colnames])
    
    if last_timestamp is not None and last_pk is not None:
        # Watermark doble: timestamp + PK para evitar perder filas con empates
        query = f"""
        SELECT {cols}
        FROM [{schema}].[{table}]
        WHERE ([{timestamp_col}] > ?)
           OR ([{timestamp_col}] = ? AND [{pk_col}] > ?)
        ORDER BY [{timestamp_col}] ASC, [{pk_col}] ASC
        """
        cursor.execute(query, (last_timestamp, last_timestamp, last_pk))
    elif last_timestamp is not None:
        query = f"""
        SELECT {cols}
        FROM [{schema}].[{table}]
        WHERE [{timestamp_col}] > ?
        ORDER BY [{timestamp_col}] ASC, [{pk_col}] ASC
        """
        cursor.execute(query, (last_timestamp,))
    else:
        query = f"""
        SELECT {cols}
        FROM [{schema}].[{table}]
        ORDER BY [{timestamp_col}] ASC, [{pk_col}] ASC
        """
        cursor.execute(query)
    
    while True:
        rows = cursor.fetchmany(chunk_size)
        if not rows:
            break
        
        out = []
        for r in rows:
            out.append([normalize_py_value(x) for x in r])
        yield out

# =========================
# ESTADO EN CLICKHOUSE
# =========================
# El estado se mantiene directamente en ClickHouse consultando max() de las columnas
# Esto permite ejecutar desde múltiples servidores sin conflictos
# No se usa archivo JSON local

def process_table_changes(cursor, ch, dest_db, schema, table, strategy_info, last_state):
    """
    Procesa cambios para una tabla usando la estrategia detectada.
    Evita duplicados manteniendo seguimiento adecuado.
    
    Retorna: (inserts, new_state)
    """
    full_table = f"`{dest_db}`.`{table}`"
    
    # Verificar que la tabla existe en ClickHouse
    try:
        exists_result = ch.query(f"EXISTS TABLE {full_table}")
        if not exists_result.result_rows or exists_result.result_rows[0][0] == 0:
            print(f"[SKIP] {schema}.{table} - Tabla no existe en ClickHouse")
            return (0, last_state)
    except Exception as e:
        print(f"[ERROR] {schema}.{table} - Error verificando tabla: {e}")
        return (0, last_state)
    
    # Obtener estructura de ClickHouse
    try:
        desc_result = ch.query(f"DESCRIBE TABLE {full_table}")
        ch_columns = [row[0] for row in desc_result.result_rows]
    except Exception as e:
        print(f"[ERROR] {schema}.{table} - Error obteniendo estructura: {e}")
        return (0, last_state)
    
    strategy = strategy_info.get('strategy')
    colnames = strategy_info.get('colnames', [])
    
    inserts = 0
    
    try:
        if strategy == 'rowversion':
            # Estrategia 1: ROWVERSION (MEJOR - más seguro y nativo)
            rowversion_col = strategy_info.get('rowversion_col')
            
            # Siempre leer desde ClickHouse (permite múltiples servidores)
            last_rowversion = get_max_rowversion_from_clickhouse(ch, dest_db, table, rowversion_col)
            if last_rowversion is not None:
                print(f"  [DEBUG] {schema}.{table} - Desde ClickHouse: last_rowversion = {last_rowversion[:16]}...")
            else:
                print(f"  [INFO] {schema}.{table} - Primera carga (sin datos en ClickHouse)")
            
            buffer = []
            max_rowversion = last_rowversion
            
            for chunk in fetch_rows_by_rowversion(cursor, schema, table, colnames, rowversion_col, last_rowversion, INSERT_BATCH_ROWS):
                for row in chunk:
                    row_dict = dict(zip(colnames, row))
                    row_list = [row_dict.get(col) for col in ch_columns]
                    buffer.append(row_list)
                    
                    # Actualizar max_rowversion comparando como bytes
                    if rowversion_col in row_dict and row_dict[rowversion_col]:
                        current_rv = row_dict[rowversion_col]
                        if max_rowversion is None or compare_rowversion_bytes(current_rv, max_rowversion):
                            max_rowversion = current_rv
                    
                    if len(buffer) >= INSERT_BATCH_ROWS:
                        ch.insert(full_table, buffer, column_names=ch_columns)
                        inserts += len(buffer)
                        buffer.clear()
            
            if buffer:
                ch.insert(full_table, buffer, column_names=ch_columns)
                inserts += len(buffer)
            
        
        elif strategy == 'id':
            # Estrategia 2: ID Incremental
            id_col = strategy_info.get('id_col')
            
            # Siempre leer desde ClickHouse
            last_id = get_max_value_from_clickhouse(ch, dest_db, table, id_col)
            if last_id is not None:
                print(f"  [DEBUG] {schema}.{table} - Desde ClickHouse: last_id = {last_id}")
            else:
                print(f"  [INFO] {schema}.{table} - Primera carga (sin datos en ClickHouse)")
            
            buffer = []
            max_id = last_id
            
            for chunk in fetch_rows_by_id(cursor, schema, table, colnames, id_col, last_id, INSERT_BATCH_ROWS):
                for row in chunk:
                    row_dict = dict(zip(colnames, row))
                    row_list = [row_dict.get(col) for col in ch_columns]
                    buffer.append(row_list)
                    
                    if id_col in row_dict and row_dict[id_col]:
                        if max_id is None or row_dict[id_col] > max_id:
                            max_id = row_dict[id_col]
                    
                    if len(buffer) >= INSERT_BATCH_ROWS:
                        ch.insert(full_table, buffer, column_names=ch_columns)
                        inserts += len(buffer)
                        buffer.clear()
            
            if buffer:
                ch.insert(full_table, buffer, column_names=ch_columns)
                inserts += len(buffer)
            
        
        elif strategy == 'timestamp':
            # Estrategia 3: Timestamp + PK (watermark doble)
            timestamp_col = strategy_info.get('timestamp_col')
            pk_col = strategy_info.get('pk_col')
            
            if not pk_col:
                print(f"[ERROR] {schema}.{table} - Timestamp requiere PK/ID para watermark doble")
                return (0, last_state)
            
            # Siempre leer desde ClickHouse
            last_timestamp = get_max_value_from_clickhouse(ch, dest_db, table, timestamp_col)
            last_pk = get_max_value_from_clickhouse(ch, dest_db, table, pk_col)
            if last_timestamp:
                print(f"  [DEBUG] {schema}.{table} - Desde ClickHouse: last_timestamp = {last_timestamp}, last_pk = {last_pk}")
            else:
                print(f"  [INFO] {schema}.{table} - Primera carga (sin datos en ClickHouse)")
                last_pk = None
            
            buffer = []
            max_timestamp = last_timestamp
            max_pk = last_pk
            
            for chunk in fetch_rows_by_timestamp_with_pk(cursor, schema, table, colnames, timestamp_col, pk_col, last_timestamp, last_pk, INSERT_BATCH_ROWS):
                for row in chunk:
                    row_dict = dict(zip(colnames, row))
                    row_list = [row_dict.get(col) for col in ch_columns]
                    buffer.append(row_list)
                    
                    # Actualizar watermark doble (ts + pk del mismo registro)
                    if timestamp_col in row_dict and row_dict[timestamp_col]:
                        current_ts = row_dict[timestamp_col]
                        current_pk = row_dict.get(pk_col) if pk_col in row_dict else None
                        
                        if max_timestamp is None:
                            max_timestamp = current_ts
                            max_pk = current_pk
                        elif current_ts > max_timestamp:
                            # Nuevo timestamp mayor: actualizar ambos del mismo registro
                            max_timestamp = current_ts
                            max_pk = current_pk
                        elif current_ts == max_timestamp and current_pk is not None:
                            # Mismo timestamp: actualizar solo si PK es mayor
                            if max_pk is None or current_pk > max_pk:
                                max_pk = current_pk
                    
                    if len(buffer) >= INSERT_BATCH_ROWS:
                        ch.insert(full_table, buffer, column_names=ch_columns)
                        inserts += len(buffer)
                        buffer.clear()
            
            if buffer:
                ch.insert(full_table, buffer, column_names=ch_columns)
                inserts += len(buffer)
            
        
        return (inserts, {})
    
    except Exception as e:
        print(f"[ERROR] {schema}.{table} - Error procesando cambios: {e}")
        import traceback
        traceback.print_exc()
        return (0, last_state)

# =========================
# MAIN SERVICE LOOP
# =========================
def main():
    global running
    
    # Configurar manejadores de señales
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)
    
    start_time = time.time()
    source_db, dest_db, requested_tables, use_prod, poll_interval = parse_args()
    
    # Conectar a SQL Server
    try:
        conn = sql_conn(source_db, use_prod)
        cursor = conn.cursor()
        print(f"[OK] Conectado a SQL Server")
    except Exception as e:
        print(f"[ERROR] Error conectando a SQL Server: {e}")
        sys.exit(1)
    
    # Lock file para evitar ejecuciones duplicadas en el mismo servidor
    lock_file_path = os.path.join(LOCK_FILE_DIR, f"streaming_v4_{source_db}_{dest_db}.lock")
    lock_file = None
    
    try:
        # Verificar si el lock file existe y el proceso está corriendo
        if os.path.exists(lock_file_path):
            try:
                with open(lock_file_path, 'r') as f:
                    old_pid = f.read().strip()
                    if old_pid:
                        # Verificar si el proceso aún existe (solo en Linux/Unix)
                        if sys.platform != 'win32':
                            try:
                                os.kill(int(old_pid), 0)  # Signal 0 solo verifica existencia
                                print(f"[ERROR] Ya hay una instancia corriendo (PID: {old_pid}). Lock file: {lock_file_path}")
                                print(f"[INFO] Si estás seguro de que no hay otra instancia, elimina el lock file y vuelve a intentar")
                                sys.exit(1)
                            except (OSError, ValueError):
                                # Proceso no existe, eliminar lock file obsoleto
                                os.remove(lock_file_path)
            except:
                pass
        
        # Crear lock file con PID actual
        lock_file = open(lock_file_path, 'w')
        lock_file.write(str(os.getpid()))
        lock_file.flush()
        
        # Aplicar lock según plataforma
        if HAS_FCNTL:
            try:
                fcntl.flock(lock_file.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
            except IOError as e:
                if e.errno in (errno.EAGAIN, errno.EWOULDBLOCK):
                    print(f"[ERROR] Ya hay una instancia corriendo. Lock file: {lock_file_path}")
                    sys.exit(1)
                else:
                    raise
        elif HAS_MSVCRT:
            try:
                msvcrt.locking(lock_file.fileno(), msvcrt.LK_NBLCK, 1)
            except IOError:
                print(f"[ERROR] Ya hay una instancia corriendo. Lock file: {lock_file_path}")
                sys.exit(1)
    except Exception as e:
        print(f"[WARN] No se pudo crear lock file (continuando): {e}")
        if lock_file:
            lock_file.close()
        lock_file = None
    
    # Conectar a ClickHouse
    ch = ch_client()
    ensure_database(ch, dest_db)
    print(f"[OK] Conectado a ClickHouse")
    
    # Obtener tablas
    tables = get_tables(cursor, requested_tables)
    
    if not tables:
        print("[ERROR] No se encontraron tablas")
        sys.exit(1)
    
    # Detectar estrategia para cada tabla
    table_strategies = {}
    for schema, table in tables:
        cols_meta = get_columns(cursor, schema, table)
        if not cols_meta:
            continue
        
        colnames = [c[0] for c in cols_meta]
        
        # Priorizar: ROWVERSION > ID > Timestamp (solo si hay PK)
        rowversion_col = detect_rowversion_column(cursor, schema, table)
        id_col = detect_incremental_column(cursor, schema, table)
        timestamp_col, timestamp_type = detect_timestamp_column(cursor, schema, table)
        
        # Mostrar información de diagnóstico si no se encuentra estrategia
        if not rowversion_col and not id_col and not (timestamp_col and id_col):
            # Buscar cualquier columna numérica que pueda servir como ID
            numeric_cols = [c[0] for c in cols_meta if c[1] in ('int', 'bigint', 'smallint', 'tinyint')]
            datetime_cols = [c[0] for c in cols_meta if c[1] in ('datetime', 'datetime2', 'smalldatetime', 'date')]
            
            print(f"[SKIP] {schema}.{table} - No tiene estrategia compatible")
            print(f"  Columnas numéricas encontradas: {', '.join(numeric_cols[:5]) if numeric_cols else 'ninguna'}")
            print(f"  Columnas datetime encontradas: {', '.join(datetime_cols[:5]) if datetime_cols else 'ninguna'}")
            print(f"  Total columnas: {len(colnames)}")
            print(f"  Sugerencia: La tabla necesita ROWVERSION, columna ID (Id/ID/id) o Timestamp+PK")
            continue
        
        if rowversion_col:
            strategy = 'rowversion'
            print(f"[INFO] {schema}.{table} -> Estrategia: ROWVERSION ({rowversion_col}) [NATIVO - Más seguro]")
        elif id_col:
            strategy = 'id'
            print(f"[INFO] {schema}.{table} -> Estrategia: ID Incremental ({id_col})")
        elif timestamp_col and id_col:
            # Timestamp solo si también hay PK para watermark doble
            strategy = 'timestamp'
            print(f"[INFO] {schema}.{table} -> Estrategia: Timestamp + PK ({timestamp_col} + {id_col}) [Watermark doble]")
        
        table_strategies[(schema, table)] = {
            'strategy': strategy,
            'colnames': colnames,
            'rowversion_col': rowversion_col,
            'id_col': id_col,
            'timestamp_col': timestamp_col if strategy == 'timestamp' else None,
            'pk_col': id_col if strategy == 'timestamp' else None
        }
    
    if not table_strategies:
        print("[ERROR] No hay tablas con estrategias compatibles para procesar")
        sys.exit(1)
    
    env_type = "PRODUCCIÓN" if use_prod else "DESARROLLO"
    server_info = SQL_SERVER_PROD if (use_prod and SQL_SERVER_PROD) else SQL_SERVER
    
    print(f"\n[START] STREAMING SEGURO ({env_type})")
    print(f"  Server: {server_info}")
    print(f"  Source DB: {source_db}")
    print(f"  Dest DB: {dest_db}")
    print(f"  Tablas: {len(table_strategies)}")
    print(f"  Poll Interval: {poll_interval} segundos")
    print(f"  Lock file: {lock_file_path}")
    print(f"  Presiona Ctrl+C para detener el servicio\n")
    
    total_inserts = 0
    iteration = 0
    
    try:
        while running:
            iteration += 1
            cycle_start = time.time()
            
            print(f"\n[ITERATION {iteration}] {datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
            
            cycle_inserts = 0
            
            for (schema, table), strategy_info in table_strategies.items():
                try:
                    # Estado siempre se lee desde ClickHouse
                    inserts, _ = process_table_changes(
                        cursor, ch, dest_db, schema, table, strategy_info, {}
                    )
                    
                    if inserts > 0:
                        print(f"  {schema}.{table}: +{inserts} inserts")
                    
                    cycle_inserts += inserts
                    
                except Exception as e:
                    print(f"[ERROR] {schema}.{table}: {e}")
            
            total_inserts += cycle_inserts
            
            cycle_time = time.time() - cycle_start
            if cycle_inserts > 0:
                print(f"  Ciclo completado en {cycle_time:.2f}s | Total inserts: {total_inserts}")
            
            # Esperar antes del siguiente ciclo
            if running:
                time.sleep(poll_interval)
    
    except KeyboardInterrupt:
        print("\n[INFO] Interrupción recibida")
    finally:
        # Liberar lock
        if lock_file:
            try:
                if HAS_FCNTL:
                    fcntl.flock(lock_file.fileno(), fcntl.LOCK_UN)
                elif HAS_MSVCRT:
                    msvcrt.locking(lock_file.fileno(), msvcrt.LK_UNLCK, 1)
                lock_file.close()
                if os.path.exists(lock_file_path):
                    os.remove(lock_file_path)
            except:
                pass
        
        cursor.close()
        conn.close()
        
        elapsed = time.time() - start_time
        print(f"\n[STOP] Servicio detenido")
        print(f"  Iteraciones: {iteration}")
        print(f"  Total inserts: {total_inserts}")
        print(f"  Tiempo total: {elapsed:.2f} segundos")

if __name__ == "__main__":
    main()
