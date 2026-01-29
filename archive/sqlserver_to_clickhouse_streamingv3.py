#!/usr/bin/env python3
"""
SQL Server to ClickHouse Streaming v3 - True Streaming con CDC
==============================================================

Este script implementa un verdadero streaming usando Change Data Capture (CDC) de SQL Server.
Funciona como un servicio de escucha continuo que captura solo los cambios (INSERT, UPDATE, DELETE)
en tiempo real, en lugar de hacer queries periódicas.

Requisitos:
- SQL Server con CDC habilitado en la base de datos y tablas
- Permisos para leer tablas CDC
- El script debe correr como servicio/daemon (no en cron)

Uso:
    python sqlserver_to_clickhouse_streamingv3.py ORIG_DB DEST_DB [tablas] [--prod] [--poll-interval SECONDS]

Ejemplo:
    python sqlserver_to_clickhouse_streamingv3.py POM_Aplicaciones POM_Aplicaciones --prod --poll-interval 5
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
DEFAULT_POLL_INTERVAL = int(os.getenv("CDC_POLL_INTERVAL", "5"))

# Tamaño de batch para insertar en ClickHouse
INSERT_BATCH_ROWS = int(os.getenv("INSERT_BATCH_ROWS", "1000"))

# Flag global para manejar señales de terminación
running = True

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
    print("  python sqlserver_to_clickhouse_streamingv3.py ORIG_DB DEST_DB [tablas] [--prod] [--poll-interval SECONDS]")
    print("\nOpciones:")
    print("  --prod              Usar credenciales de producción")
    print("  --poll-interval N   Intervalo de polling en segundos (default: 5)")
    print("\nEjemplo:")
    print("  python sqlserver_to_clickhouse_streamingv3.py POM_Aplicaciones POM_Aplicaciones --prod --poll-interval 5")
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
    """Obtiene lista de tablas, filtrando por las que tienen CDC habilitado"""
    if requested_tables is None:
        # Obtener solo tablas con CDC habilitado
        q = """
        SELECT DISTINCT 
            t.TABLE_SCHEMA, 
            t.TABLE_NAME
        FROM INFORMATION_SCHEMA.TABLES t
        INNER JOIN cdc.change_tables ct 
            ON ct.source_schema = t.TABLE_SCHEMA 
            AND ct.source_object = t.TABLE_NAME
        WHERE t.TABLE_TYPE = 'BASE TABLE'
          AND t.TABLE_NAME NOT LIKE 'TMP\\_%' ESCAPE '\\'
        ORDER BY t.TABLE_SCHEMA, t.TABLE_NAME
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

def check_cdc_enabled(cursor, database_name: str) -> bool:
    """Verifica si CDC está habilitado en la base de datos"""
    try:
        cursor.execute("SELECT is_cdc_enabled FROM sys.databases WHERE name = ?", (database_name,))
        result = cursor.fetchone()
        return result and result[0] == 1
    except Exception as e:
        print(f"[ERROR] No se pudo verificar CDC en la base de datos: {e}")
        return False

def check_table_cdc_enabled(cursor, schema: str, table: str) -> bool:
    """Verifica si CDC está habilitado para una tabla específica"""
    try:
        q = """
        SELECT COUNT(*) 
        FROM cdc.change_tables 
        WHERE source_schema = ? AND source_object = ?
        """
        cursor.execute(q, (schema, table))
        result = cursor.fetchone()
        return result and result[0] > 0
    except Exception as e:
        print(f"[WARN] Error verificando CDC para {schema}.{table}: {e}")
        return False

def get_cdc_capture_instance(cursor, schema: str, table: str) -> Optional[str]:
    """Obtiene el nombre de la instancia de captura CDC para una tabla"""
    try:
        q = """
        SELECT capture_instance 
        FROM cdc.change_tables 
        WHERE source_schema = ? AND source_object = ?
        """
        cursor.execute(q, (schema, table))
        result = cursor.fetchone()
        return result[0] if result else None
    except Exception as e:
        print(f"[WARN] Error obteniendo capture_instance para {schema}.{table}: {e}")
        return None

def get_last_processed_lsn(cursor, capture_instance: str) -> Optional[bytes]:
    """Obtiene el último LSN procesado desde una tabla de estado (o None si es primera vez)"""
    # Por ahora retornamos None, pero podrías guardar esto en una tabla de estado
    # o en ClickHouse mismo
    return None

def get_cdc_changes(cursor, capture_instance: str, from_lsn: Optional[bytes] = None, to_lsn: Optional[bytes] = None) -> List[Dict[str, Any]]:
    """
    Obtiene cambios desde CDC usando la función cdc.fn_cdc_get_all_changes_<capture_instance>
    
    Retorna lista de cambios con:
    - __$operation: 1=delete, 2=insert, 3=update (antes), 4=update (después)
    - __$start_lsn: LSN del cambio
    - Todas las columnas de la tabla original
    """
    try:
        # Obtener LSNs válidos
        if to_lsn is None:
            # Obtener el LSN máximo disponible
            lsn_query = f"SELECT sys.fn_cdc_get_max_lsn()"
            cursor.execute(lsn_query)
            to_lsn = cursor.fetchone()[0]
        
        if from_lsn is None:
            # Obtener el LSN mínimo disponible para esta instancia
            lsn_query = f"SELECT sys.fn_cdc_get_min_lsn('{capture_instance}')"
            cursor.execute(lsn_query)
            from_lsn = cursor.fetchone()[0]
        
        if from_lsn is None or to_lsn is None or from_lsn >= to_lsn:
            return []
        
        # Construir query CDC
        # Nota: El nombre de la función CDC es dinámico basado en capture_instance
        cdc_function = f"cdc.fn_cdc_get_all_changes_{capture_instance}"
        
        query = f"""
        SELECT * 
        FROM {cdc_function}(?, ?, N'all')
        ORDER BY __$start_lsn, __$seqval
        """
        
        cursor.execute(query, (from_lsn, to_lsn))
        
        # Obtener nombres de columnas
        columns = [column[0] for column in cursor.description]
        
        changes = []
        for row in cursor.fetchall():
            change = dict(zip(columns, row))
            changes.append(change)
        
        return changes
    except Exception as e:
        print(f"[ERROR] Error obteniendo cambios CDC para {capture_instance}: {e}")
        return []

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

def get_table_columns_from_cdc(cursor, capture_instance: str) -> List[str]:
    """Obtiene las columnas capturadas por CDC (excluyendo metadatos __$)"""
    try:
        q = """
        SELECT column_name 
        FROM cdc.captured_columns 
        WHERE capture_instance = ?
        ORDER BY column_ordinal
        """
        cursor.execute(q, (capture_instance,))
        return [row[0] for row in cursor.fetchall()]
    except Exception as e:
        print(f"[ERROR] Error obteniendo columnas CDC: {e}")
        return []

def process_cdc_changes(ch, dest_db: str, schema: str, table: str, changes: List[Dict[str, Any]], capture_instance: str) -> Tuple[int, int, int]:
    """
    Procesa cambios CDC y los aplica a ClickHouse
    
    Retorna: (inserts, updates, deletes)
    """
    if not changes:
        return (0, 0, 0)
    
    full_table = f"`{dest_db}`.`{table}`"
    
    # Verificar que la tabla existe en ClickHouse
    try:
        exists_result = ch.query(f"EXISTS TABLE {full_table}")
        if not exists_result.result_rows or exists_result.result_rows[0][0] == 0:
            print(f"[SKIP] {schema}.{table} - Tabla no existe en ClickHouse")
            return (0, 0, 0)
    except Exception as e:
        print(f"[ERROR] {schema}.{table} - Error verificando tabla: {e}")
        return (0, 0, 0)
    
    # Obtener columnas de datos (excluir metadatos __$)
    data_columns = [col for col in changes[0].keys() if not col.startswith("__$")]
    
    # Obtener estructura de la tabla en ClickHouse
    try:
        desc_result = ch.query(f"DESCRIBE TABLE {full_table}")
        ch_columns = [row[0] for row in desc_result.result_rows]
    except Exception as e:
        print(f"[ERROR] {schema}.{table} - Error obteniendo estructura: {e}")
        return (0, 0, 0)
    
    inserts = 0
    updates = 0
    deletes = 0
    
    insert_buffer = []
    update_buffer = []
    delete_keys = []
    
    # Identificar columna clave primaria o incremental para updates/deletes
    # Por ahora asumimos que hay una columna 'Id' o similar
    key_column = None
    for col in ['Id', 'ID', 'id']:
        if col in data_columns and col in ch_columns:
            key_column = col
            break
    
    if not key_column and data_columns:
        # Usar la primera columna como fallback
        key_column = data_columns[0]
    
    for change in changes:
        operation = change.get('__$operation')
        
        # Extraer solo columnas de datos
        row_data = {col: normalize_py_value(change.get(col)) for col in data_columns}
        
        # Filtrar columnas que existen en ClickHouse
        row_data = {k: v for k, v in row_data.items() if k in ch_columns}
        
        # Convertir a lista en el orden de las columnas de ClickHouse
        row_list = [row_data.get(col) for col in ch_columns]
        
        if operation == 1:  # DELETE
            if key_column and key_column in row_data:
                delete_keys.append(row_data[key_column])
            deletes += 1
        elif operation == 2:  # INSERT
            insert_buffer.append(row_list)
            inserts += 1
        elif operation == 4:  # UPDATE (after) - solo procesamos el "after"
            update_buffer.append(row_list)
            updates += 1
        # operation == 3 es UPDATE (before), lo ignoramos
    
    # Aplicar cambios en batch
    try:
        # INSERTs
        if insert_buffer:
            for i in range(0, len(insert_buffer), INSERT_BATCH_ROWS):
                batch = insert_buffer[i:i + INSERT_BATCH_ROWS]
                ch.insert(full_table, batch, column_names=ch_columns)
        
        # UPDATEs - ClickHouse no tiene UPDATE nativo, usamos ALTER TABLE ... UPDATE
        # O mejor: usar ReplacingMergeTree y dejar que ClickHouse maneje los duplicados
        # Por ahora, insertamos los updates como nuevos registros (asumiendo ReplacingMergeTree)
        if update_buffer:
            for i in range(0, len(update_buffer), INSERT_BATCH_ROWS):
                batch = update_buffer[i:i + INSERT_BATCH_ROWS]
                ch.insert(full_table, batch, column_names=ch_columns)
        
        # DELETEs - ClickHouse no tiene DELETE nativo
        # Opciones:
        # 1. Usar ALTER TABLE ... DELETE (requiere versión reciente)
        # 2. Marcar registros como eliminados (soft delete)
        # 3. Usar ReplacingMergeTree con una columna de versión
        # Por ahora, solo logueamos los deletes
        if delete_keys:
            print(f"[WARN] {schema}.{table} - {len(delete_keys)} deletes detectados pero no aplicados (ClickHouse requiere estrategia especial)")
        
    except Exception as e:
        print(f"[ERROR] {schema}.{table} - Error aplicando cambios: {e}")
        return (0, 0, 0)
    
    return (inserts, updates, deletes)

def process_table_cdc(cursor, ch, dest_db: str, schema: str, table: str, last_lsn: Optional[bytes]) -> Tuple[int, int, int, Optional[bytes]]:
    """
    Procesa cambios CDC para una tabla específica
    
    Retorna: (inserts, updates, deletes, new_last_lsn)
    """
    capture_instance = get_cdc_capture_instance(cursor, schema, table)
    if not capture_instance:
        return (0, 0, 0, last_lsn)
    
    # Obtener cambios desde el último LSN procesado
    changes = get_cdc_changes(cursor, capture_instance, from_lsn=last_lsn)
    
    if not changes:
        return (0, 0, 0, last_lsn)
    
    # Obtener el último LSN procesado
    new_last_lsn = changes[-1].get('__$start_lsn')
    
    # Procesar cambios
    inserts, updates, deletes = process_cdc_changes(ch, dest_db, schema, table, changes, capture_instance)
    
    return (inserts, updates, deletes, new_last_lsn)

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
    
    # Verificar conexión SQL Server
    try:
        conn = sql_conn(source_db, use_prod)
        cursor = conn.cursor()
        
        # Verificar CDC habilitado
        if not check_cdc_enabled(cursor, source_db):
            print(f"[ERROR] CDC no está habilitado en la base de datos '{source_db}'")
            print("[INFO] Para habilitar CDC, ejecuta:")
            print(f"      EXEC sys.sp_cdc_enable_db")
            sys.exit(1)
        
        print(f"[OK] CDC habilitado en '{source_db}'")
        cursor.close()
        conn.close()
    except Exception as e:
        print(f"[ERROR] Error conectando a SQL Server: {e}")
        sys.exit(1)
    
    # Conectar a ClickHouse
    ch = ch_client()
    ensure_database(ch, dest_db)
    
    # Obtener tablas con CDC
    conn = sql_conn(source_db, use_prod)
    cursor = conn.cursor()
    
    tables = get_tables(cursor, requested_tables)
    
    if not tables:
        print("[ERROR] No se encontraron tablas con CDC habilitado")
        sys.exit(1)
    
    # Filtrar tablas que realmente tienen CDC
    cdc_tables = []
    for schema, table in tables:
        if check_table_cdc_enabled(cursor, schema, table):
            cdc_tables.append((schema, table))
        else:
            print(f"[SKIP] {schema}.{table} - CDC no habilitado para esta tabla")
    
    if not cdc_tables:
        print("[ERROR] No hay tablas con CDC habilitado para procesar")
        sys.exit(1)
    
    env_type = "PRODUCCIÓN" if use_prod else "DESARROLLO"
    server_info = SQL_SERVER_PROD if (use_prod and SQL_SERVER_PROD) else SQL_SERVER
    
    print(f"[START] CDC STREAMING SERVICE ({env_type})")
    print(f"  Server: {server_info}")
    print(f"  Source DB: {source_db}")
    print(f"  Dest DB: {dest_db}")
    print(f"  Tablas con CDC: {len(cdc_tables)}")
    print(f"  Poll Interval: {poll_interval} segundos")
    print(f"  Presiona Ctrl+C para detener el servicio\n")
    
    # Diccionario para mantener último LSN procesado por tabla
    last_lsns: Dict[Tuple[str, str], Optional[bytes]] = {
        (schema, table): None for schema, table in cdc_tables
    }
    
    total_inserts = 0
    total_updates = 0
    total_deletes = 0
    iteration = 0
    
    try:
        while running:
            iteration += 1
            cycle_start = time.time()
            
            print(f"\n[ITERATION {iteration}] {datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
            
            cycle_inserts = 0
            cycle_updates = 0
            cycle_deletes = 0
            
            for schema, table in cdc_tables:
                try:
                    last_lsn = last_lsns.get((schema, table))
                    inserts, updates, deletes, new_lsn = process_table_cdc(
                        cursor, ch, dest_db, schema, table, last_lsn
                    )
                    
                    if inserts > 0 or updates > 0 or deletes > 0:
                        print(f"  {schema}.{table}: +{inserts} inserts, ~{updates} updates, -{deletes} deletes")
                    
                    if new_lsn:
                        last_lsns[(schema, table)] = new_lsn
                    
                    cycle_inserts += inserts
                    cycle_updates += updates
                    cycle_deletes += deletes
                    
                except Exception as e:
                    print(f"[ERROR] {schema}.{table}: {e}")
            
            total_inserts += cycle_inserts
            total_updates += cycle_updates
            total_deletes += cycle_deletes
            
            cycle_time = time.time() - cycle_start
            if cycle_inserts > 0 or cycle_updates > 0 or cycle_deletes > 0:
                print(f"  Ciclo completado en {cycle_time:.2f}s | Total: +{total_inserts} ~{total_updates} -{total_deletes}")
            
            # Esperar antes del siguiente ciclo
            if running:
                time.sleep(poll_interval)
    
    except KeyboardInterrupt:
        print("\n[INFO] Interrupción recibida")
    finally:
        cursor.close()
        conn.close()
        
        elapsed = time.time() - start_time
        print(f"\n[STOP] Servicio detenido")
        print(f"  Iteraciones: {iteration}")
        print(f"  Total inserts: {total_inserts}")
        print(f"  Total updates: {total_updates}")
        print(f"  Total deletes: {total_deletes}")
        print(f"  Tiempo total: {elapsed:.2f} segundos")

if __name__ == "__main__":
    main()
