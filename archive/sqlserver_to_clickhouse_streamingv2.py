#!/usr/bin/env python3
import os
import sys
import time
import datetime
import pyodbc
import clickhouse_connect
from decimal import Decimal
from dotenv import load_dotenv

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

# Subí esto. 20k es un buen default.
STREAMING_CHUNK_SIZE = int(os.getenv("STREAMING_CHUNK_SIZE", "20000"))

# Máximo de filas que vamos a acumular por insert (buffer).
INSERT_BATCH_ROWS = int(os.getenv("INSERT_BATCH_ROWS", "20000"))

# Pausa opcional entre tablas para dejar respirar merges/IO (segundos).
SLEEP_BETWEEN_TABLES = float(os.getenv("SLEEP_BETWEEN_TABLES", "0"))

# =========================
# HELPERS
# =========================
def usage():
    print("Uso:")
    print("  python sqlserver_to_clickhouse_streaming.py ORIG_DB DEST_DB [tablas] [limit] [--prod]")
    sys.exit(1)

def parse_args():
    if len(sys.argv) < 3:
        usage()

    orig_db = sys.argv[1].strip()
    dest_db = sys.argv[2].strip()

    tables_arg = "*"
    limit_arg = "0"
    use_prod = False

    args_list = sys.argv[3:]
    if "--prod" in args_list:
        use_prod = True
        args_list = [a for a in args_list if a != "--prod"]

    if len(args_list) >= 1:
        tables_arg = args_list[0].strip() or "*"
    if len(args_list) >= 2:
        limit_arg = args_list[1].strip() or "0"

    try:
        row_limit = int(limit_arg)
        if row_limit < 0:
            row_limit = 0
    except:
        raise Exception("El parámetro limit debe ser entero (usa 0 para sin límite).")

    if tables_arg == "*" or tables_arg.lower() == "all":
        tables = None
    else:
        tables = [x.strip() for x in tables_arg.split(",") if x.strip()]
        if not tables:
            raise Exception("Lista de tablas vacía.")

    return orig_db, dest_db, tables, row_limit, use_prod

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
    # fast_executemany no aplica a SELECT, pero lo dejamos fuera por claridad
    return pyodbc.connect(build_sqlserver_conn_str(database_name, use_prod))

def sql_test_connection_and_db_access(target_db: str, use_prod: bool = False):
    env_type = "PRODUCCIÓN" if use_prod else "DESARROLLO"
    try:
        c_master = sql_conn("master", use_prod)
        cur = c_master.cursor()
        cur.execute("SELECT DB_NAME()")
        server_name = cur.fetchone()[0]
        print(f"[OK] Login SQL Server ({env_type}) correcto. Conectado a: {server_name}")
        cur.close()
        c_master.close()
    except Exception as e:
        raise Exception(f"No se pudo hacer login en SQL Server ({env_type}). Detalle: {e}")

    try:
        c_target = sql_conn(target_db, use_prod)
        c_target.close()
        print(f"[OK] Acceso a base '{target_db}' confirmado.")
    except Exception as e:
        raise Exception(f"No tenés acceso a '{target_db}'. Detalle: {e}")

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

def detect_incremental_column(cursor, schema, table):
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
        return identity_cols[0][0], "id"

    q = """
    SELECT COLUMN_NAME, DATA_TYPE
    FROM INFORMATION_SCHEMA.COLUMNS
    WHERE TABLE_SCHEMA = ?
      AND TABLE_NAME = ?
      AND COLUMN_NAME IN ('Id', 'ID', 'id')
      AND DATA_TYPE IN ('int', 'bigint', 'smallint')
    ORDER BY CASE COLUMN_NAME WHEN 'Id' THEN 1 WHEN 'ID' THEN 2 ELSE 3 END
    """
    cursor.execute(q, (schema, table))
    id_cols = cursor.fetchall()
    if id_cols:
        return id_cols[0][0], "id"

    q = """
    SELECT COLUMN_NAME, DATA_TYPE
    FROM INFORMATION_SCHEMA.COLUMNS
    WHERE TABLE_SCHEMA = ?
      AND TABLE_NAME = ?
      AND DATA_TYPE IN ('int', 'bigint', 'smallint')
    ORDER BY ORDINAL_POSITION DESC
    """
    cursor.execute(q, (schema, table))
    int_cols = cursor.fetchall()
    if int_cols:
        return int_cols[0][0], "id"

    return None, None

def get_max_value_from_clickhouse(ch, dest_db, table, column):
    """
    Obtiene el valor máximo de la columna incremental desde ClickHouse.
    Retorna None si la tabla no existe, está vacía, o hay error.
    """
    try:
        full_table = f"`{dest_db}`.`{table}`"
        # Verificar que la tabla existe primero
        exists_result = ch.query(f"EXISTS TABLE {full_table}")
        if not exists_result.result_rows or exists_result.result_rows[0][0] == 0:
            return None
        
        # Verificar que la columna existe en la tabla
        try:
            desc_result = ch.query(f"DESCRIBE TABLE {full_table}")
            existing_columns = [row[0].lower() for row in desc_result.result_rows]
            if column.lower() not in existing_columns:
                print(f"  [WARN] Columna incremental '{column}' no existe en {full_table}. Columnas disponibles: {', '.join(existing_columns[:5])}...")
                return None
        except Exception as e:
            print(f"  [WARN] Error verificando columnas de {full_table}: {e}")
            # Continuar de todas formas, puede que funcione
        
        # Obtener el máximo valor
        query = f"SELECT max(`{column}`) FROM {full_table}"
        result = ch.query(query)
        
        if result.result_rows and len(result.result_rows) > 0:
            max_value = result.result_rows[0][0]
            # Si el valor es None, la tabla está vacía
            if max_value is not None:
                return max_value
        
        return None
    except Exception as e:
        # Log del error para debugging, pero retornar None para que se intente carga completa
        print(f"  [WARN] Error obteniendo max de {column} en {dest_db}.{table}: {e}")
        return None

def normalize_py_value(v):
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

def fetch_new_rows(sql_cursor, schema, table, colnames, incremental_col, last_value, chunk_size, row_limit):
    """
    Genera filas desde SQL Server en chunks (fetchmany), respetando row_limit si > 0.
    IMPORTANTE: Si hay incremental_col y last_value, solo trae registros con valor > last_value.
    """
    cols = ", ".join([f"[{c}]" for c in colnames])

    params = []
    where_clause = ""
    order_clause = ""
    top_clause = ""

    # Construir WHERE clause para incremental
    if incremental_col and last_value is not None:
        # CRÍTICO: Usar > (mayor que) para evitar duplicar el último registro
        # Si last_value es el máximo actual, no debería traer nada nuevo
        where_clause = f"WHERE [{incremental_col}] > ?"
        params.append(last_value)
        order_clause = f"ORDER BY [{incremental_col}] ASC"
    elif incremental_col:
        # Primera carga: traer todo pero ordenado
        order_clause = f"ORDER BY [{incremental_col}] ASC"

    # TOP clause solo si hay límite
    if row_limit and row_limit > 0:
        top_clause = f"TOP ({row_limit})"

    # Construir query final (limpiar espacios dobles)
    query_parts = ["SELECT"]
    if top_clause:
        query_parts.append(top_clause)
    query_parts.append(cols)
    query_parts.append(f"FROM [{schema}].[{table}]")
    if where_clause:
        query_parts.append(where_clause)
    if order_clause:
        query_parts.append(order_clause)
    
    query = " ".join(query_parts)
    
    # Log de la query para debugging (solo si hay WHERE clause para no saturar logs)
    if where_clause and last_value is not None:
        query_for_log = query.replace("?", str(last_value))
        print(f"  [DEBUG] Query incremental: {query_for_log}")
    
    # Ejecutar con parámetros
    if params:
        sql_cursor.execute(query, tuple(params))
    else:
        sql_cursor.execute(query)

    produced = 0
    while True:
        rows = sql_cursor.fetchmany(chunk_size)
        if not rows:
            break

        out = []
        for r in rows:
            out.append([normalize_py_value(x) for x in r])
        produced += len(out)
        yield out

        if row_limit and row_limit > 0 and produced >= row_limit:
            break

def stream_table(sql_cursor, ch, dest_db, schema, table, row_limit):
    cols_meta = get_columns(sql_cursor, schema, table)
    if not cols_meta:
        print(f"[SKIP] {schema}.{table} sin columnas")
        return (0, "skipped")

    colnames = [c[0] for c in cols_meta]
    num_cols = len(colnames)

    incremental_col, _ = detect_incremental_column(sql_cursor, schema, table)

    last_value = None
    if incremental_col:
        last_value = get_max_value_from_clickhouse(ch, dest_db, table, incremental_col)
        if last_value is not None:
            # Validar que last_value sea un tipo numérico válido
            try:
                # Asegurar que sea comparable (int o float)
                if isinstance(last_value, (int, float)):
                    print(f"[INFO] {schema}.{table} -> {dest_db}.{table} | cols={num_cols} | incremental={incremental_col} | desde={last_value} (solo_nuevos)")
                else:
                    # Si no es numérico, convertir a string para la comparación
                    print(f"[WARN] {schema}.{table} -> {dest_db}.{table} | incremental={incremental_col} | valor_max={last_value} (tipo={type(last_value).__name__})")
                    print(f"[INFO] {schema}.{table} -> {dest_db}.{table} | cols={num_cols} | incremental={incremental_col} | desde={last_value} (solo_nuevos)")
            except Exception as e:
                print(f"[WARN] {schema}.{table} -> Error validando last_value: {e}")
                last_value = None  # Forzar carga completa si hay error
        else:
            print(f"[INFO] {schema}.{table} -> {dest_db}.{table} | cols={num_cols} | incremental={incremental_col} | primera_carga (tabla_vacia_o_no_existe)")
    else:
        print(f"[INFO] {schema}.{table} -> {dest_db}.{table} | cols={num_cols} | sin_columna_incremental (carga_completa)")

    full_table = f"`{dest_db}`.`{table}`"
    try:
        result = ch.query(f"EXISTS TABLE {full_table}")
        if result.result_rows[0][0] == 0:
            print(f"[SKIP] {schema}.{table} - Tabla no existe en ClickHouse (usar sqlserver_to_clickhouse_silver.py primero)")
            return (0, "skipped")
    except Exception as e:
        print(f"[SKIP] {schema}.{table} - Error verificando tabla: {e}")
        return (0, "skipped")

    inserted = 0
    buffer = []

    # CHUNK size de lectura desde SQL: puede ser igual al batch o menor, pero NO lo mates por #cols
    fetch_chunk = STREAMING_CHUNK_SIZE

    # Validación: verificar cuántos registros hay actualmente en ClickHouse antes de insertar
    count_before = 0
    try:
        count_result = ch.query(f"SELECT COUNT(*) FROM {full_table}")
        if count_result.result_rows:
            count_before = count_result.result_rows[0][0] or 0
    except:
        count_before = 0

    try:
        for chunk in fetch_new_rows(sql_cursor, schema, table, colnames, incremental_col, last_value, fetch_chunk, row_limit):
            for row in chunk:
                buffer.append(row)
                if len(buffer) >= INSERT_BATCH_ROWS:
                    ch.insert(full_table, buffer, column_names=colnames)
                    inserted += len(buffer)
                    buffer.clear()

        if buffer:
            ch.insert(full_table, buffer, column_names=colnames)
            inserted += len(buffer)
            buffer.clear()

        # Validación: verificar cuántos registros hay después
        count_after = 0
        try:
            count_result = ch.query(f"SELECT COUNT(*) FROM {full_table}")
            if count_result.result_rows:
                count_after = count_result.result_rows[0][0] or 0
        except:
            count_after = count_before + inserted

        # Verificar que no haya duplicación excesiva
        expected_after = count_before + inserted
        if count_after > expected_after + (expected_after * 0.01):  # Permitir 1% de diferencia por posibles duplicados existentes
            print(f"[WARN] {schema}.{table} Posible duplicación detectada: esperado={expected_after}, actual={count_after}, diferencia={count_after - expected_after}")

        if inserted > 0:
            print(f"[OK] {schema}.{table} inserted={inserted} | registros_antes={count_before} | registros_despues={count_after}")
        else:
            print(f"[OK] {schema}.{table} sin_nuevos_registros | registros_actuales={count_before}")

        if SLEEP_BETWEEN_TABLES > 0:
            time.sleep(SLEEP_BETWEEN_TABLES)

        return (inserted, "ok")
    except Exception as e:
        print(f"[ERROR] {schema}.{table}: {e}")
        return (0, "error")

# =========================
# MAIN
# =========================
def main():
    start_time = time.time()
    source_db, dest_db, requested_tables, row_limit, use_prod = parse_args()

    sql_test_connection_and_db_access(source_db, use_prod)

    ch = ch_client()
    ensure_database(ch, dest_db)

    conn = sql_conn(source_db, use_prod)
    cur = conn.cursor()

    env_type = "PRODUCCIÓN" if use_prod else "DESARROLLO"
    server_info = SQL_SERVER_PROD if (use_prod and SQL_SERVER_PROD) else SQL_SERVER

    tables = get_tables(cur, requested_tables)
    total_tables = len(tables)

    print(f"[START] STREAMING INCREMENTAL ({env_type}) | server={server_info} source_db={source_db} dest_db={dest_db} tables={total_tables} limit={row_limit}")
    print(f"[INFO] STREAMING_CHUNK_SIZE={STREAMING_CHUNK_SIZE} INSERT_BATCH_ROWS={INSERT_BATCH_ROWS} SLEEP_BETWEEN_TABLES={SLEEP_BETWEEN_TABLES}")

    ok_count = 0
    error_count = 0
    skipped_count = 0
    total_inserted = 0

    for (schema, table) in tables:
        if table.upper().startswith("TMP_"):
            print(f"[SKIP] {schema}.{table} (TMP_)")
            skipped_count += 1
            continue

        inserted, status = stream_table(cur, ch, dest_db, schema, table, row_limit)
        total_inserted += inserted
        if status == "ok":
            ok_count += 1
        elif status == "skipped":
            skipped_count += 1
        else:
            error_count += 1

    cur.close()
    conn.close()

    elapsed = time.time() - start_time

    print(f"\n[OK] Streaming incremental completado: {ok_count} tablas OK")
    print(f" Datos cargados en: {dest_db}\n")
    print("=" * 60)
    print("RESUMEN STREAMING")
    print("=" * 60)
    print(f"Tablas procesadas: {total_tables}")
    print(f"Tablas OK: {ok_count}")
    print(f"Tablas con error: {error_count}")
    print(f"Tablas omitidas: {skipped_count}")
    print(f"Total filas insertadas: {total_inserted}")
    print(f"Tiempo de ejecución: {elapsed:.2f} segundos")
    print("=" * 60)

if __name__ == "__main__":
    main()
