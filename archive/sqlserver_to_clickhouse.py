import os
import sys
import json
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

CH_HOST = os.getenv("CH_HOST", "localhost")
CH_PORT = int(os.getenv("CH_PORT", "8123"))
CH_USER = os.getenv("CH_USER", "default")
CH_PASSWORD = os.getenv("CH_PASSWORD", "")
CH_DATABASE = os.getenv("CH_DATABASE", "default")

# ⚠️ recomendado: 500 o 1000
STREAMING_CHUNK_SIZE = int(os.getenv("STREAMING_CHUNK_SIZE", "500"))
LOOKBACK_DAYS = int(os.getenv("LOOKBACK_DAYS", "7"))

RAW_TABLE = "raw_sqlserver"
CHECKPOINTS_TABLE = "etl_checkpoints"

DATE_CANDIDATES = [
    "updated_at", "modified_at", "last_modified", "last_update",
    "fecha_modificacion", "fecha_actualizacion", "updatedate", "modifydate",
    "timestamp", "ts", "updatedon", "modifiedon"
]

ID_CANDIDATES = [
    "id", "ID", "Id",
    "identity", "pk", "codigo", "cod"
]

# =========================
# HELPERS
# =========================
def now_utc():
    return datetime.datetime.now(datetime.UTC).replace(microsecond=0)

def usage():
    print("Uso:")
    print("  python sqlserver_to_clickhouse.py ORIG_DB DEST_DB [tablas] [limit]")
    print("")
    print("Ejemplos:")
    print("  python sqlserver_to_clickhouse.py POM_Aplicaciones POM_Aplicaciones")
    print("  python sqlserver_to_clickhouse.py POM_Aplicaciones POM_Aplicaciones dbo.PG_TC")
    print("  python sqlserver_to_clickhouse.py POM_Aplicaciones POM_Aplicaciones dbo.PG_TC 5")
    print("  python sqlserver_to_clickhouse.py POM_Aplicaciones RAW_POM * 0")
    sys.exit(1)

def parse_args():
    """
    Soporta:
    - ORIG_DB DEST_DB
    - ORIG_DB DEST_DB tablas
    - ORIG_DB DEST_DB tablas limit

    Defaults:
    - tablas = "*"
    - limit = 0
    """
    if len(sys.argv) < 3:
        usage()

    orig_db = sys.argv[1].strip()
    dest_db = sys.argv[2].strip()

    tables_arg = "*"
    limit_arg = "0"

    if len(sys.argv) >= 4:
        tables_arg = sys.argv[3].strip() or "*"

    if len(sys.argv) >= 5:
        limit_arg = sys.argv[4].strip() or "0"

    if not orig_db:
        raise Exception("ORIG_DB vacío.")
    if not dest_db:
        raise Exception("DEST_DB vacío.")

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

    return orig_db, dest_db, tables, row_limit

def build_sqlserver_conn_str(database_name: str):
    if not SQL_SERVER or not SQL_USER or SQL_PASSWORD is None:
        raise Exception("Faltan SQL_SERVER / SQL_USER / SQL_PASSWORD en el .env")

    return (
        f"DRIVER={{{SQL_DRIVER}}};"
        f"SERVER={SQL_SERVER};"
        f"DATABASE={database_name};"
        f"UID={SQL_USER};"
        f"PWD={SQL_PASSWORD};"
        f"TrustServerCertificate=yes;"
    )

def sql_conn(database_name: str):
    return pyodbc.connect(build_sqlserver_conn_str(database_name))

def sql_test_connection_and_db_access(target_db: str):
    try:
        c_master = sql_conn("master")
        cur = c_master.cursor()
        cur.execute("SELECT DB_NAME()")
        print("[OK] Login SQL Server correcto. Conectado a:", cur.fetchone()[0])

        cur.execute("SELECT name FROM sys.databases ORDER BY name")
        dbs = [r[0] for r in cur.fetchall()]
        print("[INFO] Bases visibles:", ", ".join(dbs[:50]) + (" ..." if len(dbs) > 50 else ""))

        cur.close()
        c_master.close()
    except Exception as e:
        raise Exception(f"No se pudo hacer login en SQL Server. Revisa usuario/clave/instancia. Detalle: {e}")

    try:
        c_target = sql_conn(target_db)
        c_target.close()
        print(f"[OK] Acceso a base '{target_db}' confirmado.")
    except Exception as e:
        raise Exception(
            f"Login OK pero NO tenés acceso a la base '{target_db}'. "
            f"Necesitás permisos o la BD no existe en esa instancia. Detalle: {e}"
        )

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

def ensure_clickhouse_tables(ch):
    # RAW base
    ch.command(f"""
    CREATE TABLE IF NOT EXISTS {RAW_TABLE}
    (
        ingest_time DateTime DEFAULT now(),
        source_server LowCardinality(String),
        source_db LowCardinality(String),
        dest_db LowCardinality(String),
        source_schema LowCardinality(String),
        source_table LowCardinality(String),

        extract_mode LowCardinality(String),
        cursor_column String,
        cursor_value String,

        pk_hint String,
        raw_json String
    )
    ENGINE = MergeTree
    PARTITION BY toYYYYMM(ingest_time)
    ORDER BY (dest_db, source_table, ingest_time)
    """)

    # Migración automática
    ch.command(f"""
    ALTER TABLE {RAW_TABLE}
    ADD COLUMN IF NOT EXISTS columns_order_json String AFTER pk_hint
    """)

    # Checkpoints
    ch.command(f"""
    CREATE TABLE IF NOT EXISTS {CHECKPOINTS_TABLE}
    (
        source_server String,
        source_db String,
        dest_db String,
        source_schema String,
        source_table String,

        extract_mode Enum8('full'=1,'id'=2,'date'=3),
        cursor_column String,
        last_value String,

        updated_at DateTime DEFAULT now()
    )
    ENGINE = ReplacingMergeTree(updated_at)
    ORDER BY (source_server, source_db, dest_db, source_schema, source_table)
    """)

def normalize_value_for_json(v):
    if v is None:
        return None

    if isinstance(v, (datetime.datetime, datetime.date)):
        return v.isoformat()

    # ✅ FIX: TIME
    if isinstance(v, datetime.time):
        return v.isoformat()

    if isinstance(v, Decimal):
        return str(v)

    if isinstance(v, (bytes, bytearray)):
        return v.hex()

    return v

def row_to_json_dict(colnames, row):
    d = {}
    for i, col in enumerate(colnames):
        d[col] = normalize_value_for_json(row[i])
    return d

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
    SELECT COLUMN_NAME, DATA_TYPE
    FROM INFORMATION_SCHEMA.COLUMNS
    WHERE TABLE_SCHEMA = ?
      AND TABLE_NAME = ?
    ORDER BY ORDINAL_POSITION
    """
    cursor.execute(q, (schema, table))
    return cursor.fetchall()

def get_primary_key_column(cursor, schema, table):
    q = """
    SELECT k.COLUMN_NAME
    FROM INFORMATION_SCHEMA.TABLE_CONSTRAINTS t
    JOIN INFORMATION_SCHEMA.KEY_COLUMN_USAGE k
      ON t.CONSTRAINT_NAME = k.CONSTRAINT_NAME
     AND t.TABLE_SCHEMA = k.TABLE_SCHEMA
    WHERE t.CONSTRAINT_TYPE = 'PRIMARY KEY'
      AND t.TABLE_SCHEMA = ?
      AND t.TABLE_NAME = ?
    ORDER BY k.ORDINAL_POSITION
    """
    cursor.execute(q, (schema, table))
    rows = cursor.fetchall()
    if len(rows) == 1:
        return rows[0][0]
    return None

def choose_mode(columns, pk_col):
    colnames = [c[0] for c in columns]

    for c in DATE_CANDIDATES:
        if c in colnames:
            return ("date", c)

    if pk_col and pk_col in colnames:
        return ("id", pk_col)

    for c in ID_CANDIDATES:
        if c in colnames:
            return ("id", c)

    return ("full", None)

def get_checkpoint(ch, source_server, source_db, dest_db, schema, table):
    q = f"""
    SELECT extract_mode, cursor_column, last_value
    FROM {CHECKPOINTS_TABLE}
    WHERE source_server = %(srv)s
      AND source_db = %(db)s
      AND dest_db = %(dest)s
      AND source_schema = %(schema)s
      AND source_table = %(table)s
    ORDER BY updated_at DESC
    LIMIT 1
    """
    r = ch.query(q, parameters={
        "srv": source_server,
        "db": source_db,
        "dest": dest_db,
        "schema": schema,
        "table": table
    })
    if r.result_rows:
        mode, col, lastv = r.result_rows[0]
        return str(mode), (col if col else None), lastv
    return None

def save_checkpoint(ch, source_server, source_db, dest_db, schema, table, mode, cursor_col, last_value):
    ch.insert(
        CHECKPOINTS_TABLE,
        [[source_server, source_db, dest_db, schema, table, mode, cursor_col or "", str(last_value), now_utc()]],
        column_names=[
            "source_server", "source_db", "dest_db",
            "source_schema", "source_table",
            "extract_mode", "cursor_column", "last_value", "updated_at"
        ]
    )

def is_memory_limit_error(exc: Exception) -> bool:
    msg = str(exc)
    return ("MEMORY_LIMIT_EXCEEDED" in msg) or ("Code: 241" in msg)

def safe_insert_clickhouse(ch, table_name: str, rows: list, column_names: list, base_batch: int):
    """
    Inserta en ClickHouse de forma segura en micro-batches.
    Si detecta MEMORY_LIMIT_EXCEEDED, reduce batch y reintenta.
    """
    if not rows:
        return

    batch = max(50, int(base_batch))

    i = 0
    while i < len(rows):
        chunk = rows[i:i+batch]
        try:
            ch.insert(table_name, chunk, column_names=column_names)
            i += len(chunk)
        except Exception as e:
            # retry con batch más pequeño si es memoria
            if is_memory_limit_error(e) and batch > 50:
                batch = max(50, batch // 2)
                print(f"[WARN] MEMORY_LIMIT_EXCEEDED -> bajando batch a {batch}")
                continue
            raise

def ingest_table(sql_cursor, ch, source_db, dest_db, schema, table, row_limit):
    cols_meta = get_columns(sql_cursor, schema, table)
    colnames = [c[0] for c in cols_meta]

    if not colnames:
        print(f"[SKIP] {schema}.{table} sin columnas")
        return (0, "skipped")

    columns_order_json = json.dumps(colnames, ensure_ascii=False)

    pk_col = get_primary_key_column(sql_cursor, schema, table)

    cp = get_checkpoint(ch, SQL_SERVER, source_db, dest_db, schema, table)
    if cp:
        mode, cursor_col, last_value = cp
    else:
        mode, cursor_col = choose_mode(cols_meta, pk_col)
        last_value = None

    if cursor_col and cursor_col not in colnames:
        mode, cursor_col = choose_mode(cols_meta, pk_col)
        last_value = None

    print(f"[INFO] {schema}.{table} mode={mode} cursor={cursor_col} last={last_value} limit={row_limit}")

    select_cols = ", ".join([f"[{c}]" for c in colnames])

    top_clause = ""
    if row_limit and row_limit > 0:
        top_clause = f"TOP ({row_limit}) "

    base_query = f"SELECT {top_clause}{select_cols} FROM [{schema}].[{table}]"

    where_clause = ""
    order_clause = ""
    params = []

    if mode in ["date", "id"] and cursor_col:
        if last_value:
            where_clause = f" WHERE [{cursor_col}] > ?"
            params = [last_value]
        else:
            if mode == "date":
                where_clause = f" WHERE [{cursor_col}] >= DATEADD(day, -{LOOKBACK_DAYS}, GETDATE())"
        order_clause = f" ORDER BY [{cursor_col}] ASC"

    query = base_query + where_clause + order_clause
    sql_cursor.execute(query, params)

    inserted = 0
    max_cursor_seen = last_value
    buffer = []

    base_batch = STREAMING_CHUNK_SIZE  # usar el env como batch inicial

    while True:
        rows = sql_cursor.fetchmany(STREAMING_CHUNK_SIZE)
        if not rows:
            break

        for r in rows:
            json_dict = row_to_json_dict(colnames, r)
            raw_json = json.dumps(json_dict, ensure_ascii=False)

            cursor_val = ""
            if cursor_col and cursor_col in json_dict:
                cursor_val = str(json_dict.get(cursor_col) or "")
                if cursor_val:
                    max_cursor_seen = cursor_val

            pk_hint = ""
            if pk_col and pk_col in json_dict:
                pk_hint = str(json_dict.get(pk_col) or "")

            buffer.append([
                SQL_SERVER,
                source_db,
                dest_db,
                schema,
                table,
                mode,
                cursor_col or "",
                cursor_val,
                pk_hint,
                columns_order_json,
                raw_json
            ])

        # ✅ INSERT seguro con microbatches
        safe_insert_clickhouse(
            ch=ch,
            table_name=RAW_TABLE,
            rows=buffer,
            column_names=[
                "source_server", "source_db", "dest_db",
                "source_schema", "source_table",
                "extract_mode", "cursor_column", "cursor_value",
                "pk_hint", "columns_order_json", "raw_json"
            ],
            base_batch=base_batch
        )

        inserted += len(buffer)
        buffer = []

    if mode in ["date", "id"] and cursor_col:
        if max_cursor_seen and max_cursor_seen != last_value:
            save_checkpoint(ch, SQL_SERVER, source_db, dest_db, schema, table, mode, cursor_col, max_cursor_seen)

    print(f"[OK] {schema}.{table} inserted={inserted}")
    return (inserted, "ok")

def main():
    start_time = time.time()

    source_db, dest_db, requested_tables, row_limit = parse_args()

    sql_test_connection_and_db_access(source_db)

    ch = ch_client()
    ensure_clickhouse_tables(ch)

    conn = sql_conn(source_db)
    cur = conn.cursor()

    tables = get_tables(cur, requested_tables)
    total_tables = len(tables)

    print(f"[START] server={SQL_SERVER} source_db={source_db} dest_db={dest_db} tables={total_tables}")
    print(f"[INFO] STREAMING_CHUNK_SIZE={STREAMING_CHUNK_SIZE} (recomendado 500/1000)")

    ok_count = 0
    error_count = 0
    skipped_count = 0
    total_inserted = 0

    for (schema, table) in tables:
        if table.upper().startswith("TMP_"):
            print(f"[SKIP] {schema}.{table} (TMP_)")
            skipped_count += 1
            continue

        try:
            inserted, status = ingest_table(cur, ch, source_db, dest_db, schema, table, row_limit)
            total_inserted += inserted
            if status == "ok":
                ok_count += 1
            elif status == "skipped":
                skipped_count += 1
        except Exception as e:
            print(f"[ERROR] {schema}.{table}: {e}")
            error_count += 1

    cur.close()
    conn.close()

    elapsed = time.time() - start_time

    print(f"\n[OK] Exportación completada: {ok_count} tablas exportadas")
    print(f" Datos cargados en: {dest_db}\n")

    print("=" * 60)
    print("RESUMEN DE EJECUCIÓN")
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
