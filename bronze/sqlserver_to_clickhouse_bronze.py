import os
import sys
import time
import datetime
import uuid
import pyodbc
import clickhouse_connect
from decimal import Decimal
from pathlib import Path
from dotenv import load_dotenv

# =========================
# LOCK multiplataforma
# =========================
try:
    import fcntl
    import errno
    HAS_FCNTL = True
except ImportError:
    HAS_FCNTL = False
    try:
        import msvcrt
        HAS_MSVCRT = True
    except ImportError:
        HAS_MSVCRT = False

# =========================
# LOAD .env
# =========================
script_dir = Path(__file__).resolve().parent
parent_dir = script_dir.parent.parent  # etl/
env_path = parent_dir / ".env"
if env_path.exists():
    load_dotenv(env_path, override=True)
else:
    load_dotenv(override=True)

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

STREAMING_CHUNK_SIZE = int(os.getenv("STREAMING_CHUNK_SIZE", "1000"))
DEBUG_RAW = os.getenv("DEBUG_RAW", "False").lower() == "true"

# Tracking ETL en default
ETL_META_DB = "default"
ETL_WATERMARKS_TABLE = "etl_watermarks"
ETL_RUNS_TABLE = "etl_runs"
ETL_RUN_TABLES_TABLE = "etl_run_tables"

# Lock dir
if sys.platform == "win32":
    LOCK_FILE_DIR = os.getenv("LOCK_FILE_DIR") or os.getenv("TEMP") or os.getenv("TMP") or os.path.expanduser("~")
else:
    LOCK_FILE_DIR = os.getenv("LOCK_FILE_DIR", "/tmp")
os.makedirs(LOCK_FILE_DIR, exist_ok=True)

# =========================
# LOCK BRONZE
# =========================
def get_bronze_lock_path(dest_db: str) -> str:
    return os.path.join(LOCK_FILE_DIR, f"bronze_{dest_db}.lock")

def acquire_bronze_lock(dest_db: str):
    lock_file_path = get_bronze_lock_path(dest_db)
    lock_file = open(lock_file_path, "w")
    lock_file.write(f"{os.getpid()}|{time.time()}")
    lock_file.flush()

    if HAS_FCNTL:
        try:
            fcntl.flock(lock_file.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        except IOError as e:
            if e.errno in (errno.EAGAIN, errno.EWOULDBLOCK):
                raise Exception(f"Ya hay BRONZE corriendo: {lock_file_path}")
            raise
    elif HAS_MSVCRT:
        try:
            msvcrt.locking(lock_file.fileno(), msvcrt.LK_NBLCK, 1)
        except IOError:
            raise Exception(f"Ya hay BRONZE corriendo: {lock_file_path}")

    print(f"[INFO] Lock BRONZE adquirido: {lock_file_path}")
    return lock_file

def release_bronze_lock(lock_file, dest_db: str):
    if not lock_file:
        return
    try:
        if HAS_FCNTL:
            fcntl.flock(lock_file.fileno(), fcntl.LOCK_UN)
        elif HAS_MSVCRT:
            msvcrt.locking(lock_file.fileno(), msvcrt.LK_UNLCK, 1)
        lock_file.close()

        lock_file_path = get_bronze_lock_path(dest_db)
        if os.path.exists(lock_file_path):
            os.remove(lock_file_path)

        print("[INFO] Lock BRONZE liberado")
    except:
        pass

# =========================
# HELPERS
# =========================
def now_utc():
    return datetime.datetime.now(datetime.UTC).replace(microsecond=0)

def usage():
    print("Uso:")
    print("  python bronze/sqlserver_to_clickhouse_bronze.py ORIG_DB DEST_DB [tablas] [limit] [reset] [--prod] [--mode full|incremental]")
    print("")
    print("Ejemplos:")
    print("  python bronze/sqlserver_to_clickhouse_bronze.py POM_PJ POM_PJ * 0 reset --mode full")
    print("  python bronze/sqlserver_to_clickhouse_bronze.py POM_PJ POM_PJ * 0 --mode incremental")
    sys.exit(1)

def parse_args():
    if len(sys.argv) < 3:
        usage()

    orig_db = sys.argv[1].strip()
    dest_db = sys.argv[2].strip()

    if not orig_db or not dest_db:
        raise Exception("ORIG_DB/DEST_DB inválidos.")

    tables_arg = "*"
    limit_arg = "0"
    reset_flag = False
    use_prod = False
    mode = "full"

    args_list = sys.argv[3:]

    if "--prod" in args_list:
        use_prod = True
        args_list = [a for a in args_list if a != "--prod"]

    if "--mode" in args_list:
        idx = args_list.index("--mode")
        if idx + 1 < len(args_list):
            mode = args_list[idx + 1].strip().lower()
        args_list = [a for i, a in enumerate(args_list) if i not in (idx, idx + 1)]

    if mode not in ("full", "incremental"):
        raise Exception("--mode debe ser full o incremental")

    if len(args_list) >= 1:
        tables_arg = args_list[0].strip() or "*"
    if len(args_list) >= 2:
        limit_arg = args_list[1].strip() or "0"
    if len(args_list) >= 3:
        reset_flag = (args_list[2].strip().lower() == "reset")

    row_limit = int(limit_arg)

    if tables_arg in ("*", "all", "ALL"):
        tables = None
    else:
        tables = [x.strip() for x in tables_arg.split(",") if x.strip()]

    return orig_db, dest_db, tables, row_limit, reset_flag, use_prod, mode

def build_sqlserver_conn_str(database_name: str, use_prod: bool = False):
    if use_prod and SQL_SERVER_PROD and SQL_USER_PROD and SQL_PASSWORD_PROD:
        server = SQL_SERVER_PROD
        user = SQL_USER_PROD
        password = SQL_PASSWORD_PROD
    else:
        server = SQL_SERVER
        user = SQL_USER
        password = SQL_PASSWORD

    if not server or not user or password is None:
        raise Exception("Faltan credenciales SQL Server en .env")

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

def sql_test_connection_and_db_access(target_db: str, use_prod: bool = False):
    c = sql_conn(target_db, use_prod)
    c.close()
    print(f"[OK] Acceso a SQL Server DB '{target_db}' confirmado.")

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

def get_primary_key_columns(cursor, schema, table):
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
    return [r[0] for r in cursor.fetchall()]

# =========================
# BRONZE TABLE NAME: igual que origen (sin dbo)
# =========================
def make_bronze_table_name(schema: str, table: str) -> str:
    return table

# =========================
# RAW SELECT columns: fechas a texto exacto
# =========================
def build_select_columns_raw(colnames, columns_meta):
    meta = {c[0]: c for c in columns_meta}
    out = []
    for c in colnames:
        dt = (meta[c][1] or "").lower()

        if dt in ("datetime", "datetime2", "smalldatetime"):
            out.append(f"CONVERT(varchar(19), [{c}], 120) AS [{c}]")
        elif dt == "date":
            out.append(f"CONVERT(varchar(10), [{c}], 120) AS [{c}]")
        elif dt == "time":
            out.append(f"CONVERT(varchar(16), [{c}], 114) AS [{c}]")
        elif dt in ("decimal", "numeric", "money", "smallmoney"):
            out.append(f"CONVERT(varchar(100), [{c}]) AS [{c}]")
        elif dt in ("binary", "varbinary", "image"):
            out.append(f"CONVERT(varchar(max), [{c}], 2) AS [{c}]")
        else:
            out.append(f"[{c}]")
    return ", ".join(out)

def detect_watermark_column(columns_meta):
    preferred = [
        "updatedat", "modifiedat", "lastupdated", "lastmodified",
        "fechaactualizacion", "f_actualizacion", "f_modificacion",
        "fechacreacion", "createdat", "createddate",
        "f_ingreso"
    ]

    cols_lower = {c[0].lower(): c[0] for c in columns_meta}
    for p in preferred:
        if p in cols_lower:
            return cols_lower[p]

    # fallback: primera datetime
    for col_name, data_type, *_ in columns_meta:
        dt = (data_type or "").lower()
        if dt in ("datetime", "datetime2", "smalldatetime", "date"):
            return col_name

    return None

# =========================
# Tracking ETL en default
# =========================
def ensure_tracking_tables(ch):
    # Watermarks
    ch.command(f"""
    CREATE TABLE IF NOT EXISTS `{ETL_META_DB}`.`{ETL_WATERMARKS_TABLE}`
    (
        `dest_db` String,
        `source_db` String,
        `source_schema` String,
        `source_table` String,
        `watermark_col` String,
        `watermark_value` Nullable(String),
        `updated_at` DateTime
    )
    ENGINE = MergeTree
    ORDER BY (dest_db, source_db, source_schema, source_table)
    """)

    # Runs
    ch.command(f"""
    CREATE TABLE IF NOT EXISTS `{ETL_META_DB}`.`{ETL_RUNS_TABLE}`
    (
        `run_id` String,
        `started_at` DateTime,
        `finished_at` Nullable(DateTime),
        `mode` String,
        `source_db` String,
        `dest_db` String,
        `status` String,
        `error` Nullable(String)
    )
    ENGINE = MergeTree
    ORDER BY (started_at, run_id)
    """)

    # Run tables
    ch.command(f"""
    CREATE TABLE IF NOT EXISTS `{ETL_META_DB}`.`{ETL_RUN_TABLES_TABLE}`
    (
        `run_id` String,
        `source_schema` String,
        `source_table` String,
        `dest_table` String,
        `mode` String,
        `watermark_col` Nullable(String),
        `watermark_before` Nullable(String),
        `watermark_after` Nullable(String),
        `rows_inserted` Int64,
        `rows_deleted` Int64,
        `status` String,
        `error` Nullable(String),
        `logged_at` DateTime
    )
    ENGINE = MergeTree
    ORDER BY (run_id, source_schema, source_table)
    """)

def log_run_start(ch, run_id, mode, source_db, dest_db):
    ch.insert(
        f"`{ETL_META_DB}`.`{ETL_RUNS_TABLE}`",
        [[run_id, now_utc(), None, mode, source_db, dest_db, "RUNNING", None]],
        column_names=["run_id", "started_at", "finished_at", "mode", "source_db", "dest_db", "status", "error"],
    )

def log_run_finish(ch, run_id, status, error=None):
    finished = now_utc()
    err = str(error) if error else None

    ch.command(f"""
    ALTER TABLE `{ETL_META_DB}`.`{ETL_RUNS_TABLE}`
    UPDATE finished_at = toDateTime('{finished.strftime("%Y-%m-%d %H:%M:%S")}'),
           status = '{status}',
           error = {("NULL" if err is None else "'" + err.replace("'", "''") + "'")}
    WHERE run_id = '{run_id}'
    """)

def get_current_watermark(ch, dest_db: str, source_db: str, schema: str, table: str):
    q = f"""
    SELECT watermark_col, watermark_value
    FROM `{ETL_META_DB}`.`{ETL_WATERMARKS_TABLE}`
    WHERE dest_db = %(dest_db)s
      AND source_db = %(db)s
      AND source_schema = %(schema)s
      AND source_table = %(table)s
    LIMIT 1
    """
    rows = ch.query(q, parameters={
        "dest_db": dest_db,
        "db": source_db,
        "schema": schema,
        "table": table
    }).result_rows

    if not rows:
        return None, None
    return rows[0][0], rows[0][1]

def upsert_watermark(ch, dest_db: str, source_db: str, schema: str, table: str, watermark_col: str, watermark_value: str):
    now = now_utc()

    ch.command(
        f"""
        ALTER TABLE `{ETL_META_DB}`.`{ETL_WATERMARKS_TABLE}`
        DELETE WHERE dest_db = '{dest_db}'
          AND source_db = '{source_db}'
          AND source_schema = '{schema}'
          AND source_table = '{table}'
        """
    )

    ch.insert(
        f"`{ETL_META_DB}`.`{ETL_WATERMARKS_TABLE}`",
        [[dest_db, source_db, schema, table, watermark_col, watermark_value, now]],
        column_names=["dest_db", "source_db", "source_schema", "source_table", "watermark_col", "watermark_value", "updated_at"],
    )

def log_table_run(ch, run_id, schema, table, dest_table, mode, wm_col, wm_before, wm_after, rows_inserted, rows_deleted, status, error=None):
    ch.insert(
        f"`{ETL_META_DB}`.`{ETL_RUN_TABLES_TABLE}`",
        [[
            run_id,
            schema,
            table,
            dest_table,
            mode,
            wm_col,
            wm_before,
            wm_after,
            int(rows_inserted),
            int(rows_deleted),
            status,
            str(error) if error else None,
            now_utc()
        ]],
        column_names=[
            "run_id", "source_schema", "source_table", "dest_table", "mode",
            "watermark_col", "watermark_before", "watermark_after",
            "rows_inserted", "rows_deleted", "status", "error", "logged_at"
        ]
    )

# =========================
# CREATE BRONZE TABLE (sin columnas extras)
# =========================
def create_or_reset_table_bronze(ch, dest_db, schema, table, columns_meta, reset_flag):
    ch_table = make_bronze_table_name(schema, table)

    if reset_flag:
        ch.command(f"DROP TABLE IF EXISTS `{dest_db}`.`{ch_table}`")

    cols_sql = []
    for col_name, data_type, prec, scale, is_nullable in columns_meta:
        cols_sql.append(f"`{col_name}` Nullable(String)")

    ddl = f"""
    CREATE TABLE IF NOT EXISTS `{dest_db}`.`{ch_table}`
    (
        {", ".join(cols_sql)}
    )
    ENGINE = MergeTree
    ORDER BY tuple()
    """
    ch.command(ddl)
    return ch_table

# =========================
# NORMALIZATION
# =========================
def normalize_raw_value(v):
    if v is None:
        return None
    if isinstance(v, (bytes, bytearray)):
        return v.hex()
    if isinstance(v, Decimal):
        return str(v)
    if isinstance(v, (datetime.datetime, datetime.date)):
        if isinstance(v, datetime.date) and not isinstance(v, datetime.datetime):
            return v.strftime("%Y-%m-%d")
        return v.strftime("%Y-%m-%d %H:%M:%S")
    if isinstance(v, datetime.time):
        return v.isoformat()
    if isinstance(v, str):
        return v
    return str(v)

# =========================
# FETCH (FULL / INCR)
# =========================
def fetch_rows_raw(sql_cursor, schema, table, colnames, columns_meta, row_limit, chunk_size, watermark_col=None, watermark_value=None):
    cols_select = build_select_columns_raw(colnames, columns_meta)
    top_clause = f"TOP ({row_limit}) " if row_limit and row_limit > 0 else ""

    if watermark_col and watermark_value is not None:
        meta = {c[0]: (c[1] or "").lower() for c in columns_meta}
        wm_type = meta.get(watermark_col, "")

        if wm_type in ("datetime", "datetime2", "smalldatetime"):
            where = f"CONVERT(varchar(19), [{watermark_col}], 120) > '{watermark_value}'"
        elif wm_type == "date":
            where = f"CONVERT(varchar(10), [{watermark_col}], 120) > '{watermark_value}'"
        else:
            where = f"[{watermark_col}] > '{watermark_value}'"

        query = f"SELECT {top_clause}{cols_select} FROM [{schema}].[{table}] WHERE {where}"
    else:
        query = f"SELECT {top_clause}{cols_select} FROM [{schema}].[{table}]"

    if DEBUG_RAW:
        print("[DEBUG_RAW] QUERY:", query)

    sql_cursor.execute(query)

    while True:
        rows = sql_cursor.fetchmany(chunk_size)
        if not rows:
            break

        out = []
        for r in rows:
            out.append([normalize_raw_value(x) for x in r])
        yield out

def compute_max_watermark_in_batch(batch, colnames, watermark_col):
    if not batch or not watermark_col or watermark_col not in colnames:
        return None
    idx = colnames.index(watermark_col)

    mx = None
    for row in batch:
        v = row[idx]
        if v is None:
            continue
        if mx is None or v > mx:
            mx = v
    return mx

# =========================
# UPSERT SUPPORT (DELETE + INSERT)
# =========================
def find_best_pk_column(cols_meta, pk_cols):
    """
    Preferencias:
    1) PK real
    2) columna Id / ID
    """
    if pk_cols:
        return pk_cols[0]

    for col_name, *_ in cols_meta:
        if col_name.lower() == "id":
            return col_name

    return None

def delete_existing_ids_in_clickhouse(ch, dest_db, ch_table, pk_col, ids):
    """
    Borra en ClickHouse los IDs que vamos a reinsertar (upsert).
    """
    if not ids:
        return 0

    # ClickHouse acepta ALTER TABLE ... DELETE WHERE pk IN (...)
    # ids vienen como string porque bronze es String, entonces los metemos como strings
    ids_safe = []
    for x in ids:
        if x is None:
            continue
        s = str(x).replace("'", "''")
        ids_safe.append(f"'{s}'")

    if not ids_safe:
        return 0

    where = f"`{pk_col}` IN ({', '.join(ids_safe)})"
    q = f"ALTER TABLE `{dest_db}`.`{ch_table}` DELETE WHERE {where}"
    ch.command(q)
    return len(ids_safe)

# =========================
# INGEST
# =========================
def ingest_table_bronze(sql_cursor, ch, dest_db, source_db, schema, table, row_limit, reset_flag, mode, run_id):
    cols_meta = get_columns(sql_cursor, schema, table)
    if not cols_meta:
        print(f"[SKIP] {schema}.{table} sin columnas")
        return (0, 0, "skipped")

    colnames = [c[0] for c in cols_meta]
    num_cols = len(colnames)

    # PK
    pk_cols = get_primary_key_columns(sql_cursor, schema, table)
    pk_col = find_best_pk_column(cols_meta, pk_cols)

    # watermark
    watermark_col = detect_watermark_column(cols_meta)

    # Creamos tabla
    ch_table = create_or_reset_table_bronze(ch, dest_db, schema, table, cols_meta, reset_flag)

    chunk_size = min(STREAMING_CHUNK_SIZE, 1000)
    if num_cols > 30:
        chunk_size = max(100, int(chunk_size * (30 / num_cols)))

    # FULL
    if mode == "full":
        inserted = 0
        rows_deleted = 0
        loaded_wm = None

        generator = fetch_rows_raw(
            sql_cursor=sql_cursor,
            schema=schema,
            table=table,
            colnames=colnames,
            columns_meta=cols_meta,
            row_limit=row_limit,
            chunk_size=chunk_size,
            watermark_col=None,
            watermark_value=None,
        )

        max_wm = None
        for chunk in generator:
            ch.insert(f"`{dest_db}`.`{ch_table}`", chunk, column_names=colnames)
            inserted += len(chunk)

            if watermark_col:
                bmax = compute_max_watermark_in_batch(chunk, colnames, watermark_col)
                if bmax and (max_wm is None or bmax > max_wm):
                    max_wm = bmax

        # guardar watermark full para que incremental funcione
        if watermark_col and inserted > 0 and max_wm:
            upsert_watermark(ch, dest_db, source_db, schema, table, watermark_col, max_wm)
            loaded_wm = max_wm

        log_table_run(
            ch=ch,
            run_id=run_id,
            schema=schema,
            table=table,
            dest_table=ch_table,
            mode="full",
            wm_col=watermark_col,
            wm_before=None,
            wm_after=loaded_wm,
            rows_inserted=inserted,
            rows_deleted=rows_deleted,
            status="OK",
            error=None
        )

        print(f"[OK] BRONZE {schema}.{table} -> {dest_db}.{ch_table} mode=full inserted={inserted}")
        return (inserted, rows_deleted, "ok")

    # =========================
    # INCREMENTAL REAL (UPDATES)
    # =========================
    if not watermark_col:
        # SIN watermark no se puede incremental real (updates) sin duplicar,
        # entonces SKIP para proteger consistencia.
        msg = f"{schema}.{table} sin watermark -> SKIP (para evitar duplicados)"
        print(f"[WARN] {msg}")

        log_table_run(
            ch=ch,
            run_id=run_id,
            schema=schema,
            table=table,
            dest_table=ch_table,
            mode="incremental",
            wm_col=None,
            wm_before=None,
            wm_after=None,
            rows_inserted=0,
            rows_deleted=0,
            status="SKIPPED",
            error=msg
        )
        return (0, 0, "skipped")

    if not pk_col:
        msg = f"{schema}.{table} sin PK/ID -> SKIP (no se puede upsert)"
        print(f"[WARN] {msg}")

        log_table_run(
            ch=ch,
            run_id=run_id,
            schema=schema,
            table=table,
            dest_table=ch_table,
            mode="incremental",
            wm_col=watermark_col,
            wm_before=None,
            wm_after=None,
            rows_inserted=0,
            rows_deleted=0,
            status="SKIPPED",
            error=msg
        )
        return (0, 0, "skipped")

    # watermark actual
    _, watermark_before = get_current_watermark(ch, dest_db, source_db, schema, table)
    if watermark_before is None:
        print(f"[INFO] {schema}.{table} incremental sin watermark previo -> nada que hacer (corre FULL primero)")
        log_table_run(
            ch=ch,
            run_id=run_id,
            schema=schema,
            table=table,
            dest_table=ch_table,
            mode="incremental",
            wm_col=watermark_col,
            wm_before=None,
            wm_after=None,
            rows_inserted=0,
            rows_deleted=0,
            status="SKIPPED",
            error="Sin watermark previo"
        )
        return (0, 0, "skipped")

    print(f"[INFO] {schema}.{table} incremental: {watermark_col} > {watermark_before} | PK={pk_col}")

    generator = fetch_rows_raw(
        sql_cursor=sql_cursor,
        schema=schema,
        table=table,
        colnames=colnames,
        columns_meta=cols_meta,
        row_limit=row_limit,
        chunk_size=chunk_size,
        watermark_col=watermark_col,
        watermark_value=watermark_before,
    )

    inserted = 0
    rows_deleted = 0
    max_wm = None

    pk_idx = colnames.index(pk_col)

    for chunk in generator:
        # upsert: borrar ids que vienen en el batch
        ids = [row[pk_idx] for row in chunk]
        rows_deleted += delete_existing_ids_in_clickhouse(ch, dest_db, ch_table, pk_col, ids)

        # insertar batch
        ch.insert(f"`{dest_db}`.`{ch_table}`", chunk, column_names=colnames)
        inserted += len(chunk)

        bmax = compute_max_watermark_in_batch(chunk, colnames, watermark_col)
        if bmax and (max_wm is None or bmax > max_wm):
            max_wm = bmax

    watermark_after = None
    if inserted > 0 and max_wm:
        watermark_after = max_wm
        upsert_watermark(ch, dest_db, source_db, schema, table, watermark_col, watermark_after)

    log_table_run(
        ch=ch,
        run_id=run_id,
        schema=schema,
        table=table,
        dest_table=ch_table,
        mode="incremental",
        wm_col=watermark_col,
        wm_before=watermark_before,
        wm_after=watermark_after,
        rows_inserted=inserted,
        rows_deleted=rows_deleted,
        status="OK",
        error=None
    )

    print(f"[OK] BRONZE {schema}.{table} -> {dest_db}.{ch_table} mode=incremental inserted={inserted} deleted={rows_deleted}")
    return (inserted, rows_deleted, "ok")

# =========================
# MAIN
# =========================
def main():
    start_time = time.time()
    source_db, dest_db, requested_tables, row_limit, reset_flag, use_prod, mode = parse_args()

    run_id = str(uuid.uuid4())

    bronze_lock = None
    try:
        bronze_lock = acquire_bronze_lock(dest_db)
    except Exception as e:
        print(f"[ERROR] No se pudo adquirir lock de BRONZE: {e}")
        sys.exit(1)

    ch = None
    try:
        sql_test_connection_and_db_access(source_db, use_prod)

        ch = ch_client()
        ensure_database(ch, dest_db)
        ensure_tracking_tables(ch)
        log_run_start(ch, run_id, mode, source_db, dest_db)

        conn = sql_conn(source_db, use_prod)
        cur = conn.cursor()

        tables = get_tables(cur, requested_tables)
        total_tables = len(tables)

        env_type = "PRODUCCIÓN" if use_prod else "DESARROLLO"
        server_info = SQL_SERVER_PROD if (use_prod and SQL_SERVER_PROD) else SQL_SERVER

        print(f"[START] BRONZE {mode.upper()} ({env_type}) | server={server_info} source_db={source_db} dest_db={dest_db} tables={total_tables} limit={row_limit} run_id={run_id}")
        print(f"[INFO] Tracking en: {ETL_META_DB}.{ETL_RUNS_TABLE}, {ETL_META_DB}.{ETL_RUN_TABLES_TABLE}, {ETL_META_DB}.{ETL_WATERMARKS_TABLE}")

        ok_count = 0
        error_count = 0
        skipped_count = 0
        total_inserted = 0
        total_deleted = 0

        for (schema, table) in tables:
            # =========================
            # SKIP TMP_ tables
            # =========================
            if table.upper().startswith("TMP_"):
                print(f"[SKIP] {schema}.{table} (TMP_)")
                skipped_count += 1
                continue

            try:
                inserted, deleted, status = ingest_table_bronze(
                    sql_cursor=cur,
                    ch=ch,
                    dest_db=dest_db,
                    source_db=source_db,
                    schema=schema,
                    table=table,
                    row_limit=row_limit,
                    reset_flag=reset_flag,
                    mode=mode,
                    run_id=run_id
                )
                total_inserted += inserted
                total_deleted += deleted
                if status == "ok":
                    ok_count += 1
                else:
                    skipped_count += 1
            except Exception as e:
                error_count += 1
                print(f"[ERROR] BRONZE {schema}.{table}: {e}")
                try:
                    log_table_run(
                        ch=ch,
                        run_id=run_id,
                        schema=schema,
                        table=table,
                        dest_table=make_bronze_table_name(schema, table),
                        mode=mode,
                        wm_col=None,
                        wm_before=None,
                        wm_after=None,
                        rows_inserted=0,
                        rows_deleted=0,
                        status="ERROR",
                        error=str(e)
                    )
                except:
                    pass

        cur.close()
        conn.close()

        elapsed = time.time() - start_time

        print("\n" + "=" * 60)
        print("RESUMEN BRONZE")
        print("=" * 60)
        print(f"Tablas procesadas: {total_tables}")
        print(f"Tablas OK: {ok_count}")
        print(f"Tablas con error: {error_count}")
        print(f"Tablas omitidas/skipped: {skipped_count}")
        print(f"Total filas insertadas: {total_inserted}")
        print(f"Total filas borradas (upsert): {total_deleted}")
        print(f"Tiempo de ejecución: {elapsed:.2f} segundos")
        print("=" * 60)

        log_run_finish(ch, run_id, status="OK", error=None)

    except Exception as e:
        print(f"[FATAL] {e}")
        if ch:
            try:
                log_run_finish(ch, run_id, status="ERROR", error=str(e))
            except:
                pass
        raise

    finally:
        release_bronze_lock(bronze_lock, dest_db)

if __name__ == "__main__":
    main()
