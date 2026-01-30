import os
import sys
import time
import uuid
import datetime
from pathlib import Path
from dotenv import load_dotenv
import clickhouse_connect

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
# ENV CLICKHOUSE
# =========================
CH_HOST = os.getenv("CH_HOST", "localhost")
CH_PORT = int(os.getenv("CH_PORT", "8123"))
CH_USER = os.getenv("CH_USER", "default")
CH_PASSWORD = os.getenv("CH_PASSWORD", "")

STREAMING_CHUNK_SIZE = int(os.getenv("STREAMING_CHUNK_SIZE", "50000"))

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
# LOCK SILVER
# =========================
def get_silver_lock_path(dest_db: str) -> str:
    return os.path.join(LOCK_FILE_DIR, f"silver_{dest_db}.lock")

def acquire_silver_lock(dest_db: str):
    lock_file_path = get_silver_lock_path(dest_db)
    lock_file = open(lock_file_path, "w")
    lock_file.write(f"{os.getpid()}|{time.time()}")
    lock_file.flush()

    if HAS_FCNTL:
        try:
            fcntl.flock(lock_file.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        except IOError as e:
            if e.errno in (errno.EAGAIN, errno.EWOULDBLOCK):
                raise Exception(f"Ya hay SILVER corriendo: {lock_file_path}")
            raise
    elif HAS_MSVCRT:
        try:
            msvcrt.locking(lock_file.fileno(), msvcrt.LK_NBLCK, 1)
        except IOError:
            raise Exception(f"Ya hay SILVER corriendo: {lock_file_path}")

    print(f"[INFO] Lock SILVER adquirido: {lock_file_path}")
    return lock_file

def release_silver_lock(lock_file, dest_db: str):
    if not lock_file:
        return
    try:
        if HAS_FCNTL:
            fcntl.flock(lock_file.fileno(), fcntl.LOCK_UN)
        elif HAS_MSVCRT:
            msvcrt.locking(lock_file.fileno(), msvcrt.LK_UNLCK, 1)

        lock_file.close()

        lock_file_path = get_silver_lock_path(dest_db)
        if os.path.exists(lock_file_path):
            os.remove(lock_file_path)

        print("[INFO] Lock SILVER liberado")
    except:
        pass

# =========================
# HELPERS
# =========================
def now_utc():
    return datetime.datetime.now(datetime.UTC).replace(microsecond=0)

def usage():
    print("Uso:")
    print("  python silver/clickhouse_bronze_to_silver.py BRONZE_DB SILVER_DB [tablas] [reset] [--mode full|incremental]")
    print("")
    print("Ejemplos:")
    print("  python silver/clickhouse_bronze_to_silver.py POM_Aplicaciones POM_Aplicaciones_silver * reset --mode full")
    print("  python silver/clickhouse_bronze_to_silver.py POM_Aplicaciones POM_Aplicaciones_silver * --mode incremental")
    sys.exit(1)

def parse_args():
    if len(sys.argv) < 3:
        usage()

    bronze_db = sys.argv[1].strip()
    silver_db = sys.argv[2].strip()

    tables_arg = "*"
    reset_flag = False
    mode = "full"

    args_list = sys.argv[3:]

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
        reset_flag = (args_list[1].strip().lower() == "reset")

    if tables_arg in ("*", "all", "ALL"):
        tables = None
    else:
        tables = [x.strip() for x in tables_arg.split(",") if x.strip()]

    return bronze_db, silver_db, tables, reset_flag, mode

def ch_client(database="default"):
    secure = (CH_PORT == 8443)
    return clickhouse_connect.get_client(
        host=CH_HOST,
        port=CH_PORT,
        username=CH_USER,
        password=CH_PASSWORD,
        database=database,
        secure=secure,
        verify=False,
    )

def ensure_database(ch, db_name: str):
    ch.command(f"CREATE DATABASE IF NOT EXISTS `{db_name}`")

def list_tables(ch, db_name: str, requested_tables=None):
    if requested_tables is None:
        q = """
        SELECT name
        FROM system.tables
        WHERE database = %(db)s
          AND engine NOT IN ('View')
        ORDER BY name
        """
        rows = ch.query(q, parameters={"db": db_name}).result_rows
        return [r[0] for r in rows]

    q = """
    SELECT name
    FROM system.tables
    WHERE database = %(db)s
      AND name IN %(tables)s
    ORDER BY name
    """
    rows = ch.query(q, parameters={"db": db_name, "tables": tuple(requested_tables)}).result_rows
    return [r[0] for r in rows]

def get_table_columns(ch, db_name: str, table: str):
    q = """
    SELECT name, type
    FROM system.columns
    WHERE database = %(db)s AND table = %(table)s
    ORDER BY position
    """
    rows = ch.query(q, parameters={"db": db_name, "table": table}).result_rows
    return rows

# =========================
# DETECT PK / WATERMARK
# =========================
def detect_pk_column(colnames):
    cols_lower = {c.lower(): c for c in colnames}
    if "id" in cols_lower:
        return cols_lower["id"]

    for c in colnames:
        if c.lower().endswith("_id"):
            return c

    return None

def detect_watermark_column(colnames):
    preferred = [
        "updatedat", "modifiedat", "lastupdated", "lastmodified",
        "fechaactualizacion", "f_actualizacion", "f_modificacion",
        "fechacreacion", "createdat", "createddate",
        "f_ingreso"
    ]
    cols_lower = {c.lower(): c for c in colnames}
    for p in preferred:
        if p in cols_lower:
            return cols_lower[p]
    return None

# =========================
# SILVER TYPE RULES
# =========================
def guess_silver_type(col_name: str):
    c = col_name.lower()

    if c in ("id",) or c.endswith("_id"):
        return "Nullable(String)"  # si tus IDs son numéricos, podés cambiarlo a Int64

    if c in ("createdat", "updatedat", "modifiedat") or "fecha" in c or "date" in c:
        return "Nullable(DateTime)"

    if "amount" in c or "monto" in c or "total" in c or "precio" in c:
        return "Nullable(Decimal(18,4))"

    if c.startswith("is_") or c.startswith("has_") or c in ("activo", "habilitado", "enabled"):
        return "Nullable(UInt8)"

    if c.startswith("cnt_") or c.startswith("count") or c.endswith("_count"):
        return "Nullable(Int64)"

    return "Nullable(String)"

def silver_cast_expr(col_name: str, bronze_type: str, target_type: str):
    """
    FIX COMPLETO:
    - DateTime: si ya viene Date/DateTime/DateTime64, no parsear
    - UInt8: si ya viene UInt8, no usar lowerUTF8
    - Decimal: si ya viene Decimal, no usar toDecimal64OrNull (solo acepta String)
    """
    col = f"`{col_name}`"
    bronze_type_l = (bronze_type or "").lower()

    # =========================
    # DateTime
    # =========================
    if target_type == "Nullable(DateTime)":
        if "datetime" in bronze_type_l or bronze_type_l.startswith("date"):
            return f"{col} AS `{col_name}`"
        return f"parseDateTimeBestEffortOrNull(NULLIF(toString({col}), '')) AS `{col_name}`"

    # =========================
    # Decimal
    # =========================
    if target_type.startswith("Nullable(Decimal"):
        # si bronze ya es Decimal
        if "decimal" in bronze_type_l:
            return f"toDecimal64({col}, 4) AS `{col_name}`"

        # si bronze es numérico (float/int)
        if "float" in bronze_type_l or "int" in bronze_type_l or "uint" in bronze_type_l:
            return f"toDecimal64(toFloat64({col}), 4) AS `{col_name}`"

        # si bronze es string
        return f"toDecimal64OrNull(NULLIF(toString({col}), ''), 4) AS `{col_name}`"

    # =========================
    # Int64
    # =========================
    if target_type == "Nullable(Int64)":
        if "int" in bronze_type_l or "uint" in bronze_type_l:
            return f"toInt64OrNull({col}) AS `{col_name}`"
        return f"toInt64OrNull(NULLIF(toString({col}), '')) AS `{col_name}`"

    # =========================
    # Bool UInt8
    # =========================
    if target_type == "Nullable(UInt8)":
        if "uint8" in bronze_type_l:
            return f"{col} AS `{col_name}`"

        if "int" in bronze_type_l or "uint" in bronze_type_l:
            return f"toUInt8OrNull({col}) AS `{col_name}`"

        return f"""
        multiIf(
            lowerUTF8(NULLIF(toString({col}), '')) IN ('1','true','t','si','sí','yes','y'), toUInt8(1),
            lowerUTF8(NULLIF(toString({col}), '')) IN ('0','false','f','no','n'), toUInt8(0),
            NULL
        ) AS `{col_name}`
        """.strip()

    # =========================
    # Default string
    # =========================
    if "string" in bronze_type_l:
        return f"NULLIF(toString({col}), '') AS `{col_name}`"

    return f"toString({col}) AS `{col_name}`"

# =========================
# TRACKING TABLES
# =========================
def ensure_tracking_tables(ch):
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

def get_current_watermark(ch, dest_db: str, source_db: str, table: str):
    q = f"""
    SELECT watermark_col, watermark_value
    FROM `{ETL_META_DB}`.`{ETL_WATERMARKS_TABLE}`
    WHERE dest_db = %(dest_db)s
      AND source_db = %(db)s
      AND source_schema = 'bronze'
      AND source_table = %(table)s
    LIMIT 1
    """
    rows = ch.query(q, parameters={
        "dest_db": dest_db,
        "db": source_db,
        "table": table
    }).result_rows

    if not rows:
        return None, None
    return rows[0][0], rows[0][1]

def upsert_watermark(ch, dest_db: str, source_db: str, table: str, watermark_col: str, watermark_value: str):
    now = now_utc()

    ch.command(
        f"""
        ALTER TABLE `{ETL_META_DB}`.`{ETL_WATERMARKS_TABLE}`
        DELETE WHERE dest_db = '{dest_db}'
          AND source_db = '{source_db}'
          AND source_schema = 'bronze'
          AND source_table = '{table}'
        """
    )

    ch.insert(
        f"`{ETL_META_DB}`.`{ETL_WATERMARKS_TABLE}`",
        [[dest_db, source_db, "bronze", table, watermark_col, watermark_value, now]],
        column_names=["dest_db", "source_db", "source_schema", "source_table", "watermark_col", "watermark_value", "updated_at"],
    )

def log_table_run(ch, run_id, table, mode, wm_col, wm_before, wm_after, rows_inserted, rows_deleted, status, error=None):
    ch.insert(
        f"`{ETL_META_DB}`.`{ETL_RUN_TABLES_TABLE}`",
        [[
            run_id,
            "bronze",
            table,
            table,
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
# CREATE SILVER TABLE
# =========================
def create_or_reset_table_silver(ch, silver_db, table, bronze_cols, reset_flag):
    if reset_flag:
        ch.command(f"DROP TABLE IF EXISTS `{silver_db}`.`{table}`")

    cols_ddl = []
    for col_name, _bronze_type in bronze_cols:
        target_type = guess_silver_type(col_name)
        cols_ddl.append(f"`{col_name}` {target_type}")

    ddl = f"""
    CREATE TABLE IF NOT EXISTS `{silver_db}`.`{table}`
    (
        {", ".join(cols_ddl)}
    )
    ENGINE = MergeTree
    ORDER BY tuple()
    """
    ch.command(ddl)

# =========================
# UPSERT SUPPORT
# =========================
def delete_existing_ids(ch, silver_db, table, pk_col, ids):
    if not ids:
        return 0

    ids_safe = []
    for x in ids:
        if x is None:
            continue
        s = str(x).replace("'", "''")
        ids_safe.append(f"'{s}'")

    if not ids_safe:
        return 0

    q = f"ALTER TABLE `{silver_db}`.`{table}` DELETE WHERE `{pk_col}` IN ({', '.join(ids_safe)})"
    ch.command(q)
    return len(ids_safe)

# =========================
# FULL LOAD
# =========================
def full_load_table(ch, bronze_db, silver_db, table, bronze_cols, reset_flag, run_id):
    colnames = [c[0] for c in bronze_cols]
    wm_col = detect_watermark_column(colnames)

    create_or_reset_table_silver(ch, silver_db, table, bronze_cols, reset_flag)

    ch.command(f"TRUNCATE TABLE `{silver_db}`.`{table}`")

    select_exprs = []
    for col_name, bronze_type in bronze_cols:
        t = guess_silver_type(col_name)
        select_exprs.append(silver_cast_expr(col_name, bronze_type, t))

    q = f"""
    INSERT INTO `{silver_db}`.`{table}`
    SELECT {", ".join(select_exprs)}
    FROM `{bronze_db}`.`{table}`
    """
    ch.command(q)

    watermark_after = None
    if wm_col:
        wm_q = f"SELECT max(`{wm_col}`) FROM `{silver_db}`.`{table}`"
        max_val = ch.query(wm_q).result_rows[0][0]
        if max_val is not None:
            watermark_after = str(max_val)

    if wm_col and watermark_after:
        upsert_watermark(ch, silver_db, bronze_db, table, wm_col, watermark_after)

    log_table_run(
        ch=ch,
        run_id=run_id,
        table=table,
        mode="full",
        wm_col=wm_col,
        wm_before=None,
        wm_after=watermark_after,
        rows_inserted=0,
        rows_deleted=0,
        status="OK",
        error=None
    )

    print(f"[OK] SILVER FULL {bronze_db}.{table} -> {silver_db}.{table}")

# =========================
# INCREMENTAL REAL
# =========================
def incremental_load_table(ch, bronze_db, silver_db, table, bronze_cols, reset_flag, run_id):
    colnames = [c[0] for c in bronze_cols]
    pk_col = detect_pk_column(colnames)
    wm_col = detect_watermark_column(colnames)

    create_or_reset_table_silver(ch, silver_db, table, bronze_cols, reset_flag)

    if not wm_col:
        msg = f"{table} sin watermark -> SKIP incremental (evitar duplicados)"
        print(f"[WARN] {msg}")
        log_table_run(ch, run_id, table, "incremental", None, None, None, 0, 0, "SKIPPED", msg)
        return

    if not pk_col:
        msg = f"{table} sin PK -> SKIP incremental (no se puede upsert)"
        print(f"[WARN] {msg}")
        log_table_run(ch, run_id, table, "incremental", wm_col, None, None, 0, 0, "SKIPPED", msg)
        return

    _, wm_before = get_current_watermark(ch, silver_db, bronze_db, table)
    if wm_before is None:
        msg = f"{table} incremental sin watermark previo -> corre FULL primero"
        print(f"[INFO] {msg}")
        log_table_run(ch, run_id, table, "incremental", wm_col, None, None, 0, 0, "SKIPPED", msg)
        return

    select_exprs = []
    for col_name, bronze_type in bronze_cols:
        t = guess_silver_type(col_name)
        select_exprs.append(silver_cast_expr(col_name, bronze_type, t))

    ids_q = f"""
    SELECT `{pk_col}`
    FROM `{bronze_db}`.`{table}`
    WHERE `{wm_col}` > %(wm)s
    """
    ids_rows = ch.query(ids_q, parameters={"wm": wm_before}).result_rows
    ids = [r[0] for r in ids_rows]

    rows_deleted = delete_existing_ids(ch, silver_db, table, pk_col, ids)

    insert_q = f"""
    INSERT INTO `{silver_db}`.`{table}`
    SELECT {", ".join(select_exprs)}
    FROM `{bronze_db}`.`{table}`
    WHERE `{wm_col}` > %(wm)s
    """
    ch.command(insert_q, parameters={"wm": wm_before})

    wm_after = None
    max_q = f"SELECT max(`{wm_col}`) FROM `{silver_db}`.`{table}`"
    max_val = ch.query(max_q).result_rows[0][0]
    if max_val is not None:
        wm_after = str(max_val)

    if wm_after and wm_after != wm_before:
        upsert_watermark(ch, silver_db, bronze_db, table, wm_col, wm_after)

    log_table_run(
        ch=ch,
        run_id=run_id,
        table=table,
        mode="incremental",
        wm_col=wm_col,
        wm_before=wm_before,
        wm_after=wm_after,
        rows_inserted=len(ids),
        rows_deleted=rows_deleted,
        status="OK",
        error=None
    )

    print(f"[OK] SILVER INCR {bronze_db}.{table} -> {silver_db}.{table} wm={wm_col} > {wm_before} upsert_ids={len(ids)}")

# =========================
# MAIN
# =========================
def main():
    start_time = time.time()
    bronze_db, silver_db, requested_tables, reset_flag, mode = parse_args()

    run_id = str(uuid.uuid4())

    silver_lock = None
    try:
        silver_lock = acquire_silver_lock(silver_db)
    except Exception as e:
        print(f"[ERROR] No se pudo adquirir lock SILVER: {e}")
        sys.exit(1)

    ch = None
    try:
        ch = ch_client()
        ensure_database(ch, silver_db)
        ensure_tracking_tables(ch)
        log_run_start(ch, run_id, mode, bronze_db, silver_db)

        tables = list_tables(ch, bronze_db, requested_tables)
        print(f"[START] SILVER {mode.upper()} | bronze_db={bronze_db} -> silver_db={silver_db} tables={len(tables)} run_id={run_id}")

        ok = 0
        err = 0
        skip = 0

        for table in tables:
            try:
                bronze_cols = get_table_columns(ch, bronze_db, table)
                if not bronze_cols:
                    print(f"[SKIP] {table} sin columnas")
                    skip += 1
                    continue

                if mode == "full":
                    full_load_table(ch, bronze_db, silver_db, table, bronze_cols, reset_flag, run_id)
                else:
                    incremental_load_table(ch, bronze_db, silver_db, table, bronze_cols, reset_flag, run_id)

                ok += 1

            except Exception as e:
                err += 1
                print(f"[ERROR] {table}: {e}")
                try:
                    log_table_run(ch, run_id, table, mode, None, None, None, 0, 0, "ERROR", str(e))
                except:
                    pass

        elapsed = time.time() - start_time

        print("\n" + "=" * 60)
        print("RESUMEN SILVER")
        print("=" * 60)
        print(f"Tablas OK: {ok}")
        print(f"Tablas con error: {err}")
        print(f"Tablas omitidas: {skip}")
        print(f"Tiempo: {elapsed:.2f}s")
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
        release_silver_lock(silver_lock, silver_db)

if __name__ == "__main__":
    main()
