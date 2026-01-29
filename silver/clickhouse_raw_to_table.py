import os
import sys
import json
import time
import clickhouse_connect
from dotenv import load_dotenv

load_dotenv()

# =========================
# ENV CONFIG
# =========================
CH_HOST = os.getenv("CH_HOST", "localhost")
CH_PORT = int(os.getenv("CH_PORT", "8123"))
CH_USER = os.getenv("CH_USER", "default")
CH_PASSWORD = os.getenv("CH_PASSWORD", "")
CH_DATABASE = os.getenv("CH_DATABASE", "default")

RAW_TABLE = os.getenv("RAW_TABLE", "raw_sqlserver")
CHECKPOINTS_TABLE = os.getenv("RAW_TO_TABLE_CHECKPOINTS", "raw_to_table_checkpoints")


# =========================
# HELPERS
# =========================
def usage():
    print("Uso:")
    print("  python clickhouse_raw_to_table.py DEST_DB [TABLE] [LIMIT]")
    print("")
    print("Ejemplos:")
    print("  python clickhouse_raw_to_table.py POM_Aplicaciones")
    print("  python clickhouse_raw_to_table.py POM_Aplicaciones PG_TC")
    print("  python clickhouse_raw_to_table.py POM_Aplicaciones PG_TC 5000")
    sys.exit(1)

def parse_args():
    if len(sys.argv) < 2:
        usage()

    dest_db = sys.argv[1].strip()
    if not dest_db:
        raise Exception("DEST_DB vacío.")

    table = None
    limit_rows = 0

    if len(sys.argv) >= 3:
        t = sys.argv[2].strip()
        if t:
            table = t

    if len(sys.argv) >= 4:
        l = sys.argv[3].strip()
        if l:
            limit_rows = int(l)
            if limit_rows < 0:
                limit_rows = 0

    return dest_db, table, limit_rows

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

def safe_ident(name: str) -> str:
    name = name.replace("`", "``")
    return f"`{name}`"

def sql_string_literal(s: str) -> str:
    s = s.replace("\\", "\\\\").replace("'", "\\'")
    return f"'{s}'"

def ensure_database(ch, db_name: str):
    ch.command(f"CREATE DATABASE IF NOT EXISTS {safe_ident(db_name)}")

def ensure_checkpoints_table(ch):
    ch.command(f"""
    CREATE TABLE IF NOT EXISTS {CHECKPOINTS_TABLE}
    (
        dest_db String,
        source_table String,
        last_ingest_time DateTime,
        updated_at DateTime DEFAULT now()
    )
    ENGINE = ReplacingMergeTree(updated_at)
    ORDER BY (dest_db, source_table)
    """)

def get_checkpoint(ch, dest_db: str, source_table: str):
    q = f"""
    SELECT last_ingest_time
    FROM {CHECKPOINTS_TABLE}
    WHERE dest_db = %(dest)s
      AND source_table = %(tbl)s
    ORDER BY updated_at DESC
    LIMIT 1
    """
    r = ch.query(q, parameters={"dest": dest_db, "tbl": source_table})
    if r.result_rows:
        return r.result_rows[0][0]
    return None

def save_checkpoint(ch, dest_db: str, source_table: str, last_ingest_time):
    # ✅ NO insertamos updated_at, lo pone DEFAULT now()
    ch.insert(
        CHECKPOINTS_TABLE,
        [[dest_db, source_table, last_ingest_time]],
        column_names=["dest_db", "source_table", "last_ingest_time"]
    )

def list_source_tables_in_raw(ch, dest_db: str):
    q = f"""
    SELECT source_table
    FROM {RAW_TABLE}
    WHERE dest_db = %(dest)s
    GROUP BY source_table
    ORDER BY source_table
    """
    r = ch.query(q, parameters={"dest": dest_db})
    return [x[0] for x in r.result_rows]

def get_columns_order_from_raw(ch, dest_db: str, source_table: str):
    q = f"""
    SELECT columns_order_json
    FROM {RAW_TABLE}
    WHERE dest_db = %(dest)s
      AND source_table = %(tbl)s
      AND columns_order_json != ''
    ORDER BY ingest_time DESC
    LIMIT 1
    """
    r = ch.query(q, parameters={"dest": dest_db, "tbl": source_table})
    if not r.result_rows:
        return []

    raw = r.result_rows[0][0]
    try:
        cols = json.loads(raw)
        if isinstance(cols, list):
            cols = [c for c in cols if isinstance(c, str) and c.strip()]
            return cols
    except:
        pass

    return []

def get_existing_columns(ch, db_name: str, table_name: str):
    q = """
    SELECT name
    FROM system.columns
    WHERE database = %(db)s
      AND table = %(table)s
    ORDER BY position
    """
    r = ch.query(q, parameters={"db": db_name, "table": table_name})
    return [x[0] for x in r.result_rows]

def create_table_if_not_exists(ch, db_name: str, table_name: str, ordered_cols):
    full_name = f"{safe_ident(db_name)}.{safe_ident(table_name)}"
    cols_sql = ",\n        ".join([f"{safe_ident(c)} Nullable(String)" for c in ordered_cols])

    ch.command(f"""
    CREATE TABLE IF NOT EXISTS {full_name}
    (
        {cols_sql}
    )
    ENGINE = MergeTree
    ORDER BY tuple()
    """)

def add_columns_if_needed(ch, db_name: str, table_name: str, ordered_cols):
    existing_cols = get_existing_columns(ch, db_name, table_name)
    existing_set = set(existing_cols)

    new_cols = [c for c in ordered_cols if c not in existing_set]
    if not new_cols:
        return

    full_name = f"{safe_ident(db_name)}.{safe_ident(table_name)}"
    for c in new_cols:
        ch.command(f"ALTER TABLE {full_name} ADD COLUMN IF NOT EXISTS {safe_ident(c)} Nullable(String)")

def get_max_ingest_time_for_table(ch, dest_db: str, source_table: str):
    q = f"""
    SELECT max(ingest_time)
    FROM {RAW_TABLE}
    WHERE dest_db = %(dest)s
      AND source_table = %(tbl)s
    """
    r = ch.query(q, parameters={"dest": dest_db, "tbl": source_table})
    return r.result_rows[0][0]

def count_raw_new_rows(ch, dest_db: str, table_name: str, last_cp):
    where_cp = ""
    params = {"dest": dest_db, "tbl": table_name}
    if last_cp:
        where_cp = "AND ingest_time > %(last_ingest)s"
        params["last_ingest"] = last_cp

    q = f"""
    SELECT count(*)
    FROM {RAW_TABLE}
    WHERE dest_db = %(dest)s
      AND source_table = %(tbl)s
      {where_cp}
    """
    r = ch.query(q, parameters=params)
    return r.result_rows[0][0]

def copy_raw_to_table_incremental(ch, dest_db: str, table_name: str, limit_rows: int):
    ensure_database(ch, dest_db)
    ensure_checkpoints_table(ch)

    ordered_cols = get_columns_order_from_raw(ch, dest_db, table_name)
    if not ordered_cols:
        print(f"[SKIP] No hay columns_order_json en RAW para {dest_db}.{table_name}")
        return (0, 0)

    create_table_if_not_exists(ch, dest_db, table_name, ordered_cols)
    add_columns_if_needed(ch, dest_db, table_name, ordered_cols)

    cols = get_existing_columns(ch, dest_db, table_name)
    if not cols:
        print(f"[SKIP] {dest_db}.{table_name} sin columnas")
        return (0, 0)

    last_cp = get_checkpoint(ch, dest_db, table_name)
    new_rows = count_raw_new_rows(ch, dest_db, table_name, last_cp)

    if new_rows == 0:
        total = ch.query(f"SELECT count(*) FROM {safe_ident(dest_db)}.{safe_ident(table_name)}").result_rows[0][0]
        return (0, total)

    where_cp = ""
    params = {"dest": dest_db, "tbl": table_name}
    if last_cp:
        where_cp = "AND ingest_time > %(last_ingest)s"
        params["last_ingest"] = last_cp

    limit_clause = f"LIMIT {limit_rows}" if limit_rows and limit_rows > 0 else ""

    full_dest = f"{safe_ident(dest_db)}.{safe_ident(table_name)}"

    select_parts = []
    for c in cols:
        select_parts.append(f"JSONExtractString(raw_json, {sql_string_literal(c)}) AS {safe_ident(c)}")

    q = f"""
    INSERT INTO {full_dest} ({", ".join([safe_ident(c) for c in cols])})
    SELECT
      {", ".join(select_parts)}
    FROM {RAW_TABLE}
    WHERE dest_db = %(dest)s
      AND source_table = %(tbl)s
      {where_cp}
    ORDER BY ingest_time ASC
    {limit_clause}
    """

    ch.command(q, parameters=params)

    max_ing = get_max_ingest_time_for_table(ch, dest_db, table_name)
    if max_ing:
        save_checkpoint(ch, dest_db, table_name, max_ing)

    total = ch.query(f"SELECT count(*) FROM {full_dest}").result_rows[0][0]
    return (new_rows, total)


# =========================
# MAIN
# =========================
def main():
    start = time.time()
    dest_db, table, limit_rows = parse_args()
    ch = ch_client()

    if table is None:
        tables = list_source_tables_in_raw(ch, dest_db)
        if not tables:
            print(f"[ERROR] No hay tablas RAW para dest_db={dest_db}")
            return
    else:
        tables = [table]

    print(f"[START] RAW -> TABLES (incremental) | dest_db={dest_db} tables={len(tables)} limit={limit_rows}")

    ok = 0
    err = 0
    total_new = 0

    for t in tables:
        try:
            print(f"\n[INFO] Procesando: {dest_db}.{t}")
            new_rows, total_rows = copy_raw_to_table_incremental(ch, dest_db, t, limit_rows)
            ok += 1
            total_new += new_rows
            print(f"[OK] {dest_db}.{t} nuevos={new_rows} total={total_rows}")
        except Exception as e:
            err += 1
            print(f"[ERROR] {dest_db}.{t}: {e}")

    elapsed = time.time() - start

    print("\n" + "=" * 60)
    print("RESUMEN RAW -> TABLES (INCREMENTAL)")
    print("=" * 60)
    print(f"Tablas procesadas: {len(tables)}")
    print(f"Tablas OK: {ok}")
    print(f"Tablas con error: {err}")
    print(f"Total nuevos insertados: {total_new}")
    print(f"Tiempo de ejecución: {elapsed:.2f} segundos")
    print("=" * 60)

if __name__ == "__main__":
    main()
