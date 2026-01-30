import os
import sys
import json
import pyodbc
import clickhouse_connect
from pathlib import Path
from dotenv import load_dotenv
import datetime
from decimal import Decimal

# =========================
# LOAD .env
# =========================
script_dir = Path(__file__).resolve().parent
parent_dir = script_dir.parent  # etl/
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

CH_HOST = os.getenv("CH_HOST", "localhost")
CH_PORT = int(os.getenv("CH_PORT", "8123"))
CH_USER = os.getenv("CH_USER", "default")
CH_PASSWORD = os.getenv("CH_PASSWORD", "")
CH_DATABASE = os.getenv("CH_DATABASE", "default")

# =========================
# HELPERS
# =========================
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

def normalize_json_value(v):
    if v is None:
        return None
    if isinstance(v, (datetime.datetime, datetime.date)):
        return v.isoformat()
    if isinstance(v, Decimal):
        return float(v)
    if isinstance(v, bytes):
        return v.hex()
    return v

def row_to_dict(columns, row):
    return {columns[i]: normalize_json_value(row[i]) for i in range(len(columns))}

# =========================
# VERIFY SQL SERVER ROW
# =========================
def get_columns_meta(cursor, schema, table):
    """Obtiene metadatos de columnas desde SQL Server"""
    q = """
    SELECT COLUMN_NAME, DATA_TYPE, NUMERIC_PRECISION, NUMERIC_SCALE, IS_NULLABLE
    FROM INFORMATION_SCHEMA.COLUMNS
    WHERE TABLE_SCHEMA = ?
      AND TABLE_NAME = ?
    ORDER BY ORDINAL_POSITION
    """
    cursor.execute(q, (schema, table))
    return cursor.fetchall()

def build_select_columns_with_date_conversion(colnames, columns_meta):
    """
    Construye la lista de columnas SELECT convirtiendo fechas a texto exacto.
    Para datetime/datetime2/smalldatetime: CONVERT(varchar(19), <col>, 120)
    Para date: CONVERT(varchar(10), <col>, 120)
    """
    meta_dict = {col[0]: col for col in columns_meta}
    select_cols = []
    for colname in colnames:
        if colname in meta_dict:
            data_type = meta_dict[colname][1].lower()
            if data_type in ('datetime', 'datetime2', 'smalldatetime'):
                select_cols.append(f"CONVERT(varchar(19), [{colname}], 120) AS [{colname}]")
            elif data_type == 'date':
                select_cols.append(f"CONVERT(varchar(10), [{colname}], 120) AS [{colname}]")
            else:
                select_cols.append(f"[{colname}]")
        else:
            select_cols.append(f"[{colname}]")
    return ", ".join(select_cols)

def verify_sqlserver_row(db_name: str, schema: str, table: str, id_value: int, id_col="ID"):
    print("=" * 80)
    print("[SQL SERVER] Verificando extracción del registro específico")
    print("=" * 80)
    print(f"DB: {db_name}")
    print(f"Tabla: {schema}.{table}")
    print(f"Filtro: {id_col} = {id_value}")
    print("-" * 80)

    conn = sql_conn(db_name)
    cur = conn.cursor()

    # Obtener metadatos y construir SELECT con conversión de fechas
    columns_meta = get_columns_meta(cur, schema, table)
    colnames = [col[0] for col in columns_meta]
    cols = build_select_columns_with_date_conversion(colnames, columns_meta)
    
    query = f"SELECT {cols} FROM [{schema}].[{table}] WHERE [{id_col}] = ?"
    cur.execute(query, (id_value,))

    row = cur.fetchone()
    if not row:
        print("[WARN] No se encontró el registro en SQL Server.")
        cur.close()
        conn.close()
        return None

    columns = [d[0] for d in cur.description]
    data = row_to_dict(columns, row)

    print("[OK] Registro encontrado en SQL Server ✅")
    print(json.dumps(data, indent=2, ensure_ascii=False))

    cur.close()
    conn.close()
    return data

# =========================
# VERIFY CLICKHOUSE ROW (SILVER)
# =========================
def verify_clickhouse_row(dest_db: str, table: str, id_value: int, id_col="ID"):
    print("\n" + "=" * 80)
    print("[CLICKHOUSE] Verificando si existe el mismo registro en SILVER")
    print("=" * 80)
    print(f"DB: {dest_db}")
    print(f"Tabla: {table}")
    print(f"Filtro: {id_col} = {id_value}")
    print("-" * 80)

    ch = ch_client()

    # OJO: aquí tu silver usa dest_db.table (sin schema)
    query = f"SELECT * FROM `{dest_db}`.`{table}` WHERE `{id_col}` = %(id)s LIMIT 10"
    rows = ch.query(query, parameters={"id": id_value}).result_rows
    cols = ch.query(f"DESCRIBE TABLE `{dest_db}`.`{table}`").result_rows
    colnames = [c[0] for c in cols]

    if not rows:
        print("[WARN] No se encontró el registro en ClickHouse Silver.")
        return None

    # Si salen múltiples filas, las mostramos todas
    out = []
    for r in rows:
        out.append({colnames[i]: normalize_json_value(r[i]) for i in range(len(colnames))})

    print(f"[OK] Encontrado en ClickHouse Silver ✅ ({len(out)} fila(s))")
    print(json.dumps(out, indent=2, ensure_ascii=False))
    return out

# =========================
# MAIN
# =========================
def main():
    # TU CASO ESPECÍFICO (puede parametrizarse si querés)
    ORIG_DB = "POM_Aplicaciones"
    DEST_DB = "POM_Aplicaciones"

    SCHEMA = "dbo"
    TABLE = "PC_Gestiones"
    ID_COL = "ID"
    ID_VALUE = 492237

    sql_data = verify_sqlserver_row(ORIG_DB, SCHEMA, TABLE, ID_VALUE, ID_COL)

    # Si querés también validar que ya está en Silver (ClickHouse)
    # Podés comentar esto si solo querés ver SQL
    ch_data = verify_clickhouse_row(DEST_DB, TABLE, ID_VALUE, ID_COL)

    print("\n" + "=" * 80)
    print("[FIN] Verificación completada.")
    print("=" * 80)

if __name__ == "__main__":
    main()
