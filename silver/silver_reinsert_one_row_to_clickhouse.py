import os
import sys
from pathlib import Path
from dotenv import load_dotenv
import pyodbc
import clickhouse_connect

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

# Debug opcional para fechas
DEBUG_DATETIME = os.getenv("DEBUG_DATETIME", "False").lower() == "true"


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


def get_clickhouse_column_types(dest_db: str, table: str) -> dict:
    """
    Retorna dict: {colname: ch_type}
    """
    ch = ch_client()
    rows = ch.query(f"DESCRIBE TABLE `{dest_db}`.`{table}`").result_rows
    # rows: name, type, default_type, default_expression, comment, codec_expression, ttl_expression
    return {r[0]: r[1] for r in rows}


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

def fetch_one_row_sqlserver_exact(db_name: str, schema: str, table: str, id_col: str, id_value: int):
    """
    Extrae el registro tal cual.
    Todas las columnas de fecha se extraen como texto exacto usando CONVERT.
    """
    conn = sql_conn(db_name)
    cur = conn.cursor()

    # Obtener metadatos de columnas
    columns_meta = get_columns_meta(cur, schema, table)
    colnames = [col[0] for col in columns_meta]
    
    # Construir SELECT con conversión de fechas
    cols = build_select_columns_with_date_conversion(colnames, columns_meta)
    
    query = f"""
    SELECT {cols}
    FROM [{schema}].[{table}]
    WHERE [{id_col}] = ?
    """

    print("\n[SQL QUERY - EXTRACT EXACT]")
    print(query.strip())
    print(f"[SQL PARAMS] ({id_value})")

    cur.execute(query, (id_value,))
    row = cur.fetchone()

    if not row:
        cur.close()
        conn.close()
        return None, None

    columns = [d[0] for d in cur.description]
    values = list(row)

    cur.close()
    conn.close()
    return columns, values


def insert_row_clickhouse_exact(dest_db: str, table: str, columns: list, values: list):
    """
    Inserta con SQL explícito para poder controlar que DateTime entre EXACTO.
    """
    ch = ch_client()

    col_types = get_clickhouse_column_types(dest_db, table)

    cols_sql = ", ".join([f"`{c}`" for c in columns])

    def sql_literal(colname, v):
        if v is None:
            return "NULL"

        ch_type = col_types.get(colname, "")

        # Si es DateTime/Nullable(DateTime) y el valor viene como string 'YYYY-MM-DD HH:MM:SS'
        if "DateTime" in ch_type and isinstance(v, str):
            if len(v) == 19 and v[10] == ' ':
                if DEBUG_DATETIME:
                    print(f"[DEBUG_DATETIME] Insertando DateTime '{v}' en columna '{colname}' usando toDateTime()")
                return f"toDateTime('{v.replace("'", "''")}')"
            else:
                # Formato incorrecto, intentar como string normal
                return "'" + v.replace("'", "''") + "'"
        
        # Si es Date/Nullable(Date) y el valor viene como string 'YYYY-MM-DD'
        if "Date" in ch_type and isinstance(v, str) and "DateTime" not in ch_type:
            if len(v) == 10:
                if DEBUG_DATETIME:
                    print(f"[DEBUG_DATETIME] Insertando Date '{v}' en columna '{colname}' usando toDate()")
                return f"toDate('{v.replace("'", "''")}')")
            else:
                # Formato incorrecto, intentar como string normal
                return "'" + v.replace("'", "''") + "'"

        # Strings
        if isinstance(v, str):
            return "'" + v.replace("'", "''") + "'"

        # bool/int/float
        if isinstance(v, (int, float)):
            return str(v)

        # fallback
        return "'" + str(v).replace("'", "''") + "'"

    vals_sql = ", ".join([sql_literal(columns[i], values[i]) for i in range(len(columns))])

    insert_sql = f"INSERT INTO `{dest_db}`.`{table}` ({cols_sql}) VALUES ({vals_sql})"

    print("\n[CH QUERY - INSERT EXACT]")
    print(insert_sql)

    ch.command(insert_sql)


def verify_row_clickhouse(dest_db: str, table: str, id_col: str, id_value: int):
    ch = ch_client()
    q = f"SELECT * FROM `{dest_db}`.`{table}` WHERE `{id_col}` = %(id)s LIMIT 1"
    row = ch.query(q, parameters={"id": id_value}).result_rows
    return row


def main():
    ORIG_DB = "POM_Aplicaciones"
    DEST_DB = "POM_Aplicaciones"

    SCHEMA = "dbo"
    TABLE = "PC_Gestiones"

    SQL_ID_COL = "ID"
    CH_ID_COL = "Id"
    ID_VALUE = 492237

    print("=" * 80)
    print("[START] Reinsert EXACT (sin tocar la hora)")
    print("=" * 80)

    cols, vals = fetch_one_row_sqlserver_exact(ORIG_DB, SCHEMA, TABLE, SQL_ID_COL, ID_VALUE)

    if not cols:
        print("[ERROR] Registro no existe en SQL Server.")
        sys.exit(1)

    # Debug: mostrar el valor exacto de F_Ingreso
    if "F_Ingreso" in cols:
        print("\n[DEBUG] Valor exacto de F_Ingreso (texto desde SQL):")
        print(vals[cols.index("F_Ingreso")])

    insert_row_clickhouse_exact(DEST_DB, TABLE, cols, vals)

    print("\n[OK] Insert EXACT realizado ✅")

    res = verify_row_clickhouse(DEST_DB, TABLE, CH_ID_COL, ID_VALUE)
    print("\n[VERIFY] Registro en ClickHouse (1 fila):")
    print(res)

    print("=" * 80)
    print("[FIN]")
    print("=" * 80)


if __name__ == "__main__":
    main()
