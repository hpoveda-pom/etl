import os
import sys
import time
import datetime
import pyodbc
import clickhouse_connect
from decimal import Decimal
from pathlib import Path
from dotenv import load_dotenv

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

# Cargar .env desde el directorio etl/ (padre del script)
script_dir = Path(__file__).resolve().parent
parent_dir = script_dir.parent  # etl/
env_path = parent_dir / ".env"
if env_path.exists():
    load_dotenv(env_path, override=True)
else:
    # Fallback: búsqueda automática
    load_dotenv(override=True)

# =========================
# ENV CONFIG
# =========================
SQL_SERVER = os.getenv("SQL_SERVER")
SQL_USER = os.getenv("SQL_USER")
SQL_PASSWORD = os.getenv("SQL_PASSWORD")
SQL_DRIVER = os.getenv("SQL_DRIVER", "ODBC Driver 17 for SQL Server")

# SQL Server Producción (opcional, para --prod)
SQL_SERVER_PROD = os.getenv("SQL_SERVER_PROD")
SQL_USER_PROD = os.getenv("SQL_USER_PROD")
SQL_PASSWORD_PROD = os.getenv("SQL_PASSWORD_PROD")

CH_HOST = os.getenv("CH_HOST", "localhost")
CH_PORT = int(os.getenv("CH_PORT", "8123"))
CH_USER = os.getenv("CH_USER", "default")
CH_PASSWORD = os.getenv("CH_PASSWORD", "")
CH_DATABASE = os.getenv("CH_DATABASE", "default")

STREAMING_CHUNK_SIZE = int(os.getenv("STREAMING_CHUNK_SIZE", "1000"))

# Debug opcional para fechas
DEBUG_DATETIME = os.getenv("DEBUG_DATETIME", "False").lower() == "true"

# Lock file para evitar conflictos con streamingv4
# Detectar directorio temporal según plataforma
if sys.platform == 'win32':
    # Windows: usar %TEMP% o %TMP%
    LOCK_FILE_DIR = os.getenv("LOCK_FILE_DIR") or os.getenv("TEMP") or os.getenv("TMP") or os.path.expanduser("~")
else:
    # Linux/Unix: usar /tmp
    LOCK_FILE_DIR = os.getenv("LOCK_FILE_DIR", "/tmp")

# Asegurar que el directorio existe
os.makedirs(LOCK_FILE_DIR, exist_ok=True)

# =========================
# LOCK SHARED (SILVER)
# =========================
def get_silver_lock_path(dest_db: str) -> str:
    """Obtiene la ruta del lock file para silver"""
    return os.path.join(LOCK_FILE_DIR, f"silver_{dest_db}.lock")

def acquire_silver_lock(dest_db: str):
    """Adquiere el lock de silver para evitar conflictos con streamingv4"""
    lock_file_path = get_silver_lock_path(dest_db)
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
                                print(f"[WARN] Ya hay una instancia de SILVER corriendo (PID: {old_pid})")
                                print(f"[INFO] Si estás seguro de que no hay otra instancia, elimina el lock file: {lock_file_path}")
                                raise Exception("Lock file existe y proceso está activo")
                            except (OSError, ValueError):
                                # Proceso no existe, eliminar lock file obsoleto
                                os.remove(lock_file_path)
            except:
                pass
        
        # Asegurar que el directorio del lock file existe
        lock_dir = os.path.dirname(lock_file_path)
        if lock_dir:
            os.makedirs(lock_dir, exist_ok=True)
        
        # Crear lock file con PID y timestamp (formato: pid|timestamp_epoch)
        lock_file = open(lock_file_path, 'w')
        current_timestamp = time.time()
        lock_file.write(f"{os.getpid()}|{current_timestamp}")
        lock_file.flush()
        
        # Aplicar lock según plataforma
        if HAS_FCNTL:
            try:
                fcntl.flock(lock_file.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
            except IOError as e:
                if e.errno in (errno.EAGAIN, errno.EWOULDBLOCK):
                    raise Exception(f"Ya hay una instancia de SILVER corriendo. Lock file: {lock_file_path}")
                else:
                    raise
        elif HAS_MSVCRT:
            try:
                msvcrt.locking(lock_file.fileno(), msvcrt.LK_NBLCK, 1)
            except IOError:
                raise Exception(f"Ya hay una instancia de SILVER corriendo. Lock file: {lock_file_path}")
        
        print(f"[INFO] Lock SILVER adquirido: {lock_file_path}")
        return lock_file
        
    except Exception as e:
        if lock_file:
            lock_file.close()
        raise

def release_silver_lock(lock_file, dest_db: str):
    """Libera el lock de silver"""
    if lock_file:
        try:
            if HAS_FCNTL:
                fcntl.flock(lock_file.fileno(), fcntl.LOCK_UN)
            elif HAS_MSVCRT:
                msvcrt.locking(lock_file.fileno(), msvcrt.LK_UNLCK, 1)
            lock_file.close()
            
            lock_file_path = get_silver_lock_path(dest_db)
            if os.path.exists(lock_file_path):
                os.remove(lock_file_path)
            
            print(f"[INFO] Lock SILVER liberado")
        except:
            pass

def is_silver_lock_active(dest_db: str) -> bool:
    """Verifica si hay un lock de silver activo (para streamingv4)"""
    lock_file_path = get_silver_lock_path(dest_db)
    
    if not os.path.exists(lock_file_path):
        return False
    
    try:
        with open(lock_file_path, 'r') as f:
            old_pid = f.read().strip()
            if old_pid:
                # Verificar si el proceso aún existe
                if sys.platform != 'win32':
                    try:
                        os.kill(int(old_pid), 0)  # Signal 0 solo verifica existencia
                        return True  # Proceso existe, lock activo
                    except (OSError, ValueError):
                        # Proceso no existe, lock obsoleto
                        os.remove(lock_file_path)
                        return False
                else:
                    # En Windows, asumir que está activo si el archivo existe
                    return True
    except:
        return False
    
    return False

# =========================
# HELPERS
# =========================
def now_utc():
    return datetime.datetime.now(datetime.UTC).replace(microsecond=0)

def usage():
    print("Uso:")
    print("  python sqlserver_to_clickhouse_silver.py ORIG_DB DEST_DB [tablas] [limit] [reset] [--prod]")
    print("")
    print("Ejemplos:")
    print("  python sqlserver_to_clickhouse_silver.py POM_Aplicaciones POM_Aplicaciones")
    print("  python sqlserver_to_clickhouse_silver.py POM_Aplicaciones POM_Aplicaciones dbo.PG_TC")
    print("  python sqlserver_to_clickhouse_silver.py POM_Aplicaciones POM_Aplicaciones dbo.PG_TC 5000")
    print("  python sqlserver_to_clickhouse_silver.py POM_Aplicaciones POM_Aplicaciones * 0 reset")
    print("  python sqlserver_to_clickhouse_silver.py POM_Aplicaciones POM_Aplicaciones --prod")
    print("  python sqlserver_to_clickhouse_silver.py POM_Aplicaciones POM_Aplicaciones dbo.PG_TC 0 reset --prod")
    print("")
    print("Opciones:")
    print("  --prod              Usar credenciales de producción (SQL_SERVER_PROD, SQL_USER_PROD, SQL_PASSWORD_PROD)")
    sys.exit(1)

def parse_args():
    if len(sys.argv) < 3:
        usage()

    orig_db = sys.argv[1].strip()
    dest_db = sys.argv[2].strip()

    if not orig_db:
        raise Exception("ORIG_DB vacío.")
    if not dest_db:
        raise Exception("DEST_DB vacío.")

    tables_arg = "*"
    limit_arg = "0"
    reset_flag = False
    use_prod = False

    # Detectar flag --prod primero y removerlo de los argumentos
    args_list = sys.argv[3:]
    if "--prod" in args_list:
        use_prod = True
        args_list = [a for a in args_list if a != "--prod"]

    # Detectar si el shell expandió el * (hay muchos argumentos y algunos parecen archivos)
    # Estrategia: si hay más de 5 argumentos, o si el tercer argumento no es "*"/"all" y hay "reset"/números, es expansión
    args_expanded = False
    
    # Heurística simple: si hay más de 5 argumentos (sin contar --prod), probablemente el * se expandió
    if len(args_list) > 3:
        args_expanded = True
    elif len(args_list) >= 1:
        first_arg = args_list[0].strip()
        # Si el primer argumento no es "*" o "all", verificar si parece un archivo
        if first_arg not in ("*", "all"):
            # Verificar si parece un archivo (tiene extensión común o es un nombre de archivo típico)
            file_extensions = ['.py', '.log', '.txt', '.json', '.csv', '.sql', '.sh', '.md', '.yaml', '.yml']
            has_extension = any(first_arg.endswith(ext) for ext in file_extensions)
            has_path_sep = '/' in first_arg or '\\' in first_arg
            # Si parece un archivo, es expansión
            if has_extension or has_path_sep:
                args_expanded = True
            # También verificar si hay "reset" o números en los argumentos (indicando que el * se expandió)
            elif len(args_list) >= 2:
                # Buscar "reset" o números en argumentos posteriores
                for arg in args_list[1:]:
                    arg_clean = arg.strip().lower()
                    if arg_clean == "reset" or arg_clean.isdigit() or (arg_clean.startswith('-') and arg_clean[1:].isdigit()):
                        args_expanded = True
                        break

    if args_expanded:
        # El * se expandió, buscar "reset" y el número en los argumentos
        # Filtrar argumentos que parecen archivos y extraer solo los relevantes
        for arg in args_list:
            arg_clean = arg.strip().lower()
            if arg_clean == "reset":
                reset_flag = True
            elif arg_clean.isdigit() or (arg_clean.startswith('-') and arg_clean[1:].isdigit()):
                limit_arg = arg_clean
        # Usar "*" como valor por defecto para tablas
        tables_arg = "*"
    else:
        # Parsing normal
        if len(args_list) >= 1:
            tables_arg = args_list[0].strip() or "*"

        if len(args_list) >= 2:
            limit_arg = args_list[1].strip() or "0"

        if len(args_list) >= 3:
            reset_flag = (args_list[2].strip().lower() == "reset")

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

    return orig_db, dest_db, tables, row_limit, reset_flag, use_prod

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

def sql_test_connection_and_db_access(target_db: str, use_prod: bool = False):
    try:
        c_master = sql_conn("master", use_prod)
        cur = c_master.cursor()
        cur.execute("SELECT DB_NAME()")
        print("[OK] Login SQL Server correcto. Conectado a:", cur.fetchone()[0])
        cur.close()
        c_master.close()
    except Exception as e:
        raise Exception(f"No se pudo hacer login en SQL Server. Detalle: {e}")

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

def build_select_columns_with_date_conversion(colnames, columns_meta, use_convert=True):
    """
    Construye la lista de columnas SELECT convirtiendo fechas a texto exacto.
    Para datetime/datetime2/smalldatetime: CONVERT(varchar(19), <col>, 120)
    Para date: CONVERT(varchar(10), <col>, 120)
    
    use_convert: Si False, devuelve columnas originales (útil para subconsultas)
    """
    # Crear dict de metadatos para búsqueda rápida
    meta_dict = {col[0]: col for col in columns_meta}
    
    select_cols = []
    for colname in colnames:
        if not use_convert:
            # Sin conversión (para subconsultas donde necesitamos columnas originales)
            select_cols.append(f"[{colname}]")
        elif colname in meta_dict:
            data_type = meta_dict[colname][1].lower()
            if data_type in ('datetime', 'datetime2', 'smalldatetime'):
                # Convertir a varchar(19) con formato 120 (YYYY-MM-DD HH:MM:SS)
                select_cols.append(f"CONVERT(varchar(19), [{colname}], 120) AS [{colname}]")
            elif data_type == 'date':
                # Convertir a varchar(10) con formato 120 (YYYY-MM-DD)
                select_cols.append(f"CONVERT(varchar(10), [{colname}], 120) AS [{colname}]")
            else:
                # Otras columnas sin conversión
                select_cols.append(f"[{colname}]")
        else:
            # Si no está en metadatos, usar sin conversión
            select_cols.append(f"[{colname}]")
    
    return ", ".join(select_cols)

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

def map_sqlserver_to_clickhouse_type(data_type: str, precision, scale) -> str:
    """
    Mapeo razonable de SQL Server -> ClickHouse
    """
    t = (data_type or "").lower()

    if t in ("tinyint",):
        return "UInt8"
    if t in ("smallint",):
        return "Int16"
    if t in ("int",):
        return "Int32"
    if t in ("bigint",):
        return "Int64"
    if t in ("bit",):
        return "UInt8"

    if t in ("float",):
        return "Float64"
    if t in ("real",):
        return "Float32"

    if t in ("decimal", "numeric", "money", "smallmoney"):
        # fallback seguro
        p = int(precision) if precision else 18
        s = int(scale) if scale else 2
        # límites ClickHouse Decimal: p <= 76
        if p > 38:
            return "String"
        return f"Decimal({p},{s})"

    if t in ("date",):
        return "Date"
    if t in ("datetime", "smalldatetime", "datetime2"):
        return "DateTime"
    if t in ("time",):
        # ClickHouse tiene tipo Time solo en versiones nuevas, mejor string para compatibilidad
        return "String"
    if t in ("uniqueidentifier",):
        return "UUID"

    if t in ("binary", "varbinary", "image"):
        return "String"

    if t in ("xml",):
        return "String"

    # varchar/nvarchar/text/char/nchar/etc
    return "String"

def make_nullable(ch_type: str, is_nullable: str) -> str:
    """
    Hace una columna nullable en ClickHouse.
    Siempre hace DateTime y Date como Nullable para evitar errores con valores None.
    """
    # DateTime y Date siempre nullable para evitar errores con valores None
    if ch_type in ("DateTime", "Date"):
        return f"Nullable({ch_type})"
    
    if (is_nullable or "").upper() == "YES":
        return f"Nullable({ch_type})"
    return ch_type

def create_or_reset_table(ch, dest_db, schema, table, columns_meta, pk_cols, reset_flag):
    """
    Crea tabla Silver con tipos reales.
    Nombre: dest_db.table  (no metemos schema como prefijo para que sea cómodo)
    """
    ch_table = table  # simple

    if reset_flag:
        ch.command(f"DROP TABLE IF EXISTS `{dest_db}`.`{ch_table}`")

    # Construir columnas y mapear nombres a tipos
    cols_sql = []
    col_types = {}  # Mapeo nombre_columna -> tipo_ch (para verificar nullable)
    for col_name, data_type, prec, scale, is_nullable in columns_meta:
        ch_type = map_sqlserver_to_clickhouse_type(data_type, prec, scale)
        ch_type = make_nullable(ch_type, is_nullable)
        cols_sql.append(f"`{col_name}` {ch_type}")
        col_types[col_name] = ch_type

    # ORDER BY: filtrar columnas nullable de la PK
    # ClickHouse no permite columnas nullable en ORDER BY a menos que allow_nullable_key esté habilitado
    order_expr = "tuple()"
    non_nullable_pk_cols = []  # Inicializar fuera del if
    
    if pk_cols:
        # Filtrar solo columnas no-nullable de la PK
        for pk_col in pk_cols:
            ch_type = col_types.get(pk_col, "")
            # Verificar si es nullable (contiene "Nullable(")
            if ch_type and not ch_type.startswith("Nullable("):
                non_nullable_pk_cols.append(pk_col)
        
        if non_nullable_pk_cols:
            # Usar solo las columnas no-nullable
            order_expr = "(" + ", ".join([f"`{c}`" for c in non_nullable_pk_cols]) + ")"
        # Si todas las columnas de la PK son nullable, usar tuple() (sin índice)

    # Construir el DDL (extraer join para evitar problema con \n en f-string)
    cols_sql_str = ",\n        ".join(cols_sql)
    
    # Usar ReplacingMergeTree si hay PK no-nullable (para evitar duplicados en streaming)
    # Si no hay PK o todas son nullable, usar MergeTree normal
    if non_nullable_pk_cols:
        engine = "ReplacingMergeTree"
    else:
        engine = "MergeTree"
    
    ddl = f"""
    CREATE TABLE IF NOT EXISTS `{dest_db}`.`{ch_table}`
    (
        {cols_sql_str}
    )
    ENGINE = {engine}
    ORDER BY {order_expr}
    """

    ch.command(ddl)
    return ch_table

def normalize_py_value(v):
    """
    Normaliza valores de Python para ClickHouse.
    IMPORTANTE: Las fechas ahora vienen como strings desde SQL Server (CONVERT),
    así que las mantenemos como strings para insertarlas con toDateTime/toDate.
    """
    if v is None:
        return None

    if isinstance(v, Decimal):
        return float(v)  # ClickHouse Decimal acepta float o str, float suele ir bien

    # Las fechas ahora vienen como strings desde SQL Server (CONVERT), mantenerlas como strings
    if isinstance(v, str):
        # Si es un string que parece fecha (YYYY-MM-DD o YYYY-MM-DD HH:MM:SS), mantenerlo
        # Esto permite que se inserte con toDateTime/toDate en ClickHouse
        return v

    if isinstance(v, (datetime.datetime, datetime.date)):
        # Si por alguna razón aún llega como datetime (no debería con CONVERT), convertir a string
        try:
            if isinstance(v, datetime.date) and not isinstance(v, datetime.datetime):
                v = datetime.datetime.combine(v, datetime.time.min)
            min_date = datetime.datetime(1970, 1, 1, 0, 0, 0)
            max_date = datetime.datetime(2106, 2, 7, 6, 28, 15)
            if v < min_date or v > max_date:
                return None
            # Convertir a string formato YYYY-MM-DD HH:MM:SS
            return v.strftime('%Y-%m-%d %H:%M:%S')
        except (ValueError, OSError, OverflowError):
            return None

    if isinstance(v, datetime.time):
        return v.isoformat()

    if isinstance(v, (bytes, bytearray)):
        return v.hex()

    return v

def insert_clickhouse_with_date_functions(ch, table_name, data, column_names, ch_column_types):
    """
    Inserta en ClickHouse usando toDateTime/toDate para columnas de fecha.
    data: lista de listas (filas)
    column_names: lista de nombres de columnas
    ch_column_types: dict {colname: ch_type} desde DESCRIBE TABLE
    """
    if not data:
        return
    
    # Construir INSERT con toDateTime/toDate para fechas
    cols_sql = ", ".join([f"`{c}`" for c in column_names])
    
    # Construir VALUES con toDateTime/toDate
    rows_sql = []
    for row in data:
        values = []
        for i, colname in enumerate(column_names):
            val = row[i] if i < len(row) else None
            ch_type = ch_column_types.get(colname, "").upper()
            
            if val is None:
                values.append("NULL")
            elif "DATETIME" in ch_type and isinstance(val, str):
                # Validar formato YYYY-MM-DD HH:MM:SS
                if len(val) == 19 and val[10] == ' ':
                    if DEBUG_DATETIME:
                        print(f"[DEBUG_DATETIME] Insertando DateTime '{val}' en columna '{colname}' usando toDateTime()")
                    values.append(f"toDateTime('{val.replace("'", "''")}')")
                else:
                    # Formato incorrecto, intentar como string normal
                    values.append(f"'{str(val).replace("'", "''")}'")
            elif "DATE" in ch_type and isinstance(val, str) and "DATETIME" not in ch_type:
                # Validar formato YYYY-MM-DD
                if len(val) == 10:
                    if DEBUG_DATETIME:
                        print(f"[DEBUG_DATETIME] Insertando Date '{val}' en columna '{colname}' usando toDate()")
                    values.append(f"toDate('{val.replace("'", "''")}')")
                else:
                    # Formato incorrecto, intentar como string normal
                    values.append(f"'{str(val).replace("'", "''")}'")
            elif isinstance(val, str):
                values.append(f"'{val.replace("'", "''")}'")
            elif isinstance(val, (int, float)):
                values.append(str(val))
            elif isinstance(val, bool):
                values.append("1" if val else "0")
            else:
                values.append(f"'{str(val).replace("'", "''")}'")
        
        rows_sql.append(f"({', '.join(values)})")
    
    insert_sql = f"INSERT INTO {table_name} ({cols_sql}) VALUES {', '.join(rows_sql)}"
    ch.command(insert_sql)

def fetch_rows(sql_cursor, schema, table, colnames, row_limit, chunk_size, id_col=None, timestamp_col=None, columns_meta=None):
    # Construir columnas SELECT con conversión de fechas a texto
    if columns_meta:
        cols = build_select_columns_with_date_conversion(colnames, columns_meta)
    else:
        cols = ", ".join([f"[{c}]" for c in colnames])
    top_clause = f"TOP ({row_limit}) " if row_limit and row_limit > 0 else ""
    
    # Si hay ID y timestamp, deduplicar para obtener solo el más reciente por ID
    # En la subconsulta usar columnas originales (sin CONVERT) para ORDER BY correcto
    if id_col and timestamp_col:
        cols_inner = build_select_columns_with_date_conversion(colnames, columns_meta, use_convert=False) if columns_meta else ", ".join([f"[{c}]" for c in colnames])
        # Usar ROW_NUMBER para obtener solo el registro más reciente por ID
        query = f"""
        SELECT {top_clause}{cols}
        FROM (
            SELECT {cols_inner},
                   ROW_NUMBER() OVER (PARTITION BY [{id_col}] ORDER BY [{timestamp_col}] DESC) as rn
            FROM [{schema}].[{table}]
        ) ranked
        WHERE rn = 1
        """
    else:
        query = f"SELECT {top_clause}{cols} FROM [{schema}].[{table}]"
    
    sql_cursor.execute(query)

    while True:
        rows = sql_cursor.fetchmany(chunk_size)
        if not rows:
            break

        out = []
        for r in rows:
            out.append([normalize_py_value(x) for x in r])
        yield out

def ingest_table_silver(sql_cursor, ch, dest_db, schema, table, row_limit, reset_flag):
    cols_meta = get_columns(sql_cursor, schema, table)
    if not cols_meta:
        print(f"[SKIP] {schema}.{table} sin columnas")
        return (0, "skipped")

    colnames = [c[0] for c in cols_meta]
    pk_cols = get_primary_key_columns(sql_cursor, schema, table)
    num_cols = len(colnames)

    # Detectar columna ID (buscar Id, ID, o primera columna de PK)
    id_col = None
    for col_name, data_type, prec, scale, is_nullable in cols_meta:
        if col_name.lower() in ('id', 'id_', 'idnum', 'idnumero'):
            id_col = col_name
            break
    
    if not id_col and pk_cols:
        id_col = pk_cols[0]
    
    # Detectar columna de timestamp para deduplicación (priorizar F_Ingreso)
    timestamp_col = None
    for col_name, data_type, prec, scale, is_nullable in cols_meta:
        if col_name.lower() in ('f_ingreso', 'fechacreacion', 'createdat', 'createdate'):
            timestamp_col = col_name
            break
        elif data_type in ('datetime', 'datetime2', 'smalldatetime', 'date'):
            if not timestamp_col:  # Tomar la primera fecha encontrada como fallback
                timestamp_col = col_name
    
    if id_col and timestamp_col:
        print(f"[INFO] {schema}.{table} - Deduplicación activa: más reciente por {id_col} usando {timestamp_col}")
    elif id_col:
        print(f"[WARN] {schema}.{table} - ID detectado ({id_col}) pero no se encontró columna de timestamp para deduplicación")

    # Ajustar chunk size dinámicamente para evitar errores de memoria
    # Estrategia conservadora: reducir chunk cuando hay muchas columnas O cuando el chunk base es muy grande
    MAX_CHUNK_SIZE = 1000  # Límite máximo absoluto para evitar problemas de memoria
    
    if num_cols > 20:
        # Reducir chunk size proporcionalmente cuando hay muchas columnas
        dynamic_chunk_size = max(100, int(STREAMING_CHUNK_SIZE * (20 / num_cols)))
    elif STREAMING_CHUNK_SIZE > MAX_CHUNK_SIZE:
        # Si el chunk base es muy grande, reducirlo para ser más conservador
        dynamic_chunk_size = MAX_CHUNK_SIZE
    else:
        dynamic_chunk_size = STREAMING_CHUNK_SIZE
    
    # Aplicar límite máximo absoluto
    dynamic_chunk_size = min(dynamic_chunk_size, MAX_CHUNK_SIZE)

    print(f"[INFO] {schema}.{table} -> {dest_db}.{table} | cols={num_cols} limit={row_limit} reset={reset_flag} chunk_size={dynamic_chunk_size}")

    ch_table = create_or_reset_table(
        ch=ch,
        dest_db=dest_db,
        schema=schema,
        table=table,
        columns_meta=cols_meta,
        pk_cols=pk_cols,
        reset_flag=reset_flag
    )

    # Obtener tipos de columnas de ClickHouse para inserción con toDateTime/toDate
    try:
        desc_result = ch.query(f"DESCRIBE TABLE `{dest_db}`.`{ch_table}`")
        ch_column_types = {row[0]: row[1] for row in desc_result.result_rows}
    except Exception as e:
        print(f"[WARN] {schema}.{table} - Error obteniendo tipos de ClickHouse, usando inserción estándar: {e}")
        ch_column_types = {}

    inserted = 0
    for chunk in fetch_rows(sql_cursor, schema, table, colnames, row_limit, dynamic_chunk_size, 
                            id_col=id_col, timestamp_col=timestamp_col, columns_meta=cols_meta):
        # Inserción con toDateTime/toDate para fechas
        if ch_column_types:
            insert_clickhouse_with_date_functions(
                ch,
                f"`{dest_db}`.`{ch_table}`",
                chunk,
                colnames,
                ch_column_types
            )
        else:
            # Fallback a inserción estándar si no se pudieron obtener tipos
            ch.insert(
                f"`{dest_db}`.`{ch_table}`",
                chunk,
                column_names=colnames
            )
        inserted += len(chunk)

    print(f"[OK] {schema}.{table} inserted={inserted}")
    return (inserted, "ok")

# =========================
# MAIN
# =========================
def main():
    start_time = time.time()
    source_db, dest_db, requested_tables, row_limit, reset_flag, use_prod = parse_args()

    # Adquirir lock de silver para evitar conflictos con streamingv4
    silver_lock = None
    try:
        silver_lock = acquire_silver_lock(dest_db)
    except Exception as e:
        print(f"[ERROR] No se pudo adquirir lock de SILVER: {e}")
        sys.exit(1)

    try:
        sql_test_connection_and_db_access(source_db, use_prod)

        ch = ch_client()
        ensure_database(ch, dest_db)

        conn = sql_conn(source_db, use_prod)
        cur = conn.cursor()

        tables = get_tables(cur, requested_tables)
        total_tables = len(tables)

        env_type = "PRODUCCIÓN" if use_prod else "DESARROLLO"
        server_info = SQL_SERVER_PROD if (use_prod and SQL_SERVER_PROD) else SQL_SERVER
        
        print(f"[START] SILVER ONLY ({env_type}) | server={server_info} source_db={source_db} dest_db={dest_db} tables={total_tables} limit={row_limit}")
        print(f"[INFO] STREAMING_CHUNK_SIZE={STREAMING_CHUNK_SIZE}")
        print(f"[INFO] Streaming v4 será pausado mientras SILVER está activo")

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
                inserted, status = ingest_table_silver(cur, ch, dest_db, schema, table, row_limit, reset_flag)
                total_inserted += inserted
                if status == "ok":
                    ok_count += 1
                else:
                    skipped_count += 1
            except Exception as e:
                print(f"[ERROR] {schema}.{table}: {e}")
                error_count += 1

        cur.close()
        conn.close()

        elapsed = time.time() - start_time

        print(f"\n[OK] Exportación SILVER completada: {ok_count} tablas OK")
        print(f" Datos cargados en: {dest_db}\n")

        print("=" * 60)
        print("RESUMEN SILVER")
        print("=" * 60)
        print(f"Tablas procesadas: {total_tables}")
        print(f"Tablas OK: {ok_count}")
        print(f"Tablas con error: {error_count}")
        print(f"Tablas omitidas: {skipped_count}")
        print(f"Total filas insertadas: {total_inserted}")
        print(f"Tiempo de ejecución: {elapsed:.2f} segundos")
        print("=" * 60)
    
    finally:
        # Liberar lock de silver
        release_silver_lock(silver_lock, dest_db)

if __name__ == "__main__":
    main()
