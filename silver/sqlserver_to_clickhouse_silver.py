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

CH_HOST = os.getenv("CH_HOST", "localhost")
CH_PORT = int(os.getenv("CH_PORT", "8123"))
CH_USER = os.getenv("CH_USER", "default")
CH_PASSWORD = os.getenv("CH_PASSWORD", "")
CH_DATABASE = os.getenv("CH_DATABASE", "default")

STREAMING_CHUNK_SIZE = int(os.getenv("STREAMING_CHUNK_SIZE", "1000"))

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
    print("  python sqlserver_to_clickhouse_silver.py ORIG_DB DEST_DB [tablas] [limit] [reset]")
    print("")
    print("Ejemplos:")
    print("  python sqlserver_to_clickhouse_silver.py POM_Aplicaciones POM_Aplicaciones")
    print("  python sqlserver_to_clickhouse_silver.py POM_Aplicaciones POM_Aplicaciones dbo.PG_TC")
    print("  python sqlserver_to_clickhouse_silver.py POM_Aplicaciones POM_Aplicaciones dbo.PG_TC 5000")
    print("  python sqlserver_to_clickhouse_silver.py POM_Aplicaciones POM_Aplicaciones * 0 reset")
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

    # Detectar si el shell expandió el * (hay muchos argumentos y algunos parecen archivos)
    # Estrategia: si hay más de 5 argumentos, o si el tercer argumento no es "*"/"all" y hay "reset"/números, es expansión
    args_expanded = False
    
    # Heurística simple: si hay más de 5 argumentos, probablemente el * se expandió
    if len(sys.argv) > 5:
        args_expanded = True
    elif len(sys.argv) >= 4:
        third_arg = sys.argv[3].strip()
        # Si el tercer argumento no es "*" o "all", verificar si parece un archivo
        if third_arg not in ("*", "all"):
            # Verificar si parece un archivo (tiene extensión común o es un nombre de archivo típico)
            file_extensions = ['.py', '.log', '.txt', '.json', '.csv', '.sql', '.sh', '.md', '.yaml', '.yml']
            has_extension = any(third_arg.endswith(ext) for ext in file_extensions)
            has_path_sep = '/' in third_arg or '\\' in third_arg
            # Si parece un archivo, es expansión
            if has_extension or has_path_sep:
                args_expanded = True
            # También verificar si hay "reset" o números en los argumentos (indicando que el * se expandió)
            elif len(sys.argv) >= 5:
                # Buscar "reset" o números en argumentos posteriores
                for arg in sys.argv[4:]:
                    arg_clean = arg.strip().lower()
                    if arg_clean == "reset" or arg_clean.isdigit() or (arg_clean.startswith('-') and arg_clean[1:].isdigit()):
                        args_expanded = True
                        break

    if args_expanded:
        # El * se expandió, buscar "reset" y el número en los argumentos
        # Filtrar argumentos que parecen archivos y extraer solo los relevantes
        for arg in sys.argv[3:]:
            arg_clean = arg.strip().lower()
            if arg_clean == "reset":
                reset_flag = True
            elif arg_clean.isdigit() or (arg_clean.startswith('-') and arg_clean[1:].isdigit()):
                limit_arg = arg_clean
        # Usar "*" como valor por defecto para tablas
        tables_arg = "*"
    else:
        # Parsing normal
        if len(sys.argv) >= 4:
            tables_arg = sys.argv[3].strip() or "*"

        if len(sys.argv) >= 5:
            limit_arg = sys.argv[4].strip() or "0"

        if len(sys.argv) >= 6:
            reset_flag = (sys.argv[5].strip().lower() == "reset")

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

    return orig_db, dest_db, tables, row_limit, reset_flag

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
        cur.close()
        c_master.close()
    except Exception as e:
        raise Exception(f"No se pudo hacer login en SQL Server. Detalle: {e}")

    try:
        c_target = sql_conn(target_db)
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
    if v is None:
        return None

    if isinstance(v, Decimal):
        return float(v)  # ClickHouse Decimal acepta float o str, float suele ir bien

    if isinstance(v, (datetime.datetime, datetime.date)):
        # Validar que la fecha esté dentro del rango válido para timestamps
        # ClickHouse DateTime acepta fechas desde 1970-01-01 hasta 2106-02-07 aproximadamente
        try:
            if isinstance(v, datetime.date) and not isinstance(v, datetime.datetime):
                # Convertir date a datetime para validación
                v = datetime.datetime.combine(v, datetime.time.min)
            
            # Verificar que la fecha sea válida y esté en rango
            # Rango válido para ClickHouse DateTime: 1970-01-01 00:00:00 a 2106-02-07 06:28:15
            min_date = datetime.datetime(1970, 1, 1, 0, 0, 0)
            max_date = datetime.datetime(2106, 2, 7, 6, 28, 15)
            
            if v < min_date or v > max_date:
                # Fecha fuera de rango, retornar None (será NULL en ClickHouse)
                return None
            
            # Intentar convertir a timestamp para validar que sea válida
            v.timestamp()
            return v
        except (ValueError, OSError, OverflowError):
            # Fecha inválida o fuera de rango, retornar None
            return None

    if isinstance(v, datetime.time):
        return v.isoformat()

    if isinstance(v, (bytes, bytearray)):
        return v.hex()

    return v

def fetch_rows(sql_cursor, schema, table, colnames, row_limit, chunk_size):
    cols = ", ".join([f"[{c}]" for c in colnames])
    top_clause = f"TOP ({row_limit}) " if row_limit and row_limit > 0 else ""
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

    inserted = 0
    for chunk in fetch_rows(sql_cursor, schema, table, colnames, row_limit, dynamic_chunk_size):
        # Inserción directa (column_names asegura orden correcto)
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
    source_db, dest_db, requested_tables, row_limit, reset_flag = parse_args()

    # Adquirir lock de silver para evitar conflictos con streamingv4
    silver_lock = None
    try:
        silver_lock = acquire_silver_lock(dest_db)
    except Exception as e:
        print(f"[ERROR] No se pudo adquirir lock de SILVER: {e}")
        sys.exit(1)

    try:
        sql_test_connection_and_db_access(source_db)

        ch = ch_client()
        ensure_database(ch, dest_db)

        conn = sql_conn(source_db)
        cur = conn.cursor()

        tables = get_tables(cur, requested_tables)
        total_tables = len(tables)

        print(f"[START] SILVER ONLY | server={SQL_SERVER} source_db={source_db} dest_db={dest_db} tables={total_tables} limit={row_limit}")
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
