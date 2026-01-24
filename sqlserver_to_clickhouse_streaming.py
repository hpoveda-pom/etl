import os
import re
import time
import warnings
import hashlib
import json
from datetime import datetime, timedelta, date
from typing import Optional, Tuple
from decimal import Decimal

import pyodbc

# Cargar variables de entorno desde archivo .env
try:
    from dotenv import load_dotenv
    # Buscar archivo .env en el directorio actual y directorio del script
    env_path = os.path.join(os.path.dirname(__file__), '.env')
    if os.path.exists(env_path):
        load_dotenv(env_path)
        print(f"[OK] Archivo .env cargado desde: {env_path}")
    else:
        # Intentar cargar desde directorio actual
        load_dotenv()
except ImportError:
    # Si python-dotenv no está instalado, continuar sin él
    pass

try:
    import clickhouse_connect
except ImportError:
    print("[ERROR] Error: Falta la librería clickhouse-connect")
    print("[INFO] Instálala con: pip install clickhouse-connect")
    exit(1)

# Suprimir warnings
warnings.filterwarnings('ignore', category=UserWarning, module='pandas')

# ============== SQL Server config ==============
SQL_SERVER = os.getenv("SQL_SERVER", r"SRV-DESA\SQLEXPRESS")
SQL_DATABASE = os.getenv("SQL_DATABASE", "")
SQL_USER = os.getenv("SQL_USER", "")
SQL_PASSWORD = os.getenv("SQL_PASSWORD", "")
# Por defecto requiere SQL_USER y SQL_PASSWORD (autenticación SQL Server).
# Para usar autenticación Windows, define SQL_USE_WINDOWS_AUTH=true
SQL_USE_WINDOWS_AUTH = os.getenv("SQL_USE_WINDOWS_AUTH", "false").lower() in ("true", "yes", "1")
SQL_DRIVER = os.getenv("SQL_DRIVER", "ODBC Driver 17 for SQL Server")  # Ajustar según tu driver


# ============== ClickHouse Cloud config ==============
CH_HOST = os.getenv("CH_HOST", "f4rf85ygzj.eastus2.azure.clickhouse.cloud")
CH_PORT = int(os.getenv("CH_PORT", "8443"))
CH_USER = os.getenv("CH_USER", "default")
CH_PASSWORD = os.getenv("CH_PASSWORD", "")
if not CH_PASSWORD:
    raise RuntimeError(
        "[ERROR] CH_PASSWORD es obligatorio.\n"
        "[INFO] Opciones:\n"
        "   1. Crea un archivo .env en el directorio etl/ con: CH_PASSWORD=tu_password\n"
        "   2. O define la variable de entorno: set CH_PASSWORD=tu_password\n"
        "   3. O instala python-dotenv: pip install python-dotenv"
    )
CH_DATABASE = os.getenv("CH_DATABASE", "default")
CH_TABLE = os.getenv("CH_TABLE", "")  # Tabla destino (opcional, se puede especificar por archivo)

# ============== Configuración de streaming ==============
CHUNK_SIZE = int(os.getenv("STREAMING_CHUNK_SIZE", "10000"))  # Filas por chunk
TARGET_TABLE_PREFIX = os.getenv("TARGET_TABLE_PREFIX", "")  # Prefijo para tablas en ClickHouse (vacío por defecto)

# Opcional: especificar tablas a exportar (coma-separado)
TABLES_FILTER = [s.strip() for s in os.getenv("TABLES_FILTER", "").split(",") if s.strip()]

# Prefijos de nombres de tablas a excluir
EXCLUDED_TABLE_PREFIXES = ["TMP_"]

# ============== Configuración avanzada ==============
# Lookback window para detectar updates/deletes (días hacia atrás)
LOOKBACK_DAYS = int(os.getenv("LOOKBACK_DAYS", "7"))  # Por defecto 7 días

# Usar ReplacingMergeTree para deduplicación automática
USE_REPLACING_MERGE_TREE = os.getenv("USE_REPLACING_MERGE_TREE", "true").lower() == "true"

# Timezone para DateTime64 (ej: 'UTC', 'America/New_York')
CH_TIMEZONE = os.getenv("CH_TIMEZONE", "UTC")


def sanitize_token(s: str, maxlen: int = 120) -> str:
    """Sanitiza un string para usarlo como identificador SQL."""
    s = (s or "").strip()
    s = re.sub(r"[^\w\-\.]+", "_", s, flags=re.UNICODE)
    s = re.sub(r"_+", "_", s).strip("_")
    return s[:maxlen] if s else "NA"


def get_sql_connection():
    """
    Crea una conexión a SQL Server.
    Por defecto requiere SQL_USER y SQL_PASSWORD (autenticación SQL Server).
    Para usar autenticación Windows, define SQL_USE_WINDOWS_AUTH=true en las variables de entorno.
    """
    if not SQL_DATABASE:
        raise RuntimeError("Falta SQL_DATABASE (definí la variable de entorno).")
    
    available_drivers = [driver for driver in pyodbc.drivers()]
    driver_to_use = SQL_DRIVER
    
    if driver_to_use not in available_drivers:
        alternative_drivers = [
            "ODBC Driver 17 for SQL Server",
            "ODBC Driver 13 for SQL Server",
            "SQL Server Native Client 11.0",
            "SQL Server",
        ]
        for alt_driver in alternative_drivers:
            if alt_driver in available_drivers:
                driver_to_use = alt_driver
                print(f"[WARN]  Driver '{SQL_DRIVER}' no encontrado. Usando '{driver_to_use}'")
                break
        else:
            raise RuntimeError(
                f"No se encontró un driver ODBC compatible. Drivers disponibles: {', '.join(available_drivers)}"
            )
    
    # Construir connection string
    if SQL_USE_WINDOWS_AUTH:
        # Autenticación Windows (Integrated Security)
        print(f"🔐 Usando autenticación Windows (usuario actual: {os.getenv('USERNAME', 'N/A')})")
        conn_str = (
            f"DRIVER={{{driver_to_use}}};"
            f"SERVER={SQL_SERVER};"
            f"DATABASE={SQL_DATABASE};"
            f"Trusted_Connection=yes;"
            f"TrustServerCertificate=yes;"
        )
    else:
        # Autenticación SQL Server (requiere usuario y password)
        if not SQL_USER or not SQL_PASSWORD:
            raise RuntimeError(
                "SQL_USER y SQL_PASSWORD son requeridos para autenticación SQL Server.\n"
                "Define las variables de entorno SQL_USER y SQL_PASSWORD, o\n"
                "define SQL_USE_WINDOWS_AUTH=true para usar autenticación Windows."
            )
        print(f"🔐 Usando autenticación SQL Server (usuario: {SQL_USER})")
        conn_str = (
            f"DRIVER={{{driver_to_use}}};"
            f"SERVER={SQL_SERVER};"
            f"DATABASE={SQL_DATABASE};"
            f"UID={SQL_USER};"
            f"PWD={SQL_PASSWORD};"
            f"TrustServerCertificate=yes;"
        )
    
    try:
        conn = pyodbc.connect(conn_str, timeout=30)
        print(f"[OK] Conectado a SQL Server: {SQL_SERVER}/{SQL_DATABASE}")
        return conn
    except pyodbc.Error as e:
        error_msg = str(e)
        if "login failed" in error_msg.lower():
            auth_type = "Windows" if SQL_USE_WINDOWS_AUTH else "SQL Server"
            raise RuntimeError(f"Error de autenticación {auth_type}. Verifica las credenciales o permisos.")
        elif "driver" in error_msg.lower():
            raise RuntimeError(f"Error con el driver ODBC. Verifica que '{driver_to_use}' esté instalado.")
        else:
            raise RuntimeError(f"Error conectando a SQL Server: {error_msg}")
    except Exception as e:
        raise RuntimeError(f"Error conectando a SQL Server: {e}")


def list_available_databases(client):
    """
    Lista las bases de datos disponibles en ClickHouse.
    """
    try:
        result = client.query("SHOW DATABASES")
        databases = [row[0] for row in result.result_rows] if result.result_rows else []
        return databases
    except Exception:
        return []


def connect_clickhouse(database: str = None):
    """
    Crea una conexión a ClickHouse.
    Si la base de datos no existe, intenta crearla.
    """
    global CH_DATABASE
    
    if database:
        CH_DATABASE = database
    
    if not CH_PASSWORD:
        raise RuntimeError("Falta CH_PASSWORD (definí la variable de entorno).")
    
    # Primero intentar conectarse sin especificar la base de datos (o con "default")
    # para poder verificar/crear la base de datos si es necesario
    try:
        # Conectar primero a una base de datos que siempre existe (default)
        temp_client = clickhouse_connect.get_client(
            host=CH_HOST,
            port=CH_PORT,
            username=CH_USER,
            password=CH_PASSWORD,
            database="default",  # Conectar a "default" primero
            secure=True,
            verify=True
        )
        
        # Verificar si la base de datos existe
        try:
            check_sql = f"EXISTS DATABASE `{CH_DATABASE}`"
            result = temp_client.query(check_sql)
            db_exists = result.result_rows[0][0] == 1 if result.result_rows else False
        except:
            # Si EXISTS no funciona, intentar listar bases de datos
            available_dbs = list_available_databases(temp_client)
            db_exists = CH_DATABASE in available_dbs
        
        if not db_exists:
            # La base de datos no existe, intentar crearla
            print(f"📦 La base de datos '{CH_DATABASE}' no existe. Intentando crearla...")
            try:
                create_sql = f"CREATE DATABASE IF NOT EXISTS `{CH_DATABASE}`"
                temp_client.command(create_sql)
                print(f"[OK] Base de datos '{CH_DATABASE}' creada exitosamente")
            except Exception as create_err:
                # Si falla la creación, listar bases de datos disponibles
                available_dbs = list_available_databases(temp_client)
                temp_client.close()
                
                if available_dbs:
                    db_list = "\n   - ".join(available_dbs[:15])
                    if len(available_dbs) > 15:
                        db_list += f"\n   ... y {len(available_dbs) - 15} más"
                    raise RuntimeError(
                        f"[ERROR] No se pudo crear la base de datos '{CH_DATABASE}'.\n"
                        f"Error: {create_err}\n\n"
                        f"[INFO] Bases de datos disponibles ({len(available_dbs)}):\n   - {db_list}\n\n"
                        f"[INFO] Sugerencias:\n"
                        f"   - Usa una de las bases de datos listadas arriba\n"
                        f"   - Ejemplo: python sqlserver_to_clickhouse_streaming.py POM_DBS default\n"
                        f"   - O crea la base de datos '{CH_DATABASE}' en ClickHouse primero"
                    )
                else:
                    raise RuntimeError(
                        f"[ERROR] No se pudo crear la base de datos '{CH_DATABASE}'.\n"
                        f"Error: {create_err}\n"
                        f"[INFO] No se pudieron listar las bases de datos disponibles. Verifica tus permisos."
                    )
        else:
            print(f"[OK] Base de datos '{CH_DATABASE}' encontrada")
        
        # Cerrar conexión temporal
        temp_client.close()
        
        # Ahora conectar a la base de datos correcta
        client = clickhouse_connect.get_client(
            host=CH_HOST,
            port=CH_PORT,
            username=CH_USER,
            password=CH_PASSWORD,
            database=CH_DATABASE,
            secure=True,  # HTTPS para ClickHouse Cloud
            verify=True
        )
        
        # Probar la conexión
        result = client.query("SELECT 1")
        print(f"[OK] Conectado a ClickHouse: {CH_HOST}:{CH_PORT}/{CH_DATABASE}")
        return client
        
    except RuntimeError:
        # Re-lanzar RuntimeError sin modificar
        raise
    except Exception as e:
        error_msg = str(e)
        if "does not exist" in error_msg.lower() or "UNKNOWN_DATABASE" in error_msg:
            # Intentar listar bases de datos disponibles
            try:
                temp_client = clickhouse_connect.get_client(
                    host=CH_HOST,
                    port=CH_PORT,
                    username=CH_USER,
                    password=CH_PASSWORD,
                    database="default",
                    secure=True,
                    verify=True
                )
                available_dbs = list_available_databases(temp_client)
                temp_client.close()
                
                if available_dbs:
                    db_list = "\n   - ".join(available_dbs[:15])
                    if len(available_dbs) > 15:
                        db_list += f"\n   ... y {len(available_dbs) - 15} más"
                    raise RuntimeError(
                        f"[ERROR] La base de datos '{CH_DATABASE}' no existe.\n"
                        f"Error: {error_msg}\n\n"
                        f"[INFO] Bases de datos disponibles ({len(available_dbs)}):\n   - {db_list}\n\n"
                        f"[INFO] Sugerencias:\n"
                        f"   - Usa una de las bases de datos listadas arriba\n"
                        f"   - Ejemplo: python sqlserver_to_clickhouse_streaming.py POM_DBS default\n"
                        f"   - O crea la base de datos '{CH_DATABASE}' en ClickHouse primero"
                    )
            except:
                pass
            
            raise RuntimeError(
                f"[ERROR] La base de datos '{CH_DATABASE}' no existe.\n"
                f"Error: {error_msg}\n"
                f"[INFO] Verifica el nombre de la base de datos o créala primero en ClickHouse."
            )
        else:
            raise RuntimeError(f"Error conectando a ClickHouse: {error_msg}")


def get_table_columns_sqlserver(conn, table_name: str) -> list:
    """
    Obtiene las columnas de una tabla en SQL Server.
    Retorna lista de tuplas (column_name, data_type, max_length, is_nullable).
    """
    schema, table = table_name.split('.', 1) if '.' in table_name else ('dbo', table_name)
    
    query = """
    SELECT COLUMN_NAME, DATA_TYPE, CHARACTER_MAXIMUM_LENGTH, IS_NULLABLE
    FROM INFORMATION_SCHEMA.COLUMNS
    WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ?
    ORDER BY ORDINAL_POSITION;
    """
    
    cursor = conn.cursor()
    cursor.execute(query, schema, table)
    columns = cursor.fetchall()
    cursor.close()
    
    return columns


def detect_id_column(conn, table_name: str, preferred_id: str = "Id") -> Optional[str]:
    """
    Detecta automáticamente la mejor columna ID para usar en modo incremental.
    
    Prioridad:
    1. Columnas IDENTITY (auto-incrementales)
    2. Primary Keys
    3. Columnas con nombres comunes de ID (Id, ID, id, Codigo, etc.)
    4. Primera columna numérica (INT, BIGINT, etc.)
    
    Args:
        conn: Conexión a SQL Server
        table_name: Nombre completo de la tabla (schema.table)
        preferred_id: Nombre preferido de columna ID (por defecto "Id")
    
    Returns:
        Nombre de la columna ID encontrada, o None si no se encuentra ninguna adecuada
    """
    schema, table = table_name.split('.', 1) if '.' in table_name else ('dbo', table_name)
    
    # 1. Buscar columnas IDENTITY (auto-incrementales) - máxima prioridad
    identity_query = """
    SELECT c.COLUMN_NAME
    FROM INFORMATION_SCHEMA.COLUMNS c
    INNER JOIN sys.columns sc ON sc.object_id = OBJECT_ID(QUOTENAME(c.TABLE_SCHEMA) + '.' + QUOTENAME(c.TABLE_NAME))
        AND sc.name = c.COLUMN_NAME
    INNER JOIN sys.tables st ON st.object_id = sc.object_id
    WHERE c.TABLE_SCHEMA = ? 
        AND c.TABLE_NAME = ?
        AND sc.is_identity = 1
    ORDER BY sc.column_id;
    """
    
    try:
        cursor = conn.cursor()
        cursor.execute(identity_query, schema, table)
        identity_cols = cursor.fetchall()
        if identity_cols:
            id_col = identity_cols[0][0]
            cursor.close()
            return id_col
        cursor.close()
    except:
        pass
    
    # 2. Buscar Primary Keys
    pk_query = """
    SELECT c.COLUMN_NAME
    FROM INFORMATION_SCHEMA.TABLE_CONSTRAINTS tc
    INNER JOIN INFORMATION_SCHEMA.KEY_COLUMN_USAGE kcu 
        ON tc.CONSTRAINT_NAME = kcu.CONSTRAINT_NAME
    INNER JOIN INFORMATION_SCHEMA.COLUMNS c 
        ON kcu.COLUMN_NAME = c.COLUMN_NAME
        AND kcu.TABLE_NAME = c.TABLE_NAME
        AND kcu.TABLE_SCHEMA = c.TABLE_SCHEMA
    WHERE tc.CONSTRAINT_TYPE = 'PRIMARY KEY'
        AND tc.TABLE_SCHEMA = ?
        AND tc.TABLE_NAME = ?
    ORDER BY kcu.ORDINAL_POSITION;
    """
    
    try:
        cursor = conn.cursor()
        cursor.execute(pk_query, schema, table)
        pk_cols = cursor.fetchall()
        if pk_cols:
            # Verificar que sea numérica
            pk_col = pk_cols[0][0]
            # Verificar tipo de dato
            type_query = """
            SELECT DATA_TYPE
            FROM INFORMATION_SCHEMA.COLUMNS
            WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ? AND COLUMN_NAME = ?;
            """
            cursor.execute(type_query, schema, table, pk_col)
            col_type = cursor.fetchone()
            if col_type and col_type[0].upper() in ('INT', 'BIGINT', 'SMALLINT', 'TINYINT', 'DECIMAL', 'NUMERIC'):
                cursor.close()
                return pk_col
        cursor.close()
    except:
        pass
    
    # 3. Buscar columnas con nombres comunes de ID
    common_id_names = [preferred_id, "ID", "id", "Codigo", "CODIGO", "codigo", 
                       "Numero", "NUMERO", "numero", "Code", "CODE", "code",
                       "Key", "KEY", "key", "PK", "pk"]
    
    columns_query = """
    SELECT COLUMN_NAME, DATA_TYPE
    FROM INFORMATION_SCHEMA.COLUMNS
    WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ?
    ORDER BY ORDINAL_POSITION;
    """
    
    try:
        cursor = conn.cursor()
        cursor.execute(columns_query, schema, table)
        all_columns = cursor.fetchall()
        
        # Buscar por nombre común
        for col_name, col_type in all_columns:
            if col_name in common_id_names:
                # Verificar que sea numérica
                if col_type.upper() in ('INT', 'BIGINT', 'SMALLINT', 'TINYINT', 'DECIMAL', 'NUMERIC'):
                    cursor.close()
                    return col_name
        
        # 4. Si no se encuentra por nombre, buscar primera columna numérica
        for col_name, col_type in all_columns:
            if col_type.upper() in ('INT', 'BIGINT', 'SMALLINT', 'TINYINT', 'DECIMAL', 'NUMERIC'):
                cursor.close()
                return col_name
        
        cursor.close()
    except:
        pass
    
    return None


def detect_timestamp_column(conn, table_name: str) -> Optional[str]:
    """
    Detecta automáticamente una columna de fecha/timestamp para usar en modo incremental.
    
    Prioridad:
    1. Columnas con nombres comunes de fecha (created_at, fecha_ingreso, F_Ingreso, etc.)
    2. Cualquier columna de tipo DATE, DATETIME, DATETIME2, TIMESTAMP
    
    Args:
        conn: Conexión a SQL Server
        table_name: Nombre completo de la tabla (schema.table)
    
    Returns:
        Nombre de la columna de fecha encontrada, o None si no se encuentra
    """
    schema, table = table_name.split('.', 1) if '.' in table_name else ('dbo', table_name)
    
    # Nombres comunes de columnas de fecha
    common_date_names = [
        "created_at", "CREATED_AT", "Created_At",
        "fecha_ingreso", "FECHA_INGRESO", "Fecha_Ingreso",
        "f_ingreso", "F_Ingreso", "F_INGRESO",
        "fecha_creacion", "FECHA_CREACION", "Fecha_Creacion",
        "fecha_modificacion", "FECHA_MODIFICACION", "Fecha_Modificacion",
        "fecha", "FECHA", "Fecha",
        "fecha_alta", "FECHA_ALTA", "Fecha_Alta",
        "fecha_insert", "FECHA_INSERT", "Fecha_Insert",
        "timestamp", "TIMESTAMP", "Timestamp",
        "updated_at", "UPDATED_AT", "Updated_At",
        "modified_at", "MODIFIED_AT", "Modified_At"
    ]
    
    columns_query = """
    SELECT COLUMN_NAME, DATA_TYPE
    FROM INFORMATION_SCHEMA.COLUMNS
    WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ?
    ORDER BY ORDINAL_POSITION;
    """
    
    try:
        cursor = conn.cursor()
        cursor.execute(columns_query, schema, table)
        all_columns = cursor.fetchall()
        
        # 1. Buscar por nombre común
        for col_name, col_type in all_columns:
            if col_name in common_date_names:
                # Verificar que sea tipo fecha
                if col_type.upper() in ('DATE', 'DATETIME', 'DATETIME2', 'SMALLDATETIME', 'TIMESTAMP', 'DATETIMEOFFSET'):
                    cursor.close()
                    return col_name
        
        # 2. Buscar cualquier columna de tipo fecha
        for col_name, col_type in all_columns:
            if col_type.upper() in ('DATE', 'DATETIME', 'DATETIME2', 'SMALLDATETIME', 'TIMESTAMP', 'DATETIMEOFFSET'):
                cursor.close()
                return col_name
        
        cursor.close()
    except:
        pass
    
    return None


def normalize_value(value) -> str:
    """
    Normaliza un valor para hashing consistente.
    Maneja datetimes, floats, decimals, bytes, etc.
    """
    if value is None:
        return "NULL"
    elif isinstance(value, (datetime,)):
        # Normalizar datetime a ISO format
        return value.isoformat()
    elif isinstance(value, (int,)):
        return str(value)
    elif isinstance(value, (float,)):
        # Redondear floats a 6 decimales para consistencia
        return f"{value:.6f}"
    elif isinstance(value, (bytes, bytearray)):
        # Convertir bytes a base64
        import base64
        return base64.b64encode(value).decode('ascii')
    elif isinstance(value, (bool,)):
        return "1" if value else "0"
    else:
        # Convertir a string y normalizar encoding
        return str(value).encode('utf-8', errors='replace').decode('utf-8')


def clean_datetime_value(value):
    """
    Limpia y valida un valor datetime para insertar en ClickHouse.
    Convierte valores inválidos a None.
    
    ClickHouse usa timestamp() internamente, que requiere fechas >= 1970-01-01.
    Fechas anteriores causarán OSError: [Errno 22] Invalid argument.
    
    Args:
        value: Valor datetime a validar
    
    Returns:
        datetime válido o None si el valor es inválido
    """
    if value is None:
        return None
    
    # Si ya es un datetime, validarlo
    if isinstance(value, datetime):
        try:
            # Verificar que el datetime tiene atributos válidos
            if not hasattr(value, 'year') or not hasattr(value, 'month') or not hasattr(value, 'day'):
                return None
            
            # Verificar que los componentes son válidos
            year = value.year
            month = value.month
            day = value.day
            
            # Verificar rango básico
            if year < 1970 or year > 2100:
                # Fechas antes de 1970-01-01 no pueden convertirse a timestamp Unix
                # Fechas después de 2100 son poco probables y pueden causar problemas
                return None
            
            # Verificar que los componentes están en rangos válidos
            if month < 1 or month > 12 or day < 1 or day > 31:
                return None
            
            # Intentar convertir a timestamp para validar que es una fecha válida
            # Esto es lo que ClickHouse hace internamente, así que si falla aquí, fallará en ClickHouse
            try:
                timestamp_value = value.timestamp()
                # Verificar que el timestamp es un número válido (positivo para fechas >= 1970)
                if not isinstance(timestamp_value, (int, float)) or timestamp_value < 0:
                    return None
            except (OSError, ValueError, OverflowError) as e:
                # Si no se puede convertir a timestamp, es inválido para ClickHouse
                # Esto captura fechas antes de 1970-01-01 que causan OSError: [Errno 22]
                return None
            
            return value
        except (ValueError, OSError, OverflowError, AttributeError, TypeError):
            # Fecha inválida o fuera de rango
            return None
    
    # Si es un string, intentar parsearlo
    if isinstance(value, str):
        try:
            # Intentar parsear como datetime usando dateutil si está disponible
            try:
                from dateutil import parser
                parsed = parser.parse(value)
                return clean_datetime_value(parsed)  # Validar recursivamente
            except ImportError:
                # Si dateutil no está disponible, intentar con datetime.strptime
                # Intentar formatos comunes
                for fmt in ['%Y-%m-%d %H:%M:%S', '%Y-%m-%d', '%Y-%m-%d %H:%M:%S.%f']:
                    try:
                        parsed = datetime.strptime(value, fmt)
                        return clean_datetime_value(parsed)
                    except ValueError:
                        continue
                return None
        except:
            return None
    
    return None


def calculate_row_key(row_data: tuple, columns: list, key_columns: list) -> str:
    """
    Calcula un hash MD5 de las columnas clave (PK lógica) para identificar la entidad de forma estable.
    
    Args:
        row_data: Tupla con los valores de la fila
        columns: Lista de nombres de columnas
        key_columns: Lista de nombres de columnas que forman la clave lógica
    
    Returns:
        Hash MD5 hexadecimal de la clave lógica
    """
    # Obtener valores de las columnas clave
    key_values = []
    for key_col in key_columns:
        if key_col in columns:
            idx = columns.index(key_col)
            key_values.append(normalize_value(row_data[idx]))
        else:
            key_values.append("NULL")
    
    # Crear representación consistente de la clave
    key_string = "|".join([f"{col}:{val}" for col, val in zip(key_columns, key_values)])
    
    # Calcular hash MD5
    hash_obj = hashlib.md5(key_string.encode('utf-8'))
    return hash_obj.hexdigest()


def calculate_row_hash(row_data: tuple, columns: list) -> str:
    """
    Calcula un hash MD5 de toda la fila para detectar cambios en el contenido (CDC artesanal).
    Usa normalización explícita para garantizar consistencia.
    
    IMPORTANTE: Este hash cambia cuando cualquier columna cambia.
    Se usa para detectar si el contenido de la entidad cambió.
    
    Args:
        row_data: Tupla con los valores de la fila
        columns: Lista de nombres de columnas
    
    Returns:
        Hash MD5 hexadecimal del contenido de la fila
    """
    # Normalizar cada valor y crear representación consistente
    normalized_parts = []
    for col, val in zip(columns, row_data):
        normalized_val = normalize_value(val)
        normalized_parts.append(f"{col}:{normalized_val}")
    
    # Unir todas las partes normalizadas (ordenadas por nombre de columna)
    # Ordenar por nombre de columna para garantizar consistencia
    sorted_parts = sorted(normalized_parts)
    row_string = "|".join(sorted_parts)
    
    # Calcular hash MD5
    hash_obj = hashlib.md5(row_string.encode('utf-8'))
    return hash_obj.hexdigest()


def get_existing_hashes_for_chunk(ch_client, table_name: str, hashes: list, column_name: str = "row_hash") -> set:
    """
    Obtiene solo los hashes/keys del chunk actual que ya existen en ClickHouse.
    Esto escala mucho mejor que cargar todos los hashes.
    
    Args:
        ch_client: Cliente de ClickHouse
        table_name: Nombre completo de la tabla
        hashes: Lista de hashes/keys del chunk actual a verificar
        column_name: Nombre de la columna a verificar ("row_hash" o "row_key")
    
    Returns:
        Set de hashes/keys que ya existen
    """
    if not hashes:
        return set()
    
    try:
        # Construir query con IN para solo los hashes/keys del chunk
        # Limitar a 1000 hashes por query para evitar queries gigantes
        existing = set()
        batch_size = 1000
        
        for i in range(0, len(hashes), batch_size):
            batch = hashes[i:i+batch_size]
            # Escapar hashes para SQL (son hex strings, así que son seguros)
            hash_list = "', '".join(batch)
            query = f"SELECT DISTINCT `{column_name}` FROM {table_name} WHERE `{column_name}` IN ('{hash_list}')"
            result = ch_client.query(query)
            if result.result_rows:
                existing.update(row[0] for row in result.result_rows if row[0])
        
        return existing
    except:
        # Si la columna no existe o hay error, retornar set vacío
        return set()


def get_existing_row_hashes_by_key(ch_client, table_name: str, row_keys: list) -> dict:
    """
    Obtiene los row_hash existentes en ClickHouse para los row_keys del chunk.
    Retorna un diccionario {row_key: row_hash} para comparar si el contenido cambió.
    
    Args:
        ch_client: Cliente de ClickHouse
        table_name: Nombre completo de la tabla
        row_keys: Lista de row_keys del chunk actual a verificar
    
    Returns:
        Diccionario {row_key: row_hash} de entidades existentes (más reciente por row_key)
    """
    if not row_keys:
        return {}
    
    try:
        existing = {}
        batch_size = 1000
        
        for i in range(0, len(row_keys), batch_size):
            batch = row_keys[i:i+batch_size]
            key_list = "', '".join(batch)
            # Obtener el row_hash más reciente para cada row_key (por ingested_at)
            # Usamos ReplacingMergeTree, así que necesitamos el más reciente
            query = f"""
            SELECT `row_key`, `row_hash`
            FROM (
                SELECT `row_key`, `row_hash`, 
                       ROW_NUMBER() OVER (PARTITION BY `row_key` ORDER BY `ingested_at` DESC) as rn
                FROM {table_name}
                WHERE `row_key` IN ('{key_list}')
            )
            WHERE rn = 1
            """
            result = ch_client.query(query)
            if result.result_rows:
                for row in result.result_rows:
                    if row[0] and row[1]:
                        existing[row[0]] = row[1]
        
        return existing
    except:
        # Si hay error, retornar diccionario vacío
        return {}


def get_existing_ids(ch_client, table_name: str, id_column: str, lookback_days: Optional[int] = None) -> set:
    """
    Obtiene IDs existentes en ClickHouse, opcionalmente filtrados por lookback window.
    Convierte todos los IDs a int para consistencia (maneja Decimal, float, etc.).
    
    Args:
        ch_client: Cliente de ClickHouse
        table_name: Nombre completo de la tabla
        id_column: Nombre de la columna ID
        lookback_days: Si se especifica, solo retorna IDs de registros dentro del lookback window
    
    Returns:
        Set de IDs existentes (como enteros)
    """
    try:
        safe_id_col = sanitize_token(id_column)
        
        if lookback_days:
            # Solo IDs dentro del lookback window (para detectar updates)
            lookback_date = datetime.now() - timedelta(days=lookback_days)
            query = f"""
            SELECT DISTINCT `{safe_id_col}` 
            FROM {table_name} 
            WHERE `ingested_at` >= '{lookback_date.strftime('%Y-%m-%d %H:%M:%S')}'
            """
        else:
            # Todos los IDs
            query = f"SELECT DISTINCT `{safe_id_col}` FROM {table_name}"
        
        result = ch_client.query(query)
        if result.result_rows:
            # Convertir todos los IDs a int para consistencia (maneja Decimal, float, etc.)
            from decimal import Decimal
            ids = set()
            for row in result.result_rows:
                if row[0] is not None:
                    value = row[0]
                    # Convertir Decimal, float, int, etc. a int
                    if isinstance(value, Decimal):
                        ids.add(int(value))
                    elif isinstance(value, float):
                        ids.add(int(value))
                    else:
                        ids.add(int(value))
            return ids
        return set()
    except Exception as e:
        # Si hay error, retornar set vacío para que el script continúe
        return set()


def detect_logical_key(conn, table_name: str, preferred_id: str = "Id") -> Optional[Tuple[list, str]]:
    """
    Detecta la clave lógica (PK lógica) para identificar entidades de forma estable.
    
    Estrategia en cascada:
    1. Primary Key (simple o compuesta)
    2. Columna ID (numérica, identity)
    3. Business keys comunes (Codigo, Numero, etc.)
    4. None (usar hash de toda la fila como último recurso)
    
    Args:
        conn: Conexión a SQL Server
        table_name: Nombre completo de la tabla (schema.table)
        preferred_id: Nombre preferido de columna ID
    
    Returns:
        Tupla (lista_columnas, tipo) donde tipo es "pk", "id", "business_key" o None
        None si no se encuentra clave lógica (usar hash de toda la fila)
    """
    schema, table = table_name.split('.', 1) if '.' in table_name else ('dbo', table_name)
    
    # 1. Buscar Primary Key (simple o compuesta)
    pk_query = """
    SELECT c.COLUMN_NAME
    FROM INFORMATION_SCHEMA.TABLE_CONSTRAINTS tc
    INNER JOIN INFORMATION_SCHEMA.KEY_COLUMN_USAGE kcu 
        ON tc.CONSTRAINT_NAME = kcu.CONSTRAINT_NAME
    INNER JOIN INFORMATION_SCHEMA.COLUMNS c
        ON kcu.COLUMN_NAME = c.COLUMN_NAME
        AND kcu.TABLE_NAME = c.TABLE_NAME
        AND kcu.TABLE_SCHEMA = c.TABLE_SCHEMA
    WHERE tc.CONSTRAINT_TYPE = 'PRIMARY KEY'
        AND tc.TABLE_SCHEMA = ?
        AND tc.TABLE_NAME = ?
    ORDER BY kcu.ORDINAL_POSITION;
    """
    
    try:
        cursor = conn.cursor()
        cursor.execute(pk_query, schema, table)
        pk_cols = [row[0] for row in cursor.fetchall()]
        cursor.close()
        if pk_cols:
            return (pk_cols, "pk")
    except:
        pass
    
    # 2. Buscar columna ID (identity o numérica común)
    id_col = detect_id_column(conn, table_name, preferred_id)
    if id_col:
        return ([id_col], "id")
    
    # 3. Buscar business keys comunes
    business_key_names = ["Codigo", "CODIGO", "codigo", "Code", "CODE", "code",
                         "Numero", "NUMERO", "numero", "Number", "NUMBER", "number",
                         "Clave", "CLAVE", "clave", "Key", "KEY", "key"]
    
    columns_query = """
    SELECT COLUMN_NAME, DATA_TYPE
    FROM INFORMATION_SCHEMA.COLUMNS
    WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ?
    ORDER BY ORDINAL_POSITION;
    """
    
    try:
        cursor = conn.cursor()
        cursor.execute(columns_query, schema, table)
        all_columns = cursor.fetchall()
        
        # Buscar business keys
        for col_name, col_type in all_columns:
            if col_name in business_key_names:
                # Verificar que sea tipo adecuado (numérica o string)
                if col_type.upper() in ('INT', 'BIGINT', 'SMALLINT', 'TINYINT', 'DECIMAL', 'NUMERIC', 
                                       'VARCHAR', 'NVARCHAR', 'CHAR', 'NCHAR'):
                    cursor.close()
                    return ([col_name], "business_key")
        
        cursor.close()
    except:
        pass
    
    # 4. No se encontró clave lógica - usar hash de toda la fila como último recurso
    return None


def detect_incremental_column(conn, table_name: str, preferred_id: str = "Id") -> Tuple[Optional[str], str]:
    """
    Detecta automáticamente la mejor columna para modo incremental.
    
    Estrategia en cascada:
    1. Columna ID (numérica, identity, PK)
    2. Columna de fecha/timestamp
    3. Modo hash (CDC artesanal) - siempre disponible como fallback
    
    Args:
        conn: Conexión a SQL Server
        table_name: Nombre completo de la tabla (schema.table)
        preferred_id: Nombre preferido de columna ID
    
    Returns:
        Tupla (nombre_columna, tipo) donde tipo es "id", "timestamp" o "hash"
    """
    # Primero intentar detectar ID
    id_col = detect_id_column(conn, table_name, preferred_id)
    if id_col:
        return (id_col, "id")
    
    # Si no hay ID, intentar detectar fecha/timestamp
    timestamp_col = detect_timestamp_column(conn, table_name)
    if timestamp_col:
        return (timestamp_col, "timestamp")
    
    # Si no hay ID ni fecha, usar modo hash (CDC artesanal - siempre disponible)
    return (None, "hash")


def ensure_tracking_columns(client, table_name: str, incremental_type: str):
    """
    Verifica y agrega columnas de seguimiento (ingested_at, row_key, row_hash) si faltan.
    Esto permite que tablas antiguas funcionen con el nuevo sistema de seguimiento.
    
    Args:
        client: Cliente de ClickHouse
        table_name: Nombre completo de la tabla (con database)
        incremental_type: Tipo de modo incremental ("id", "timestamp", "hash")
    """
    try:
        # Obtener columnas existentes
        desc_sql = f"DESCRIBE TABLE {table_name}"
        result = client.query(desc_sql)
        existing_columns = {row[0] for row in result.result_rows if row}
        
        # Verificar y agregar ingested_at si falta
        if 'ingested_at' not in existing_columns:
            try:
                alter_sql = f"ALTER TABLE {table_name} ADD COLUMN IF NOT EXISTS `ingested_at` DateTime64(3, '{CH_TIMEZONE}') DEFAULT now64()"
                client.command(alter_sql)
                print(f"  [INFO] Columna 'ingested_at' agregada a tabla existente")
            except Exception as e:
                print(f"  [WARN] No se pudo agregar columna 'ingested_at': {e}")
        
        # Si es modo hash, verificar y agregar row_key y row_hash si faltan
        if incremental_type == "hash":
            if 'row_key' not in existing_columns:
                try:
                    alter_sql = f"ALTER TABLE {table_name} ADD COLUMN IF NOT EXISTS `row_key` String"
                    client.command(alter_sql)
                    print(f"  [INFO] Columna 'row_key' agregada a tabla existente")
                except Exception as e:
                    print(f"  [WARN] No se pudo agregar columna 'row_key': {e}")
            
            if 'row_hash' not in existing_columns:
                try:
                    alter_sql = f"ALTER TABLE {table_name} ADD COLUMN IF NOT EXISTS `row_hash` String"
                    client.command(alter_sql)
                    print(f"  [INFO] Columna 'row_hash' agregada a tabla existente")
                except Exception as e:
                    print(f"  [WARN] No se pudo agregar columna 'row_hash': {e}")
    except Exception as e:
        # Si hay error verificando columnas, continuar (la tabla podría no tener permisos o no existir)
        print(f"  [WARN] No se pudieron verificar columnas de seguimiento: {e}")


def create_clickhouse_table(
    client, 
    table_name: str, 
    columns: list, 
    if_exists: str = "replace",
    use_replacing_merge_tree: bool = None,
    version_column: str = "ingested_at",
    incremental_type: str = "id",
    incremental_column: Optional[str] = None
) -> bool:
    """
    Crea una tabla en ClickHouse basada en las columnas de SQL Server.
    
    Args:
        client: Cliente de ClickHouse
        table_name: Nombre de la tabla (sin database)
        columns: Lista de tuplas (column_name, data_type, max_length, is_nullable) de SQL Server
        if_exists: "replace", "append" o "skip"
        use_replacing_merge_tree: Si es True, usa ReplacingMergeTree para deduplicación
        version_column: Columna para versionado en ReplacingMergeTree
        incremental_type: Tipo de modo incremental ("id", "timestamp", "hash")
        incremental_column: Columna para modo incremental (si aplica)
    """
    full_table_name = f"`{CH_DATABASE}`.`{table_name}`"
    
    # Verificar si la tabla existe
    try:
        check_sql = f"EXISTS TABLE {full_table_name}"
        result = client.query(check_sql)
        table_exists = result.result_rows[0][0] == 1 if result.result_rows else False
    except:
        table_exists = False
    
    if table_exists and if_exists == "replace":
        print(f"   Reemplazando tabla existente: {table_name}")
        drop_sql = f"DROP TABLE IF EXISTS {full_table_name}"
        client.command(drop_sql)
        table_exists = False
    elif table_exists and if_exists == "skip":
        print(f"  [SKIP]  Tabla {table_name} ya existe, omitiendo creación")
        return False
    elif table_exists and if_exists == "append":
        # Tabla existe y queremos hacer append: verificar y agregar columnas de seguimiento si faltan
        ensure_tracking_columns(client, full_table_name, incremental_type)
    
    # Determinar si usar ReplacingMergeTree (por defecto True si no se especifica)
    if use_replacing_merge_tree is None:
        use_replacing_merge_tree = USE_REPLACING_MERGE_TREE
    
    # Mapear tipos de datos de SQL Server a ClickHouse
    def map_data_type(sql_type: str, max_length: Optional[int]) -> str:
        sql_type_upper = sql_type.upper()
        
        if sql_type_upper in ('VARCHAR', 'NVARCHAR', 'CHAR', 'NCHAR', 'TEXT', 'NTEXT'):
            # ClickHouse String puede ser muy grande
            return "String"
        elif sql_type_upper in ('INT', 'INTEGER'):
            return "Int32"
        elif sql_type_upper in ('BIGINT',):
            return "Int64"
        elif sql_type_upper in ('SMALLINT',):
            return "Int16"
        elif sql_type_upper in ('TINYINT',):
            return "Int8"
        elif sql_type_upper in ('DECIMAL', 'NUMERIC'):
            return "Decimal64(2)"  # Ajustar según necesidad
        elif sql_type_upper in ('FLOAT', 'REAL'):
            return "Float32"
        elif sql_type_upper in ('DOUBLE', 'DOUBLE PRECISION'):
            return "Float64"
        elif sql_type_upper in ('BIT',):
            return "UInt8"
        elif sql_type_upper in ('DATE',):
            return "Date"
        elif sql_type_upper in ('TIME',):
            return "String"  # ClickHouse no tiene tipo TIME nativo
        elif sql_type_upper in ('DATETIME', 'DATETIME2'):
            # Usar DateTime64(3) con timezone para mejor precisión
            return f"DateTime64(3, '{CH_TIMEZONE}')"
        elif sql_type_upper in ('SMALLDATETIME',):
            # SMALLDATETIME tiene precisión de minutos, pero usamos DateTime64(3) para consistencia
            return f"DateTime64(3, '{CH_TIMEZONE}')"
        elif sql_type_upper in ('DATETIMEOFFSET',):
            return f"DateTime64(3, '{CH_TIMEZONE}')"
        elif sql_type_upper in ('UNIQUEIDENTIFIER',):
            return "String"
        elif sql_type_upper in ('BINARY', 'VARBINARY', 'IMAGE'):
            return "String"  # ClickHouse no tiene tipo BINARY nativo
        else:
            return "String"  # Tipo por defecto
    
    # Crear definición de columnas
    column_defs = []
    for col_info in columns:
        # Manejar tanto el formato antiguo (3 elementos) como el nuevo (4 elementos)
        if len(col_info) == 4:
            col_name, sql_type, max_length, is_nullable = col_info
        else:
            col_name, sql_type, max_length = col_info
            is_nullable = 'YES'  # Por defecto, asumir nullable para compatibilidad
        
        safe_col_name = sanitize_token(col_name)
        clickhouse_type = map_data_type(sql_type, max_length)
        
        # Si la columna se usará en ORDER BY (ID incremental), no hacerla nullable
        # ClickHouse no permite columnas nullable en ORDER BY si allow_nullable_key está deshabilitado
        is_order_by_column = (incremental_type == "id" and 
                             incremental_column and 
                             col_name == incremental_column)
        
        if is_order_by_column and is_nullable == 'NO':
            # Columna ID que será usada en ORDER BY y no es nullable en SQL Server
            # Mantenerla como no-nullable
            pass  # No agregar Nullable()
        else:
            # Hacer todas las demás columnas nullable por defecto para evitar problemas con NULL
            clickhouse_type = f"Nullable({clickhouse_type})"
        
        column_defs.append(f"`{safe_col_name}` {clickhouse_type}")
    
    # Agregar columna de metadatos (usar DateTime64 para consistencia)
    column_defs.append(f"`ingested_at` DateTime64(3, '{CH_TIMEZONE}') DEFAULT now64()")
    
    # Si es modo hash, agregar row_key (identificador estable) y row_hash (detección de cambios)
    if incremental_type == "hash":
        column_defs.append("`row_key` String")  # Clave lógica estable (PK lógica)
        column_defs.append("`row_hash` String")  # Hash del contenido (detecta cambios)
    
    # Determinar ORDER BY según el tipo incremental
    # CRÍTICO: ORDER BY debe ser por la clave de deduplicación, no por ingested_at
    # ReplacingMergeTree deduplica por la clave ORDER BY, no por la columna de versión
    
    if incremental_type == "hash":
        # Modo hash: ORDER BY row_key (clave lógica estable) para que ReplacingMergeTree reemplace correctamente
        # row_key identifica la entidad, row_hash detecta cambios en el contenido
        order_by = "ORDER BY (`row_key`)"
        print(f"   ORDER BY: row_key (clave lógica estable para reemplazo de entidades)")
    elif incremental_type == "id" and incremental_column:
        # Verificar si la columna ID es nullable
        id_col_info = next((col for col in columns if col[0] == incremental_column), None)
        id_is_nullable = id_col_info and len(id_col_info) >= 4 and id_col_info[3] == 'YES'
        
        if not id_is_nullable:
            # Si la columna ID no es nullable, usarla en ORDER BY
            safe_id_col = sanitize_token(incremental_column)
            order_by = f"ORDER BY (`{safe_id_col}`)"
            print(f"   ORDER BY: {safe_id_col} (para deduplicación por ID)")
        else:
            # Si es nullable, usar ingested_at como fallback (pero no ideal para dedupe)
            order_by = "ORDER BY (`ingested_at`)"
            print(f"  [WARN]  Columna ID '{incremental_column}' es nullable, usando 'ingested_at' en ORDER BY (dedupe limitado)")
    elif incremental_type == "timestamp" and incremental_column:
        # Modo timestamp: ORDER BY por la columna de fecha
        safe_timestamp_col = sanitize_token(incremental_column)
        order_by = f"ORDER BY (`{safe_timestamp_col}`)"
        print(f"   ORDER BY: {safe_timestamp_col} (para deduplicación por fecha)")
    else:
        # Fallback: usar ingested_at
        order_by = "ORDER BY (`ingested_at`)"
        print(f"  [WARN]  ORDER BY: ingested_at (sin deduplicación automática)")
    
    # Determinar ENGINE según configuración
    if use_replacing_merge_tree:
        safe_version_col = sanitize_token(version_column)
        engine_clause = f"ENGINE = ReplacingMergeTree(`{safe_version_col}`)"
        print(f"   Usando ReplacingMergeTree con versión: {safe_version_col}")
    else:
        engine_clause = "ENGINE = MergeTree()"
    
    create_sql = f"""
    CREATE TABLE IF NOT EXISTS {full_table_name} (
        {', '.join(column_defs)}
    ) {engine_clause}
    {order_by};
    """
    
    client.command(create_sql)
    print(f"  [OK] Tabla creada: {table_name} ({len(columns)} columnas)")
    return True


def get_max_value_from_clickhouse(ch_client, table_name: str, column: str, column_type: str = "id") -> Optional[any]:
    """
    Obtiene el máximo valor de una columna en ClickHouse (ID o fecha).
    
    Args:
        ch_client: Cliente de ClickHouse
        table_name: Nombre completo de la tabla (con database)
        column: Nombre de la columna
        column_type: Tipo de columna ("id" o "timestamp")
    
    Returns:
        El máximo valor encontrado, o None si la tabla está vacía o no existe
    """
    try:
        safe_col = sanitize_token(column)
        query = f"SELECT max(`{safe_col}`) FROM {table_name}"
        result = ch_client.query(query)
        
        # Verificar que hay resultados y que el resultado es válido
        if not result.result_rows:
            return None
        
        # ClickHouse siempre retorna una lista de tuplas
        # result.result_rows[0] es una tupla con los valores de las columnas
        first_row = result.result_rows[0]
        
        # Manejar diferentes tipos de retorno de forma segura
        value = None
        try:
            # Primero verificar si es una tupla/lista
            if isinstance(first_row, (list, tuple)):
                # Es una tupla/lista: tomar el primer elemento
                if len(first_row) > 0:
                    value = first_row[0]
            # Si es un tipo numérico directamente (caso raro pero posible)
            elif isinstance(first_row, (int, float)):
                value = first_row
            # Si es Decimal
            elif isinstance(first_row, Decimal):
                value = first_row
            # Si es None
            elif first_row is None:
                return None
            # Otros tipos: intentar acceder como tupla/lista
            else:
                try:
                    # Verificar si tiene método __getitem__ antes de usarlo
                    if hasattr(first_row, '__getitem__') and hasattr(first_row, '__len__'):
                        if len(first_row) > 0:
                            value = first_row[0]
                    elif hasattr(first_row, '__getitem__'):
                        # Intentar acceder sin verificar len
                        value = first_row[0]
                    else:
                        # Usar el valor directamente
                        value = first_row
                except (TypeError, IndexError, AttributeError) as e:
                    # Si no se puede acceder, usar el valor directamente
                    value = first_row
        except Exception as e:
            # Si hay cualquier error, intentar usar el valor directamente
            try:
                value = first_row if first_row is not None else None
            except:
                return None
        
        # Verificar que el valor no sea None
        if value is None:
            return None
        
        # Convertir Decimal a int si es necesario
        if column_type == "id":
            # Manejar Decimal, float, int, etc.
            if isinstance(value, Decimal):
                return int(value)
            elif isinstance(value, float):
                return int(value)
            else:
                return int(value)
        else:  # timestamp
            return value  # Retornar como datetime/string
    except Exception as e:
        # Si la tabla no existe, no tiene la columna, o hay error de conversión, retornar None
        # Esto permite que el script continúe procesando desde el inicio si es necesario
        return None


def stream_table_to_clickhouse(
    sql_conn, 
    ch_client, 
    table_name: str, 
    target_table_name: str,
    chunk_size: int = CHUNK_SIZE,
    max_rows: Optional[int] = None,
    incremental: bool = False,
    incremental_column: Optional[str] = None,
    incremental_type: str = "id",
    lookback_days: Optional[int] = None,
    logical_key: Optional[list] = None
) -> Tuple[int, int]:
    """
    Hace streaming de una tabla de SQL Server a ClickHouse.
    
    Args:
        sql_conn: Conexión a SQL Server
        ch_client: Cliente de ClickHouse
        table_name: Nombre completo de la tabla en SQL Server (schema.table)
        target_table_name: Nombre de la tabla destino en ClickHouse
        chunk_size: Tamaño del chunk para streaming
        max_rows: Número máximo de filas a procesar (None = todas)
        incremental: Si es True, solo procesa registros nuevos
        incremental_column: Nombre de la columna para modo incremental (ID o fecha)
        incremental_type: Tipo de columna ("id", "timestamp" o "hash")
        lookback_days: Días hacia atrás para lookback window (detectar updates/deletes)
        logical_key: Lista de columnas que forman la clave lógica (PK lógica) para modo hash
    
    Returns:
        (row_count, col_count)
    """
    # Eliminado pandas - trabajar directamente con listas para mejor rendimiento
    import math  # Para verificar NaN sin pandas
    
    schema, table = table_name.split('.', 1) if '.' in table_name else ('dbo', table_name)
    full_table_name = f"`{CH_DATABASE}`.`{target_table_name}`"
    
    # NO cargar todos los hashes de una vez (no escala)
    # En su lugar, verificaremos por chunk (ver más abajo)
    if incremental and incremental_type == "hash":
        print(f"     Modo incremental (hash): verificando duplicados por chunk (escalable)")
    
    # Si es modo ID con lookback window, obtener IDs existentes en el rango
    existing_ids = set()
    if incremental and incremental_type == "id" and incremental_column and lookback_days:
        existing_ids = get_existing_ids(ch_client, full_table_name, incremental_column, lookback_days)
        if existing_ids:
            print(f"     Lookback window ({lookback_days} días): {len(existing_ids)} IDs en rango (para detectar updates)")
    
    # Si es incremental, obtener el máximo valor de ClickHouse
    last_value = None
    if incremental and incremental_column and incremental_type != "hash":
        last_value = get_max_value_from_clickhouse(ch_client, full_table_name, incremental_column, incremental_type)
        if last_value is not None:
            if incremental_type == "id":
                print(f"     Modo incremental (ID): último valor procesado = {last_value}")
            else:
                print(f"     Modo incremental (fecha): última fecha procesada = {last_value}")
    
    # Construir query SQL
    where_clause = ""
    order_clause = ""
    
    if incremental and incremental_column and incremental_type != "hash" and last_value is not None:
        safe_col = f"[{incremental_column}]"
        if incremental_type == "id":
            # Asegurar que last_value sea int (maneja Decimal, float, etc.)
            last_value_int = int(last_value) if last_value is not None else None
            where_clause = f"WHERE {safe_col} > {last_value_int}"
        else:  # timestamp
            # Para fechas, usar formato SQL Server
            where_clause = f"WHERE {safe_col} > '{last_value}'"
        order_clause = f"ORDER BY {safe_col}"
    elif incremental and incremental_type == "id" and lookback_days and existing_ids:
        # Lookback window: intentar usar rango de fechas si hay columna updated_at/modified_at
        # Si no, usar IN() solo si son pocos IDs
        safe_col = f"[{incremental_column}]"
        
        # Intentar detectar columna de fecha de modificación para usar rango en lugar de IN()
        try:
            schema, table = table_name.split('.', 1) if '.' in table_name else ('dbo', table_name)
            updated_at_col = detect_timestamp_column(sql_conn, table_name)
            
            if updated_at_col and len(existing_ids) > 1000:
                # Usar rango de fechas en lugar de IN() grande
                lookback_date = datetime.now() - timedelta(days=lookback_days)
                safe_updated_col = f"[{updated_at_col}]"
                if last_value is not None:
                    last_value_int = int(last_value) if last_value is not None else None
                    where_clause = f"WHERE ({safe_col} > {last_value_int} OR {safe_updated_col} >= '{lookback_date.strftime('%Y-%m-%d %H:%M:%S')}')"
                else:
                    where_clause = f"WHERE {safe_updated_col} >= '{lookback_date.strftime('%Y-%m-%d %H:%M:%S')}'"
                print(f"    [OK] Lookback window usando rango de fechas ({updated_at_col}) en lugar de IN() grande")
                order_clause = f"ORDER BY {safe_col}"
            elif len(existing_ids) > 1000:
                # No hay columna de fecha, pero hay muchos IDs - procesar solo nuevos
                print(f"    [WARN]  Lookback window tiene {len(existing_ids)} IDs, procesando solo nuevos (evitando IN() grande)")
                print(f"    [INFO] Sugerencia: Si la tabla tiene columna updated_at/modified_at, se usará rango de fechas automáticamente")
                if last_value is not None:
                    last_value_int = int(last_value) if last_value is not None else None
                    where_clause = f"WHERE {safe_col} > {last_value_int}"
                else:
                    where_clause = ""
                order_clause = f"ORDER BY {safe_col}"
            else:
                # Pocos IDs, usar IN() es aceptable
                id_list = ','.join(map(str, existing_ids))
                if last_value is not None:
                    last_value_int = int(last_value) if last_value is not None else None
                    where_clause = f"WHERE ({safe_col} > {last_value_int} OR {safe_col} IN ({id_list}))"
                else:
                    where_clause = f"WHERE {safe_col} IN ({id_list})"
                order_clause = f"ORDER BY {safe_col}"
        except:
            # Si falla la detección, usar lógica simple
            if len(existing_ids) > 1000:
                print(f"    [WARN]  Lookback window tiene {len(existing_ids)} IDs, procesando solo nuevos (evitando IN() grande)")
                if last_value is not None:
                    last_value_int = int(last_value) if last_value is not None else None
                    where_clause = f"WHERE {safe_col} > {last_value_int}"
                else:
                    where_clause = ""
            else:
                id_list = ','.join(map(str, existing_ids))
                if last_value is not None:
                    last_value_int = int(last_value) if last_value is not None else None
                    where_clause = f"WHERE ({safe_col} > {last_value_int} OR {safe_col} IN ({id_list}))"
                else:
                    where_clause = f"WHERE {safe_col} IN ({id_list})"
            order_clause = f"ORDER BY {safe_col}"
    
    # Construir query con TOP si se especifica max_rows
    if max_rows and max_rows > 0:
        query = f"SELECT TOP {max_rows} * FROM [{schema}].[{table}] {where_clause} {order_clause}"
        print(f"    [WARN]  Limitando a {max_rows} registros")
    else:
        query = f"SELECT * FROM [{schema}].[{table}] {where_clause} {order_clause}"
    
    sql_cursor = None
    # Inicializar variables fuera del try para que estén disponibles después del finally
    row_count = 0
    new_rows_count = 0
    updated_rows_count = 0
    duplicate_rows_count = 0
    chunk_num = 0
    chunk_times = []  # Para calcular tiempo promedio
    start_time = time.time()
    col_count = 0
    columns = []
    safe_columns = []
    
    try:
        sql_cursor = sql_conn.cursor()
        sql_cursor.execute(query)
        
        # Obtener nombres de columnas
        columns = [desc[0] for desc in sql_cursor.description]
        col_count = len(columns)
        
        # Sanitizar nombres de columnas para ClickHouse
        safe_columns = [sanitize_token(col) for col in columns]
        
        full_table_name = f"`{CH_DATABASE}`.`{target_table_name}`"
        
        print(f"     Iniciando streaming (chunk size: {chunk_size})...")
        start_time = time.time()
        
        while True:
            # Leer chunk desde SQL Server
            rows = sql_cursor.fetchmany(chunk_size)
            
            if not rows:
                break
            
            # Si hay límite de filas, ajustar el chunk para no excederlo
            if max_rows and max_rows > 0:
                remaining = max_rows - row_count
                if remaining <= 0:
                    break
                if len(rows) > remaining:
                    rows = rows[:remaining]
            
            chunk_num += 1
            chunk_start_time = time.time()
            
            # Convertir filas de pyodbc a lista de listas
            rows_list = [list(row) for row in rows]
            
            # Si es modo hash, calcular row_key (identificador estable) y row_hash (detección de cambios)
            if incremental_type == "hash":
                # Calcular row_key y row_hash para todas las filas del chunk
                chunk_keys = []
                chunk_hashes = []
                for row_data in rows_list:
                    # Calcular row_key (clave lógica estable)
                    if logical_key:
                        # Usar clave lógica detectada (PK, ID, business key)
                        row_key = calculate_row_key(tuple(row_data), columns, logical_key)
                    else:
                        # Como último recurso, usar hash de toda la fila como row_key
                        # Esto solo deduplicará filas idénticas, no hará updates reales
                        row_key = calculate_row_hash(tuple(row_data), columns)
                    
                    # Calcular row_hash (hash del contenido para detectar cambios)
                    row_hash = calculate_row_hash(tuple(row_data), columns)
                    
                    chunk_keys.append(row_key)
                    chunk_hashes.append(row_hash)
                
                # Obtener row_hash existentes por row_key para comparar si el contenido cambió
                existing_row_hashes = get_existing_row_hashes_by_key(ch_client, full_table_name, chunk_keys)
                
                # Filtrar y clasificar: nuevas, updates, duplicadas
                filtered_rows = []
                for i, row_data in enumerate(rows_list):
                    row_key = chunk_keys[i]
                    row_hash = chunk_hashes[i]
                    
                    if row_key not in existing_row_hashes:
                        # Nueva entidad: no existe en ClickHouse
                        row_data.append(row_key)  # Primero row_key
                        row_data.append(row_hash)  # Luego row_hash
                        filtered_rows.append(row_data)
                        new_rows_count += 1
                    else:
                        # Entidad existente: verificar si el contenido cambió
                        existing_hash = existing_row_hashes[row_key]
                        if row_hash != existing_hash:
                            # Contenido cambió: insertar para que ReplacingMergeTree lo reemplace
                            row_data.append(row_key)
                            row_data.append(row_hash)
                            filtered_rows.append(row_data)
                            updated_rows_count += 1
                        else:
                            # Mismo contenido: duplicado real, omitir
                            duplicate_rows_count += 1
                
                if not filtered_rows:
                    print(f"    [SKIP]  Chunk {chunk_num}: todas las filas ya existen (duplicadas)")
                    row_count += len(rows_list)
                    continue
                
                rows_list = filtered_rows
                # Agregar 'row_key' y 'row_hash' a las columnas (en ese orden)
                safe_columns_with_keys = safe_columns + ['row_key', 'row_hash']
                columns_to_use = safe_columns_with_keys
            else:
                columns_to_use = safe_columns
            
            # Si es modo ID con lookback window, detectar updates
            if incremental_type == "id" and incremental_column and lookback_days and existing_ids:
                # Identificar cuáles son updates (ya existen en el rango)
                updated_rows = []
                new_rows = []
                id_col_index = columns.index(incremental_column) if incremental_column in columns else None
                
                for row_data in rows_list:
                    if id_col_index is not None:
                        # Convertir el ID a int para comparar (maneja Decimal, float, etc.)
                        try:
                            from decimal import Decimal
                            row_id = row_data[id_col_index]
                            if row_id is not None:
                                # Convertir a int para comparar con existing_ids (que son int)
                                if isinstance(row_id, Decimal):
                                    row_id_int = int(row_id)
                                elif isinstance(row_id, float):
                                    row_id_int = int(row_id)
                                else:
                                    row_id_int = int(row_id)
                                
                                if row_id_int in existing_ids:
                                    updated_rows.append(row_data)
                                    updated_rows_count += 1
                                else:
                                    new_rows.append(row_data)
                                    new_rows_count += 1
                            else:
                                new_rows.append(row_data)
                                new_rows_count += 1
                        except (ValueError, TypeError):
                            # Si no se puede convertir, tratarlo como nuevo
                            new_rows.append(row_data)
                            new_rows_count += 1
                    else:
                        new_rows.append(row_data)
                        new_rows_count += 1
                
                if updated_rows:
                    print(f"     Chunk {chunk_num}: {len(updated_rows)} updates detectados, {len(new_rows)} nuevos")
                rows_list = updated_rows + new_rows
            
            # Convertir directamente a lista de listas (sin pandas para mejor rendimiento)
            # Limpiar None/NaN y convertir Decimal a tipos nativos
            import math  # Para verificar NaN sin pandas
            from decimal import Decimal
            data = []
            for row in rows_list:
                # Convertir a lista y limpiar None/NaN/Decimal
                cleaned_row = []
                for val in row:
                    if val is None:
                        cleaned_row.append(None)
                    elif isinstance(val, float) and math.isnan(val):
                        cleaned_row.append(None)
                    elif isinstance(val, Decimal):
                        # Convertir Decimal a int o float según corresponda
                        # Si tiene decimales, convertir a float; si no, a int
                        if val % 1 == 0:
                            cleaned_row.append(int(val))
                        else:
                            cleaned_row.append(float(val))
                    elif isinstance(val, datetime):
                        # Validar y limpiar datetime antes de insertar
                        cleaned_dt = clean_datetime_value(val)
                        cleaned_row.append(cleaned_dt)
                    elif isinstance(val, date) and not isinstance(val, datetime):
                        # Es un objeto date (sin hora), convertir a datetime
                        try:
                            dt_val = datetime.combine(val, datetime.min.time())
                            cleaned_dt = clean_datetime_value(dt_val)
                            cleaned_row.append(cleaned_dt)
                        except (ValueError, TypeError, AttributeError):
                            # No se pudo convertir, usar None
                            cleaned_row.append(None)
                    elif hasattr(val, 'year') and hasattr(val, 'month') and hasattr(val, 'day'):
                        # Otros tipos de fecha/hora (datetime de otros módulos, etc.)
                        try:
                            # Intentar convertir directamente
                            dt_val = datetime(val.year, val.month, val.day, 
                                             getattr(val, 'hour', 0),
                                             getattr(val, 'minute', 0),
                                             getattr(val, 'second', 0),
                                             getattr(val, 'microsecond', 0))
                            cleaned_dt = clean_datetime_value(dt_val)
                            cleaned_row.append(cleaned_dt)
                        except (ValueError, TypeError, AttributeError):
                            # No se pudo convertir, usar None
                            cleaned_row.append(None)
                    else:
                        cleaned_row.append(val)
                data.append(cleaned_row)
            
            # Insertar chunk en ClickHouse
            try:
                ch_client.insert(full_table_name, data, column_names=columns_to_use if incremental_type == "hash" else safe_columns)
                inserted_count = len(data)
                row_count += inserted_count
                
                # Calcular tiempo del chunk
                chunk_elapsed = time.time() - chunk_start_time
                chunk_times.append(chunk_elapsed)
                
                # Formatear tiempo
                if chunk_elapsed < 1:
                    time_str = f"{chunk_elapsed*1000:.0f}ms"
                else:
                    time_str = f"{chunk_elapsed:.2f}s"
                
                # Calcular velocidad (filas por segundo)
                rows_per_sec = inserted_count / chunk_elapsed if chunk_elapsed > 0 else 0
                speed_str = f"{rows_per_sec:,.0f} filas/s" if rows_per_sec > 0 else ""
                
                if incremental_type == "hash":
                    print(f"    [OK] Chunk {chunk_num}: {inserted_count} filas insertadas (nuevas: {new_rows_count}, updates: {updated_rows_count}, duplicadas: {duplicate_rows_count}) [{time_str}] {speed_str}")
                elif incremental_type == "id" and lookback_days:
                    print(f"    [OK] Chunk {chunk_num}: {inserted_count} filas insertadas (nuevas: {new_rows_count}, updates: {updated_rows_count}) [{time_str}] {speed_str}")
                else:
                    print(f"    [OK] Chunk {chunk_num}: {inserted_count} filas insertadas (total: {row_count}) [{time_str}] {speed_str}")
                    
            except Exception as e:
                error_msg = str(e)
                if "non-Nullable column" in error_msg or "Invalid None value" in error_msg:
                    print(f"    [ERROR] Error en chunk {chunk_num}: {e}")
                    print(f"    [INFO] La tabla tiene columnas no-nullable pero hay valores NULL en los datos.")
                    print(f"    [INFO] Solución: Elimina la tabla y vuelve a ejecutar, o cambia if_exists='replace' en el código.")
                    print(f"    [INFO] Comando SQL: DROP TABLE IF EXISTS {full_table_name}")
                raise
    finally:
        # Asegurar que el cursor se cierre incluso si hay errores
        if sql_cursor:
            try:
                sql_cursor.close()
            except:
                pass
    
    # Calcular tiempo total
    total_elapsed = time.time() - start_time
    avg_chunk_time = sum(chunk_times) / len(chunk_times) if chunk_times else 0
    
    # Formatear tiempo total
    if total_elapsed < 60:
        total_time_str = f"{total_elapsed:.2f}s"
    else:
        minutes = int(total_elapsed // 60)
        seconds = total_elapsed % 60
        total_time_str = f"{minutes}m {seconds:.1f}s"
    
    # Calcular velocidad promedio
    avg_speed = row_count / total_elapsed if total_elapsed > 0 else 0
    avg_speed_str = f"{avg_speed:,.0f} filas/s" if avg_speed > 0 else ""
    
    # Resumen final
    print(f"      Tiempo total: {total_time_str} | Tiempo promedio por chunk: {avg_chunk_time:.2f}s | Velocidad: {avg_speed_str}")
    
    if incremental_type == "hash":
        print(f"     Resumen: {new_rows_count} nuevas, {updated_rows_count} updates, {duplicate_rows_count} duplicadas (omitidas)")
        return new_rows_count + updated_rows_count, col_count
    elif incremental_type == "id" and lookback_days:
        print(f"     Resumen: {new_rows_count} nuevas, {updated_rows_count} updates")
        return row_count, col_count
    
    return row_count, col_count


def list_tables(conn):
    """
    Lista las tablas de la base de datos SQL Server.
    """
    cursor = conn.cursor()
    query = """
    SELECT TABLE_SCHEMA, TABLE_NAME
    FROM INFORMATION_SCHEMA.TABLES
    WHERE TABLE_TYPE = 'BASE TABLE'
    ORDER BY TABLE_SCHEMA, TABLE_NAME;
    """
    cursor.execute(query)
    tables = []
    for row in cursor.fetchall():
        schema, table = row
        full_name = f"{schema}.{table}"
        tables.append(full_name)
    cursor.close()
    return tables


def export_database_to_clickhouse_streaming(
    database_name: str = None, 
    tables: list = None,
    clickhouse_db: str = None,
    max_rows: Optional[int] = None,
    table_prefix: Optional[str] = None,
    incremental: bool = True,
    id_column: str = "Id"
) -> int:
    """
    Exporta tablas de SQL Server a ClickHouse usando streaming directo.
    
    Args:
        database_name: Nombre de la base de datos SQL Server
        tables: Lista de tablas a exportar
        clickhouse_db: Base de datos de ClickHouse
        max_rows: Número máximo de filas a procesar por tabla
        table_prefix: Prefijo para nombres de tablas destino
        incremental: Si es True, solo procesa registros nuevos (basado en ID)
        id_column: Nombre de la columna ID para modo incremental
    
    Returns:
        Número de tablas exportadas exitosamente
    """
    global SQL_DATABASE, CH_DATABASE, TARGET_TABLE_PREFIX
    
    if database_name:
        SQL_DATABASE = database_name
    if clickhouse_db:
        CH_DATABASE = clickhouse_db
    if table_prefix is not None:
        TARGET_TABLE_PREFIX = table_prefix
    
    if not SQL_DATABASE:
        raise RuntimeError("Debes especificar la base de datos SQL Server")
    if not CH_DATABASE:
        raise RuntimeError("Debes especificar la base de datos de ClickHouse")
    
    sql_conn = None
    ch_client = None
    
    try:
        # Conectar a SQL Server
        sql_conn = get_sql_connection()
        
        # Conectar a ClickHouse
        ch_client = connect_clickhouse(CH_DATABASE)
        
        # Obtener lista de tablas
        all_tables = list_tables(sql_conn)
        
        # Filtrar tablas excluidas por prefijos
        if EXCLUDED_TABLE_PREFIXES:
            filtered_tables = []
            excluded_count = 0
            for t in all_tables:
                table_only = t.split(".", 1)[1] if "." in t else t
                should_exclude = any(table_only.startswith(prefix) for prefix in EXCLUDED_TABLE_PREFIXES)
                if not should_exclude:
                    filtered_tables.append(t)
                else:
                    excluded_count += 1
            all_tables = filtered_tables
            if excluded_count > 0:
                print(f" Tablas excluidas por prefijos {EXCLUDED_TABLE_PREFIXES}: {excluded_count}")
        
        # Filtrar tablas si es necesario
        if tables:
            tables_to_export = [t for t in all_tables if t in tables or any(t.endswith(f".{tb}") for tb in tables)]
        elif TABLES_FILTER:
            tables_to_export = [t for t in all_tables if any(t.endswith(f".{tb}") or t == tb for tb in TABLES_FILTER)]
        else:
            tables_to_export = all_tables
        
        if not tables_to_export:
            print(f"[WARN]  No hay tablas para exportar")
            return 0
        
        print(f" Base de datos SQL Server: {SQL_DATABASE}")
        print(f" Base de datos ClickHouse: {CH_DATABASE}")
        print(f" Tablas encontradas: {len(all_tables)}")
        print(f" Tablas a exportar: {len(tables_to_export)}")
        if max_rows and max_rows > 0:
            print(f" Límite de registros por tabla: {max_rows:,}")
        if incremental:
            print(f" Modo: INCREMENTAL (columna ID: {id_column})")
        else:
            print(f" Modo: REEMPLAZO (tabla completa)")
        print()
        
        ok = 0
        
        for table_name in tables_to_export:
            # Extraer nombre de tabla sin schema
            if "." in table_name:
                table_only = table_name.split(".", 1)[1]
            else:
                table_only = table_name
            
            # Crear nombre de tabla destino en ClickHouse
            table_safe = sanitize_token(table_only)
            target_table_name = f"{TARGET_TABLE_PREFIX}{table_safe}"
            
            try:
                print(f"  -> Exportando: {table_name} -> {target_table_name}")
                
                # Obtener columnas de SQL Server
                columns = get_table_columns_sqlserver(sql_conn, table_name)
                
                # Detectar columna para modo incremental automáticamente
                incremental_col = None
                incremental_col_type = "id"
                can_use_incremental = incremental
                logical_key = None  # Clave lógica para modo hash
                
                if incremental:
                    detected_col, detected_type = detect_incremental_column(sql_conn, table_name, id_column)
                    incremental_col = detected_col
                    incremental_col_type = detected_type
                    
                    if detected_type == "id" and detected_col:
                        if detected_col != id_column:
                            print(f"     Columna ID detectada automáticamente: {detected_col}")
                        else:
                            print(f"     Usando columna ID: {detected_col}")
                    elif detected_type == "timestamp" and detected_col:
                        print(f"     Columna de fecha detectada automáticamente: {detected_col}")
                    elif detected_type == "hash":
                        # Detectar clave lógica para modo hash
                        logical_key_result = detect_logical_key(sql_conn, table_name, id_column)
                        if logical_key_result:
                            logical_key_cols, logical_key_type = logical_key_result
                            logical_key = logical_key_cols
                            print(f"     Modo hash (CDC artesanal) activado")
                            print(f"     Clave lógica detectada ({logical_key_type}): {', '.join(logical_key_cols)}")
                        else:
                            print(f"     Modo hash (CDC artesanal) activado")
                            print(f"    [WARN]  No se encontró clave lógica (PK/business key), usando hash de toda la fila como row_key")
                            print(f"    [INFO] Esto solo deduplicará filas idénticas, no hará updates reales")
                            logical_key = None  # Usar hash de toda la fila como último recurso
                    else:
                        # No se encontró columna adecuada, desactivar modo incremental
                        print(f"    [WARN]  No se encontró columna ID ni fecha adecuada. Desactivando modo incremental para esta tabla.")
                        print(f"    [INFO] La tabla se procesará en modo REEMPLAZO (tabla completa cada vez).")
                        can_use_incremental = False
                
                # Crear tabla en ClickHouse
                # Usar "append" para modo incremental, "replace" para reemplazar todo
                if_exists_mode = "append" if can_use_incremental else "replace"
                created = create_clickhouse_table(
                    ch_client, 
                    target_table_name, 
                    columns, 
                    if_exists=if_exists_mode,
                    use_replacing_merge_tree=USE_REPLACING_MERGE_TREE,
                    version_column="ingested_at",
                    incremental_type=incremental_col_type,
                    incremental_column=incremental_col
                )
                
                if created:
                    # Si se creó nueva, hacer truncate primero (opcional, comentar si quieres append)
                    # ch_client.command(f"TRUNCATE TABLE `{CH_DATABASE}`.`{target_table_name}`")
                    pass
                
                # Hacer streaming de datos con lookback window si es modo ID
                lookback = LOOKBACK_DAYS if (can_use_incremental and incremental_col_type == "id") else None
                row_count, col_count = stream_table_to_clickhouse(
                    sql_conn,
                    ch_client,
                    table_name,
                    target_table_name,
                    chunk_size=CHUNK_SIZE,
                    max_rows=max_rows,
                    incremental=can_use_incremental,
                    incremental_column=incremental_col,
                    incremental_type=incremental_col_type,
                    lookback_days=lookback,
                    logical_key=logical_key  # Pasar clave lógica para modo hash
                )
                
                print(f"    [OK] {row_count} filas, {col_count} columnas")
                ok += 1
                
            except Exception as e:
                print(f"    [ERROR] Error exportando {table_name}: {e}")
        
        return ok
        
    finally:
        if sql_conn:
            sql_conn.close()
        if ch_client:
            ch_client.close()


def main():
    """
    Función principal para streaming directo SQL Server -> ClickHouse.
    
    Uso:
        python sqlserver_to_clickhouse_streaming.py [SQL_DB] [CH_DB] [tablas] [max_rows] [table_prefix] [incremental] [id_column]
    
    Argumentos:
        SQL_DB: Nombre de la base de datos SQL Server
        CH_DB: Nombre de la base de datos ClickHouse
        tablas: Lista de tablas separadas por comas (opcional)
        max_rows: Número máximo de registros a procesar por tabla (opcional)
        table_prefix: Prefijo para nombres de tablas destino (opcional, default: "")
        incremental: "true" o "false" para modo incremental (opcional, default: "true")
        id_column: Nombre de la columna ID para modo incremental (opcional, default: "Id")
    
    Ejemplos:
        python sqlserver_to_clickhouse_streaming.py POM_DBS default
        python sqlserver_to_clickhouse_streaming.py POM_DBS default "Tabla1,Tabla2"
        python sqlserver_to_clickhouse_streaming.py POM_DBS default "Tabla1" 10000
        python sqlserver_to_clickhouse_streaming.py POM_DBS default "Tabla1" 10000 "" "false"  # Modo reemplazo
        python sqlserver_to_clickhouse_streaming.py POM_DBS default "Tabla1" 10000 "" "true" "Id"  # Modo incremental
    """
    import sys
    
    start_time = time.time()
    
    # Parsear argumentos
    sql_database = None
    ch_database = None
    tables = None
    max_rows = None
    table_prefix = None
    incremental = True  # Por defecto incremental
    id_column = "Id"   # Por defecto usar columna "Id"
    
    if len(sys.argv) > 1:
        sql_database = sys.argv[1]
    if len(sys.argv) > 2:
        ch_database = sys.argv[2]
    if len(sys.argv) > 3:
        tables = [t.strip() for t in sys.argv[3].split(",")]
    if len(sys.argv) > 4:
        # El cuarto argumento puede ser max_rows (número) o table_prefix (string)
        # Intentar parsear como número primero
        try:
            max_rows = int(sys.argv[4])
            if max_rows <= 0:
                print("[WARN]  max_rows debe ser un número positivo. Ignorando límite.")
                max_rows = None
        except ValueError:
            # Si no es un número, asumir que es table_prefix
            table_prefix = sys.argv[4]
    if len(sys.argv) > 5:
        # El quinto argumento es table_prefix si el cuarto era max_rows, o incremental si no
        if table_prefix is None and max_rows is not None:
            table_prefix = sys.argv[5]
        elif table_prefix is not None:
            # Ya tenemos table_prefix, este debe ser incremental
            incremental_str = sys.argv[5].lower()
            incremental = incremental_str in ("true", "1", "yes", "y", "si", "s")
    if len(sys.argv) > 6:
        # Sexto argumento: incremental o id_column
        if incremental is True and table_prefix is None:
            incremental_str = sys.argv[6].lower()
            incremental = incremental_str in ("true", "1", "yes", "y", "si", "s")
        else:
            id_column = sys.argv[6]
    if len(sys.argv) > 7:
        # Séptimo argumento: id_column
        id_column = sys.argv[7]
    
    try:
        ok_tables = export_database_to_clickhouse_streaming(
            database_name=sql_database,
            tables=tables,
            clickhouse_db=ch_database,
            max_rows=max_rows,
            table_prefix=table_prefix,
            incremental=incremental,
            id_column=id_column
        )
        
        elapsed_time = time.time() - start_time
        
        if ok_tables > 0:
            print()
            print(f"[OK] Exportación completada: {ok_tables} tablas exportadas")
            print(f" Datos cargados en: {CH_DATABASE}")
        else:
            print()
            print("[WARN]  No se exportaron tablas")
        
        print()
        print("=" * 60)
        print(f"RESUMEN DE EJECUCIÓN")
        print("=" * 60)
        print(f"Tablas procesadas: {ok_tables}")
        print(f"Tiempo de ejecución: {elapsed_time:.2f} segundos")
        print("=" * 60)
        
    except Exception as e:
        elapsed_time = time.time() - start_time
        print(f"[ERROR] Error: {e}")
        print()
        print("=" * 60)
        print(f"RESUMEN DE EJECUCIÓN")
        print("=" * 60)
        print(f"Tablas procesadas: 0")
        print(f"Tiempo de ejecución: {elapsed_time:.2f} segundos")
        print(f"Estado: ERROR")
        print("=" * 60)
        return 1
    
    return 0


if __name__ == "__main__":
    exit(main())
