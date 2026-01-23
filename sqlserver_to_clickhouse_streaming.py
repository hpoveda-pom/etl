import os
import re
import time
import warnings
import hashlib
import json
from datetime import datetime, timedelta
from typing import Optional, Tuple

import pyodbc

try:
    import clickhouse_connect
except ImportError:
    print("❌ Error: Falta la librería clickhouse-connect")
    print("💡 Instálala con: pip install clickhouse-connect")
    exit(1)

# Suprimir warnings
warnings.filterwarnings('ignore', category=UserWarning, module='pandas')

# ============== SQL Server config ==============
SQL_SERVER = os.getenv("SQL_SERVER", r"SRV-DESA\SQLEXPRESS")
SQL_DATABASE = os.getenv("SQL_DATABASE", "")
SQL_USER = os.getenv("SQL_USER", "")
SQL_PASSWORD = os.getenv("SQL_PASSWORD", "")
SQL_DRIVER = os.getenv("SQL_DRIVER", "ODBC Driver 17 for SQL Server")  # Ajustar según tu driver


# ============== ClickHouse Cloud config ==============
CH_HOST = os.getenv("CH_HOST", "f4rf85ygzj.eastus2.azure.clickhouse.cloud")
CH_PORT = int(os.getenv("CH_PORT", "8443"))
CH_USER = os.getenv("CH_USER", "default")
CH_PASSWORD = os.getenv("CH_PASSWORD", "Tsm1e.3Wgbw5P")
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
    Reutiliza la lógica del script original.
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
                print(f"⚠️  Driver '{SQL_DRIVER}' no encontrado. Usando '{driver_to_use}'")
                break
        else:
            raise RuntimeError(
                f"No se encontró un driver ODBC compatible. Drivers disponibles: {', '.join(available_drivers)}"
            )
    
    if SQL_USER and SQL_PASSWORD:
        print(f"🔐 Usando autenticación SQL Server (usuario: {SQL_USER})")
        conn_str = (
            f"DRIVER={{{driver_to_use}}};"
            f"SERVER={SQL_SERVER};"
            f"DATABASE={SQL_DATABASE};"
            f"UID={SQL_USER};"
            f"PWD={SQL_PASSWORD};"
            f"TrustServerCertificate=yes;"
        )
    else:
        print(f"🔐 Usando autenticación Windows (usuario actual: {os.getenv('USERNAME', 'N/A')})")
        conn_str = (
            f"DRIVER={{{driver_to_use}}};"
            f"SERVER={SQL_SERVER};"
            f"DATABASE={SQL_DATABASE};"
            f"Trusted_Connection=yes;"
            f"TrustServerCertificate=yes;"
        )
    
    try:
        conn = pyodbc.connect(conn_str, timeout=30)
        print(f"✅ Conectado a SQL Server: {SQL_SERVER}/{SQL_DATABASE}")
        return conn
    except pyodbc.Error as e:
        error_msg = str(e)
        if "login failed" in error_msg.lower():
            raise RuntimeError(f"Error de autenticación. Verifica las credenciales o permisos de Windows.")
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
                print(f"✅ Base de datos '{CH_DATABASE}' creada exitosamente")
            except Exception as create_err:
                # Si falla la creación, listar bases de datos disponibles
                available_dbs = list_available_databases(temp_client)
                temp_client.close()
                
                if available_dbs:
                    db_list = "\n   - ".join(available_dbs[:15])
                    if len(available_dbs) > 15:
                        db_list += f"\n   ... y {len(available_dbs) - 15} más"
                    raise RuntimeError(
                        f"❌ No se pudo crear la base de datos '{CH_DATABASE}'.\n"
                        f"Error: {create_err}\n\n"
                        f"💡 Bases de datos disponibles ({len(available_dbs)}):\n   - {db_list}\n\n"
                        f"💡 Sugerencias:\n"
                        f"   - Usa una de las bases de datos listadas arriba\n"
                        f"   - Ejemplo: python sqlserver_to_clickhouse_streaming.py POM_DBS default\n"
                        f"   - O crea la base de datos '{CH_DATABASE}' en ClickHouse primero"
                    )
                else:
                    raise RuntimeError(
                        f"❌ No se pudo crear la base de datos '{CH_DATABASE}'.\n"
                        f"Error: {create_err}\n"
                        f"💡 No se pudieron listar las bases de datos disponibles. Verifica tus permisos."
                    )
        else:
            print(f"✅ Base de datos '{CH_DATABASE}' encontrada")
        
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
        print(f"✅ Conectado a ClickHouse: {CH_HOST}:{CH_PORT}/{CH_DATABASE}")
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
                        f"❌ La base de datos '{CH_DATABASE}' no existe.\n"
                        f"Error: {error_msg}\n\n"
                        f"💡 Bases de datos disponibles ({len(available_dbs)}):\n   - {db_list}\n\n"
                        f"💡 Sugerencias:\n"
                        f"   - Usa una de las bases de datos listadas arriba\n"
                        f"   - Ejemplo: python sqlserver_to_clickhouse_streaming.py POM_DBS default\n"
                        f"   - O crea la base de datos '{CH_DATABASE}' en ClickHouse primero"
                    )
            except:
                pass
            
            raise RuntimeError(
                f"❌ La base de datos '{CH_DATABASE}' no existe.\n"
                f"Error: {error_msg}\n"
                f"💡 Verifica el nombre de la base de datos o créala primero en ClickHouse."
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


def calculate_row_hash(row_data: tuple, columns: list) -> str:
    """
    Calcula un hash MD5 de toda la fila para detectar cambios (CDC artesanal).
    
    Args:
        row_data: Tupla con los valores de la fila
        columns: Lista de nombres de columnas
    
    Returns:
        Hash MD5 hexadecimal de la fila
    """
    # Convertir la fila a un diccionario ordenado
    row_dict = {col: val for col, val in zip(columns, row_data)}
    
    # Serializar a JSON para obtener una representación consistente
    # Usar sort_keys=True para garantizar orden consistente
    row_json = json.dumps(row_dict, sort_keys=True, default=str, ensure_ascii=False)
    
    # Calcular hash MD5
    hash_obj = hashlib.md5(row_json.encode('utf-8'))
    return hash_obj.hexdigest()


def get_existing_hashes(ch_client, table_name: str) -> set:
    """
    Obtiene todos los hashes existentes en ClickHouse para deduplicación.
    
    Args:
        ch_client: Cliente de ClickHouse
        table_name: Nombre completo de la tabla
    
    Returns:
        Set de hashes existentes
    """
    try:
        query = f"SELECT DISTINCT `row_hash` FROM {table_name} WHERE `row_hash` != ''"
        result = ch_client.query(query)
        if result.result_rows:
            return {row[0] for row in result.result_rows if row[0]}
        return set()
    except:
        # Si la columna row_hash no existe, retornar set vacío
        return set()


def get_existing_ids(ch_client, table_name: str, id_column: str, lookback_days: Optional[int] = None) -> set:
    """
    Obtiene IDs existentes en ClickHouse, opcionalmente filtrados por lookback window.
    
    Args:
        ch_client: Cliente de ClickHouse
        table_name: Nombre completo de la tabla
        id_column: Nombre de la columna ID
        lookback_days: Si se especifica, solo retorna IDs de registros dentro del lookback window
    
    Returns:
        Set de IDs existentes
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
            return {row[0] for row in result.result_rows if row[0] is not None}
        return set()
    except:
        return set()


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
        print(f"  🔄 Reemplazando tabla existente: {table_name}")
        drop_sql = f"DROP TABLE IF EXISTS {full_table_name}"
        client.command(drop_sql)
        table_exists = False
    elif table_exists and if_exists == "skip":
        print(f"  ⏭️  Tabla {table_name} ya existe, omitiendo creación")
        return False
    
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
    
    # Si es modo hash, agregar columna row_hash
    if incremental_type == "hash":
        column_defs.append("`row_hash` String")
    
    # Determinar ORDER BY según el tipo incremental
    # IMPORTANTE: ClickHouse no permite columnas nullable en ORDER BY si allow_nullable_key está deshabilitado
    # Por lo tanto, siempre usamos ingested_at que es no-nullable (tiene DEFAULT)
    if incremental_type == "id" and incremental_column:
        # Verificar si la columna ID es nullable
        id_col_info = next((col for col in columns if col[0] == incremental_column), None)
        id_is_nullable = id_col_info and len(id_col_info) >= 4 and id_col_info[3] == 'YES'
        
        if not id_is_nullable:
            # Si la columna ID no es nullable, podemos usarla en ORDER BY
            safe_id_col = sanitize_token(incremental_column)
            order_by = f"ORDER BY (`{safe_id_col}`)"
        else:
            # Si es nullable, usar ingested_at (siempre no-nullable)
            order_by = "ORDER BY (`ingested_at`)"
            print(f"  ⚠️  Columna ID '{incremental_column}' es nullable, usando 'ingested_at' en ORDER BY")
    else:
        # Si no hay ID o es modo hash, ordenar por ingested_at
        order_by = "ORDER BY (`ingested_at`)"
    
    # Determinar ENGINE según configuración
    if use_replacing_merge_tree:
        safe_version_col = sanitize_token(version_column)
        engine_clause = f"ENGINE = ReplacingMergeTree(`{safe_version_col}`)"
        print(f"  🔄 Usando ReplacingMergeTree con versión: {safe_version_col}")
    else:
        engine_clause = "ENGINE = MergeTree()"
    
    create_sql = f"""
    CREATE TABLE IF NOT EXISTS {full_table_name} (
        {', '.join(column_defs)}
    ) {engine_clause}
    {order_by};
    """
    
    client.command(create_sql)
    print(f"  ✅ Tabla creada: {table_name} ({len(columns)} columnas)")
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
        if result.result_rows and result.result_rows[0] and result.result_rows[0][0] is not None:
            value = result.result_rows[0][0]
            if column_type == "id":
                return int(value)
            else:  # timestamp
                return value  # Retornar como datetime/string
        return None
    except Exception as e:
        # Si la tabla no existe o no tiene la columna, retornar None
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
    lookback_days: Optional[int] = None
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
    
    Returns:
        (row_count, col_count)
    """
    import pandas as pd
    
    schema, table = table_name.split('.', 1) if '.' in table_name else ('dbo', table_name)
    full_table_name = f"`{CH_DATABASE}`.`{target_table_name}`"
    
    # Si es modo hash, obtener hashes existentes
    existing_hashes = set()
    if incremental and incremental_type == "hash":
        existing_hashes = get_existing_hashes(ch_client, full_table_name)
        if existing_hashes:
            print(f"    🔄 Modo incremental (hash): {len(existing_hashes)} registros existentes")
        else:
            print(f"    🔄 Modo incremental (hash): tabla vacía, procesando todos los registros")
    
    # Si es modo ID con lookback window, obtener IDs existentes en el rango
    existing_ids = set()
    if incremental and incremental_type == "id" and incremental_column and lookback_days:
        existing_ids = get_existing_ids(ch_client, full_table_name, incremental_column, lookback_days)
        if existing_ids:
            print(f"    🔄 Lookback window ({lookback_days} días): {len(existing_ids)} IDs en rango (para detectar updates)")
    
    # Si es incremental, obtener el máximo valor de ClickHouse
    last_value = None
    if incremental and incremental_column and incremental_type != "hash":
        last_value = get_max_value_from_clickhouse(ch_client, full_table_name, incremental_column, incremental_type)
        if last_value is not None:
            if incremental_type == "id":
                print(f"    🔄 Modo incremental (ID): último valor procesado = {last_value}")
            else:
                print(f"    🔄 Modo incremental (fecha): última fecha procesada = {last_value}")
    
    # Construir query SQL
    where_clause = ""
    order_clause = ""
    
    if incremental and incremental_column and incremental_type != "hash" and last_value is not None:
        safe_col = f"[{incremental_column}]"
        if incremental_type == "id":
            where_clause = f"WHERE {safe_col} > {last_value}"
        else:  # timestamp
            # Para fechas, usar formato SQL Server
            where_clause = f"WHERE {safe_col} > '{last_value}'"
        order_clause = f"ORDER BY {safe_col}"
    elif incremental and incremental_type == "id" and lookback_days and existing_ids:
        # Lookback window: incluir también IDs existentes en el rango (para detectar updates)
        safe_col = f"[{incremental_column}]"
        id_list = ','.join(map(str, existing_ids))
        if last_value is not None:
            where_clause = f"WHERE ({safe_col} > {last_value} OR {safe_col} IN ({id_list}))"
        else:
            where_clause = f"WHERE {safe_col} IN ({id_list})"
        order_clause = f"ORDER BY {safe_col}"
    
    # Construir query con TOP si se especifica max_rows
    if max_rows and max_rows > 0:
        query = f"SELECT TOP {max_rows} * FROM [{schema}].[{table}] {where_clause} {order_clause}"
        print(f"    ⚠️  Limitando a {max_rows} registros")
    else:
        query = f"SELECT * FROM [{schema}].[{table}] {where_clause} {order_clause}"
    
    sql_cursor = sql_conn.cursor()
    sql_cursor.execute(query)
    
    # Obtener nombres de columnas
    columns = [desc[0] for desc in sql_cursor.description]
    col_count = len(columns)
    
    # Sanitizar nombres de columnas para ClickHouse
    safe_columns = [sanitize_token(col) for col in columns]
    
    full_table_name = f"`{CH_DATABASE}`.`{target_table_name}`"
    
    row_count = 0
    chunk_num = 0
    
    print(f"    📊 Iniciando streaming (chunk size: {chunk_size})...")
    
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
        
        # Convertir filas de pyodbc a lista de listas
        # Asegurarse de que cada fila sea una lista, no una tupla
        rows_list = [list(row) for row in rows]
        
        # Convertir a DataFrame
        df = pd.DataFrame(rows_list, columns=columns)
        df.columns = safe_columns
        
        # Reemplazar NaN por None
        df = df.where(pd.notnull(df), None)
        
        # Convertir DataFrame a lista de listas para ClickHouse
        data = df.values.tolist()
        
        # Insertar chunk en ClickHouse
        try:
            ch_client.insert(full_table_name, data, column_names=safe_columns)
            row_count += len(data)
            print(f"    ✓ Chunk {chunk_num}: {len(data)} filas insertadas (total: {row_count})")
                
        except Exception as e:
            error_msg = str(e)
            if "non-Nullable column" in error_msg or "Invalid None value" in error_msg:
                print(f"    ❌ Error en chunk {chunk_num}: {e}")
                print(f"    💡 La tabla tiene columnas no-nullable pero hay valores NULL en los datos.")
                print(f"    💡 Solución: Elimina la tabla y vuelve a ejecutar, o cambia if_exists='replace' en el código.")
                print(f"    💡 Comando SQL: DROP TABLE IF EXISTS {full_table_name}")
            raise
    
    sql_cursor.close()
    
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
                print(f"🚫 Tablas excluidas por prefijos {EXCLUDED_TABLE_PREFIXES}: {excluded_count}")
        
        # Filtrar tablas si es necesario
        if tables:
            tables_to_export = [t for t in all_tables if t in tables or any(t.endswith(f".{tb}") for tb in tables)]
        elif TABLES_FILTER:
            tables_to_export = [t for t in all_tables if any(t.endswith(f".{tb}") or t == tb for tb in TABLES_FILTER)]
        else:
            tables_to_export = all_tables
        
        if not tables_to_export:
            print(f"⚠️  No hay tablas para exportar")
            return 0
        
        print(f"📊 Base de datos SQL Server: {SQL_DATABASE}")
        print(f"📊 Base de datos ClickHouse: {CH_DATABASE}")
        print(f"📋 Tablas encontradas: {len(all_tables)}")
        print(f"📤 Tablas a exportar: {len(tables_to_export)}")
        if max_rows and max_rows > 0:
            print(f"🔢 Límite de registros por tabla: {max_rows:,}")
        if incremental:
            print(f"🔄 Modo: INCREMENTAL (columna ID: {id_column})")
        else:
            print(f"🔄 Modo: REEMPLAZO (tabla completa)")
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
                print(f"  → Exportando: {table_name} → {target_table_name}")
                
                # Obtener columnas de SQL Server
                columns = get_table_columns_sqlserver(sql_conn, table_name)
                
                # Detectar columna para modo incremental automáticamente
                incremental_col = None
                incremental_col_type = "id"
                can_use_incremental = incremental
                
                if incremental:
                    detected_col, detected_type = detect_incremental_column(sql_conn, table_name, id_column)
                    incremental_col = detected_col
                    incremental_col_type = detected_type
                    
                    if detected_type == "id" and detected_col:
                        if detected_col != id_column:
                            print(f"    🔍 Columna ID detectada automáticamente: {detected_col}")
                        else:
                            print(f"    🔍 Usando columna ID: {detected_col}")
                    elif detected_type == "timestamp" and detected_col:
                        print(f"    🔍 Columna de fecha detectada automáticamente: {detected_col}")
                    elif detected_type == "hash":
                        print(f"    🔍 Modo hash (CDC artesanal) activado: detectará cambios por hash de fila")
                    else:
                        # No se encontró columna adecuada, desactivar modo incremental
                        print(f"    ⚠️  No se encontró columna ID ni fecha adecuada. Desactivando modo incremental para esta tabla.")
                        print(f"    💡 La tabla se procesará en modo REEMPLAZO (tabla completa cada vez).")
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
                    sql_conn, ch_client, table_name, target_table_name, 
                    CHUNK_SIZE, max_rows, can_use_incremental, 
                    incremental_col, incremental_col_type, lookback
                )
                
                print(f"    ✅ {row_count} filas, {col_count} columnas")
                ok += 1
                
            except Exception as e:
                print(f"    ❌ Error exportando {table_name}: {e}")
        
        return ok
        
    finally:
        if sql_conn:
            sql_conn.close()
        if ch_client:
            ch_client.close()


def main():
    """
    Función principal para streaming directo SQL Server → ClickHouse.
    
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
                print("⚠️  max_rows debe ser un número positivo. Ignorando límite.")
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
            print(f"✅ Exportación completada: {ok_tables} tablas exportadas")
            print(f"📊 Datos cargados en: {CH_DATABASE}")
        else:
            print()
            print("⚠️  No se exportaron tablas")
        
        print()
        print("=" * 60)
        print(f"RESUMEN DE EJECUCIÓN")
        print("=" * 60)
        print(f"Tablas procesadas: {ok_tables}")
        print(f"Tiempo de ejecución: {elapsed_time:.2f} segundos")
        print("=" * 60)
        
    except Exception as e:
        elapsed_time = time.time() - start_time
        print(f"❌ Error: {e}")
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
