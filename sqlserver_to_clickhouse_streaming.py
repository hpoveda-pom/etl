import os
import re
import time
import warnings
from datetime import datetime
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
SQL_DRIVER = os.getenv("SQL_DRIVER", "ODBC Driver 17 for SQL Server")

# ============== ClickHouse config ==============
CH_HOST = os.getenv("CH_HOST", "f4rf85ygzj.eastus2.azure.clickhouse.cloud")
CH_PORT = int(os.getenv("CH_PORT", "8443"))
CH_USER = os.getenv("CH_USER", "default")
CH_PASSWORD = os.getenv("CH_PASSWORD", "")
CH_DATABASE = os.getenv("CH_DATABASE", "default")

# ============== Configuración de streaming ==============
CHUNK_SIZE = int(os.getenv("STREAMING_CHUNK_SIZE", "10000"))  # Filas por chunk
TARGET_TABLE_PREFIX = os.getenv("TARGET_TABLE_PREFIX", "SQLSERVER_")  # Prefijo para tablas en ClickHouse

# Opcional: especificar tablas a exportar (coma-separado)
TABLES_FILTER = [s.strip() for s in os.getenv("TABLES_FILTER", "").split(",") if s.strip()]

# Prefijos de nombres de tablas a excluir
EXCLUDED_TABLE_PREFIXES = ["TMP_"]


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
    Retorna lista de tuplas (column_name, data_type, max_length).
    """
    schema, table = table_name.split('.', 1) if '.' in table_name else ('dbo', table_name)
    
    query = """
    SELECT COLUMN_NAME, DATA_TYPE, CHARACTER_MAXIMUM_LENGTH
    FROM INFORMATION_SCHEMA.COLUMNS
    WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ?
    ORDER BY ORDINAL_POSITION;
    """
    
    cursor = conn.cursor()
    cursor.execute(query, schema, table)
    columns = cursor.fetchall()
    cursor.close()
    
    return columns


def create_clickhouse_table(client, table_name: str, columns: list, if_exists: str = "replace"):
    """
    Crea una tabla en ClickHouse basada en las columnas de SQL Server.
    
    Args:
        client: Cliente de ClickHouse
        table_name: Nombre de la tabla (sin database)
        columns: Lista de tuplas (column_name, data_type, max_length) de SQL Server
        if_exists: "replace", "append" o "skip"
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
        elif sql_type_upper in ('DATETIME', 'DATETIME2', 'SMALLDATETIME'):
            return "DateTime"
        elif sql_type_upper in ('DATETIMEOFFSET',):
            return "DateTime64(3)"
        elif sql_type_upper in ('UNIQUEIDENTIFIER',):
            return "String"
        elif sql_type_upper in ('BINARY', 'VARBINARY', 'IMAGE'):
            return "String"  # ClickHouse no tiene tipo BINARY nativo
        else:
            return "String"  # Tipo por defecto
    
    # Crear definición de columnas
    column_defs = []
    for col_name, sql_type, max_length in columns:
        safe_col_name = sanitize_token(col_name)
        clickhouse_type = map_data_type(sql_type, max_length)
        column_defs.append(f"`{safe_col_name}` {clickhouse_type}")
    
    # Agregar columna de metadatos
    column_defs.append("`ingested_at` DateTime DEFAULT now()")
    
    create_sql = f"""
    CREATE TABLE IF NOT EXISTS {full_table_name} (
        {', '.join(column_defs)}
    ) ENGINE = MergeTree()
    ORDER BY tuple();
    """
    
    client.command(create_sql)
    print(f"  ✅ Tabla creada: {table_name} ({len(columns)} columnas)")
    return True


def stream_table_to_clickhouse(
    sql_conn, 
    ch_client, 
    table_name: str, 
    target_table_name: str,
    chunk_size: int = CHUNK_SIZE
) -> Tuple[int, int]:
    """
    Hace streaming de una tabla de SQL Server a ClickHouse.
    
    Args:
        sql_conn: Conexión a SQL Server
        ch_client: Cliente de ClickHouse
        table_name: Nombre completo de la tabla en SQL Server (schema.table)
        target_table_name: Nombre de la tabla destino en ClickHouse
        chunk_size: Tamaño del chunk para streaming
    
    Returns:
        (row_count, col_count)
    """
    import pandas as pd
    
    schema, table = table_name.split('.', 1) if '.' in table_name else ('dbo', table_name)
    query = f"SELECT * FROM [{schema}].[{table}]"
    
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
        
        chunk_num += 1
        
        # Convertir a DataFrame
        df = pd.DataFrame(rows, columns=columns)
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
            print(f"    ❌ Error en chunk {chunk_num}: {e}")
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
    clickhouse_db: str = None
) -> int:
    """
    Exporta tablas de SQL Server a ClickHouse usando streaming directo.
    
    Args:
        database_name: Nombre de la base de datos SQL Server
        tables: Lista de tablas a exportar
        clickhouse_db: Base de datos de ClickHouse
    
    Returns:
        Número de tablas exportadas exitosamente
    """
    global SQL_DATABASE, CH_DATABASE
    
    if database_name:
        SQL_DATABASE = database_name
    if clickhouse_db:
        CH_DATABASE = clickhouse_db
    
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
                
                # Crear tabla en ClickHouse
                created = create_clickhouse_table(ch_client, target_table_name, columns, if_exists="append")
                
                if created:
                    # Si se creó nueva, hacer truncate primero (opcional, comentar si quieres append)
                    # ch_client.command(f"TRUNCATE TABLE `{CH_DATABASE}`.`{target_table_name}`")
                    pass
                
                # Hacer streaming de datos
                row_count, col_count = stream_table_to_clickhouse(
                    sql_conn, ch_client, table_name, target_table_name, CHUNK_SIZE
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
        python sqlserver_to_clickhouse_streaming.py [SQL_DB] [CH_DB] [tablas]
    
    Ejemplos:
        python sqlserver_to_clickhouse_streaming.py
        python sqlserver_to_clickhouse_streaming.py POM_DBS default
        python sqlserver_to_clickhouse_streaming.py POM_DBS default "Tabla1,Tabla2"
    """
    import sys
    
    start_time = time.time()
    
    # Parsear argumentos
    sql_database = None
    ch_database = None
    tables = None
    
    if len(sys.argv) > 1:
        sql_database = sys.argv[1]
    if len(sys.argv) > 2:
        ch_database = sys.argv[2]
    if len(sys.argv) > 3:
        tables = [t.strip() for t in sys.argv[3].split(",")]
    
    try:
        ok_tables = export_database_to_clickhouse_streaming(
            database_name=sql_database,
            tables=tables,
            clickhouse_db=ch_database
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
