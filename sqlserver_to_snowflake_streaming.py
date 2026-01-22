import os
import re
import time
import warnings
from datetime import datetime
from typing import Optional, Tuple

import pyodbc
import snowflake.connector
from snowflake.connector.pandas_tools import write_pandas

# Suprimir warnings
warnings.filterwarnings('ignore', category=UserWarning, module='pandas')

# ============== SQL Server config ==============
SQL_SERVER = os.getenv("SQL_SERVER", r"SRV-DESA\SQLEXPRESS")
SQL_DATABASE = os.getenv("SQL_DATABASE", "")
SQL_USER = os.getenv("SQL_USER", "")
SQL_PASSWORD = os.getenv("SQL_PASSWORD", "")
SQL_DRIVER = os.getenv("SQL_DRIVER", "ODBC Driver 17 for SQL Server")

# ============== Snowflake config ==============
SF_ACCOUNT = os.getenv("SF_ACCOUNT", "")
SF_USER = os.getenv("SF_USER", "")
SF_PASSWORD = os.getenv("SF_PASSWORD", "")
SF_ROLE = os.getenv("SF_ROLE", "ACCOUNTADMIN")
SF_WH = os.getenv("SF_WAREHOUSE", "COMPUTE_WH")
SF_DB = os.getenv("SF_DATABASE", "")
SF_SCHEMA = os.getenv("SF_SCHEMA", "RAW")

# ============== Configuración de streaming ==============
CHUNK_SIZE = int(os.getenv("STREAMING_CHUNK_SIZE", "10000"))  # Filas por chunk
TARGET_TABLE_PREFIX = os.getenv("TARGET_TABLE_PREFIX", "SQLSERVER_")  # Prefijo para tablas en Snowflake

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


def connect_snowflake(database: str = None, schema: str = None):
    """
    Crea una conexión a Snowflake.
    """
    global SF_DB, SF_SCHEMA
    
    if database:
        SF_DB = database
    if schema:
        SF_SCHEMA = schema
    
    if not SF_PASSWORD:
        raise RuntimeError("Falta SF_PASSWORD (definí la variable de entorno).")
    
    try:
        conn = snowflake.connector.connect(
            account=SF_ACCOUNT,
            user=SF_USER,
            password=SF_PASSWORD,
            role=SF_ROLE,
            warehouse=SF_WH,
        )
        
        cur = conn.cursor()
        
        # Usar base de datos
        try:
            cur.execute(f'USE DATABASE "{SF_DB}";')
        except:
            try:
                cur.execute(f"USE DATABASE {SF_DB.upper()};")
                SF_DB = SF_DB.upper()
            except:
                raise RuntimeError(f"No se pudo usar la base de datos '{SF_DB}'")
        
        # Usar schema
        try:
            if SF_SCHEMA != SF_SCHEMA.upper():
                cur.execute(f'USE SCHEMA "{SF_SCHEMA}";')
            else:
                cur.execute(f"USE SCHEMA {SF_SCHEMA};")
        except:
            raise RuntimeError(f"No se pudo usar el schema '{SF_SCHEMA}'")
        
        print(f"✅ Conectado a Snowflake: {SF_DB}/{SF_SCHEMA}")
        return conn, cur
        
    except Exception as e:
        raise RuntimeError(f"Error conectando a Snowflake: {e}")


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


def create_snowflake_table(cur, table_name: str, columns: list, if_exists: str = "replace"):
    """
    Crea una tabla en Snowflake basada en las columnas de SQL Server.
    
    Args:
        cur: Cursor de Snowflake
        table_name: Nombre de la tabla (sin schema)
        columns: Lista de tuplas (column_name, data_type, max_length) de SQL Server
        if_exists: "replace", "append" o "skip"
    """
    full_table_name = f'"{SF_DB}"."{SF_SCHEMA}"."{table_name}"'
    
    # Verificar si la tabla existe
    cur.execute(f"SHOW TABLES LIKE '{table_name}';")
    table_exists = len(cur.fetchall()) > 0
    
    if table_exists and if_exists == "replace":
        print(f"  🔄 Reemplazando tabla existente: {table_name}")
        cur.execute(f"DROP TABLE IF EXISTS {full_table_name};")
        table_exists = False
    elif table_exists and if_exists == "skip":
        print(f"  ⏭️  Tabla {table_name} ya existe, omitiendo creación")
        return False
    
    # Mapear tipos de datos de SQL Server a Snowflake
    def map_data_type(sql_type: str, max_length: Optional[int]) -> str:
        sql_type_upper = sql_type.upper()
        
        if sql_type_upper in ('VARCHAR', 'NVARCHAR', 'CHAR', 'NCHAR', 'TEXT', 'NTEXT'):
            length = max_length if max_length and max_length > 0 else 16777216
            if length > 16777216:
                length = 16777216  # Máximo de Snowflake
            return f"VARCHAR({length})"
        elif sql_type_upper in ('INT', 'INTEGER'):
            return "INTEGER"
        elif sql_type_upper in ('BIGINT',):
            return "BIGINT"
        elif sql_type_upper in ('SMALLINT',):
            return "SMALLINT"
        elif sql_type_upper in ('TINYINT',):
            return "TINYINT"
        elif sql_type_upper in ('DECIMAL', 'NUMERIC'):
            return "NUMBER(38, 0)"  # Ajustar según necesidad
        elif sql_type_upper in ('FLOAT', 'REAL'):
            return "FLOAT"
        elif sql_type_upper in ('DOUBLE', 'DOUBLE PRECISION'):
            return "DOUBLE"
        elif sql_type_upper in ('BIT',):
            return "BOOLEAN"
        elif sql_type_upper in ('DATE',):
            return "DATE"
        elif sql_type_upper in ('TIME',):
            return "TIME"
        elif sql_type_upper in ('DATETIME', 'DATETIME2', 'SMALLDATETIME'):
            return "TIMESTAMP_NTZ"
        elif sql_type_upper in ('DATETIMEOFFSET',):
            return "TIMESTAMP_TZ"
        elif sql_type_upper in ('UNIQUEIDENTIFIER',):
            return "VARCHAR(36)"
        elif sql_type_upper in ('BINARY', 'VARBINARY', 'IMAGE'):
            return "VARCHAR"  # Snowflake no tiene tipo BINARY nativo, usar VARCHAR
        else:
            return "VARCHAR(16777216)"  # Tipo por defecto
    
    # Crear definición de columnas
    column_defs = []
    for col_name, sql_type, max_length in columns:
        safe_col_name = sanitize_token(col_name)
        snowflake_type = map_data_type(sql_type, max_length)
        column_defs.append(f'"{safe_col_name}" {snowflake_type}')
    
    # Agregar columna de metadatos
    column_defs.append('"ingested_at" TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()')
    
    create_sql = f"""
    CREATE TABLE IF NOT EXISTS {full_table_name} (
        {', '.join(column_defs)}
    );
    """
    
    cur.execute(create_sql)
    print(f"  ✅ Tabla creada: {table_name} ({len(columns)} columnas)")
    return True


def stream_table_to_snowflake(
    sql_conn, 
    sf_conn, 
    sf_cur, 
    table_name: str, 
    target_table_name: str,
    chunk_size: int = CHUNK_SIZE
) -> Tuple[int, int]:
    """
    Hace streaming de una tabla de SQL Server a Snowflake.
    
    Args:
        sql_conn: Conexión a SQL Server
        sf_conn: Conexión a Snowflake
        sf_cur: Cursor de Snowflake
        table_name: Nombre completo de la tabla en SQL Server (schema.table)
        target_table_name: Nombre de la tabla destino en Snowflake
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
    
    # Sanitizar nombres de columnas para Snowflake
    safe_columns = [sanitize_token(col) for col in columns]
    
    full_table_name = f'"{SF_DB}"."{SF_SCHEMA}"."{target_table_name}"'
    
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
        
        # Insertar chunk en Snowflake usando write_pandas
        try:
            success, nchunks, nrows, output = write_pandas(
                conn=sf_conn,
                df=df,
                table_name=target_table_name,
                schema=SF_SCHEMA,
                database=SF_DB,
                auto_create_table=False,  # Ya creamos la tabla
                overwrite=False,  # Append mode
            )
            
            if success:
                row_count += nrows
                print(f"    ✓ Chunk {chunk_num}: {nrows} filas insertadas (total: {row_count})")
            else:
                raise RuntimeError(f"Error al insertar chunk {chunk_num}: {output}")
                
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


def export_database_to_snowflake_streaming(
    database_name: str = None, 
    tables: list = None,
    snowflake_db: str = None,
    snowflake_schema: str = None
) -> int:
    """
    Exporta tablas de SQL Server a Snowflake usando streaming directo.
    
    Args:
        database_name: Nombre de la base de datos SQL Server
        tables: Lista de tablas a exportar
        snowflake_db: Base de datos de Snowflake
        snowflake_schema: Schema de Snowflake
    
    Returns:
        Número de tablas exportadas exitosamente
    """
    global SQL_DATABASE, SF_DB, SF_SCHEMA
    
    if database_name:
        SQL_DATABASE = database_name
    if snowflake_db:
        SF_DB = snowflake_db
    if snowflake_schema:
        SF_SCHEMA = snowflake_schema
    
    if not SQL_DATABASE:
        raise RuntimeError("Debes especificar la base de datos SQL Server")
    if not SF_DB:
        raise RuntimeError("Debes especificar la base de datos de Snowflake")
    
    sql_conn = None
    sf_conn = None
    sf_cur = None
    
    try:
        # Conectar a SQL Server
        sql_conn = get_sql_connection()
        
        # Conectar a Snowflake
        sf_conn, sf_cur = connect_snowflake(SF_DB, SF_SCHEMA)
        
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
        print(f"📊 Base de datos Snowflake: {SF_DB}/{SF_SCHEMA}")
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
            
            # Crear nombre de tabla destino en Snowflake
            table_safe = sanitize_token(table_only)
            target_table_name = f"{TARGET_TABLE_PREFIX}{table_safe}"
            
            try:
                print(f"  → Exportando: {table_name} → {target_table_name}")
                
                # Obtener columnas de SQL Server
                columns = get_table_columns_sqlserver(sql_conn, table_name)
                
                # Crear tabla en Snowflake
                created = create_snowflake_table(sf_cur, target_table_name, columns, if_exists="append")
                
                if created:
                    # Si se creó nueva, hacer truncate primero (opcional, comentar si quieres append)
                    # sf_cur.execute(f'TRUNCATE TABLE "{SF_DB}"."{SF_SCHEMA}"."{target_table_name}";')
                    pass
                
                # Hacer streaming de datos
                row_count, col_count = stream_table_to_snowflake(
                    sql_conn, sf_conn, sf_cur, table_name, target_table_name, CHUNK_SIZE
                )
                
                print(f"    ✅ {row_count} filas, {col_count} columnas")
                ok += 1
                
            except Exception as e:
                print(f"    ❌ Error exportando {table_name}: {e}")
        
        return ok
        
    finally:
        if sql_conn:
            sql_conn.close()
        if sf_cur:
            sf_cur.close()
        if sf_conn:
            sf_conn.close()


def main():
    """
    Función principal para streaming directo SQL Server → Snowflake.
    
    Uso:
        python sqlserver_to_snowflake_streaming.py [SQL_DB] [SF_DB] [SF_SCHEMA] [tablas]
    
    Ejemplos:
        python sqlserver_to_snowflake_streaming.py
        python sqlserver_to_snowflake_streaming.py POM_DBS POM_TEST01 RAW
        python sqlserver_to_snowflake_streaming.py POM_DBS POM_TEST01 RAW "Tabla1,Tabla2"
    """
    import sys
    
    start_time = time.time()
    
    # Parsear argumentos
    sql_database = None
    sf_database = None
    sf_schema = None
    tables = None
    
    if len(sys.argv) > 1:
        sql_database = sys.argv[1]
    if len(sys.argv) > 2:
        sf_database = sys.argv[2]
    if len(sys.argv) > 3:
        sf_schema = sys.argv[3]
    if len(sys.argv) > 4:
        tables = [t.strip() for t in sys.argv[4].split(",")]
    
    try:
        ok_tables = export_database_to_snowflake_streaming(
            database_name=sql_database,
            tables=tables,
            snowflake_db=sf_database,
            snowflake_schema=sf_schema
        )
        
        elapsed_time = time.time() - start_time
        
        if ok_tables > 0:
            print()
            print(f"✅ Exportación completada: {ok_tables} tablas exportadas")
            print(f"📊 Datos cargados en: {SF_DB}/{SF_SCHEMA}")
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
