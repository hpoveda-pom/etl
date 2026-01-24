import os
import re
import shutil
import time
import warnings
import json
import gzip
import base64
from datetime import datetime, date, time as dt_time, timezone

import pandas as pd
import pyodbc

# Suprimir warnings de pandas sobre pyodbc
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
# Alternativas comunes: "SQL Server", "SQL Server Native Client 11.0", "ODBC Driver 13 for SQL Server"

# ============== Carpetas ==============
JSON_STAGING_DIR = os.getenv("JSON_STAGING_DIR", r"UPLOADS\POM_DROP\json_staging")

# Opcional: especificar tablas a exportar (coma-separado). Si está vacío, exporta todas.
TABLES_FILTER = [s.strip() for s in os.getenv("TABLES_FILTER", "").split(",") if s.strip()]

# Prefijos de nombres de tablas a excluir (no se exportarán tablas que empiecen con estos prefijos)
EXCLUDED_TABLE_PREFIXES = ["TMP_"]  # Ejemplo: excluir todas las tablas que empiecen con "TMP_"

# Tamaño del chunk para procesamiento en lotes
CHUNK_SIZE = 10000  # Filas por chunk


def json_serializer(obj):
    """
    Serializador custom para json.dumps que convierte datetime/date/time a ISO 8601
    y datos binarios (bytes) a base64.
    """
    if isinstance(obj, (datetime, date, dt_time)):
        return obj.isoformat()
    elif isinstance(obj, bytes):
        # Convertir datos binarios a base64 para JSON
        return base64.b64encode(obj).decode('utf-8')
    raise TypeError(f"Type {type(obj)} not serializable")


def ensure_dirs():
    os.makedirs(JSON_STAGING_DIR, exist_ok=True)


def sanitize_token(s: str, maxlen: int = 120) -> str:
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
    
    # Detectar qué drivers ODBC están disponibles
    available_drivers = [driver for driver in pyodbc.drivers()]
    
    # Intentar encontrar un driver compatible
    driver_to_use = SQL_DRIVER
    if driver_to_use not in available_drivers:
        # Buscar drivers alternativos comunes
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


def export_table_to_json(conn, table_name: str, output_path: str, reconnect_fn=None) -> tuple[int, int]:
    """
    Exporta una tabla de SQL Server a JSON Lines comprimido (gzip), respetando los tipos de datos.
    Usa procesamiento por chunks para optimizar memoria y rendimiento.
    Retorna (row_count, col_count)
    """
    max_retries = 3
    current_conn = conn
    
    # Timestamp UTC para metadata
    extract_ts = datetime.now(timezone.utc).isoformat()
    
    for attempt in range(max_retries):
        try:
            # Construir query
            query = f"SELECT * FROM [{table_name.split('.')[0]}].[{table_name.split('.')[1]}]"
            
            # Inicializar contadores
            total_row_count = 0
            col_count = 0
            
            # Abrir archivo gzip una sola vez para streaming
            with gzip.open(output_path, 'wt', encoding='utf-8') as f:
                # Procesar tabla en chunks
                chunk_iter = pd.read_sql(query, current_conn, chunksize=CHUNK_SIZE)
                
                first_chunk = True
                for df_chunk in chunk_iter:
                    # Obtener número de columnas del primer chunk
                    if first_chunk:
                        col_count = int(df_chunk.shape[1])
                        first_chunk = False
                    
                    # Convertir NaN a None para JSON
                    df_chunk = df_chunk.where(pd.notnull(df_chunk), None)
                    
                    # Convertir DataFrame chunk a lista de diccionarios (respetando tipos de pandas)
                    # pandas.to_dict mantiene los tipos nativos (int, float, datetime, etc.)
                    records = df_chunk.to_dict(orient='records')
                    
                    # Escribir cada registro como JSON Line con metadata
                    for record in records:
                        # Añadir bloque _meta a cada registro
                        record_with_meta = {
                            **record,
                            '_meta': {
                                'source_table': table_name,
                                'extract_ts': extract_ts
                            }
                        }
                        
                        # Escribir línea JSON (JSON Lines format)
                        f.write(json.dumps(record_with_meta, ensure_ascii=False, default=json_serializer))
                        f.write('\n')
                    
                    total_row_count += len(records)
            
            return total_row_count, col_count
            
        except (pyodbc.Error, Exception) as e:
            error_str = str(e).lower()
            if ('communication link failure' in error_str or '08s01' in error_str or 
                'connection' in error_str) and attempt < max_retries - 1:
                if reconnect_fn:
                    print(f"    [WARN]  Conexión perdida, reintentando... (intento {attempt + 1}/{max_retries})")
                    current_conn = reconnect_fn()
                    time.sleep(2)  # Esperar antes de reintentar
                    continue
            # Si no es error de conexión o ya se agotaron los reintentos, lanzar el error
            raise


def list_tables(conn):
    """
    Lista las tablas de la base de datos.
    Retorna lista de nombres de tablas.
    """
    cursor = conn.cursor()
    # Obtener tablas de usuario (excluir tablas del sistema)
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
        # Formato: schema.tablename
        full_name = f"{schema}.{table}"
        tables.append(full_name)
    cursor.close()
    return tables


def export_database_to_json(database_name: str = None, tables: list = None) -> int:
    """
    Exporta tablas de SQL Server a JSON.
    
    Args:
        database_name: Nombre de la base de datos (si None, usa SQL_DATABASE de env)
        tables: Lista de tablas a exportar (si None, usa TABLES_FILTER o todas)
    
    Retorna el número de tablas exportadas exitosamente.
    """
    global SQL_DATABASE
    
    if database_name:
        SQL_DATABASE = database_name
    
    if not SQL_DATABASE:
        raise RuntimeError("Debes especificar la base de datos (variable SQL_DATABASE o parámetro database_name)")
    
    # Crear carpeta de destino para esta base de datos
    db_structure = sanitize_token(SQL_DATABASE)
    db_output_dir = os.path.join(JSON_STAGING_DIR, f"SQLSERVER_{db_structure}")
    os.makedirs(db_output_dir, exist_ok=True)
    
    conn = None
    reconnect_fn = lambda: get_sql_connection()  # Función para reconectar
    
    try:
        conn = get_sql_connection()
        
        # Obtener lista de tablas
        all_tables = list_tables(conn)
        total_tables_found = len(all_tables)
        
        # Filtrar tablas excluidas por prefijos
        if EXCLUDED_TABLE_PREFIXES:
            # Extraer solo el nombre de la tabla (sin schema) para verificar prefijos
            filtered_tables = []
            excluded_count = 0
            for t in all_tables:
                # Extraer nombre de tabla sin schema
                if "." in t:
                    table_only = t.split(".", 1)[1]
                else:
                    table_only = t
                
                # Verificar si la tabla empieza con algún prefijo excluido
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
            # Filtrar por las tablas especificadas
            tables_to_export = [t for t in all_tables if t in tables or any(t.endswith(f".{tb}") for tb in tables)]
        elif TABLES_FILTER:
            # Filtrar por TABLES_FILTER de env vars
            tables_to_export = [t for t in all_tables if any(t.endswith(f".{tb}") or t == tb for tb in TABLES_FILTER)]
        else:
            # Exportar todas las tablas (ya filtradas por prefijos excluidos)
            tables_to_export = all_tables
        
        if not tables_to_export:
            print(f"[WARN]  No hay tablas para exportar (filtro aplicado: {len(TABLES_FILTER)} tablas)")
            return 0
        
        print(f" Base de datos: {SQL_DATABASE}")
        print(f" Tablas encontradas: {len(all_tables)}")
        print(f" Tablas a exportar: {len(tables_to_export)}")
        print()
        
        ok = 0
        
        for table_name in tables_to_export:
            # Extraer solo el nombre de la tabla (sin el schema)
            # table_name viene como "schema.tablename", extraer solo "tablename"
            if "." in table_name:
                table_only = table_name.split(".", 1)[1]  # Tomar solo la parte después del punto
            else:
                table_only = table_name
            
            # Sanitizar nombre de tabla para nombre de archivo
            table_safe = sanitize_token(table_only)
            output_filename = f"{table_safe}.json.gz"
            output_path = os.path.join(db_output_dir, output_filename)
            
            try:
                print(f"  -> Exportando: {table_name} -> {output_filename}")
                row_count, col_count = export_table_to_json(conn, table_name, output_path, reconnect_fn)
                file_bytes = os.path.getsize(output_path)
                
                print(f"    [OK] {row_count} filas, {col_count} columnas, {file_bytes} bytes")
                ok += 1
                
            except Exception as e:
                print(f"    [ERROR] Error exportando {table_name}: {e}")
        
        return ok
        
    finally:
        if conn:
            conn.close()


def main():
    """
    Función principal.
    Puedes especificar la base de datos y tablas como argumentos o usar variables de entorno.
    """
    import sys
    
    start_time = time.time()
    ensure_dirs()
    
    # Si se pasan argumentos, usarlos
    database = None
    tables = None
    
    if len(sys.argv) > 1:
        database = sys.argv[1]
    
    if len(sys.argv) > 2:
        # Tablas separadas por comas
        tables = [t.strip() for t in sys.argv[2].split(",")]
    
    try:
        ok_tables = export_database_to_json(database_name=database, tables=tables)
        
        elapsed_time = time.time() - start_time
        
        if ok_tables > 0:
            print()
            print(f"[OK] Exportación completada: {ok_tables} tablas exportadas")
            print(f"📁 Archivos guardados en: {JSON_STAGING_DIR}")
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
