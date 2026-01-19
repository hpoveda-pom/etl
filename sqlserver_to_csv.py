import os
import re
import shutil
import time
import warnings
from datetime import datetime

import pandas as pd
import pyodbc

# Suprimir warnings de pandas sobre pyodbc
warnings.filterwarnings('ignore', category=UserWarning, module='pandas')

# ============== SQL Server config ==============
SQL_SERVER = os.getenv("SQL_SERVER", r"SRV-DESA\SQLEXPRESS")
SQL_DATABASE = os.getenv("SQL_DATABASE", "")
SQL_USER = os.getenv("SQL_USER", "")
SQL_PASSWORD = os.getenv("SQL_PASSWORD", "")
# Por defecto usa autenticación Windows. Si especificas USER y PASSWORD, usa autenticación SQL
SQL_DRIVER = os.getenv("SQL_DRIVER", "ODBC Driver 17 for SQL Server")  # Ajustar según tu driver
# Alternativas comunes: "SQL Server", "SQL Server Native Client 11.0", "ODBC Driver 13 for SQL Server"

# ============== Carpetas ==============
CSV_STAGING_DIR = os.getenv("CSV_STAGING_DIR", r"UPLOADS\POM_DROP\csv_staging")

# Opcional: especificar tablas a exportar (coma-separado). Si está vacío, exporta todas.
TABLES_FILTER = [s.strip() for s in os.getenv("TABLES_FILTER", "").split(",") if s.strip()]

# Prefijos de nombres de tablas a excluir (no se exportarán tablas que empiecen con estos prefijos)
EXCLUDED_TABLE_PREFIXES = ["TMP_"]  # Ejemplo: excluir todas las tablas que empiecen con "TMP_"


def ensure_dirs():
    os.makedirs(CSV_STAGING_DIR, exist_ok=True)


def sanitize_token(s: str, maxlen: int = 120) -> str:
    s = (s or "").strip()
    s = re.sub(r"[^\w\-\.]+", "_", s, flags=re.UNICODE)
    s = re.sub(r"_+", "_", s).strip("_")
    return s[:maxlen] if s else "NA"


def get_sql_connection():
    """
    Crea una conexión a SQL Server.
    Por defecto usa autenticación Windows (Integrated Security).
    Si se especifican SQL_USER y SQL_PASSWORD, usa autenticación SQL Server.
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
                print(f"⚠️  Driver '{SQL_DRIVER}' no encontrado. Usando '{driver_to_use}'")
                break
        else:
            raise RuntimeError(
                f"No se encontró un driver ODBC compatible. Drivers disponibles: {', '.join(available_drivers)}"
            )
    
    # Construir connection string
    if SQL_USER and SQL_PASSWORD:
        # Autenticación SQL Server
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
        # Autenticación Windows (Integrated Security) - POR DEFECTO
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


def export_table_to_csv(conn, table_name: str, output_path: str, reconnect_fn=None) -> tuple[int, int]:
    """
    Exporta una tabla de SQL Server a CSV.
    Retorna (row_count, col_count)
    """
    max_retries = 3
    current_conn = conn
    
    for attempt in range(max_retries):
        try:
            # Leer tabla completa con pandas
            query = f"SELECT * FROM [{table_name.split('.')[0]}].[{table_name.split('.')[1]}]"
            df = pd.read_sql(query, current_conn)
            
            # Reemplazar NaN por None para consistencia
            df = df.where(pd.notnull(df), None)
            
            # Guardar a CSV
            df.to_csv(output_path, index=False, encoding='utf-8')
            
            row_count = int(df.shape[0])
            col_count = int(df.shape[1])
            
            return row_count, col_count
            
        except (pyodbc.Error, Exception) as e:
            error_str = str(e).lower()
            if ('communication link failure' in error_str or '08s01' in error_str or 
                'connection' in error_str) and attempt < max_retries - 1:
                if reconnect_fn:
                    print(f"    ⚠️  Conexión perdida, reintentando... (intento {attempt + 1}/{max_retries})")
                    current_conn = reconnect_fn()
                    time.sleep(2)  # Esperar antes de reintentar
                    continue
            # Si no es error de conexión o ya se agotaron los reintentos, lanzar el error
            raise


def export_database_to_csv(database_name: str = None, tables: list = None) -> int:
    """
    Exporta tablas de SQL Server a CSV.
    
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
    db_output_dir = os.path.join(CSV_STAGING_DIR, f"SQLSERVER_{db_structure}")
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
                print(f"🚫 Tablas excluidas por prefijos {EXCLUDED_TABLE_PREFIXES}: {excluded_count}")
        
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
            print(f"⚠️  No hay tablas para exportar (filtro aplicado: {len(TABLES_FILTER)} tablas)")
            return 0
        
        print(f"📊 Base de datos: {SQL_DATABASE}")
        print(f"📋 Tablas encontradas: {len(all_tables)}")
        print(f"📤 Tablas a exportar: {len(tables_to_export)}")
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
            output_filename = f"{table_safe}.csv"
            output_path = os.path.join(db_output_dir, output_filename)
            
            try:
                print(f"  → Exportando: {table_name} → {output_filename}")
                row_count, col_count = export_table_to_csv(conn, table_name, output_path, reconnect_fn)
                file_bytes = os.path.getsize(output_path)
                
                print(f"    ✓ {row_count} filas, {col_count} columnas, {file_bytes} bytes")
                ok += 1
                
            except Exception as e:
                print(f"    ❌ Error exportando {table_name}: {e}")
        
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
        ok_tables = export_database_to_csv(database_name=database, tables=tables)
        
        if ok_tables > 0:
            print()
            print(f"✅ Exportación completada: {ok_tables} tablas exportadas")
            print(f"📁 Archivos guardados en: {CSV_STAGING_DIR}")
        else:
            print()
            print("⚠️  No se exportaron tablas")
            
    except Exception as e:
        print(f"❌ Error: {e}")
        return 1
    
    return 0


if __name__ == "__main__":
    exit(main())
