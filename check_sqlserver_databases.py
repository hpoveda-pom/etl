import os
import sys
import pyodbc
from dotenv import load_dotenv

load_dotenv()

# =========================
# ENV CONFIG
# =========================
SQL_SERVER = os.getenv("SQL_SERVER")
SQL_USER = os.getenv("SQL_USER")
SQL_PASSWORD = os.getenv("SQL_PASSWORD")
SQL_DRIVER = os.getenv("SQL_DRIVER", "ODBC Driver 17 for SQL Server")

# =========================
# HELPERS
# =========================
def format_bytes(bytes_size):
    """Formatea bytes a KB con separadores de miles"""
    kb = bytes_size / 1024
    return f"{kb:,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")

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

def get_databases_info(conn):
    """Obtiene lista de bases de datos"""
    cursor = conn.cursor()
    query = """
    SELECT name
    FROM sys.databases
    WHERE name NOT IN ('master', 'tempdb', 'model', 'msdb')
      AND state_desc = 'ONLINE'
    ORDER BY name
    """
    cursor.execute(query)
    databases = [row[0] for row in cursor.fetchall()]
    cursor.close()
    return databases

def get_database_stats(conn, db_name):
    """Obtiene estadísticas de una base de datos"""
    cursor = conn.cursor()
    
    # Cambiar contexto a la base de datos
    cursor.execute(f"USE [{db_name}]")
    
    # Tablas
    tables_query = """
    SELECT COUNT(*)
    FROM INFORMATION_SCHEMA.TABLES
    WHERE TABLE_TYPE = 'BASE TABLE'
    """
    cursor.execute(tables_query)
    tables_count = cursor.fetchone()[0]
    
    # Vistas
    views_query = """
    SELECT COUNT(*)
    FROM INFORMATION_SCHEMA.VIEWS
    """
    cursor.execute(views_query)
    views_count = cursor.fetchone()[0]
    
    # Stored Procedures
    sp_query = """
    SELECT COUNT(*)
    FROM INFORMATION_SCHEMA.ROUTINES
    WHERE ROUTINE_TYPE = 'PROCEDURE'
    """
    cursor.execute(sp_query)
    sp_count = cursor.fetchone()[0]
    
    # Tamaño en bytes (suma de data + index)
    size_query = """
    SELECT 
        COALESCE(SUM(a.total_pages) * 8192, 0) as total_bytes
    FROM sys.tables t
    INNER JOIN sys.indexes i ON t.OBJECT_ID = i.object_id
    INNER JOIN sys.partitions p ON i.object_id = p.OBJECT_ID AND i.index_id = p.index_id
    INNER JOIN sys.allocation_units a ON p.partition_id = a.container_id
    WHERE t.is_ms_shipped = 0
      AND i.OBJECT_ID > 255
    """
    cursor.execute(size_query)
    size_result = cursor.fetchone()
    total_bytes = size_result[0] if size_result and size_result[0] else 0
    
    cursor.close()
    
    return {
        'tables': tables_count,
        'views': views_count,
        'sp': sp_count,
        'size_bytes': total_bytes
    }

def main():
    print("=" * 80)
    print("INFORMACIÓN DE BASES DE DATOS - SQL SERVER")
    print("=" * 80)
    print(f"Servidor: {SQL_SERVER}")
    print(f"Usuario: {SQL_USER}")
    print()
    
    try:
        # Conectar a master para obtener lista de bases de datos
        conn = sql_conn("master")
        cursor = conn.cursor()
        cursor.execute("SELECT DB_NAME()")
        print(f"[OK] Conexión a SQL Server establecida. Conectado a: {cursor.fetchone()[0]}")
        cursor.close()
        print()
        
        databases = get_databases_info(conn)
        conn.close()
        
        if not databases:
            print("No se encontraron bases de datos.")
            return
        
        # Encabezado de la tabla
        print(f"{'Base de Datos':<30} {'Tablas':<10} {'Vistas':<10} {'SP':<10} {'Tamaño (KB)':<20}")
        print("-" * 80)
        
        total_tables = 0
        total_views = 0
        total_sp = 0
        total_size = 0
        
        for db_name in databases:
            try:
                # Conectar a cada base de datos para obtener estadísticas
                db_conn = sql_conn(db_name)
                stats = get_database_stats(db_conn, db_name)
                db_conn.close()
                
                size_kb = format_bytes(stats['size_bytes'])
                
                print(f"{db_name:<30} {stats['tables']:<10} {stats['views']:<10} {stats['sp']:<10} {size_kb:<20}")
                
                total_tables += stats['tables']
                total_views += stats['views']
                total_sp += stats['sp']
                total_size += stats['size_bytes']
            except Exception as e:
                print(f"{db_name:<30} {'ERROR':<10} {'ERROR':<10} {'ERROR':<10} {'ERROR':<20}")
                print(f"  [WARN] Error obteniendo estadísticas de {db_name}: {e}")
        
        # Totales
        print("-" * 80)
        print(f"{'TOTAL':<30} {total_tables:<10} {total_views:<10} {total_sp:<10} {format_bytes(total_size):<20}")
        print("=" * 80)
        
    except Exception as e:
        print(f"[ERROR] Error conectando a SQL Server: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()
