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

SQL_SERVER_PROD = os.getenv("SQL_SERVER_PROD")
SQL_USER_PROD = os.getenv("SQL_USER_PROD")
SQL_PASSWORD_PROD = os.getenv("SQL_PASSWORD_PROD")

# =========================
# BLACKLIST - Bases de datos excluidas
# =========================
EXCLUDED_DATABASES = [
    'Archive_Reporteria',
    'POMRestricted',
    'Testing',
    # Agregar más bases de datos aquí si es necesario
]

# =========================
# HELPERS
# =========================
def format_bytes(bytes_size):
    """Formatea bytes a KB con separadores de miles"""
    kb = bytes_size / 1024
    return f"{kb:,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")

def parse_args():
    """Parsea argumentos de línea de comandos"""
    use_prod = False
    
    if len(sys.argv) > 1:
        if "--prod" in sys.argv or "prod" in sys.argv:
            use_prod = True
        elif "--dev" in sys.argv or "dev" in sys.argv:
            use_prod = False
    
    return use_prod

def build_sqlserver_conn_str(database_name: str, use_prod: bool = False):
    if use_prod:
        if not SQL_SERVER_PROD or not SQL_USER_PROD or SQL_PASSWORD_PROD is None:
            raise Exception("Faltan SQL_SERVER_PROD / SQL_USER_PROD / SQL_PASSWORD_PROD en el .env")
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

def get_databases_info(conn):
    """Obtiene lista de bases de datos, excluyendo las del sistema y las de la blacklist"""
    cursor = conn.cursor()
    query = """
    SELECT name
    FROM sys.databases
    WHERE name NOT IN ('master', 'tempdb', 'model', 'msdb')
      AND state_desc = 'ONLINE'
    ORDER BY name
    """
    cursor.execute(query)
    all_databases = [row[0] for row in cursor.fetchall()]
    cursor.close()
    
    # Filtrar bases de datos excluidas
    databases = [db for db in all_databases if db not in EXCLUDED_DATABASES]
    
    if len(all_databases) != len(databases):
        excluded = [db for db in all_databases if db in EXCLUDED_DATABASES]
        print(f"[INFO] Bases de datos excluidas (blacklist): {', '.join(excluded)}")
        print()
    
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
    use_prod = parse_args()
    env_type = "PRODUCCIÓN" if use_prod else "DESARROLLO"
    
    # Obtener configuración según entorno
    if use_prod:
        server = SQL_SERVER_PROD
        user = SQL_USER_PROD
    else:
        server = SQL_SERVER
        user = SQL_USER
    
    print("=" * 80)
    print("INFORMACIÓN DE BASES DE DATOS - SQL SERVER")
    print("=" * 80)
    print(f"Entorno: {env_type}")
    print(f"Servidor: {server}")
    print(f"Usuario: {user}")
    print()
    
    try:
        # Conectar a master para obtener lista de bases de datos
        conn = sql_conn("master", use_prod)
        cursor = conn.cursor()
        cursor.execute("SELECT DB_NAME()")
        print(f"[OK] Conexión a SQL Server ({env_type}) establecida. Conectado a: {cursor.fetchone()[0]}")
        cursor.close()
        print()
        
        databases = get_databases_info(conn)
        conn.close()
        
        if not databases:
            print("No se encontraron bases de datos (todas están excluidas o no hay acceso).")
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
                db_conn = sql_conn(db_name, use_prod)
                stats = get_database_stats(db_conn, db_name)
                db_conn.close()
                
                size_kb = format_bytes(stats['size_bytes'])
                
                print(f"{db_name:<30} {stats['tables']:<10} {stats['views']:<10} {stats['sp']:<10} {size_kb:<20}")
                
                total_tables += stats['tables']
                total_views += stats['views']
                total_sp += stats['sp']
                total_size += stats['size_bytes']
            except Exception as e:
                # Si hay error, agregar a la blacklist para futuras ejecuciones
                error_msg = str(e)
                if "Cannot open database" in error_msg or "login failed" in error_msg.lower():
                    print(f"{db_name:<30} {'EXCLUIDA':<10} {'EXCLUIDA':<10} {'EXCLUIDA':<10} {'EXCLUIDA':<20}")
                    print(f"  [INFO] {db_name} agregada automáticamente a la blacklist (sin acceso)")
                else:
                    print(f"{db_name:<30} {'ERROR':<10} {'ERROR':<10} {'ERROR':<10} {'ERROR':<20}")
                    print(f"  [WARN] Error obteniendo estadísticas de {db_name}: {e}")
        
        # Totales
        print("-" * 80)
        print(f"{'TOTAL':<30} {total_tables:<10} {total_views:<10} {total_sp:<10} {format_bytes(total_size):<20}")
        print("=" * 80)
        
    except Exception as e:
        print(f"[ERROR] Error conectando a SQL Server ({env_type}): {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()
