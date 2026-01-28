import os
import sys
import clickhouse_connect
from dotenv import load_dotenv

load_dotenv()

# =========================
# ENV CONFIG
# =========================
CH_HOST = os.getenv("CH_HOST", "localhost")
CH_PORT = int(os.getenv("CH_PORT", "8123"))
CH_USER = os.getenv("CH_USER", "default")
CH_PASSWORD = os.getenv("CH_PASSWORD", "")
CH_DATABASE = os.getenv("CH_DATABASE", "default")

# =========================
# HELPERS
# =========================
def format_bytes(bytes_size):
    """Formatea bytes a KB con separadores de miles"""
    kb = bytes_size / 1024
    return f"{kb:,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")

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

def get_databases_info(ch):
    """Obtiene información de todas las bases de datos"""
    query = """
    SELECT 
        name as database_name
    FROM system.databases
    WHERE name NOT IN ('system', 'information_schema', 'INFORMATION_SCHEMA')
    ORDER BY name
    """
    result = ch.query(query)
    databases = [row[0] for row in result.result_rows]
    return databases

def get_database_stats(ch, db_name):
    """Obtiene estadísticas de una base de datos"""
    # Tablas
    tables_query = f"""
    SELECT COUNT(*) as count
    FROM system.tables
    WHERE database = '{db_name}'
      AND engine NOT IN ('View', 'MaterializedView')
    """
    tables_result = ch.query(tables_query)
    tables_count = tables_result.result_rows[0][0] if tables_result.result_rows else 0
    
    # Vistas (incluye MaterializedView)
    views_query = f"""
    SELECT COUNT(*) as count
    FROM system.tables
    WHERE database = '{db_name}'
      AND engine IN ('View', 'MaterializedView')
    """
    views_result = ch.query(views_query)
    views_count = views_result.result_rows[0][0] if views_result.result_rows else 0
    
    # Stored Procedures (ClickHouse no tiene SP tradicionales, pero tiene funciones)
    # Verificamos funciones del sistema
    functions_query = f"""
    SELECT COUNT(*) as count
    FROM system.functions
    WHERE origin = 'SQLUserDefined'
    """
    try:
        functions_result = ch.query(functions_query)
        sp_count = functions_result.result_rows[0][0] if functions_result.result_rows else 0
    except:
        sp_count = 0  # Si no hay funciones personalizadas
    
    # Tamaño en bytes
    size_query = f"""
    SELECT 
        COALESCE(SUM(bytes), 0) as total_bytes
    FROM system.parts
    WHERE database = '{db_name}'
      AND active = 1
    """
    size_result = ch.query(size_query)
    total_bytes = size_result.result_rows[0][0] if size_result.result_rows else 0
    
    return {
        'tables': tables_count,
        'views': views_count,
        'sp': sp_count,
        'size_bytes': total_bytes
    }

def main():
    print("=" * 80)
    print("INFORMACIÓN DE BASES DE DATOS - CLICKHOUSE")
    print("=" * 80)
    print(f"Servidor: {CH_HOST}:{CH_PORT}")
    print(f"Usuario: {CH_USER}")
    print()
    
    try:
        ch = ch_client()
        
        # Verificar conexión
        ch.query("SELECT 1")
        print("[OK] Conexión a ClickHouse establecida")
        print()
        
        databases = get_databases_info(ch)
        
        if not databases:
            print("No se encontraron bases de datos.")
            return
        
        # Encabezado de la tabla
        print(f"{'Base de Datos':<30} {'Tablas':<10} {'Vistas':<10} {'SP/Func':<10} {'Tamaño (KB)':<20}")
        print("-" * 80)
        
        total_tables = 0
        total_views = 0
        total_sp = 0
        total_size = 0
        
        for db_name in databases:
            stats = get_database_stats(ch, db_name)
            size_kb = format_bytes(stats['size_bytes'])
            
            print(f"{db_name:<30} {stats['tables']:<10} {stats['views']:<10} {stats['sp']:<10} {size_kb:<20}")
            
            total_tables += stats['tables']
            total_views += stats['views']
            total_sp += stats['sp']
            total_size += stats['size_bytes']
        
        # Totales
        print("-" * 80)
        print(f"{'TOTAL':<30} {total_tables:<10} {total_views:<10} {total_sp:<10} {format_bytes(total_size):<20}")
        print("=" * 80)
        
    except Exception as e:
        print(f"[ERROR] Error conectando a ClickHouse: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()
