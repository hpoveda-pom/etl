#!/usr/bin/env python3
"""
Script para comparar tablas entre SQL Server y ClickHouse
==========================================================

Compara el número de filas entre tablas de SQL Server y ClickHouse
y muestra alertas solo cuando hay inconsistencias.

Uso:
    python compare_sqlserver_clickhouse.py SQL_DB CH_DB [tablas] [--prod]
    
Ejemplos:
    # Comparar todas las tablas
    python compare_sqlserver_clickhouse.py POM_Aplicaciones POM_Aplicaciones
    
    # Comparar tablas específicas
    python compare_sqlserver_clickhouse.py POM_Aplicaciones POM_Aplicaciones "dbo.Tabla1,dbo.Tabla2"
    
    # Usar producción
    python compare_sqlserver_clickhouse.py POM_Aplicaciones POM_Aplicaciones --prod
"""

import os
import sys
from pathlib import Path
import pyodbc
import clickhouse_connect
from dotenv import load_dotenv

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

SQL_SERVER_PROD = os.getenv("SQL_SERVER_PROD")
SQL_USER_PROD = os.getenv("SQL_USER_PROD")
SQL_PASSWORD_PROD = os.getenv("SQL_PASSWORD_PROD")

CH_HOST = os.getenv("CH_HOST", "localhost")
CH_PORT = int(os.getenv("CH_PORT", "8123"))
CH_USER = os.getenv("CH_USER", "default")
CH_PASSWORD = os.getenv("CH_PASSWORD", "")

# =========================
# HELPERS
# =========================
def parse_args():
    """Parsea argumentos de línea de comandos"""
    if len(sys.argv) < 3:
        print("Uso: python compare_sqlserver_clickhouse.py SQL_DB CH_DB [tablas] [--prod]")
        print("Ejemplo: python compare_sqlserver_clickhouse.py POM_Aplicaciones POM_Aplicaciones")
        print("Ejemplo: python compare_sqlserver_clickhouse.py POM_Aplicaciones POM_Aplicaciones \"dbo.Tabla1,dbo.Tabla2\"")
        sys.exit(1)
    
    sql_db = sys.argv[1].strip()
    ch_db = sys.argv[2].strip()
    
    use_prod = "--prod" in sys.argv or "prod" in sys.argv
    
    requested_tables = None
    if len(sys.argv) >= 4:
        tables_arg = sys.argv[3].strip()
        if tables_arg and tables_arg not in ("--prod", "prod"):
            if tables_arg == "*" or tables_arg.lower() == "all":
                requested_tables = None
            else:
                requested_tables = [t.strip() for t in tables_arg.split(",") if t.strip()]
    
    return sql_db, ch_db, requested_tables, use_prod

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

def ch_client():
    secure = (CH_PORT == 8443)
    return clickhouse_connect.get_client(
        host=CH_HOST,
        port=CH_PORT,
        username=CH_USER,
        password=CH_PASSWORD,
        database="default",
        secure=secure,
        verify=False,
    )

def get_sqlserver_tables(cursor, requested_tables=None):
    """Obtiene lista de tablas de SQL Server"""
    if requested_tables is None:
        query = """
        SELECT TABLE_SCHEMA, TABLE_NAME
        FROM INFORMATION_SCHEMA.TABLES
        WHERE TABLE_TYPE = 'BASE TABLE'
          AND TABLE_NAME NOT LIKE 'TMP\\_%' ESCAPE '\\'
        ORDER BY TABLE_SCHEMA, TABLE_NAME
        """
        cursor.execute(query)
        return cursor.fetchall()
    
    normalized = []
    for t in requested_tables:
        if "." in t:
            schema, table = t.split(".", 1)
            normalized.append((schema.strip(), table.strip()))
        else:
            normalized.append(("dbo", t.strip()))
    return normalized

def get_clickhouse_tables(ch, db_name):
    """Obtiene lista de tablas de ClickHouse"""
    query = f"""
    SELECT name
    FROM system.tables
    WHERE database = '{db_name}'
      AND engine NOT IN ('View', 'MaterializedView')
    ORDER BY name
    """
    result = ch.query(query)
    return [row[0] for row in result.result_rows]

def get_sqlserver_row_count(cursor, schema, table):
    """Obtiene el número de filas de una tabla en SQL Server"""
    try:
        query = f"SELECT COUNT(*) FROM [{schema}].[{table}]"
        cursor.execute(query)
        result = cursor.fetchone()
        return result[0] if result else 0
    except Exception as e:
        return None

def get_clickhouse_row_count(ch, db_name, table):
    """Obtiene el número de filas de una tabla en ClickHouse"""
    try:
        query = f"SELECT count() FROM `{db_name}`.`{table}`"
        result = ch.query(query)
        return result.result_rows[0][0] if result.result_rows else 0
    except Exception as e:
        return None

def format_number(num):
    """Formatea número con separadores de miles"""
    if num is None:
        return "N/A"
    return f"{num:,}".replace(",", ".")

def main():
    sql_db, ch_db, requested_tables, use_prod = parse_args()
    
    env_type = "PRODUCCIÓN" if use_prod else "DESARROLLO"
    
    print("=" * 80)
    print("COMPARACIÓN SQL SERVER vs CLICKHOUSE")
    print("=" * 80)
    print(f"Entorno: {env_type}")
    print(f"SQL Server DB: {sql_db}")
    print(f"ClickHouse DB: {ch_db}")
    if requested_tables:
        print(f"Tablas: {', '.join([f'{s}.{t}' for s, t in requested_tables])}")
    else:
        print(f"Tablas: Todas")
    print()
    
    try:
        # Conectar a SQL Server
        sql_conn_obj = sql_conn(sql_db, use_prod)
        sql_cursor = sql_conn_obj.cursor()
        print(f"[OK] Conexión a SQL Server establecida")
        
        # Conectar a ClickHouse
        ch = ch_client()
        ch.query("SELECT 1")
        print(f"[OK] Conexión a ClickHouse establecida")
        print()
        
        # Verificar que la base de datos de ClickHouse existe
        databases = ch.query(f"SHOW DATABASES LIKE '{ch_db}'")
        if not databases.result_rows:
            print(f"[ERROR] La base de datos '{ch_db}' no existe en ClickHouse")
            sys.exit(1)
        
        # Obtener tablas de SQL Server
        sql_tables = get_sqlserver_tables(sql_cursor, requested_tables)
        
        if not sql_tables:
            print("[INFO] No se encontraron tablas para comparar")
            return
        
        print(f"[INFO] Comparando {len(sql_tables)} tabla(s)...")
        print()
        
        # Comparar cada tabla
        inconsistencies = []
        not_found_in_ch = []
        errors = []
        
        for schema, table in sql_tables:
            sql_count = get_sqlserver_row_count(sql_cursor, schema, table)
            
            if sql_count is None:
                errors.append((f"{schema}.{table}", "Error obteniendo conteo de SQL Server"))
                continue
            
            # Buscar la tabla en ClickHouse (puede estar sin schema)
            ch_count = get_clickhouse_row_count(ch, ch_db, table)
            
            if ch_count is None:
                not_found_in_ch.append(f"{schema}.{table}")
                continue
            
            # Comparar conteos
            if sql_count != ch_count:
                inconsistencies.append({
                    'table': f"{schema}.{table}",
                    'sql_count': sql_count,
                    'ch_count': ch_count,
                    'diff': sql_count - ch_count
                })
        
        # Cerrar conexiones
        sql_cursor.close()
        sql_conn_obj.close()
        
        # Mostrar resultados
        if not inconsistencies and not not_found_in_ch and not errors:
            print("[OK] Todas las tablas son consistentes")
            print(f"    {len(sql_tables)} tabla(s) comparada(s) correctamente")
            return
        
        # Mostrar inconsistencias
        if inconsistencies:
            print("=" * 80)
            print("INCONSISTENCIAS DETECTADAS")
            print("=" * 80)
            print(f"{'Tabla':<40} {'SQL Server':<15} {'ClickHouse':<15} {'Diferencia':<15}")
            print("-" * 80)
            
            for inc in inconsistencies:
                diff_str = f"{inc['diff']:+,}" if inc['diff'] != 0 else "0"
                diff_str = diff_str.replace(",", ".")
                print(f"{inc['table']:<40} {format_number(inc['sql_count']):<15} {format_number(inc['ch_count']):<15} {diff_str:<15}")
            
            print("-" * 80)
            print(f"Total inconsistencias: {len(inconsistencies)}")
            print()
        
        # Mostrar tablas no encontradas en ClickHouse
        if not_found_in_ch:
            print("=" * 80)
            print("TABLAS NO ENCONTRADAS EN CLICKHOUSE")
            print("=" * 80)
            for table in not_found_in_ch:
                print(f"  - {table}")
            print(f"Total: {len(not_found_in_ch)} tabla(s)")
            print()
        
        # Mostrar errores
        if errors:
            print("=" * 80)
            print("ERRORES")
            print("=" * 80)
            for table, error in errors:
                print(f"  - {table}: {error}")
            print(f"Total: {len(errors)} error(es)")
            print()
        
        # Resumen
        consistent_count = len(sql_tables) - len(inconsistencies) - len(not_found_in_ch) - len(errors)
        if consistent_count > 0:
            print(f"[INFO] Tablas consistentes: {consistent_count}")
        
        print("=" * 80)
        
        # Exit code 1 si hay inconsistencias
        if inconsistencies or not_found_in_ch:
            sys.exit(1)
        
    except Exception as e:
        print(f"[ERROR] Error durante la comparación: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)

if __name__ == "__main__":
    main()
