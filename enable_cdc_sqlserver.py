#!/usr/bin/env python3
"""
Script auxiliar para habilitar CDC en SQL Server
================================================

Este script ayuda a habilitar Change Data Capture (CDC) en SQL Server
para las tablas que quieres monitorear.

Uso:
    python enable_cdc_sqlserver.py DATABASE [tablas] [--prod] [--enable-db]

Ejemplo:
    # Habilitar CDC en la base de datos
    python enable_cdc_sqlserver.py POM_Aplicaciones --enable-db --prod
    
    # Habilitar CDC en tablas específicas
    python enable_cdc_sqlserver.py POM_Aplicaciones "Tabla1,Tabla2" --prod
"""

import os
import sys
import pyodbc
from dotenv import load_dotenv

load_dotenv()

SQL_SERVER = os.getenv("SQL_SERVER")
SQL_USER = os.getenv("SQL_USER")
SQL_PASSWORD = os.getenv("SQL_PASSWORD")
SQL_DRIVER = os.getenv("SQL_DRIVER", "ODBC Driver 17 for SQL Server")

SQL_SERVER_PROD = os.getenv("SQL_SERVER_PROD")
SQL_USER_PROD = os.getenv("SQL_USER_PROD")
SQL_PASSWORD_PROD = os.getenv("SQL_PASSWORD_PROD")

def build_sqlserver_conn_str(database_name: str, use_prod: bool = False):
    if use_prod and SQL_SERVER_PROD and SQL_USER_PROD and SQL_PASSWORD_PROD:
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

def check_cdc_enabled(cursor, database_name: str) -> bool:
    """Verifica si CDC está habilitado en la base de datos"""
    try:
        cursor.execute("SELECT is_cdc_enabled FROM sys.databases WHERE name = ?", (database_name,))
        result = cursor.fetchone()
        return result and result[0] == 1
    except Exception as e:
        print(f"[ERROR] No se pudo verificar CDC: {e}")
        return False

def enable_cdc_database(cursor, database_name: str):
    """Habilita CDC en la base de datos"""
    try:
        print(f"[INFO] Habilitando CDC en la base de datos '{database_name}'...")
        cursor.execute(f"EXEC sys.sp_cdc_enable_db")
        cursor.commit()
        print(f"[OK] CDC habilitado en '{database_name}'")
        return True
    except Exception as e:
        print(f"[ERROR] Error habilitando CDC en la base de datos: {e}")
        return False

def get_tables(cursor, requested_tables=None):
    """Obtiene lista de tablas"""
    if requested_tables is None:
        q = """
        SELECT TABLE_SCHEMA, TABLE_NAME
        FROM INFORMATION_SCHEMA.TABLES
        WHERE TABLE_TYPE='BASE TABLE'
          AND TABLE_NAME NOT LIKE 'TMP\\_%' ESCAPE '\\'
        ORDER BY TABLE_SCHEMA, TABLE_NAME
        """
        cursor.execute(q)
        return cursor.fetchall()
    else:
        normalized = []
        for t in requested_tables:
            if "." in t:
                schema, table = t.split(".", 1)
                normalized.append((schema.strip(), table.strip()))
            else:
                normalized.append(("dbo", t.strip()))
        return normalized

def check_table_cdc_enabled(cursor, schema: str, table: str) -> bool:
    """Verifica si CDC está habilitado para una tabla"""
    try:
        q = """
        SELECT COUNT(*) 
        FROM cdc.change_tables 
        WHERE source_schema = ? AND source_object = ?
        """
        cursor.execute(q, (schema, table))
        result = cursor.fetchone()
        return result and result[0] > 0
    except Exception as e:
        return False

def enable_cdc_table(cursor, schema: str, table: str, capture_instance: str = None):
    """Habilita CDC para una tabla específica"""
    if capture_instance is None:
        capture_instance = f"{schema}_{table}"
    
    try:
        print(f"[INFO] Habilitando CDC en {schema}.{table}...")
        
        # Construir query para habilitar CDC
        # Nota: @capture_instance es opcional, SQL Server lo genera automáticamente si no se especifica
        query = f"""
        EXEC sys.sp_cdc_enable_table
            @source_schema = N'{schema}',
            @source_name = N'{table}',
            @role_name = NULL,
            @capture_instance = N'{capture_instance}'
        """
        
        cursor.execute(query)
        cursor.commit()
        print(f"[OK] CDC habilitado en {schema}.{table} (capture_instance: {capture_instance})")
        return True
    except Exception as e:
        print(f"[ERROR] Error habilitando CDC en {schema}.{table}: {e}")
        return False

def main():
    if len(sys.argv) < 2:
        print("Uso: python enable_cdc_sqlserver.py DATABASE [tablas] [--prod] [--enable-db]")
        print("\nEjemplos:")
        print("  python enable_cdc_sqlserver.py POM_Aplicaciones --enable-db --prod")
        print("  python enable_cdc_sqlserver.py POM_Aplicaciones \"Tabla1,Tabla2\" --prod")
        sys.exit(1)
    
    database_name = sys.argv[1]
    use_prod = "--prod" in sys.argv
    enable_db = "--enable-db" in sys.argv
    
    tables_arg = None
    for i, arg in enumerate(sys.argv[2:], 2):
        if arg not in ["--prod", "--enable-db"]:
            tables_arg = arg
            break
    
    try:
        conn = sql_conn(database_name, use_prod)
        cursor = conn.cursor()
        
        # Habilitar CDC en la base de datos si se solicita
        if enable_db:
            if check_cdc_enabled(cursor, database_name):
                print(f"[INFO] CDC ya está habilitado en '{database_name}'")
            else:
                enable_cdc_database(cursor, database_name)
        else:
            if not check_cdc_enabled(cursor, database_name):
                print(f"[ERROR] CDC no está habilitado en '{database_name}'")
                print(f"[INFO] Ejecuta primero: python enable_cdc_sqlserver.py {database_name} --enable-db")
                sys.exit(1)
        
        # Habilitar CDC en tablas
        if tables_arg:
            tables = get_tables(cursor, [t.strip() for t in tables_arg.split(",")])
        else:
            tables = get_tables(cursor, None)
        
        enabled_count = 0
        already_enabled_count = 0
        error_count = 0
        
        for schema, table in tables:
            if check_table_cdc_enabled(cursor, schema, table):
                print(f"[SKIP] {schema}.{table} - CDC ya habilitado")
                already_enabled_count += 1
            else:
                if enable_cdc_table(cursor, schema, table):
                    enabled_count += 1
                else:
                    error_count += 1
        
        print(f"\n[RESUMEN]")
        print(f"  Habilitadas: {enabled_count}")
        print(f"  Ya habilitadas: {already_enabled_count}")
        print(f"  Errores: {error_count}")
        
        cursor.close()
        conn.close()
        
    except Exception as e:
        print(f"[ERROR] {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()
