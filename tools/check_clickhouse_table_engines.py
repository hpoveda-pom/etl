#!/usr/bin/env python3
"""
Script para verificar los engines de las tablas en ClickHouse
==============================================================

Muestra qué tablas están usando ReplacingMergeTree (recomendado para streaming)
y cuáles están usando otros engines.

Uso:
    python check_clickhouse_table_engines.py [DATABASE] [--fix]
    
Ejemplos:
    # Verificar todas las bases de datos
    python check_clickhouse_table_engines.py
    
    # Verificar una base de datos específica
    python check_clickhouse_table_engines.py POM_Aplicaciones
    
    # Verificar y mostrar sugerencias de ALTER TABLE
    python check_clickhouse_table_engines.py POM_Aplicaciones --fix
"""

import os
import sys
from pathlib import Path
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
CH_HOST = os.getenv("CH_HOST", "localhost")
CH_PORT = int(os.getenv("CH_PORT", "8123"))
CH_USER = os.getenv("CH_USER", "default")
CH_PASSWORD = os.getenv("CH_PASSWORD", "")

# =========================
# HELPERS
# =========================
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

def get_databases(ch):
    """Obtiene lista de bases de datos"""
    result = ch.query("SHOW DATABASES")
    databases = [row[0] for row in result.result_rows 
                 if row[0] not in ('system', 'information_schema', 'INFORMATION_SCHEMA', 'default')]
    return databases

def get_tables_info(ch, db_name):
    """Obtiene información de tablas de una base de datos"""
    query = f"""
    SELECT 
        name,
        engine,
        engine_full,
        total_rows,
        total_bytes
    FROM system.tables
    WHERE database = '{db_name}'
      AND engine NOT IN ('View', 'MaterializedView')
    ORDER BY name
    """
    result = ch.query(query)
    return result.result_rows

def get_primary_key(ch, db_name, table_name):
    """Obtiene la clave primaria de una tabla"""
    try:
        query = f"""
        SELECT name
        FROM system.columns
        WHERE database = '{db_name}'
          AND table = '{table_name}'
          AND is_in_primary_key = 1
        ORDER BY position
        """
        result = ch.query(query)
        pk_cols = [row[0] for row in result.result_rows]
        return pk_cols if pk_cols else None
    except:
        return None

def parse_args():
    """Parsea argumentos de línea de comandos"""
    db_name = None
    show_fix = False
    
    if len(sys.argv) > 1:
        for arg in sys.argv[1:]:
            if arg == "--fix":
                show_fix = True
            elif not arg.startswith("--"):
                db_name = arg.strip()
    
    return db_name, show_fix

def main():
    db_name, show_fix = parse_args()
    
    print("=" * 80)
    print("VERIFICACIÓN DE ENGINES - CLICKHOUSE")
    print("=" * 80)
    print(f"Servidor: {CH_HOST}:{CH_PORT}")
    print()
    
    try:
        ch = ch_client()
        ch.query("SELECT 1")
        print("[OK] Conexión a ClickHouse establecida")
        print()
        
        if db_name:
            databases = [db_name]
        else:
            databases = get_databases(ch)
        
        if not databases:
            print("No se encontraron bases de datos.")
            return
        
        total_tables = 0
        replacing_tables = 0
        merge_tree_tables = 0
        other_tables = 0
        
        for db in databases:
            print(f"Base de datos: {db}")
            print("-" * 80)
            
            tables_info = get_tables_info(ch, db)
            
            if not tables_info:
                print("  No hay tablas en esta base de datos")
                print()
                continue
            
            print(f"{'Tabla':<40} {'Engine':<25} {'Filas':<15} {'Tamaño (MB)':<15}")
            print("-" * 80)
            
            for name, engine, engine_full, rows, bytes_size in tables_info:
                total_tables += 1
                size_mb = (bytes_size / 1024 / 1024) if bytes_size else 0
                rows_str = f"{rows:,}".replace(",", ".") if rows else "0"
                size_str = f"{size_mb:,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")
                
                engine_display = engine
                if "ReplacingMergeTree" in engine:
                    replacing_tables += 1
                    engine_display = f"{engine} [OK]"
                elif "MergeTree" in engine:
                    merge_tree_tables += 1
                    engine_display = f"{engine} [Cambiar a ReplacingMergeTree]"
                else:
                    other_tables += 1
                    engine_display = f"{engine} [Revisar]"
                
                print(f"{name:<40} {engine_display:<25} {rows_str:<15} {size_str:<15}")
                
                if show_fix and "ReplacingMergeTree" not in engine and "MergeTree" in engine:
                    # Obtener PK para sugerir ALTER TABLE
                    pk_cols = get_primary_key(ch, db, name)
                    if pk_cols:
                        pk_str = ", ".join([f"`{col}`" for col in pk_cols])
                        print(f"  [SUGERENCIA] ALTER TABLE `{db}`.`{name}` MODIFY ENGINE ReplacingMergeTree ORDER BY ({pk_str})")
                    else:
                        print(f"  [SUGERENCIA] Tabla sin PK definida - considerar agregar PK antes de cambiar a ReplacingMergeTree")
            
            print()
        
        # Resumen
        print("=" * 80)
        print("RESUMEN")
        print("=" * 80)
        print(f"Total tablas: {total_tables}")
        print(f"ReplacingMergeTree: {replacing_tables} [OK para streaming]")
        print(f"MergeTree: {merge_tree_tables} [Considerar cambiar a ReplacingMergeTree]")
        print(f"Otros engines: {other_tables}")
        print("=" * 80)
        
        if merge_tree_tables > 0:
            print()
            print("[INFO] Las tablas con MergeTree pueden acumular duplicados durante el streaming.")
            print("       Se recomienda cambiarlas a ReplacingMergeTree si tienen clave primaria.")
            print("       Usa --fix para ver sugerencias de ALTER TABLE.")
        
    except Exception as e:
        print(f"[ERROR] Error: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)

if __name__ == "__main__":
    main()
