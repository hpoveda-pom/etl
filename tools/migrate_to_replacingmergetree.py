#!/usr/bin/env python3
"""
Script para migrar tablas de MergeTree a ReplacingMergeTree
============================================================

Migra tablas existentes de MergeTree a ReplacingMergeTree para evitar duplicados
durante el streaming. Esto es necesario porque Silver creó las tablas con MergeTree.

Uso:
    python migrate_to_replacingmergetree.py DATABASE [tablas] [--dry-run] [--force]
    
Ejemplos:
    # Ver qué se haría
    python migrate_to_replacingmergetree.py POM_Aplicaciones --dry-run
    
    # Migrar todas las tablas con PK
    python migrate_to_replacingmergetree.py POM_Aplicaciones
    
    # Migrar tablas específicas
    python migrate_to_replacingmergetree.py POM_Aplicaciones "Bitacora_Acceso,EstadoLegal_Caso"
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

def get_table_info(ch, db_name, table_name):
    """Obtiene información de una tabla"""
    try:
        # Obtener CREATE TABLE
        create_result = ch.query(f"SHOW CREATE TABLE `{db_name}`.`{table_name}`")
        create_sql = create_result.result_rows[0][0] if create_result.result_rows else None
        
        # Obtener engine
        engine_result = ch.query(f"""
        SELECT engine
        FROM system.tables
        WHERE database = '{db_name}' AND name = '{table_name}'
        """)
        engine = engine_result.result_rows[0][0] if engine_result.result_rows else None
        
        # Obtener PK
        pk_result = ch.query(f"""
        SELECT name
        FROM system.columns
        WHERE database = '{db_name}' AND table = '{table_name}' AND is_in_primary_key = 1
        ORDER BY position
        """)
        pk_cols = [row[0] for row in pk_result.result_rows] if pk_result.result_rows else []
        
        return {
            'create_sql': create_sql,
            'engine': engine,
            'pk_cols': pk_cols
        }
    except Exception as e:
        return None

def migrate_table(ch, db_name, table_name, dry_run=False):
    """Migra una tabla de MergeTree a ReplacingMergeTree"""
    full_table = f"`{db_name}`.`{table_name}`"
    
    info = get_table_info(ch, db_name, table_name)
    if not info:
        print(f"  [ERROR] No se pudo obtener información de la tabla")
        return False
    
    engine = info['engine']
    pk_cols = info['pk_cols']
    create_sql = info['create_sql']
    
    # Verificar si ya es ReplacingMergeTree
    if "ReplacingMergeTree" in engine:
        print(f"  [SKIP] Ya es ReplacingMergeTree")
        return True
    
    # Verificar si es MergeTree
    if "MergeTree" not in engine:
        print(f"  [SKIP] Engine '{engine}' no es MergeTree, no se puede migrar")
        return False
    
    # Verificar si tiene PK
    if not pk_cols:
        print(f"  [SKIP] Tabla sin PK - no se puede migrar a ReplacingMergeTree")
        return False
    
    # Extraer ORDER BY del CREATE TABLE
    order_by = "tuple()"
    if "ORDER BY" in create_sql:
        try:
            order_by_part = create_sql.split("ORDER BY")[1]
            # Tomar hasta el siguiente ENGINE o PARTITION o SETTINGS o fin de línea
            for delimiter in ["\n", "ENGINE", "PARTITION", "SETTINGS"]:
                if delimiter in order_by_part:
                    order_by = order_by_part.split(delimiter)[0].strip()
                    break
            else:
                order_by = order_by_part.strip()
        except:
            pass
    
    if dry_run:
        print(f"  [DRY-RUN] Migraría de {engine} a ReplacingMergeTree")
        print(f"  [DRY-RUN] ORDER BY: {order_by}")
        print(f"  [DRY-RUN] PK: {', '.join(pk_cols)}")
        return True
    
    try:
        # Crear tabla temporal con ReplacingMergeTree
        temp_table = f"`{db_name}`.`{table_name}_replacing_temp`"
        backup_table = f"`{db_name}`.`{table_name}_backup`"
        
        print(f"  [1/4] Creando backup...")
        ch.command(f"RENAME TABLE {full_table} TO {backup_table}")
        
        print(f"  [2/4] Creando tabla temporal con ReplacingMergeTree...")
        # Modificar CREATE TABLE
        new_create_sql = create_sql.replace(f"`{table_name}`", f"`{table_name}_replacing_temp`")
        # Reemplazar ENGINE
        if "ENGINE = MergeTree" in new_create_sql:
            new_create_sql = new_create_sql.replace("ENGINE = MergeTree", "ENGINE = ReplacingMergeTree")
        elif "ENGINE=MergeTree" in new_create_sql:
            new_create_sql = new_create_sql.replace("ENGINE=MergeTree", "ENGINE=ReplacingMergeTree")
        
        ch.command(new_create_sql)
        
        print(f"  [3/4] Copiando datos...")
        ch.command(f"INSERT INTO {temp_table} SELECT * FROM {backup_table}")
        
        print(f"  [4/4] Reemplazando tabla original...")
        ch.command(f"RENAME TABLE {temp_table} TO {full_table}")
        
        print(f"  [INFO] Eliminando backup...")
        ch.command(f"DROP TABLE IF EXISTS {backup_table}")
        
        return True
        
    except Exception as e:
        print(f"  [ERROR] Error en migración: {e}")
        import traceback
        traceback.print_exc()
        
        # Intentar restaurar desde backup
        try:
            print(f"  [RECOVERY] Intentando restaurar desde backup...")
            ch.command(f"DROP TABLE IF EXISTS {full_table}")
            ch.command(f"DROP TABLE IF EXISTS {temp_table}")
            ch.command(f"RENAME TABLE {backup_table} TO {full_table}")
            print(f"  [OK] Tabla restaurada desde backup")
        except:
            print(f"  [ERROR] No se pudo restaurar desde backup")
        
        return False

def parse_args():
    """Parsea argumentos de línea de comandos"""
    if len(sys.argv) < 2:
        print("Uso: python migrate_to_replacingmergetree.py DATABASE [tablas] [--dry-run] [--force]")
        sys.exit(1)
    
    db_name = sys.argv[1].strip()
    requested_tables = None
    dry_run = "--dry-run" in sys.argv
    
    if len(sys.argv) >= 3:
        for arg in sys.argv[2:]:
            if arg != "--dry-run":
                if arg == "*" or arg.lower() == "all":
                    requested_tables = None
                else:
                    requested_tables = [t.strip() for t in arg.split(",") if t.strip()]
    
    return db_name, requested_tables, dry_run

def get_tables_to_migrate(ch, db_name, requested_tables=None):
    """Obtiene lista de tablas que necesitan migración"""
    if requested_tables:
        return requested_tables
    
    # Obtener tablas MergeTree con PK
    query = f"""
    SELECT t.name
    FROM system.tables t
    WHERE t.database = '{db_name}'
      AND t.engine = 'MergeTree'
      AND EXISTS (
          SELECT 1
          FROM system.columns c
          WHERE c.database = t.database
            AND c.table = t.name
            AND c.is_in_primary_key = 1
      )
    ORDER BY t.name
    """
    result = ch.query(query)
    return [row[0] for row in result.result_rows]

def main():
    db_name, requested_tables, dry_run = parse_args()
    
    print("=" * 80)
    print("MIGRACIÓN A REPLACINGMERGETREE")
    print("=" * 80)
    print(f"Base de datos: {db_name}")
    if dry_run:
        print("[MODO DRY-RUN - No se realizarán cambios]")
    print()
    print("[ADVERTENCIA] Este script recreará las tablas. Asegúrate de tener backup.")
    print()
    
    try:
        ch = ch_client()
        ch.query("SELECT 1")
        print("[OK] Conexión a ClickHouse establecida")
        print()
        
        # Verificar que la base de datos existe
        databases = ch.query(f"SHOW DATABASES LIKE '{db_name}'")
        if not databases.result_rows:
            print(f"[ERROR] La base de datos '{db_name}' no existe")
            sys.exit(1)
        
        tables = get_tables_to_migrate(ch, db_name, requested_tables)
        
        if not tables:
            print("No se encontraron tablas MergeTree con PK para migrar")
            return
        
        print(f"[INFO] Encontradas {len(tables)} tabla(s) para migrar")
        print()
        
        processed = 0
        success = 0
        skipped = 0
        errors = 0
        
        for table_name in tables:
            print(f"Procesando: {table_name}")
            
            result = migrate_table(ch, db_name, table_name, dry_run)
            if result:
                success += 1
            else:
                skipped += 1
            
            processed += 1
            print()
        
        # Resumen
        print("=" * 80)
        print("RESUMEN")
        print("=" * 80)
        print(f"Tablas procesadas: {processed}")
        print(f"Migradas exitosamente: {success}")
        print(f"Omitidas: {skipped}")
        print(f"Errores: {errors}")
        print("=" * 80)
        
        if success > 0:
            print()
            print("[INFO] Después de la migración, ejecuta OPTIMIZE TABLE ... FINAL para limpiar duplicados:")
            for table_name in tables[:5]:  # Mostrar solo las primeras 5
                print(f"  OPTIMIZE TABLE `{db_name}`.`{table_name}` FINAL;")
            if len(tables) > 5:
                print(f"  ... y {len(tables) - 5} tabla(s) más")
        
    except Exception as e:
        print(f"[ERROR] Error: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)

if __name__ == "__main__":
    main()
