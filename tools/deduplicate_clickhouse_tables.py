#!/usr/bin/env python3
"""
Script para deduplicar tablas en ClickHouse
============================================

Elimina duplicados de tablas en ClickHouse usando diferentes estrategias:
1. Para tablas con ReplacingMergeTree: ejecuta OPTIMIZE TABLE ... FINAL
2. Para tablas con MergeTree: crea tabla temporal, inserta DISTINCT, reemplaza

Uso:
    python deduplicate_clickhouse_tables.py DATABASE [tablas] [--dry-run] [--force]
    
Ejemplos:
    # Ver qué se haría (dry-run)
    python deduplicate_clickhouse_tables.py POM_Aplicaciones --dry-run
    
    # Deduplicar todas las tablas
    python deduplicate_clickhouse_tables.py POM_Aplicaciones
    
    # Deduplicar tablas específicas
    python deduplicate_clickhouse_tables.py POM_Aplicaciones "Bitacora_Acceso,EstadoLegal_Caso"
    
    # Forzar deduplicación incluso en ReplacingMergeTree
    python deduplicate_clickhouse_tables.py POM_Aplicaciones --force
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

def get_table_engine(ch, db_name, table_name):
    """Obtiene el engine de una tabla"""
    try:
        query = f"""
        SELECT engine
        FROM system.tables
        WHERE database = '{db_name}' AND name = '{table_name}'
        """
        result = ch.query(query)
        if result.result_rows:
            return result.result_rows[0][0]
        return None
    except:
        return None

def get_table_columns(ch, db_name, table_name):
    """Obtiene las columnas de una tabla"""
    try:
        query = f"""
        SELECT name, type
        FROM system.columns
        WHERE database = '{db_name}' AND table = '{table_name}'
        ORDER BY position
        """
        result = ch.query(query)
        return [(row[0], row[1]) for row in result.result_rows]
    except:
        return []

def get_primary_key(ch, db_name, table_name):
    """Obtiene la clave primaria de una tabla"""
    try:
        query = f"""
        SELECT name
        FROM system.columns
        WHERE database = '{db_name}' AND table = '{table_name}' AND is_in_primary_key = 1
        ORDER BY position
        """
        result = ch.query(query)
        pk_cols = [row[0] for row in result.result_rows]
        return pk_cols if pk_cols else None
    except:
        return None

def get_table_row_count(ch, db_name, table_name, use_final=False):
    """Obtiene el conteo de filas de una tabla"""
    try:
        full_table = f"`{db_name}`.`{table_name}`"
        if use_final:
            query = f"SELECT count() FROM {full_table} FINAL"
        else:
            query = f"SELECT count() FROM {full_table}"
        result = ch.query(query)
        return result.result_rows[0][0] if result.result_rows else 0
    except:
        return None

def get_distinct_row_count(ch, db_name, table_name, pk_cols):
    """Obtiene el conteo de filas únicas usando la PK"""
    try:
        full_table = f"`{db_name}`.`{table_name}`"
        if pk_cols:
            pk_str = ", ".join([f"`{col}`" for col in pk_cols])
            query = f"SELECT count() FROM (SELECT DISTINCT {pk_str} FROM {full_table})"
        else:
            # Sin PK, contar todas las columnas
            columns = get_table_columns(ch, db_name, table_name)
            if columns:
                all_cols = ", ".join([f"`{col[0]}`" for col in columns])
                query = f"SELECT count() FROM (SELECT DISTINCT {all_cols} FROM {full_table})"
            else:
                return None
        result = ch.query(query)
        return result.result_rows[0][0] if result.result_rows else 0
    except:
        return None

def optimize_replacing_mergetree(ch, db_name, table_name, dry_run=False):
    """Optimiza una tabla ReplacingMergeTree"""
    full_table = f"`{db_name}`.`{table_name}`"
    query = f"OPTIMIZE TABLE {full_table} FINAL"
    
    if dry_run:
        print(f"  [DRY-RUN] Ejecutaría: {query}")
        return True
    
    try:
        ch.command(query)
        return True
    except Exception as e:
        print(f"  [ERROR] Error ejecutando OPTIMIZE: {e}")
        return False

def deduplicate_mergetree(ch, db_name, table_name, pk_cols, dry_run=False):
    """Deduplica una tabla MergeTree creando una nueva tabla y reemplazando"""
    full_table = f"`{db_name}`.`{table_name}`"
    temp_table = f"`{db_name}`.`{table_name}_dedup_temp`"
    
    # Obtener estructura de la tabla
    columns = get_table_columns(ch, db_name, table_name)
    if not columns:
        print(f"  [ERROR] No se pudieron obtener las columnas")
        return False
    
    # Crear definición de columnas
    cols_def = ",\n    ".join([f"`{col[0]}` {col[1]}" for col in columns])
    
    # Obtener ORDER BY de la tabla original
    try:
        create_query = ch.query(f"SHOW CREATE TABLE {full_table}")
        create_sql = create_query.result_rows[0][0] if create_query.result_rows else ""
        # Extraer ORDER BY del CREATE TABLE
        if "ORDER BY" in create_sql:
            order_by_part = create_sql.split("ORDER BY")[1].split("\n")[0].strip()
        else:
            order_by_part = "tuple()"
    except:
        order_by_part = "tuple()"
    
    if dry_run:
        print(f"  [DRY-RUN] Crearía tabla temporal: {temp_table}")
        print(f"  [DRY-RUN] Insertaría datos DISTINCT")
        print(f"  [DRY-RUN] Reemplazaría tabla original")
        return True
    
    try:
        # Crear tabla temporal
        create_temp = f"""
        CREATE TABLE {temp_table}
        (
            {cols_def}
        )
        ENGINE = MergeTree
        ORDER BY {order_by_part}
        """
        ch.command(create_temp)
        
        # Insertar datos únicos usando DISTINCT con todas las columnas
        # Esto elimina filas completamente duplicadas
        all_cols = ", ".join([f"`{col[0]}`" for col in columns])
        insert_query = f"""
        INSERT INTO {temp_table}
        SELECT DISTINCT {all_cols}
        FROM {full_table}
        """
        
        ch.command(insert_query)
        
        # Reemplazar tabla original
        ch.command(f"RENAME TABLE {full_table} TO `{db_name}`.`{table_name}_old`, {temp_table} TO {full_table}")
        
        # Eliminar tabla antigua
        ch.command(f"DROP TABLE IF EXISTS `{db_name}`.`{table_name}_old`")
        
        return True
    except Exception as e:
        print(f"  [ERROR] Error en deduplicación: {e}")
        # Limpiar tabla temporal si existe
        try:
            ch.command(f"DROP TABLE IF EXISTS {temp_table}")
        except:
            pass
        return False

def parse_args():
    """Parsea argumentos de línea de comandos"""
    if len(sys.argv) < 2:
        print("Uso: python deduplicate_clickhouse_tables.py DATABASE [tablas] [--dry-run] [--force]")
        sys.exit(1)
    
    db_name = sys.argv[1].strip()
    requested_tables = None
    dry_run = "--dry-run" in sys.argv
    force = "--force" in sys.argv
    
    if len(sys.argv) >= 3:
        for arg in sys.argv[2:]:
            if arg not in ("--dry-run", "--force"):
                if arg == "*" or arg.lower() == "all":
                    requested_tables = None
                else:
                    requested_tables = [t.strip() for t in arg.split(",") if t.strip()]
    
    return db_name, requested_tables, dry_run, force

def get_tables(ch, db_name, requested_tables=None):
    """Obtiene lista de tablas"""
    if requested_tables:
        return requested_tables
    
    query = f"""
    SELECT name
    FROM system.tables
    WHERE database = '{db_name}'
      AND engine NOT IN ('View', 'MaterializedView')
    ORDER BY name
    """
    result = ch.query(query)
    return [row[0] for row in result.result_rows]

def main():
    db_name, requested_tables, dry_run, force = parse_args()
    
    print("=" * 80)
    print("DEDUPLICACIÓN DE TABLAS - CLICKHOUSE")
    print("=" * 80)
    print(f"Base de datos: {db_name}")
    if dry_run:
        print("[MODO DRY-RUN - No se realizarán cambios]")
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
        
        tables = get_tables(ch, db_name, requested_tables)
        
        if not tables:
            print("No se encontraron tablas para procesar")
            return
        
        print(f"[INFO] Procesando {len(tables)} tabla(s)...")
        print()
        
        processed = 0
        optimized = 0
        deduplicated = 0
        skipped = 0
        errors = 0
        
        for table_name in tables:
            print(f"Procesando: {table_name}")
            
            # Obtener engine
            engine = get_table_engine(ch, db_name, table_name)
            if not engine:
                print(f"  [SKIP] No se pudo obtener el engine")
                skipped += 1
                print()
                continue
            
            # Obtener conteos
            total_rows = get_table_row_count(ch, db_name, table_name, use_final=False)
            distinct_rows = None
            
            pk_cols = get_primary_key(ch, db_name, table_name)
            if pk_cols:
                distinct_rows = get_distinct_row_count(ch, db_name, table_name, pk_cols)
            
            if total_rows is None:
                print(f"  [SKIP] No se pudo obtener el conteo de filas")
                skipped += 1
                print()
                continue
            
            print(f"  Engine: {engine}")
            print(f"  Filas totales: {total_rows:,}".replace(",", "."))
            if distinct_rows is not None:
                print(f"  Filas únicas (PK): {distinct_rows:,}".replace(",", "."))
                duplicates = total_rows - distinct_rows
                if duplicates > 0:
                    print(f"  Duplicados: {duplicates:,}".replace(",", "."))
                else:
                    print(f"  Sin duplicados detectados")
            
            # Procesar según el engine
            if "ReplacingMergeTree" in engine:
                if force or (distinct_rows is not None and total_rows > distinct_rows):
                    if optimize_replacing_mergetree(ch, db_name, table_name, dry_run):
                        optimized += 1
                        print(f"  [OK] Tabla optimizada")
                    else:
                        errors += 1
                else:
                    print(f"  [SKIP] ReplacingMergeTree sin duplicados aparentes (usa --force para optimizar de todas formas)")
                    skipped += 1
            
            elif "MergeTree" in engine:
                if distinct_rows is not None and total_rows > distinct_rows:
                    if deduplicate_mergetree(ch, db_name, table_name, pk_cols, dry_run):
                        deduplicated += 1
                        print(f"  [OK] Tabla deduplicada")
                    else:
                        errors += 1
                else:
                    print(f"  [SKIP] Sin duplicados detectados")
                    skipped += 1
            else:
                print(f"  [SKIP] Engine '{engine}' no soportado para deduplicación automática")
                skipped += 1
            
            processed += 1
            print()
        
        # Resumen
        print("=" * 80)
        print("RESUMEN")
        print("=" * 80)
        print(f"Tablas procesadas: {processed}")
        print(f"Optimizadas (ReplacingMergeTree): {optimized}")
        print(f"Deduplicadas (MergeTree): {deduplicated}")
        print(f"Omitidas: {skipped}")
        print(f"Errores: {errors}")
        print("=" * 80)
        
    except Exception as e:
        print(f"[ERROR] Error: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)

if __name__ == "__main__":
    main()
