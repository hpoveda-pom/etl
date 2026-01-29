#!/usr/bin/env python3
"""
Script para forzar deduplicación de tablas en ClickHouse
=========================================================

Estrategia agresiva: Recrea las tablas eliminando duplicados completamente.
Útil cuando hay muchos duplicados y OPTIMIZE no es suficiente.

Uso:
    python force_deduplicate_clickhouse.py DATABASE [tablas] [--dry-run]
    
Ejemplos:
    # Ver qué se haría
    python force_deduplicate_clickhouse.py POM_Aplicaciones --dry-run
    
    # Deduplicar todas las tablas con problemas
    python force_deduplicate_clickhouse.py POM_Aplicaciones
    
    # Deduplicar tablas específicas
    python force_deduplicate_clickhouse.py POM_Aplicaciones "Bitacora_Acceso,EstadoLegal_Caso"
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
    """Obtiene información completa de una tabla"""
    try:
        # Obtener CREATE TABLE
        create_result = ch.query(f"SHOW CREATE TABLE `{db_name}`.`{table_name}`")
        create_sql = create_result.result_rows[0][0] if create_result.result_rows else None
        
        # Obtener columnas
        columns_result = ch.query(f"DESCRIBE TABLE `{db_name}`.`{table_name}`")
        columns = [(row[0], row[1]) for row in columns_result.result_rows]
        
        # Obtener engine
        engine_result = ch.query(f"""
        SELECT engine, engine_full
        FROM system.tables
        WHERE database = '{db_name}' AND name = '{table_name}'
        """)
        engine = engine_result.result_rows[0][0] if engine_result.result_rows else None
        engine_full = engine_result.result_rows[0][1] if engine_result.result_rows and len(engine_result.result_rows[0]) > 1 else None
        
        # Obtener conteos
        total_result = ch.query(f"SELECT count() FROM `{db_name}`.`{table_name}`")
        total_rows = total_result.result_rows[0][0] if total_result.result_rows else 0
        
        # Intentar obtener conteo con FINAL (para ReplacingMergeTree)
        try:
            final_result = ch.query(f"SELECT count() FROM `{db_name}`.`{table_name}` FINAL")
            final_rows = final_result.result_rows[0][0] if final_result.result_rows else total_rows
        except:
            final_rows = total_rows
        
        return {
            'create_sql': create_sql,
            'columns': columns,
            'engine': engine,
            'engine_full': engine_full,
            'total_rows': total_rows,
            'final_rows': final_rows
        }
    except Exception as e:
        print(f"  [ERROR] Error obteniendo información: {e}")
        return None

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

def force_deduplicate_table(ch, db_name, table_name, dry_run=False):
    """Fuerza deduplicación recreando la tabla"""
    full_table = f"`{db_name}`.`{table_name}`"
    temp_table = f"`{db_name}`.`{table_name}_dedup_temp`"
    backup_table = f"`{db_name}`.`{table_name}_backup`"
    
    # Obtener información de la tabla
    info = get_table_info(ch, db_name, table_name)
    if not info:
        return False
    
    create_sql = info['create_sql']
    columns = info['columns']
    total_rows = info['total_rows']
    final_rows = info['final_rows']
    
    if not create_sql:
        print(f"  [ERROR] No se pudo obtener CREATE TABLE")
        return False
    
    # Obtener PK
    pk_cols = get_primary_key(ch, db_name, table_name)
    
    print(f"  Filas totales: {total_rows:,}".replace(",", "."))
    print(f"  Filas después de FINAL: {final_rows:,}".replace(",", "."))
    duplicates = total_rows - final_rows
    if duplicates > 0:
        print(f"  Duplicados detectados: {duplicates:,}".replace(",", "."))
    
    if dry_run:
        print(f"  [DRY-RUN] Crearía tabla temporal: {temp_table}")
        if pk_cols:
            print(f"  [DRY-RUN] Usaría PK para deduplicación: {', '.join(pk_cols)}")
        print(f"  [DRY-RUN] Insertaría datos únicos")
        print(f"  [DRY-RUN] Reemplazaría tabla original")
        return True
    
    try:
        # Paso 1: Crear backup de la tabla original
        print(f"  [1/4] Creando backup...")
        ch.command(f"RENAME TABLE {full_table} TO {backup_table}")
        
        # Paso 2: Crear tabla temporal con la misma estructura
        print(f"  [2/4] Creando tabla temporal...")
        # Modificar CREATE TABLE para usar la tabla temporal
        temp_create_sql = create_sql.replace(f"`{table_name}`", f"`{table_name}_dedup_temp`")
        ch.command(temp_create_sql)
        
        # Paso 3: Insertar datos únicos
        print(f"  [3/4] Insertando datos únicos...")
        all_cols = ", ".join([f"`{col[0]}`" for col in columns])
        
        if pk_cols:
            # Si hay PK, usar GROUP BY con argMax para obtener la última versión
            # Pero primero intentar con DISTINCT simple
            insert_query = f"""
            INSERT INTO {temp_table}
            SELECT DISTINCT {all_cols}
            FROM {backup_table}
            """
        else:
            # Sin PK, usar DISTINCT con todas las columnas
            insert_query = f"""
            INSERT INTO {temp_table}
            SELECT DISTINCT {all_cols}
            FROM {backup_table}
            """
        
        ch.command(insert_query)
        
        # Verificar conteo en tabla temporal
        temp_count_result = ch.query(f"SELECT count() FROM {temp_table}")
        temp_count = temp_count_result.result_rows[0][0] if temp_count_result.result_rows else 0
        print(f"  Filas en tabla temporal: {temp_count:,}".replace(",", "."))
        
        # Paso 4: Reemplazar tabla original
        print(f"  [4/4] Reemplazando tabla original...")
        ch.command(f"RENAME TABLE {temp_table} TO {full_table}")
        
        # Mantener backup por seguridad (comentado para no ocupar espacio)
        # print(f"  [INFO] Backup guardado en: {backup_table}")
        # O eliminar backup
        print(f"  [INFO] Eliminando backup...")
        ch.command(f"DROP TABLE IF EXISTS {backup_table}")
        
        return True
        
    except Exception as e:
        print(f"  [ERROR] Error en deduplicación: {e}")
        import traceback
        traceback.print_exc()
        
        # Intentar restaurar desde backup
        try:
            print(f"  [RECOVERY] Intentando restaurar desde backup...")
            ch.command(f"DROP TABLE IF EXISTS {full_table}")
            ch.command(f"RENAME TABLE {backup_table} TO {full_table}")
            print(f"  [OK] Tabla restaurada desde backup")
        except:
            print(f"  [ERROR] No se pudo restaurar desde backup")
        
        return False

def parse_args():
    """Parsea argumentos de línea de comandos"""
    if len(sys.argv) < 2:
        print("Uso: python force_deduplicate_clickhouse.py DATABASE [tablas] [--dry-run]")
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

def get_tables_with_duplicates(ch, db_name, requested_tables=None):
    """Obtiene lista de tablas que tienen duplicados"""
    if requested_tables:
        return requested_tables
    
    # Obtener todas las tablas y verificar cuáles tienen duplicados
    query = f"""
    SELECT name
    FROM system.tables
    WHERE database = '{db_name}'
      AND engine NOT IN ('View', 'MaterializedView')
    ORDER BY name
    """
    result = ch.query(query)
    all_tables = [row[0] for row in result.result_rows]
    
    tables_with_dups = []
    for table_name in all_tables:
        try:
            total_result = ch.query(f"SELECT count() FROM `{db_name}`.`{table_name}`")
            total_rows = total_result.result_rows[0][0] if total_result.result_rows else 0
            
            # Intentar con FINAL
            try:
                final_result = ch.query(f"SELECT count() FROM `{db_name}`.`{table_name}` FINAL")
                final_rows = final_result.result_rows[0][0] if final_result.result_rows else total_rows
            except:
                final_rows = total_rows
            
            if total_rows > final_rows:
                tables_with_dups.append(table_name)
        except:
            pass
    
    return tables_with_dups if tables_with_dups else all_tables

def main():
    db_name, requested_tables, dry_run = parse_args()
    
    print("=" * 80)
    print("DEDUPLICACIÓN FORZADA - CLICKHOUSE")
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
        
        if requested_tables:
            tables = requested_tables
        else:
            # Obtener tablas con duplicados
            tables = get_tables_with_duplicates(ch, db_name)
        
        if not tables:
            print("No se encontraron tablas para procesar")
            return
        
        print(f"[INFO] Procesando {len(tables)} tabla(s)...")
        print()
        
        processed = 0
        success = 0
        errors = 0
        
        for table_name in tables:
            print(f"Procesando: {table_name}")
            
            if force_deduplicate_table(ch, db_name, table_name, dry_run):
                success += 1
                print(f"  [OK] Tabla procesada correctamente")
            else:
                errors += 1
                print(f"  [ERROR] Error procesando tabla")
            
            processed += 1
            print()
        
        # Resumen
        print("=" * 80)
        print("RESUMEN")
        print("=" * 80)
        print(f"Tablas procesadas: {processed}")
        print(f"Exitosas: {success}")
        print(f"Errores: {errors}")
        print("=" * 80)
        
    except Exception as e:
        print(f"[ERROR] Error: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)

if __name__ == "__main__":
    main()
