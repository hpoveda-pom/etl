#!/usr/bin/env python3
import os
import sys
import time
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
CH_DATABASE = os.getenv("CH_DATABASE", "default")

# =========================
# HELPERS
# =========================
def usage():
    print("Uso:")
    print("  python clone_clickhouse_database.py ORIG_DB DEST_DB [--data] [--drop-existing]")
    print("")
    print("Parámetros:")
    print("  ORIG_DB          Base de datos origen")
    print("  DEST_DB          Base de datos destino")
    print("  --data           Clonar también los datos (por defecto solo estructura)")
    print("  --drop-existing  Eliminar tablas existentes en destino antes de clonar")
    print("")
    print("Ejemplos:")
    print("  # Clonar solo estructura")
    print("  python clone_clickhouse_database.py POM_Aplicaciones POM_Aplicaciones_backup")
    print("")
    print("  # Clonar estructura + datos")
    print("  python clone_clickhouse_database.py POM_Aplicaciones POM_Aplicaciones_backup --data")
    print("")
    print("  # Clonar eliminando tablas existentes")
    print("  python clone_clickhouse_database.py POM_Aplicaciones POM_Aplicaciones_backup --data --drop-existing")
    sys.exit(1)

def parse_args():
    if len(sys.argv) < 3:
        usage()
    
    orig_db = sys.argv[1].strip()
    dest_db = sys.argv[2].strip()
    
    if not orig_db or not dest_db:
        usage()
    
    clone_data = "--data" in sys.argv
    drop_existing = "--drop-existing" in sys.argv
    
    return orig_db, dest_db, clone_data, drop_existing

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

def ensure_database(ch, db_name: str):
    """Crea la base de datos si no existe"""
    ch.command(f"CREATE DATABASE IF NOT EXISTS `{db_name}`")

def get_tables(ch, db_name: str):
    """Obtiene lista de tablas en la base de datos"""
    query = f"""
    SELECT name
    FROM system.tables
    WHERE database = '{db_name}'
      AND engine NOT IN ('View', 'MaterializedView')
    ORDER BY name
    """
    result = ch.query(query)
    return [row[0] for row in result.result_rows]

def get_views(ch, db_name: str):
    """Obtiene lista de vistas en la base de datos"""
    query = f"""
    SELECT name
    FROM system.tables
    WHERE database = '{db_name}'
      AND engine IN ('View', 'MaterializedView')
    ORDER BY name
    """
    result = ch.query(query)
    return [row[0] for row in result.result_rows]

def get_table_ddl(ch, db_name: str, table_name: str):
    """Obtiene el DDL (CREATE TABLE) de una tabla"""
    try:
        full_table = f"`{db_name}`.`{table_name}`"
        query = f"SHOW CREATE TABLE {full_table}"
        result = ch.query(query)
        if result.result_rows:
            return result.result_rows[0][0]  # El DDL está en la primera columna
        return None
    except Exception as e:
        print(f"  [ERROR] Error obteniendo DDL de {db_name}.{table_name}: {e}")
        return None

def get_view_ddl(ch, db_name: str, view_name: str):
    """Obtiene el DDL (CREATE VIEW) de una vista"""
    try:
        full_view = f"`{db_name}`.`{view_name}`"
        query = f"SHOW CREATE TABLE {full_view}"
        result = ch.query(query)
        if result.result_rows:
            return result.result_rows[0][0]
        return None
    except Exception as e:
        print(f"  [ERROR] Error obteniendo DDL de vista {db_name}.{view_name}: {e}")
        return None

def modify_ddl_for_destination(ddl: str, orig_db: str, dest_db: str, table_name: str):
    """Modifica el DDL para cambiar el nombre de la base de datos destino"""
    ddl_modified = ddl
    
    # Reemplazar referencias a la base de datos origen en el nombre de la tabla
    # Formato: CREATE TABLE `db`.`table` ...
    ddl_modified = ddl_modified.replace(f"`{orig_db}`.`{table_name}`", f"`{dest_db}`.`{table_name}`")
    ddl_modified = ddl_modified.replace(f"{orig_db}.{table_name}", f"{dest_db}.{table_name}")
    
    # Reemplazar referencias a la base de datos origen en otras partes del DDL (por ejemplo, en ENGINE)
    ddl_modified = ddl_modified.replace(f"`{orig_db}`.", f"`{dest_db}`.")
    ddl_modified = ddl_modified.replace(f"{orig_db}.", f"{dest_db}.")
    
    # Si el DDL no tiene la base de datos destino en el CREATE, agregarla
    # El DDL de ClickHouse puede venir como: CREATE TABLE `table_name` ... o CREATE TABLE `db`.`table_name` ...
    if f"`{dest_db}`.`{table_name}`" not in ddl_modified and f"CREATE TABLE `{table_name}`" in ddl_modified:
        # Reemplazar CREATE TABLE `table_name` por CREATE TABLE `dest_db`.`table_name`
        ddl_modified = ddl_modified.replace(f"CREATE TABLE `{table_name}`", f"CREATE TABLE `{dest_db}`.`{table_name}`", 1)
    elif f"`{dest_db}`.`{table_name}`" not in ddl_modified and f"CREATE TABLE {table_name}" in ddl_modified:
        # Reemplazar CREATE TABLE table_name por CREATE TABLE `dest_db`.`table_name`
        ddl_modified = ddl_modified.replace(f"CREATE TABLE {table_name}", f"CREATE TABLE `{dest_db}`.`{table_name}`", 1)
    
    return ddl_modified

def clone_table(ch, orig_db: str, dest_db: str, table_name: str, clone_data: bool, drop_existing: bool):
    """Clona una tabla de origen a destino"""
    full_orig_table = f"`{orig_db}`.`{table_name}`"
    full_dest_table = f"`{dest_db}`.`{table_name}`"
    
    try:
        # Verificar si la tabla destino ya existe
        exists_result = ch.query(f"EXISTS TABLE {full_dest_table}")
        table_exists = exists_result.result_rows[0][0] == 1 if exists_result.result_rows else False
        
        if table_exists:
            if drop_existing:
                print(f"  [INFO] Eliminando tabla existente: {dest_db}.{table_name}")
                ch.command(f"DROP TABLE IF EXISTS {full_dest_table}")
            else:
                print(f"  [SKIP] Tabla {dest_db}.{table_name} ya existe (usar --drop-existing para reemplazar)")
                return False
        
        # Obtener DDL de la tabla origen
        ddl = get_table_ddl(ch, orig_db, table_name)
        if not ddl:
            print(f"  [ERROR] No se pudo obtener DDL de {orig_db}.{table_name}")
            return False
        
        # Modificar DDL para destino
        ddl_modified = modify_ddl_for_destination(ddl, orig_db, dest_db, table_name)
        
        # Crear tabla en destino
        print(f"  [INFO] Creando tabla: {dest_db}.{table_name}")
        ch.command(ddl_modified)
        
        # Clonar datos si se solicita
        if clone_data:
            # Obtener conteo de filas origen
            count_result = ch.query(f"SELECT COUNT(*) FROM {full_orig_table}")
            row_count = count_result.result_rows[0][0] if count_result.result_rows else 0
            
            if row_count > 0:
                print(f"  [INFO] Copiando {row_count:,} filas...")
                ch.command(f"INSERT INTO {full_dest_table} SELECT * FROM {full_orig_table}")
                print(f"  [OK] Datos copiados: {row_count:,} filas")
            else:
                print(f"  [INFO] Tabla origen vacía, sin datos para copiar")
        
        return True
        
    except Exception as e:
        print(f"  [ERROR] Error clonando tabla {orig_db}.{table_name}: {e}")
        return False

def clone_view(ch, orig_db: str, dest_db: str, view_name: str, drop_existing: bool):
    """Clona una vista de origen a destino"""
    full_orig_view = f"`{orig_db}`.`{view_name}`"
    full_dest_view = f"`{dest_db}`.`{view_name}`"
    
    try:
        # Verificar si la vista destino ya existe
        exists_result = ch.query(f"EXISTS TABLE {full_dest_view}")
        view_exists = exists_result.result_rows[0][0] == 1 if exists_result.result_rows else False
        
        if view_exists:
            if drop_existing:
                print(f"  [INFO] Eliminando vista existente: {dest_db}.{view_name}")
                ch.command(f"DROP TABLE IF EXISTS {full_dest_view}")
            else:
                print(f"  [SKIP] Vista {dest_db}.{view_name} ya existe (usar --drop-existing para reemplazar)")
                return False
        
        # Obtener DDL de la vista origen
        ddl = get_view_ddl(ch, orig_db, view_name)
        if not ddl:
            print(f"  [ERROR] No se pudo obtener DDL de vista {orig_db}.{view_name}")
            return False
        
        # Modificar DDL para destino
        ddl_modified = modify_ddl_for_destination(ddl, orig_db, dest_db, view_name)
        
        # Crear vista en destino
        print(f"  [INFO] Creando vista: {dest_db}.{view_name}")
        ch.command(ddl_modified)
        
        return True
        
    except Exception as e:
        print(f"  [ERROR] Error clonando vista {orig_db}.{view_name}: {e}")
        return False

def main():
    start_time = time.time()
    orig_db, dest_db, clone_data, drop_existing = parse_args()
    
    print("=" * 80)
    print("CLONAR BASE DE DATOS - CLICKHOUSE")
    print("=" * 80)
    print(f"Origen: {orig_db}")
    print(f"Destino: {dest_db}")
    print(f"Clonar datos: {'Sí' if clone_data else 'No (solo estructura)'}")
    print(f"Eliminar existentes: {'Sí' if drop_existing else 'No'}")
    print()
    
    try:
        ch = ch_client()
        
        # Verificar conexión
        ch.query("SELECT 1")
        print("[OK] Conexión a ClickHouse establecida")
        print()
        
        # Verificar que la base de datos origen existe
        exists_result = ch.query(f"EXISTS DATABASE `{orig_db}`")
        if not exists_result.result_rows or exists_result.result_rows[0][0] == 0:
            print(f"[ERROR] La base de datos origen '{orig_db}' no existe")
            sys.exit(1)
        
        print(f"[OK] Base de datos origen '{orig_db}' encontrada")
        
        # Crear base de datos destino si no existe
        ensure_database(ch, dest_db)
        print(f"[OK] Base de datos destino '{dest_db}' lista")
        print()
        
        # Obtener tablas y vistas
        tables = get_tables(ch, orig_db)
        views = get_views(ch, orig_db)
        
        print(f"[INFO] Tablas encontradas: {len(tables)}")
        print(f"[INFO] Vistas encontradas: {len(views)}")
        print()
        
        if not tables and not views:
            print("[WARN] No se encontraron tablas ni vistas para clonar")
            return
        
        # Clonar tablas
        tables_ok = 0
        tables_error = 0
        tables_skipped = 0
        
        if tables:
            print("=" * 80)
            print("CLONANDO TABLAS")
            print("=" * 80)
            
            for table_name in tables:
                if clone_table(ch, orig_db, dest_db, table_name, clone_data, drop_existing):
                    tables_ok += 1
                else:
                    tables_error += 1
                print()
        
        # Clonar vistas
        views_ok = 0
        views_error = 0
        
        if views:
            print("=" * 80)
            print("CLONANDO VISTAS")
            print("=" * 80)
            
            for view_name in views:
                if clone_view(ch, orig_db, dest_db, view_name, drop_existing):
                    views_ok += 1
                else:
                    views_error += 1
                print()
        
        elapsed = time.time() - start_time
        
        # Resumen
        print("=" * 80)
        print("RESUMEN")
        print("=" * 80)
        print(f"Tablas clonadas: {tables_ok}")
        print(f"Tablas con error: {tables_error}")
        print(f"Vistas clonadas: {views_ok}")
        print(f"Vistas con error: {views_error}")
        print(f"Tiempo de ejecución: {elapsed:.2f} segundos")
        print("=" * 80)
        
    except Exception as e:
        print(f"[ERROR] Error: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)

if __name__ == "__main__":
    main()
