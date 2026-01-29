import os
import re
import time
from datetime import datetime

import snowflake.connector

# ============== Snowflake config (env vars recomendado) ==============
SF_ACCOUNT   = os.getenv("SF_ACCOUNT", "fkwugeu-qic97823")
SF_USER      = os.getenv("SF_USER", "HPOVEDAPOMCR")
SF_PASSWORD  = os.getenv("SF_PASSWORD", "ik5niBj5FiXN4px")
SF_ROLE      = os.getenv("SF_ROLE", "ACCOUNTADMIN")
SF_WH        = os.getenv("SF_WAREHOUSE", "COMPUTE_WH")
SF_DB        = os.getenv("SF_DATABASE", "POM_TEST01")
SF_SCHEMA    = os.getenv("SF_SCHEMA", "RAW")

# ============== Configuración ==============
# Confirmación requerida por defecto (seguridad)
REQUIRE_CONFIRMATION = os.getenv("REQUIRE_CONFIRMATION", "true").lower() in ("true", "1", "yes")


def sanitize_token(s: str, maxlen: int = 120) -> str:
    """Sanitiza un string para usarlo como nombre de tabla/columna en Snowflake."""
    s = (s or "").strip()
    s = re.sub(r"[^\w\-\.]+", "_", s, flags=re.UNICODE)
    s = re.sub(r"_+", "_", s).strip("_")
    return s[:maxlen] if s else "NA"


def update_snowflake_config(database: str = None, schema: str = None, db_actual: str = None):
    """
    Actualiza la configuración de Snowflake (base de datos y schema).
    
    Args:
        database: Nombre de la base de datos configurada
        schema: Nombre del schema
        db_actual: Nombre real de la base de datos que funcionó (para PUT)
    """
    global SF_DB, SF_SCHEMA, SF_DB_ACTUAL
    
    if database:
        SF_DB = database
    if schema:
        SF_SCHEMA = schema
    if db_actual:
        SF_DB_ACTUAL = db_actual


def connect_sf(database: str = None, schema: str = None):
    """
    Crea una conexión a Snowflake.
    Si se especifican database o schema, actualiza la configuración.
    """
    if database or schema:
        update_snowflake_config(database, schema)
    else:
        update_snowflake_config()  # Usar valores por defecto
    
    if not SF_PASSWORD:
        raise RuntimeError("Falta SF_PASSWORD (definí la variable de entorno).")
    
    try:
        conn = snowflake.connector.connect(
            account=SF_ACCOUNT,
            user=SF_USER,
            password=SF_PASSWORD,
            role=SF_ROLE,
            warehouse=SF_WH,
        )
        
        cur = conn.cursor()
        try:
            db_name_used = None
            db_exists = False
            
            try:
                cur.execute(f'USE DATABASE "{SF_DB}";')
                db_name_used = SF_DB
                db_exists = True
                print(f"[OK] Base de datos '{SF_DB}' encontrada")
            except Exception as e1:
                try:
                    db_upper = SF_DB.upper()
                    cur.execute(f"USE DATABASE {db_upper};")
                    db_name_used = db_upper
                    db_exists = True
                    update_snowflake_config(db_upper, None)
                    print(f"[OK] Base de datos '{db_upper}' encontrada")
                except Exception as e2:
                    raise RuntimeError(
                        f"Error al usar la base de datos '{SF_DB}':\n"
                        f"  - Error al usar: {e2}"
                    )
            
            if not db_name_used or not db_exists:
                raise RuntimeError(f"No se pudo usar la base de datos '{SF_DB}'")
            
            try:
                cur.execute("SHOW SCHEMAS;")
            except Exception:
                cur.execute(f"SHOW SCHEMAS IN DATABASE {db_name_used};")
            
            schemas = [row[1] for row in cur.fetchall()]
            
            if SF_SCHEMA.upper() not in [s.upper() for s in schemas]:
                raise RuntimeError(f"El schema '{SF_SCHEMA}' no existe.")
            else:
                print(f"[OK] Schema '{SF_SCHEMA}' encontrado")
            
            cur.execute(f"USE SCHEMA {SF_SCHEMA};")
            
            update_snowflake_config(db_name_used, SF_SCHEMA, db_name_used)
            
            print(f"[OK] Conectado a Snowflake: {SF_DB}.{SF_SCHEMA}")
        except RuntimeError:
            raise
        except Exception as e:
            error_msg = str(e)
            raise RuntimeError(f"Error al conectar: {error_msg}")
        finally:
            cur.close()
        
        return conn
    except snowflake.connector.errors.ProgrammingError as e:
        error_str = str(e).lower()
        if "does not exist" in error_str or "not authorized" in error_str:
            raise RuntimeError(
                f"[ERROR] Error de conexión: La base de datos '{SF_DB}' no existe o no tienes permisos.\n"
                f"[INFO] Verifica el nombre exacto de la base de datos en Snowflake y tus permisos."
            )
        raise


def sf_exec(cur, sql: str):
    """Ejecuta SQL en Snowflake y maneja errores."""
    try:
        cur.execute(sql)
        return cur.fetchall() if cur.description else None
    except Exception as e:
        print(f"  [ERROR] Error SQL: {e}")
        print(f"  SQL: {sql[:200]}...")
        raise


def list_tables_in_schema(cur, pattern: str = None):
    """
    Lista las tablas en el schema actual.
    
    Args:
        cur: Cursor de Snowflake
        pattern: Patrón para filtrar tablas (opcional, ej: "PC_%")
    
    Returns:
        Lista de nombres de tablas
    """
    try:
        if pattern:
            sql = f"SHOW TABLES LIKE '{pattern}' IN SCHEMA {SF_SCHEMA};"
        else:
            sql = f"SHOW TABLES IN SCHEMA {SF_SCHEMA};"
        
        result = sf_exec(cur, sql)
        
        tables = []
        if result:
            for row in result:
                # El nombre de la tabla está en la columna 1 (índice 1)
                if len(row) > 1:
                    table_name = row[1]
                    tables.append(table_name)
        
        return tables
    except Exception as e:
        print(f"[WARN]  Error al listar tablas: {e}")
        return []


def format_table_name(table_name: str) -> str:
    """
    Formatea el nombre de la tabla para usar en SQL.
    Si el nombre tiene case mixto o contiene puntos, agrega comillas.
    
    Args:
        table_name: Nombre de la tabla (puede ser "DB.SCHEMA.TABLE" o solo "TABLE")
    
    Returns:
        Nombre formateado con comillas si es necesario
    """
    if not table_name:
        return None
    
    # Si ya tiene comillas, devolverlo tal cual
    if table_name.startswith('"') and table_name.endswith('"'):
        return table_name
    
    # Si contiene puntos, es un nombre completo (DB.SCHEMA.TABLE)
    if '.' in table_name:
        parts = table_name.split('.')
        # Si alguna parte tiene case mixto, usar comillas para todas
        if any(part != part.upper() for part in parts):
            return '.'.join([f'"{part}"' for part in parts])
        return table_name
    
    # Si es solo el nombre de la tabla, verificar si necesita comillas
    if table_name != table_name.upper():
        return f'"{table_name}"'
    
    return table_name


def drop_table(cur, table_name: str) -> bool:
    """
    Elimina una tabla en Snowflake.
    
    Args:
        cur: Cursor de Snowflake
        table_name: Nombre de la tabla a eliminar
    
    Returns:
        True si se eliminó exitosamente, False si hubo error
    """
    try:
        # Formatear nombre de tabla
        if '.' in table_name:
            # Nombre completo con DB.SCHEMA.TABLE
            full_table_name = table_name
        else:
            # Solo nombre de tabla, construir nombre completo
            if SF_DB != SF_DB.upper():
                full_table_name = f'"{SF_DB}"."{SF_SCHEMA}"."{table_name}"'
            else:
                full_table_name = f'{SF_DB}.{SF_SCHEMA}."{table_name}"'
        
        drop_sql = f"DROP TABLE IF EXISTS {full_table_name};"
        sf_exec(cur, drop_sql)
        return True
    except Exception as e:
        print(f"  [ERROR] Error eliminando tabla '{table_name}': {e}")
        return False


def drop_tables(cur, table_names: list = None, pattern: str = None, all_tables: bool = False) -> tuple:
    """
    Elimina tablas en Snowflake.
    
    Args:
        cur: Cursor de Snowflake
        table_names: Lista de nombres de tablas a eliminar (opcional)
        pattern: Patrón para filtrar tablas (opcional, ej: "PC_%")
        all_tables: Si es True, elimina todas las tablas del schema (peligroso)
    
    Returns:
        (dropped_count, error_count, total_tables)
    """
    dropped = 0
    errors = 0
    
    if all_tables:
        # Eliminar todas las tablas
        tables = list_tables_in_schema(cur)
        total_tables = len(tables)
    elif pattern:
        # Filtrar por patrón
        tables = list_tables_in_schema(cur, pattern)
        total_tables = len(tables)
    elif table_names:
        # Usar lista específica de tablas
        tables = table_names
        total_tables = len(tables)
    else:
        print("[WARN]  No se especificaron tablas para eliminar.")
        return 0, 0, 0
    
    if not tables:
        print("[WARN]  No se encontraron tablas que coincidan con los criterios.")
        return 0, 0, 0
    
    print(f"\n Tablas a eliminar: {len(tables)}")
    print("=" * 60)
    for i, table in enumerate(tables[:20], 1):  # Mostrar hasta 20
        print(f"  {i}. {table}")
    if len(tables) > 20:
        print(f"  ... y {len(tables) - 20} más")
    print("=" * 60)
    
    # Confirmación de seguridad
    if REQUIRE_CONFIRMATION:
        print(f"\n[WARN]  ADVERTENCIA: Se eliminarán {len(tables)} tabla(s) en {SF_DB}.{SF_SCHEMA}")
        confirmation = input("¿Estás seguro? Escribe 'SI' para confirmar: ")
        if confirmation.upper() != "SI":
            print("[ERROR] Operación cancelada.")
            return 0, 0, total_tables
    
    print(f"\n🗑️  Eliminando tablas...")
    
    for table_name in tables:
        try:
            print(f"  -> Eliminando: {table_name}")
            if drop_table(cur, table_name):
                dropped += 1
                print(f"    [OK] Tabla '{table_name}' eliminada")
            else:
                errors += 1
        except Exception as e:
            errors += 1
            print(f"    [ERROR] Error: {e}")
    
    return dropped, errors, total_tables


def main():
    """
    Función principal.
    Uso:
        python snowflake_drop_tables.py [database] [schema] [tables|pattern|--all] [--no-confirm]
    
    Args:
        database: Base de datos de Snowflake (opcional)
        schema: Schema de Snowflake (opcional, default: RAW)
        tables: Tablas a eliminar, separadas por comas (opcional)
        pattern: Patrón para filtrar tablas, usar comillas (opcional, ej: "PC_%")
        --all: Eliminar todas las tablas del schema (peligroso)
        --no-confirm: Omitir confirmación (peligroso)
    
    Ejemplos:
        python snowflake_drop_tables.py
        python snowflake_drop_tables.py POM_TEST01 RAW
        python snowflake_drop_tables.py POM_TEST01 RAW Tabla1,Tabla2,Tabla3
        python snowflake_drop_tables.py POM_TEST01 RAW "PC_%"
        python snowflake_drop_tables.py POM_TEST01 RAW --all
        python snowflake_drop_tables.py POM_TEST01 RAW Tabla1 --no-confirm
    """
    import sys
    
    start_time = time.time()
    
    # Parsear argumentos
    database = None
    schema = None
    table_names = None
    pattern = None
    all_tables = False
    no_confirm = False
    
    if len(sys.argv) > 1:
        database = sys.argv[1]
    if len(sys.argv) > 2:
        schema = sys.argv[2]
    if len(sys.argv) > 3:
        arg = sys.argv[3].strip()
        if arg == "--all":
            all_tables = True
        elif arg.startswith("--"):
            # Otros flags
            if arg == "--no-confirm":
                no_confirm = True
        elif "%" in arg or "_" in arg:
            # Es un patrón
            pattern = arg
        else:
            # Lista de tablas separadas por comas
            table_names = [t.strip() for t in arg.split(",") if t.strip()]
    
    if len(sys.argv) > 4:
        arg = sys.argv[4].strip()
        if arg == "--all":
            all_tables = True
        elif arg == "--no-confirm":
            no_confirm = True
    
    # Si no hay argumentos pero hay variables de entorno, usarlas
    if not database and os.getenv("SF_DATABASE"):
        database = os.getenv("SF_DATABASE")
    if not schema and os.getenv("SF_SCHEMA"):
        schema = os.getenv("SF_SCHEMA")
    
    # Desactivar confirmación si se especificó --no-confirm
    global REQUIRE_CONFIRMATION
    if no_confirm:
        REQUIRE_CONFIRMATION = False
    
    # Conectar a Snowflake
    conn = connect_sf(database, schema)
    
    print(f" Base de datos Snowflake: {SF_DB}")
    print(f" Schema: {SF_SCHEMA}")
    if table_names:
        print(f"🗑️  Tablas a eliminar: {', '.join(table_names)}")
    elif pattern:
        print(f" Patrón de búsqueda: {pattern}")
    elif all_tables:
        print(f"[WARN]  MODO PELIGROSO: Eliminar TODAS las tablas del schema")
    print()
    
    try:
        cur = conn.cursor()
        try:
            dropped, errors, total = drop_tables(cur, table_names, pattern, all_tables)
            conn.commit()
            
            elapsed_time = time.time() - start_time
            
            print()
            print("=" * 60)
            print(f"RESUMEN DE EJECUCIÓN")
            print("=" * 60)
            print(f"Tablas encontradas: {total}")
            print(f"Tablas eliminadas: {dropped}")
            print(f"Errores: {errors}")
            print(f"Tiempo de ejecución: {elapsed_time:.2f} segundos")
            print("=" * 60)
            
        finally:
            cur.close()
    finally:
        conn.close()


if __name__ == "__main__":
    main()
