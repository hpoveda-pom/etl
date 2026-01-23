import os
import re
import time
from datetime import datetime

try:
    import clickhouse_connect
except ImportError:
    print("❌ Error: Falta la librería clickhouse-connect")
    print("💡 Instálala con: pip install clickhouse-connect")
    exit(1)

# ============== ClickHouse Cloud config ==============
CH_HOST = os.getenv("CH_HOST", "f4rf85ygzj.eastus2.azure.clickhouse.cloud")
CH_PORT = int(os.getenv("CH_PORT", "8443"))
CH_USER = os.getenv("CH_USER", "default")
CH_PASSWORD = os.getenv("CH_PASSWORD", "Tsm1e.3Wgbw5P")
CH_DATABASE = os.getenv("CH_DATABASE", "default")
CH_TABLE = os.getenv("CH_TABLE", "")  # Tabla destino (opcional, se puede especificar por archivo)

# ============== Configuración ==============
# Confirmación requerida por defecto (seguridad)
REQUIRE_CONFIRMATION = os.getenv("REQUIRE_CONFIRMATION", "true").lower() in ("true", "1", "yes")


def sanitize_token(s: str, maxlen: int = 120) -> str:
    """Sanitiza un string para usarlo como nombre de tabla/columna en ClickHouse."""
    s = (s or "").strip()
    s = re.sub(r"[^\w\-\.]+", "_", s, flags=re.UNICODE)
    s = re.sub(r"_+", "_", s).strip("_")
    return s[:maxlen] if s else "NA"


def list_available_databases(client):
    """
    Lista las bases de datos disponibles en ClickHouse.
    """
    try:
        result = client.query("SHOW DATABASES")
        databases = [row[0] for row in result.result_rows] if result.result_rows else []
        return databases
    except Exception:
        return []


def connect_ch(database: str = None):
    """
    Crea una conexión a ClickHouse.
    Si la base de datos no existe, muestra un mensaje claro con las bases disponibles.
    """
    global CH_DATABASE
    
    if database:
        CH_DATABASE = database
    
    if not CH_PASSWORD:
        raise RuntimeError("Falta CH_PASSWORD (definí la variable de entorno).")
    
    # Primero intentar conectarse sin especificar la base de datos (o con "default")
    # para poder verificar si existe
    try:
        # Conectar primero a una base de datos que siempre existe (default)
        temp_client = clickhouse_connect.get_client(
            host=CH_HOST,
            port=CH_PORT,
            username=CH_USER,
            password=CH_PASSWORD,
            database="default",  # Conectar a "default" primero
            secure=True,
            verify=True
        )
        
        # Verificar si la base de datos existe
        try:
            check_sql = f"EXISTS DATABASE `{CH_DATABASE}`"
            result = temp_client.query(check_sql)
            db_exists = result.result_rows[0][0] == 1 if result.result_rows else False
        except:
            # Si EXISTS no funciona, intentar listar bases de datos
            available_dbs = list_available_databases(temp_client)
            db_exists = CH_DATABASE in available_dbs
        
        if not db_exists:
            # La base de datos no existe, listar disponibles
            available_dbs = list_available_databases(temp_client)
            temp_client.close()
            
            if available_dbs:
                db_list = "\n   - ".join(available_dbs[:15])
                if len(available_dbs) > 15:
                    db_list += f"\n   ... y {len(available_dbs) - 15} más"
                raise RuntimeError(
                    f"❌ La base de datos '{CH_DATABASE}' no existe.\n\n"
                    f"💡 Bases de datos disponibles ({len(available_dbs)}):\n   - {db_list}\n\n"
                    f"💡 Sugerencias:\n"
                    f"   - Usa una de las bases de datos listadas arriba\n"
                    f"   - Ejemplo: python clickhouse_drop_tables.py default ...\n"
                    f"   - O crea la base de datos '{CH_DATABASE}' en ClickHouse primero"
                )
            else:
                raise RuntimeError(
                    f"❌ La base de datos '{CH_DATABASE}' no existe.\n"
                    f"💡 No se pudieron listar las bases de datos disponibles. Verifica tus permisos."
                )
        else:
            print(f"✅ Base de datos '{CH_DATABASE}' encontrada")
        
        # Cerrar conexión temporal
        temp_client.close()
        
        # Ahora conectar a la base de datos correcta
        client = clickhouse_connect.get_client(
            host=CH_HOST,
            port=CH_PORT,
            username=CH_USER,
            password=CH_PASSWORD,
            database=CH_DATABASE,
            secure=True,  # HTTPS
            verify=True  # Verificar certificado SSL
        )
        
        # Probar la conexión
        result = client.query("SELECT 1")
        print(f"✅ Conectado a ClickHouse: {CH_HOST}:{CH_PORT}")
        print(f"📊 Base de datos: {CH_DATABASE}")
        
        return client
    except RuntimeError:
        # Re-lanzar RuntimeError sin modificar
        raise
    except Exception as e:
        error_msg = str(e)
        if "authentication" in error_msg.lower() or "password" in error_msg.lower():
            raise RuntimeError(
                f"❌ Error de autenticación. Verifica CH_USER y CH_PASSWORD.\n"
                f"Error: {error_msg}"
            )
        elif "connection" in error_msg.lower() or "timeout" in error_msg.lower():
            raise RuntimeError(
                f"❌ Error de conexión. Verifica CH_HOST y CH_PORT.\n"
                f"Error: {error_msg}"
            )
        elif "does not exist" in error_msg.lower() or "UNKNOWN_DATABASE" in error_msg:
            # Intentar listar bases de datos disponibles
            try:
                temp_client = clickhouse_connect.get_client(
                    host=CH_HOST,
                    port=CH_PORT,
                    username=CH_USER,
                    password=CH_PASSWORD,
                    database="default",
                    secure=True,
                    verify=True
                )
                available_dbs = list_available_databases(temp_client)
                temp_client.close()
                
                if available_dbs:
                    db_list = "\n   - ".join(available_dbs[:15])
                    if len(available_dbs) > 15:
                        db_list += f"\n   ... y {len(available_dbs) - 15} más"
                    raise RuntimeError(
                        f"❌ La base de datos '{CH_DATABASE}' no existe.\n"
                        f"Error: {error_msg}\n\n"
                        f"💡 Bases de datos disponibles ({len(available_dbs)}):\n   - {db_list}\n\n"
                        f"💡 Sugerencias:\n"
                        f"   - Usa una de las bases de datos listadas arriba\n"
                        f"   - Ejemplo: python clickhouse_drop_tables.py default ...\n"
                        f"   - O crea la base de datos '{CH_DATABASE}' en ClickHouse primero"
                    )
            except:
                pass
            
            raise RuntimeError(
                f"❌ La base de datos '{CH_DATABASE}' no existe.\n"
                f"Error: {error_msg}\n"
                f"💡 Verifica el nombre de la base de datos o créala primero en ClickHouse."
            )
        else:
            raise RuntimeError(f"❌ Error conectando a ClickHouse: {error_msg}")


def ch_exec(client, sql: str):
    """Ejecuta SQL en ClickHouse y maneja errores."""
    try:
        if sql.strip().upper().startswith("SELECT") or sql.strip().upper().startswith("SHOW") or sql.strip().upper().startswith("DESCRIBE") or sql.strip().upper().startswith("EXISTS"):
            result = client.query(sql)
            return result.result_rows if result.result_rows else None
        else:
            client.command(sql)
            return None
    except Exception as e:
        print(f"  ❌ Error SQL: {e}")
        print(f"  SQL: {sql[:200]}...")
        raise


def list_tables_in_database(client, pattern: str = None):
    """
    Lista las tablas en la base de datos actual.
    
    Args:
        client: Cliente de ClickHouse
        pattern: Patrón para filtrar tablas (opcional, ej: "PC_%")
    
    Returns:
        Lista de nombres de tablas
    """
    try:
        if pattern:
            # ClickHouse usa LIKE para patrones
            pattern_escaped = pattern.replace('%', '\\%').replace('_', '\\_')
            sql = f"SELECT name FROM system.tables WHERE database = '{CH_DATABASE}' AND name LIKE '{pattern_escaped}'"
        else:
            sql = f"SELECT name FROM system.tables WHERE database = '{CH_DATABASE}'"
        
        result = ch_exec(client, sql)
        
        tables = []
        if result:
            for row in result:
                if len(row) > 0:
                    table_name = row[0]
                    tables.append(table_name)
        
        return tables
    except Exception as e:
        print(f"⚠️  Error al listar tablas: {e}")
        return []


def format_table_name(table_name: str) -> str:
    """
    Formatea el nombre de la tabla para usar en SQL.
    
    Args:
        table_name: Nombre de la tabla (puede ser "DB.TABLE" o solo "TABLE")
    
    Returns:
        Nombre formateado con comillas invertidas si es necesario
    """
    if not table_name:
        return None
    
    # Si ya tiene comillas invertidas, devolverlo tal cual
    if table_name.startswith('`') and table_name.endswith('`'):
        return table_name
    
    # Si contiene puntos, es un nombre completo (DB.TABLE)
    if '.' in table_name:
        parts = table_name.split('.')
        return '.'.join([f'`{part}`' for part in parts])
    
    # Si es solo el nombre de la tabla, usar comillas invertidas
    return f"`{CH_DATABASE}`.`{table_name}`"


def drop_table(client, table_name: str) -> bool:
    """
    Elimina una tabla en ClickHouse.
    
    Args:
        client: Cliente de ClickHouse
        table_name: Nombre de la tabla a eliminar
    
    Returns:
        True si se eliminó exitosamente, False si hubo error
    """
    try:
        # Formatear nombre de tabla
        if '.' in table_name:
            # Nombre completo con DB.TABLE
            full_table_name = format_table_name(table_name)
        else:
            # Solo nombre de tabla, construir nombre completo
            full_table_name = f"`{CH_DATABASE}`.`{table_name}`"
        
        drop_sql = f"DROP TABLE IF EXISTS {full_table_name}"
        ch_exec(client, drop_sql)
        return True
    except Exception as e:
        print(f"  ❌ Error eliminando tabla '{table_name}': {e}")
        return False


def drop_tables(client, table_names: list = None, pattern: str = None, all_tables: bool = False) -> tuple:
    """
    Elimina tablas en ClickHouse.
    
    Args:
        client: Cliente de ClickHouse
        table_names: Lista de nombres de tablas a eliminar (opcional)
        pattern: Patrón para filtrar tablas (opcional, ej: "PC_%")
        all_tables: Si es True, elimina todas las tablas de la base de datos (peligroso)
    
    Returns:
        (dropped_count, error_count, total_tables)
    """
    dropped = 0
    errors = 0
    
    if all_tables:
        # Eliminar todas las tablas
        tables = list_tables_in_database(client)
        total_tables = len(tables)
    elif pattern:
        # Filtrar por patrón
        tables = list_tables_in_database(client, pattern)
        total_tables = len(tables)
    elif table_names:
        # Usar lista específica de tablas
        tables = table_names
        total_tables = len(tables)
    else:
        print("⚠️  No se especificaron tablas para eliminar.")
        return 0, 0, 0
    
    if not tables:
        print("⚠️  No se encontraron tablas que coincidan con los criterios.")
        return 0, 0, 0
    
    print(f"\n📋 Tablas a eliminar: {len(tables)}")
    print("=" * 60)
    for i, table in enumerate(tables[:20], 1):  # Mostrar hasta 20
        print(f"  {i}. {table}")
    if len(tables) > 20:
        print(f"  ... y {len(tables) - 20} más")
    print("=" * 60)
    
    # Confirmación de seguridad
    if REQUIRE_CONFIRMATION:
        print(f"\n⚠️  ADVERTENCIA: Se eliminarán {len(tables)} tabla(s) en {CH_DATABASE}")
        confirmation = input("¿Estás seguro? Escribe 'SI' para confirmar: ")
        if confirmation.upper() != "SI":
            print("❌ Operación cancelada.")
            return 0, 0, total_tables
    
    print(f"\n🗑️  Eliminando tablas...")
    
    for table_name in tables:
        try:
            print(f"  → Eliminando: {table_name}")
            if drop_table(client, table_name):
                dropped += 1
                print(f"    ✅ Tabla '{table_name}' eliminada")
            else:
                errors += 1
        except Exception as e:
            errors += 1
            print(f"    ❌ Error: {e}")
    
    return dropped, errors, total_tables


def main():
    """
    Función principal.
    Uso:
        python clickhouse_drop_tables.py [database] [tables|pattern|--all] [--no-confirm]
    
    Args:
        database: Base de datos de ClickHouse (opcional)
        tables: Tablas a eliminar, separadas por comas (opcional)
        pattern: Patrón para filtrar tablas, usar comillas (opcional, ej: "PC_%")
        --all: Eliminar todas las tablas de la base de datos (peligroso)
        --no-confirm: Omitir confirmación (peligroso)
    
    Ejemplos:
        python clickhouse_drop_tables.py
        python clickhouse_drop_tables.py default
        python clickhouse_drop_tables.py default Tabla1,Tabla2,Tabla3
        python clickhouse_drop_tables.py default "PC_%"
        python clickhouse_drop_tables.py default --all
        python clickhouse_drop_tables.py default Tabla1 --no-confirm
    """
    import sys
    
    start_time = time.time()
    
    # Parsear argumentos
    database = None
    table_names = None
    pattern = None
    all_tables = False
    no_confirm = False
    
    if len(sys.argv) > 1:
        database = sys.argv[1]
    if len(sys.argv) > 2:
        arg = sys.argv[2].strip()
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
    
    if len(sys.argv) > 3:
        arg = sys.argv[3].strip()
        if arg == "--all":
            all_tables = True
        elif arg == "--no-confirm":
            no_confirm = True
    
    # Si no hay argumentos pero hay variables de entorno, usarlas
    if not database and os.getenv("CH_DATABASE"):
        database = os.getenv("CH_DATABASE")
    
    # Desactivar confirmación si se especificó --no-confirm
    global REQUIRE_CONFIRMATION
    if no_confirm:
        REQUIRE_CONFIRMATION = False
    
    # Conectar a ClickHouse
    client = connect_ch(database)
    
    print(f"📊 Base de datos ClickHouse: {CH_DATABASE}")
    if table_names:
        print(f"🗑️  Tablas a eliminar: {', '.join(table_names)}")
    elif pattern:
        print(f"🔍 Patrón de búsqueda: {pattern}")
    elif all_tables:
        print(f"⚠️  MODO PELIGROSO: Eliminar TODAS las tablas de la base de datos")
    print()
    
    try:
        dropped, errors, total = drop_tables(client, table_names, pattern, all_tables)
        
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
        client.close()


if __name__ == "__main__":
    main()
