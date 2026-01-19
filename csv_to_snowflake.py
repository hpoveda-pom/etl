import os
import re
import gzip
import shutil
import tempfile
import uuid
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

# ============== Carpetas ==============
CSV_STAGING_DIR = os.getenv("CSV_STAGING_DIR", r"UPLOADS\POM_DROP\csv_staging")
CSV_PROCESSED_DIR = os.getenv("CSV_PROCESSED_DIR", r"UPLOADS\POM_DROP\csv_processed")
CSV_ERROR_DIR = os.getenv("CSV_ERROR_DIR", r"UPLOADS\POM_DROP\csv_error")

# Opcional: filtrar carpetas específicas (coma-separado). Si está vacío, procesa todas.
FOLDERS_FILTER = [s.strip() for s in os.getenv("FOLDERS_FILTER", "").split(",") if s.strip()]

# Opcional: filtrar CSV específicos dentro de las carpetas (coma-separado, sin extensión .csv)
CSV_FILTER = [s.strip() for s in os.getenv("CSV_FILTER", "").split(",") if s.strip()]

# ============== Stage y log table (se inicializan con update_snowflake_config) ==============
# STAGE_FQN_PUT: versión sin comillas para usar en comandos PUT
# STAGE_FQN: versión con comillas para usar en otros comandos SQL
# SF_DB_ACTUAL: nombre real de la base de datos que funcionó (puede ser diferente del configurado)
SF_DB_ACTUAL = None  # Se actualiza en connect_sf cuando se determina el nombre real

def _init_snowflake_config():
    """Inicializa las variables de configuración de Snowflake."""
    global STAGE_FQN, STAGE_FQN_PUT, LOG_TABLE, TARGET_TABLE
    STAGE_FQN = f"{SF_DB}.{SF_SCHEMA}.RAW_STAGE"
    STAGE_FQN_PUT = f"{SF_DB}.{SF_SCHEMA}.RAW_STAGE"  # Sin comillas para PUT
    LOG_TABLE = f"{SF_DB}.{SF_SCHEMA}.INGEST_LOG"
    TARGET_TABLE = f"{SF_DB}.{SF_SCHEMA}.INGEST_GENERIC_RAW"

_init_snowflake_config()


def ensure_dirs():
    for d in (CSV_STAGING_DIR, CSV_PROCESSED_DIR, CSV_ERROR_DIR):
        os.makedirs(d, exist_ok=True)


def sanitize_token(s: str, maxlen: int = 120) -> str:
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
    global SF_DB, SF_SCHEMA, STAGE_FQN, STAGE_FQN_PUT, LOG_TABLE, TARGET_TABLE, SF_DB_ACTUAL
    
    if database:
        SF_DB = database
    if schema:
        SF_SCHEMA = schema
    if db_actual:
        SF_DB_ACTUAL = db_actual
    
    # Para identificadores con case mixto, usar comillas dobles
    # En Snowflake, si un identificador tiene comillas, todos deben tenerlas
    # STAGE_FQN_PUT: usar el nombre real de la DB que funcionó (sin comillas para PUT)
    # STAGE_FQN: con comillas para usar en otros comandos SQL
    db_for_put = SF_DB_ACTUAL if SF_DB_ACTUAL else SF_DB
    
    if SF_DB != SF_DB.upper():
        # Case mixto: usar comillas para todos los componentes (excepto en PUT)
        STAGE_FQN = f'"{SF_DB}"."{SF_SCHEMA}"."RAW_STAGE"'
        # Para PUT, usar solo el nombre del stage porque ya estamos en el contexto correcto
        # después de USE DATABASE y USE SCHEMA. Esto evita problemas de case-sensitive.
        STAGE_FQN_PUT = "RAW_STAGE"
        LOG_TABLE = f'"{SF_DB}"."{SF_SCHEMA}"."INGEST_LOG"'
        TARGET_TABLE = f'"{SF_DB}"."{SF_SCHEMA}"."INGEST_GENERIC_RAW"'
    else:
        # Todo en mayúsculas: no usar comillas
        STAGE_FQN = f"{SF_DB}.{SF_SCHEMA}.RAW_STAGE"
        STAGE_FQN_PUT = "RAW_STAGE"
        LOG_TABLE = f"{SF_DB}.{SF_SCHEMA}.INGEST_LOG"
        TARGET_TABLE = f"{SF_DB}.{SF_SCHEMA}.INGEST_GENERIC_RAW"


def list_available_databases(conn):
    """
    Lista las bases de datos disponibles en Snowflake.
    """
    try:
        cur = conn.cursor()
        cur.execute("SHOW DATABASES;")
        databases = [row[1] for row in cur.fetchall()]  # El nombre está en la columna 1
        cur.close()
        return databases
    except Exception:
        return []


def ensure_snowflake_environment(cur):
    """
    Verifica y prepara el entorno de Snowflake:
    - Verifica/crea el stage RAW_STAGE
    - Verifica/crea las tablas INGEST_LOG e INGEST_GENERIC_RAW
    
    Nota: El schema ya debe estar creado y en uso (se crea en connect_sf).
    """
    print("🔍 Verificando entorno de Snowflake...")
    
    # Asegurar que estamos usando el schema correcto
    # El schema ya debería estar en uso después de connect_sf, pero lo verificamos
    try:
        # Intentar usar el schema con comillas si tiene case mixto
        if SF_SCHEMA != SF_SCHEMA.upper():
            try:
                cur.execute(f'USE SCHEMA "{SF_SCHEMA}";')
            except:
                cur.execute(f"USE SCHEMA {SF_SCHEMA};")
        else:
            cur.execute(f"USE SCHEMA {SF_SCHEMA};")
    except Exception as e:
        print(f"⚠️  Advertencia al usar schema: {e}")
        # Continuar de todas formas, puede que ya estemos en el schema correcto
    
    # 1. Verificar/crear stage RAW_STAGE
    try:
        cur.execute("SHOW STAGES;")
        stages = [row[1] for row in cur.fetchall()]
        
        stage_name = "RAW_STAGE"
        if stage_name.upper() not in [s.upper() for s in stages]:
            print(f"📦 Creando stage '{stage_name}'...")
            create_stage_sql = f"""
            CREATE STAGE IF NOT EXISTS {stage_name}
            FILE_FORMAT = (TYPE = CSV, FIELD_DELIMITER = ',', SKIP_HEADER = 1, FIELD_OPTIONALLY_ENCLOSED_BY = '"');
            """
            cur.execute(create_stage_sql)
            print(f"✅ Stage '{stage_name}' creado exitosamente")
        else:
            print(f"✅ Stage '{stage_name}' ya existe")
    except Exception as e:
        print(f"⚠️  Advertencia al verificar stage: {e}")
        try:
            stage_name = "RAW_STAGE"
            create_stage_sql = f"""
            CREATE STAGE IF NOT EXISTS {stage_name}
            FILE_FORMAT = (TYPE = CSV, FIELD_DELIMITER = ',', SKIP_HEADER = 1, FIELD_OPTIONALLY_ENCLOSED_BY = '"');
            """
            cur.execute(create_stage_sql)
            print(f"✅ Stage '{stage_name}' creado exitosamente")
        except Exception as create_err:
            print(f"❌ No se pudo crear el stage '{stage_name}': {create_err}")
            raise
    
    # 2. Verificar/crear tabla INGEST_LOG
    # Solo crear INGEST_LOG si estamos en el schema RAW (no en otros schemas)
    # Para otros schemas, no es necesario crear esta tabla
    if SF_SCHEMA.upper() == "RAW":
        try:
            cur.execute("SHOW TABLES LIKE 'INGEST_LOG';")
            tables = cur.fetchall()
            if not tables:
                print(f"📦 Creando tabla 'INGEST_LOG'...")
                create_log_sql = f"""
                CREATE TABLE IF NOT EXISTS {LOG_TABLE} (
                    batch_id VARCHAR(36),
                    original_file VARCHAR(500),
                    sheet_name VARCHAR(200),
                    stage_path VARCHAR(1000),
                    row_count INTEGER,
                    col_count INTEGER,
                    file_bytes BIGINT,
                    status VARCHAR(20),
                    error_message VARCHAR(4000),
                    ingested_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
                );
                """
                cur.execute(create_log_sql)
                print(f"✅ Tabla 'INGEST_LOG' creada exitosamente")
            else:
                print(f"✅ Tabla 'INGEST_LOG' ya existe")
        except Exception as e:
            print(f"⚠️  Advertencia al verificar tabla INGEST_LOG: {e}")
            try:
                create_log_sql = f"""
                CREATE TABLE IF NOT EXISTS {LOG_TABLE} (
                    batch_id VARCHAR(36),
                    original_file VARCHAR(500),
                    sheet_name VARCHAR(200),
                    stage_path VARCHAR(1000),
                    row_count INTEGER,
                    col_count INTEGER,
                    file_bytes BIGINT,
                    status VARCHAR(20),
                    error_message VARCHAR(4000),
                    ingested_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
                );
                """
                cur.execute(create_log_sql)
                print(f"✅ Tabla 'INGEST_LOG' creada exitosamente")
            except Exception as create_err:
                print(f"❌ No se pudo crear la tabla 'INGEST_LOG': {create_err}")
                raise
    else:
        print(f"💡 Omitiendo creación de INGEST_LOG en schema '{SF_SCHEMA}' (solo se crea en RAW)")
    
    # 3. Verificar/crear tabla INGEST_GENERIC_RAW
    # Solo crear INGEST_GENERIC_RAW si estamos en el schema RAW (no en otros schemas)
    # Para otros schemas, el usuario especificará su propia tabla destino
    if SF_SCHEMA.upper() == "RAW":
        try:
            cur.execute("SHOW TABLES LIKE 'INGEST_GENERIC_RAW';")
            tables = cur.fetchall()
            if not tables:
                print(f"📦 Creando tabla 'INGEST_GENERIC_RAW'...")
                # Crear columnas dinámicas (col1 a col50) más metadatos
                cols = []
                for i in range(1, 51):
                    cols.append(f"col{i} VARCHAR(16777216)")
                
                create_target_sql = f"""
                CREATE TABLE IF NOT EXISTS {TARGET_TABLE} (
                    file_source VARCHAR(500),
                    sheet_name VARCHAR(200),
                    row_number INTEGER,
                    {', '.join(cols)},
                    ingested_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
                );
                """
                cur.execute(create_target_sql)
                print(f"✅ Tabla 'INGEST_GENERIC_RAW' creada exitosamente")
            else:
                print(f"✅ Tabla 'INGEST_GENERIC_RAW' ya existe")
        except Exception as e:
            print(f"⚠️  Advertencia al verificar tabla INGEST_GENERIC_RAW: {e}")
            try:
                cols = []
                for i in range(1, 51):
                    cols.append(f"col{i} VARCHAR(16777216)")
                
                create_target_sql = f"""
                CREATE TABLE IF NOT EXISTS {TARGET_TABLE} (
                    file_source VARCHAR(500),
                    sheet_name VARCHAR(200),
                    row_number INTEGER,
                    {', '.join(cols)},
                    ingested_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
                );
                """
                cur.execute(create_target_sql)
                print(f"✅ Tabla 'INGEST_GENERIC_RAW' creada exitosamente")
            except Exception as create_err:
                print(f"❌ No se pudo crear la tabla 'INGEST_GENERIC_RAW': {create_err}")
                raise
    else:
        print(f"💡 Omitiendo creación de INGEST_GENERIC_RAW en schema '{SF_SCHEMA}' (solo se crea en RAW)")
    
    print("✅ Entorno de Snowflake verificado y preparado correctamente\n")


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
    
    # Conectar sin especificar database/schema primero para evitar errores tempranos
    # Luego intentaremos usar la base de datos
    try:
        conn = snowflake.connector.connect(
            account=SF_ACCOUNT,
            user=SF_USER,
            password=SF_PASSWORD,
            role=SF_ROLE,
            warehouse=SF_WH,
            # No especificar database/schema en la conexión inicial para mayor flexibilidad
        )
        
        # Intentar usar la base de datos y schema especificados
        cur = conn.cursor()
        try:
            # Intentar usar la base de datos (con comillas dobles para preservar case si es necesario)
            db_name_used = None
            db_exists = False
            
            # Primero intentar usar la base de datos
            try:
                cur.execute(f'USE DATABASE "{SF_DB}";')
                db_name_used = SF_DB
                db_exists = True
                print(f"✅ Base de datos '{SF_DB}' encontrada")
            except Exception as e1:
                # Si falla con comillas, intentar sin comillas (Snowflake convierte a mayúsculas)
                try:
                    db_upper = SF_DB.upper()
                    cur.execute(f"USE DATABASE {db_upper};")
                    db_name_used = db_upper
                    db_exists = True
                    # Actualizar configuración global si funcionó
                    update_snowflake_config(db_upper, None)
                    print(f"✅ Base de datos '{db_upper}' encontrada")
                except Exception as e2:
                    # Si ambos intentos fallaron, intentar crear la base de datos
                    # (puede que no exista o que haya otro problema, pero intentar crearla)
                    print(f"📦 La base de datos '{SF_DB}' no se pudo usar. Intentando crearla...")
                    try:
                        # Intentar crear con el nombre original (con comillas para preservar case)
                        cur.execute(f'CREATE DATABASE IF NOT EXISTS "{SF_DB}";')
                        cur.execute(f'USE DATABASE "{SF_DB}";')
                        db_name_used = SF_DB
                        db_exists = True
                        print(f"✅ Base de datos '{SF_DB}' creada exitosamente")
                    except Exception as create_err:
                        # Si falla con comillas, intentar sin comillas
                        try:
                            db_upper = SF_DB.upper()
                            cur.execute(f"CREATE DATABASE IF NOT EXISTS {db_upper};")
                            cur.execute(f"USE DATABASE {db_upper};")
                            db_name_used = db_upper
                            db_exists = True
                            update_snowflake_config(db_upper, None)
                            print(f"✅ Base de datos '{db_upper}' creada exitosamente")
                        except Exception as create_err2:
                            # Si también falla la creación, lanzar el error original
                            raise RuntimeError(
                                f"Error al usar o crear la base de datos '{SF_DB}':\n"
                                f"  - Error al usar: {e2}\n"
                                f"  - Error al crear: {create_err2}"
                            )
            
            if not db_name_used or not db_exists:
                raise RuntimeError(f"No se pudo usar o crear la base de datos '{SF_DB}'")
            
            # Verificar si el schema existe, si no existe, crearlo
            # Después de USE DATABASE, podemos hacer SHOW SCHEMAS sin especificar la DB
            try:
                cur.execute("SHOW SCHEMAS;")
            except Exception:
                # Si falla, intentar con el nombre de la base de datos
                cur.execute(f"SHOW SCHEMAS IN DATABASE {db_name_used};")
            
            schemas = [row[1] for row in cur.fetchall()]
            
            if SF_SCHEMA.upper() not in [s.upper() for s in schemas]:
                # El schema no existe, crearlo
                print(f"📦 El schema '{SF_SCHEMA}' no existe. Creándolo...")
                try:
                    cur.execute(f"CREATE SCHEMA IF NOT EXISTS {SF_SCHEMA};")
                    print(f"✅ Schema '{SF_SCHEMA}' creado exitosamente")
                except Exception as create_schema_err:
                    raise RuntimeError(f"Error al crear el schema '{SF_SCHEMA}': {create_schema_err}")
            else:
                print(f"✅ Schema '{SF_SCHEMA}' encontrado")
            
            cur.execute(f"USE SCHEMA {SF_SCHEMA};")
            
            # Actualizar configuración con el nombre de DB que realmente funcionó
            # Esto asegura que STAGE_FQN use el nombre correcto (con comillas si es necesario)
            # También guardamos el nombre real para STAGE_FQN_PUT
            update_snowflake_config(db_name_used, SF_SCHEMA, db_name_used)
            
            # Asegurar que el entorno completo esté preparado (schema, stage, tablas)
            ensure_snowflake_environment(cur)
            
            print(f"✅ Conectado a Snowflake: {SF_DB}.{SF_SCHEMA}")
        except RuntimeError:
            # Re-lanzar RuntimeError sin modificar
            raise
        except Exception as e:
            error_msg = str(e)
            if "does not exist" in error_msg.lower() or "not authorized" in error_msg.lower() or "Object does not exist" in error_msg:
                # Listar bases de datos disponibles antes de cerrar la conexión
                print(f"⚠️  La base de datos '{SF_DB}' no existe. Buscando bases de datos disponibles...")
                available_dbs = list_available_databases(conn)
                cur.close()
                conn.close()
                
                if available_dbs:
                    db_list = "\n   - ".join(available_dbs[:15])  # Mostrar hasta 15
                    if len(available_dbs) > 15:
                        db_list += f"\n   ... y {len(available_dbs) - 15} más"
                    raise RuntimeError(
                        f"❌ Base de datos '{SF_DB}' o schema '{SF_SCHEMA}' no existe o no tienes permisos.\n"
                        f"Error: {error_msg}\n\n"
                        f"💡 Bases de datos disponibles ({len(available_dbs)}):\n   - {db_list}\n\n"
                        f"💡 Sugerencias:\n"
                        f"   - Usa una de las bases de datos listadas arriba\n"
                        f"   - Ejemplo: python csv_to_snowflake.py POM_TEST01 RAW ...\n"
                        f"   - O crea la base de datos '{SF_DB}' en Snowflake primero"
                    )
                else:
                    raise RuntimeError(
                        f"❌ Base de datos '{SF_DB}' o schema '{SF_SCHEMA}' no existe o no tienes permisos.\n"
                        f"Error: {error_msg}\n"
                        f"💡 No se pudieron listar las bases de datos disponibles. Verifica tus permisos."
                    )
            # Si es otro error, continuar pero mostrar advertencia
            print(f"⚠️  Advertencia al usar {SF_DB}.{SF_SCHEMA}: {error_msg}")
        finally:
            cur.close()
        
        return conn
    except snowflake.connector.errors.ProgrammingError as e:
        error_str = str(e).lower()
        if "does not exist" in error_str or "not authorized" in error_str or "object does not exist" in error_str:
            raise RuntimeError(
                f"❌ Error de conexión: La base de datos '{SF_DB}' no existe o no tienes permisos.\n"
                f"💡 Verifica el nombre exacto de la base de datos en Snowflake y tus permisos."
            )
        raise


def list_csv_folders(folders_filter: list = None):
    """
    Lista las carpetas en CSV_STAGING_DIR (cada carpeta representa un Excel procesado).
    Retorna lista de rutas de carpetas.
    
    Args:
        folders_filter: Lista de nombres de carpetas a filtrar (si None, usa FOLDERS_FILTER)
    """
    if folders_filter is None:
        folders_filter = FOLDERS_FILTER
    
    folders = []
    if not os.path.exists(CSV_STAGING_DIR):
        return folders
    
    for item in os.listdir(CSV_STAGING_DIR):
        folder_path = os.path.join(CSV_STAGING_DIR, item)
        if os.path.isdir(folder_path):
            # Filtrar si hay filtro especificado
            if folders_filter:
                folder_name = os.path.basename(folder_path)
                # Buscar coincidencias (exacta o parcial)
                if not any(folder_name == f or folder_name.startswith(f + "_") or f in folder_name 
                          for f in folders_filter):
                    continue
            folders.append(folder_path)
    return sorted(folders)


def list_csvs_in_folder(folder_path: str, csv_filter: list = None):
    """
    Lista los archivos CSV (sin comprimir) en una carpeta.
    
    Args:
        folder_path: Ruta de la carpeta
        csv_filter: Lista de nombres de CSV a filtrar (sin extensión .csv). Si None, usa CSV_FILTER
    """
    if csv_filter is None:
        csv_filter = CSV_FILTER
    
    files = []
    for name in os.listdir(folder_path):
        if name.lower().endswith(".csv") and not name.lower().endswith(".csv.gz"):
            # Filtrar si hay filtro especificado
            if csv_filter:
                csv_name = name[:-4]  # Remover .csv
                if not any(csv_name == f or csv_name.startswith(f + "_") or f in csv_name 
                          for f in csv_filter):
                    continue
            files.append(os.path.join(folder_path, name))
    return sorted(files)


def move_folder(src_folder: str, dst_dir: str, max_retries: int = 5, retry_delay: float = 0.5):
    """
    Mueve una carpeta completa con reintentos para manejar bloqueos en Windows.
    """
    os.makedirs(dst_dir, exist_ok=True)
    folder_name = os.path.basename(src_folder)
    dst = os.path.join(dst_dir, folder_name)
    
    if os.path.exists(dst):
        dst = os.path.join(dst_dir, f"{folder_name}_{datetime.now().strftime('%Y%m%d_%H%M%S')}")
    
    for attempt in range(max_retries):
        try:
            shutil.move(src_folder, dst)
            return
        except PermissionError as e:
            if attempt < max_retries - 1:
                time.sleep(retry_delay)
                continue
            raise


def sf_exec(cur, sql: str):
    try:
        cur.execute(sql)
        return cur.fetchall() if cur.description else None
    except Exception as e:
        print(f"  ❌ Error SQL: {e}")
        print(f"  SQL completo:")
        # Mostrar el SQL completo en líneas para mejor debugging
        sql_lines = sql.strip().split('\n')
        for i, line in enumerate(sql_lines[:20], 1):  # Mostrar hasta 20 líneas
            print(f"    {i}: {line}")
        if len(sql_lines) > 20:
            print(f"    ... ({len(sql_lines) - 20} líneas más)")
        raise


def log_ingest(cur, batch_id: str, original_file: str, sheet_name: str, stage_path: str,
              row_count: int, col_count: int, file_bytes: int, status: str, error_message: str = ""):
    def esc(x: str) -> str:
        return (x or "").replace("'", "''")

    sql = f"""
    INSERT INTO {LOG_TABLE} (
      batch_id, original_file, sheet_name, stage_path,
      row_count, col_count, file_bytes, status, error_message
    )
    VALUES (
      '{esc(batch_id)}', '{esc(original_file)}', '{esc(sheet_name)}', '{esc(stage_path)}',
      {row_count}, {col_count}, {file_bytes}, '{esc(status)}', '{esc(error_message[:4000])}'
    );
    """
    sf_exec(cur, sql)


def put_to_stage(cur, local_path: str, stage_target: str):
    """
    Sube un archivo local al stage de Snowflake.
    local_path: ruta absoluta del archivo local
    stage_target: ruta en el stage, ej: @DB.SCHEMA.STAGE/prefix/file.csv.gz
    """
    abs_path = os.path.abspath(local_path)
    abs_path_normalized = abs_path.replace("\\", "/")
    
    file_url = f"file:///{abs_path_normalized}"
    stage_target_clean = stage_target
    
    # El comando PUT no acepta comillas en el path del stage
    # Si el stage_target tiene comillas, necesitamos removerlas
    # Pero Snowflake convertirá a mayúsculas sin comillas
    # La solución es asegurarnos de que estamos en el contexto correcto de la base de datos
    # antes de ejecutar PUT (ya deberíamos estar después de USE DATABASE)
    
    put_sql = f"PUT '{file_url}' {stage_target_clean} AUTO_COMPRESS=FALSE OVERWRITE=TRUE;"
    print(f"  → PUT: {file_url}")
    print(f"  → Stage target: {stage_target_clean}")
    result = sf_exec(cur, put_sql)
    if result:
        print(f"  → PUT result: {result}")


def copy_from_stage_to_table(cur, stage_path: str, table_name: str):
    """
    Carga datos desde el stage a la tabla usando COPY INTO.
    stage_path: ruta completa en el stage, ej: @POM_TEST01.RAW.RAW_STAGE/archivo.csv.gz
    table_name: Nombre de la tabla destino (puede ser INGEST_GENERIC_RAW u otra tabla)
    Nota: stage_path viene con STAGE_FQN_PUT (sin comillas) del comando PUT
    
    Si la tabla es INGEST_GENERIC_RAW, agrega metadatos (file_source, sheet_name, row_number).
    Si es otra tabla, carga directamente sin metadatos (asume que la estructura coincide con el CSV).
    """
    # Extraer el path relativo del stage_path
    # stage_path tiene formato: @STAGE_FQN_PUT/folder/file.csv.gz (sin comillas)
    # Pero para COPY INTO necesitamos usar STAGE_FQN (con comillas si es necesario)
    stage_prefix_put = f"@{STAGE_FQN_PUT}/"
    if stage_path.startswith(stage_prefix_put):
        relative_path = stage_path[len(stage_prefix_put):]
    else:
        # Intentar sin el @ inicial
        stage_prefix_no_at = f"{STAGE_FQN_PUT}/"
        if stage_path.startswith(stage_prefix_no_at):
            relative_path = stage_path[len(stage_prefix_no_at):]
        else:
            # Si no coincide, asumir que todo después del último / es el path relativo
            relative_path = stage_path.split("/", 1)[-1] if "/" in stage_path else stage_path
    
    relative_path = relative_path.lstrip('/')
    
    # Verificar si la tabla destino es INGEST_GENERIC_RAW (necesita metadatos)
    # o es otra tabla (carga directa)
    is_generic_raw = "INGEST_GENERIC_RAW" in table_name.upper()
    
    if is_generic_raw:
        # Para INGEST_GENERIC_RAW: usar tabla temporal y agregar metadatos
        # Extraer file_source y sheet_name del path para los metadatos
        path_parts = relative_path.split('/')
        folder_name = path_parts[0] if len(path_parts) > 0 else "UNKNOWN"
        file_name = path_parts[-1] if len(path_parts) > 0 else "UNKNOWN"
        sheet_name = file_name.replace('.csv.gz', '').replace('.csv', '')
        
        # Escapar comillas simples en los valores para SQL
        folder_name_escaped = folder_name.replace("'", "''")
        sheet_name_escaped = sheet_name.replace("'", "''")
        
        # Crear tabla temporal, copiar ahí, luego INSERT SELECT con metadatos
        temp_table_name = f"TEMP_INGEST_{uuid.uuid4().hex[:8].upper()}"
        
        try:
            # Crear tabla temporal con solo las columnas de datos (col1-col50)
            temp_cols = ', '.join([f"col{i} VARCHAR(16777216)" for i in range(1, 51)])
            create_temp_sql = f"""
            CREATE TEMPORARY TABLE {temp_table_name} (
                {temp_cols}
            );
            """
            sf_exec(cur, create_temp_sql)
            
            # Copiar datos del CSV a la tabla temporal
            copy_temp_sql = f"""
            COPY INTO {temp_table_name}
            FROM @{STAGE_FQN}/{relative_path}
            FILE_FORMAT = (TYPE = CSV, FIELD_DELIMITER = ',', SKIP_HEADER = 1, FIELD_OPTIONALLY_ENCLOSED_BY = '"')
            ON_ERROR = 'CONTINUE'
            FORCE = FALSE;
            """
            sf_exec(cur, copy_temp_sql)
            
            # Insertar desde la tabla temporal a la tabla final con metadatos
            insert_sql = f"""
            INSERT INTO {table_name} (
                file_source, sheet_name, row_number,
                {', '.join([f'col{i}' for i in range(1, 51)])}
            )
            SELECT 
                '{folder_name_escaped}' as file_source,
                '{sheet_name_escaped}' as sheet_name,
                ROW_NUMBER() OVER (ORDER BY col1) as row_number,
                {', '.join([f'col{i}' for i in range(1, 51)])}
            FROM {temp_table_name};
            """
            sf_exec(cur, insert_sql)
            
            # Eliminar tabla temporal
            try:
                cur.execute(f"DROP TABLE IF EXISTS {temp_table_name};")
            except:
                pass  # Ignorar errores al eliminar tabla temporal
            
        except Exception as e:
            # Limpiar tabla temporal en caso de error
            try:
                cur.execute(f"DROP TABLE IF EXISTS {temp_table_name};")
            except:
                pass
            raise
    else:
        # Para otras tablas: carga directa sin metadatos
        # Asume que la estructura de la tabla coincide con el CSV
        copy_sql = f"""
        COPY INTO {table_name}
        FROM @{STAGE_FQN}/{relative_path}
        FILE_FORMAT = (TYPE = CSV, FIELD_DELIMITER = ',', SKIP_HEADER = 1, FIELD_OPTIONALLY_ENCLOSED_BY = '"')
        ON_ERROR = 'CONTINUE'
        FORCE = FALSE;
        """
        sf_exec(cur, copy_sql)
    
    print(f"  → COPY INTO: @{STAGE_FQN}/{relative_path} → {table_name}")


def get_csv_info(csv_path: str) -> tuple[int, int]:
    """
    Obtiene el número de filas y columnas de un CSV.
    Retorna (row_count, col_count)
    """
    try:
        with open(csv_path, 'rt', encoding='utf-8', newline='') as f:
            # Leer header
            header = f.readline()
            col_count = len(header.split(','))
            
            # Contar filas restantes
            row_count = sum(1 for _ in f)
            
        return row_count, col_count
    except Exception:
        return 0, 0


def compress_csv_to_gz(csv_path: str, output_gz_path: str):
    """
    Comprime un archivo CSV a CSV.gz.
    """
    with open(csv_path, 'rt', encoding='utf-8', newline='') as f_in:
        with gzip.open(output_gz_path, 'wt', encoding='utf-8', newline='') as f_out:
            shutil.copyfileobj(f_in, f_out)


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
        return TARGET_TABLE
    
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


def ingest_csv_folder(cur, folder_path: str, batch_id: str, csv_filter: list = None, target_table: str = None) -> int:
    """
    Procesa todos los CSV en una carpeta: los comprime, los sube al stage y los carga a la tabla.
    Retorna el número de archivos procesados exitosamente.
    
    Args:
        cur: Cursor de Snowflake
        folder_path: Ruta de la carpeta
        batch_id: ID del batch
        csv_filter: Lista de nombres de CSV a filtrar (sin extensión .csv)
        target_table: Tabla destino. Si es None, usa TARGET_TABLE (INGEST_GENERIC_RAW)
    """
    folder_name = os.path.basename(folder_path)
    csv_files = list_csvs_in_folder(folder_path, csv_filter)
    
    if not csv_files:
        print(f"  ⚠️  No hay archivos CSV en {folder_name}")
        return 0
    
    ok = 0
    tmp_dir = tempfile.mkdtemp(prefix="etl_csv_compress_")
    
    try:
        for csv_path in csv_files:
            csv_filename = os.path.basename(csv_path)
            # El nombre del archivo es el nombre del sheet (sin .csv)
            sheet_name = csv_filename[:-4]  # Remover .csv
            
            try:
                # Obtener información del CSV sin comprimir
                row_count, col_count = get_csv_info(csv_path)
                
                # Comprimir CSV a CSV.gz temporalmente
                csv_gz_filename = f"{csv_filename}.gz"
                csv_gz_path = os.path.join(tmp_dir, csv_gz_filename)
                compress_csv_to_gz(csv_path, csv_gz_path)
                file_bytes = os.path.getsize(csv_gz_path)
                
                # Path en stage: carpeta con nombre del Excel, dentro archivos con nombre del sheet
                # Estructura: @STAGE/{estructura}/{sheet}.csv.gz
                # Usar STAGE_FQN_PUT (sin comillas) para el comando PUT
                # wrong -- stage_path = f"@{STAGE_FQN_PUT}/{folder_name}/{csv_gz_filename}"
                stage_path = f"@{STAGE_FQN_PUT}/{folder_name}/"

                
                print(f"  → Archivo: {csv_filename} → {csv_gz_filename} ({row_count} filas, {col_count} columnas)")
                
                # Usar la tabla destino especificada o la tabla por defecto
                table_to_use = format_table_name(target_table) if target_table else TARGET_TABLE
                
                put_to_stage(cur, csv_gz_path, stage_path)
                copy_from_stage_to_table(cur, stage_path, table_to_use)
                
                # Para original_file, usar el nombre de la carpeta (que es el nombre del Excel)
                log_ingest(cur, batch_id, folder_name, sheet_name, stage_path,
                           row_count, col_count, file_bytes, "OK", "")
                ok += 1
                
            except Exception as e:
                file_bytes = os.path.getsize(csv_gz_path) if os.path.exists(csv_gz_path) else 0
                log_ingest(cur, batch_id, folder_name, sheet_name, "",
                           0, 0, file_bytes, "ERROR", str(e))
                print(f"  ❌ Error procesando {csv_filename}: {e}")
    finally:
        # Limpiar archivos temporales comprimidos
        shutil.rmtree(tmp_dir, ignore_errors=True)
    
    return ok


def main():
    """
    Función principal.
    Uso:
        python csv_to_snowflake.py [database] [schema] [folders] [csvs] [table]
    
    Args:
        database: Base de datos de Snowflake (opcional)
        schema: Schema de Snowflake (opcional, default: RAW)
        folders: Carpetas a procesar, separadas por comas (opcional, todas por defecto)
        csvs: CSV a procesar, separados por comas, sin extensión (opcional, todos por defecto)
        table: Tabla destino (opcional, default: INGEST_GENERIC_RAW)
              Puede ser "TABLE_NAME" o "DB.SCHEMA.TABLE_NAME"
    
    Ejemplos:
        python csv_to_snowflake.py
        python csv_to_snowflake.py POM_TEST01
        python csv_to_snowflake.py POM_TEST01 RAW
        python csv_to_snowflake.py POM_TEST01 RAW CIERRE_PROPIAS___7084110
        python csv_to_snowflake.py POM_TEST01 RAW CIERRE_PROPIAS___7084110 Estados_Cuenta,Desgloce_Cierre
        python csv_to_snowflake.py POM_Aplicaciones RAW SQLSERVER_POM_Aplicaciones dbo_EstadoLegal_Caso "POM_Aplicaciones.RAW.MI_TABLA"
    """
    import sys
    
    ensure_dirs()
    
    # Parsear argumentos
    database = None
    schema = None
    folders_filter = None
    csv_filter = None
    target_table = None
    
    if len(sys.argv) > 1:
        database = sys.argv[1]
    if len(sys.argv) > 2:
        schema = sys.argv[2]
    if len(sys.argv) > 3:
        folders_filter = [f.strip() for f in sys.argv[3].split(",") if f.strip()]
    if len(sys.argv) > 4:
        csv_filter = [c.strip() for c in sys.argv[4].split(",") if c.strip()]
    if len(sys.argv) > 5:
        target_table = sys.argv[5].strip()
    
    # Si no hay argumentos pero hay variables de entorno, usarlas
    if not database and os.getenv("SF_DATABASE"):
        database = os.getenv("SF_DATABASE")
    if not schema and os.getenv("SF_SCHEMA"):
        schema = os.getenv("SF_SCHEMA")
    
    # Conectar a Snowflake (actualiza configuración si se especificó database/schema)
    conn = connect_sf(database, schema)
    
    print(f"📊 Base de datos Snowflake: {SF_DB}")
    print(f"📋 Schema: {SF_SCHEMA}")
    if folders_filter:
        print(f"📁 Carpetas a procesar: {', '.join(folders_filter)}")
    if csv_filter:
        print(f"📄 CSV a procesar: {', '.join(csv_filter)}")
    if target_table:
        print(f"🎯 Tabla destino: {target_table}")
    else:
        print(f"🎯 Tabla destino: {TARGET_TABLE} (por defecto)")
    print()
    
    try:
        cur = conn.cursor()
        try:
            folders = list_csv_folders(folders_filter)
            
            if not folders:
                print("No hay carpetas con CSV en staging que coincidan con el filtro.")
                return
            
            print(f"📤 Carpetas encontradas: {len(folders)}")
            print()
            
            for folder_path in folders:
                folder_name = os.path.basename(folder_path)
                batch_id = str(uuid.uuid4())
                print(f"Procesando carpeta: {folder_name} (batch={batch_id})")
                
                try:
                    ok_files = ingest_csv_folder(cur, folder_path, batch_id, csv_filter, target_table)
                    conn.commit()
                    
                    if ok_files > 0:
                        move_folder(folder_path, CSV_PROCESSED_DIR)
                        print(f"OK → processed ({ok_files} archivos): {folder_name}")
                    else:
                        move_folder(folder_path, CSV_ERROR_DIR)
                        print(f"ERROR → error (0 archivos OK): {folder_name}")
                        
                except Exception as e:
                    conn.rollback()
                    move_folder(folder_path, CSV_ERROR_DIR)
                    print(f"ERROR → error: {folder_name} | {e}")
                print()
                    
        finally:
            cur.close()
    finally:
        conn.close()


if __name__ == "__main__":
    main()
