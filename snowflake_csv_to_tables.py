import os
import re
import uuid
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

# ============== Stage y log table (se inicializan con update_snowflake_config) ==============
SF_DB_ACTUAL = None  # Se actualiza en connect_sf cuando se determina el nombre real

def _init_snowflake_config():
    """Inicializa las variables de configuración de Snowflake."""
    global STAGE_FQN, STAGE_FQN_PUT, LOG_TABLE, TARGET_TABLE
    STAGE_FQN = f"{SF_DB}.{SF_SCHEMA}.RAW_STAGE"
    STAGE_FQN_PUT = f"{SF_DB}.{SF_SCHEMA}.RAW_STAGE"  # Sin comillas para PUT
    LOG_TABLE = f"{SF_DB}.{SF_SCHEMA}.INGEST_LOG"
    TARGET_TABLE = f"{SF_DB}.{SF_SCHEMA}.INGEST_GENERIC_RAW"

_init_snowflake_config()


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
    global SF_DB, SF_SCHEMA, STAGE_FQN, STAGE_FQN_PUT, LOG_TABLE, TARGET_TABLE, SF_DB_ACTUAL
    
    if database:
        SF_DB = database
    if schema:
        SF_SCHEMA = schema
    if db_actual:
        SF_DB_ACTUAL = db_actual
    
    # Para identificadores con case mixto, usar comillas dobles
    db_for_put = SF_DB_ACTUAL if SF_DB_ACTUAL else SF_DB
    
    if SF_DB != SF_DB.upper():
        # Case mixto: usar comillas para todos los componentes (excepto en PUT)
        STAGE_FQN = f'"{SF_DB}"."{SF_SCHEMA}"."RAW_STAGE"'
        STAGE_FQN_PUT = "RAW_STAGE"
        LOG_TABLE = f'"{SF_DB}"."{SF_SCHEMA}"."INGEST_LOG"'
        TARGET_TABLE = f'"{SF_DB}"."{SF_SCHEMA}"."INGEST_GENERIC_RAW"'
    else:
        # Todo en mayúsculas: no usar comillas
        STAGE_FQN = f"{SF_DB}.{SF_SCHEMA}.RAW_STAGE"
        STAGE_FQN_PUT = "RAW_STAGE"
        LOG_TABLE = f"{SF_DB}.{SF_SCHEMA}.INGEST_LOG"
        TARGET_TABLE = f"{SF_DB}.{SF_SCHEMA}.INGEST_GENERIC_RAW"


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
                print(f"✅ Base de datos '{SF_DB}' encontrada")
            except Exception as e1:
                try:
                    db_upper = SF_DB.upper()
                    cur.execute(f"USE DATABASE {db_upper};")
                    db_name_used = db_upper
                    db_exists = True
                    update_snowflake_config(db_upper, None)
                    print(f"✅ Base de datos '{db_upper}' encontrada")
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
                print(f"✅ Schema '{SF_SCHEMA}' encontrado")
            
            cur.execute(f"USE SCHEMA {SF_SCHEMA};")
            
            update_snowflake_config(db_name_used, SF_SCHEMA, db_name_used)
            
            print(f"✅ Conectado a Snowflake: {SF_DB}.{SF_SCHEMA}")
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
                f"❌ Error de conexión: La base de datos '{SF_DB}' no existe o no tienes permisos.\n"
                f"💡 Verifica el nombre exacto de la base de datos en Snowflake y tus permisos."
            )
        raise


def sf_exec(cur, sql: str):
    """Ejecuta SQL en Snowflake y maneja errores."""
    try:
        cur.execute(sql)
        return cur.fetchall() if cur.description else None
    except Exception as e:
        print(f"  ❌ Error SQL: {e}")
        print(f"  SQL: {sql[:200]}...")
        raise


def list_files_in_stage(cur):
    """
    Lista todos los archivos CSV en el stage RAW_STAGE.
    Retorna lista de tuplas (file_path, file_name, folder_name).
    
    El comando LIST devuelve: name (ruta completa), size, md5, last_modified
    """
    try:
        # Listar archivos en el stage
        list_sql = f"LIST @{STAGE_FQN};"
        result = sf_exec(cur, list_sql)
        
        files = []
        for row in result:
            file_path = str(row[0]) if row[0] else ""  # Ruta completa en el stage (name)
            file_size = row[1] if len(row) > 1 else 0  # Tamaño (size)
            
            if not file_path:
                continue
            
            # Extraer nombre del archivo desde la ruta completa
            # Formato: @STAGE/folder/file.csv.gz o folder/file.csv.gz
            path_parts = file_path.split('/')
            file_name = path_parts[-1] if path_parts else ""
            
            # Solo procesar archivos .csv.gz
            if not file_name or not str(file_name).lower().endswith('.csv.gz'):
                continue
            
            # Extraer folder_name del path
            # Si hay al menos 2 partes, la penúltima es el folder
            if len(path_parts) >= 2:
                folder_name = path_parts[-2]  # El folder está antes del archivo
            else:
                folder_name = "UNKNOWN"
            
            files.append((file_path, file_name, folder_name))
        
        return files
    except Exception as e:
        print(f"⚠️  Error al listar archivos en stage: {e}")
        import traceback
        traceback.print_exc()
        return []


def get_csv_headers_from_stage(cur, stage_path: str, file_name: str):
    """
    Obtiene los headers (nombres de columnas) de un CSV en el stage.
    Lee solo la primera línea del archivo usando una tabla temporal.
    """
    temp_table = None
    try:
        # Asegurar que file_name sea string
        file_name = str(file_name)
        stage_path = str(stage_path)
        
        # Crear tabla temporal para leer el header
        temp_table = f"TEMP_HEADER_{uuid.uuid4().hex[:8].upper()}"
        
        # Crear tabla temporal con columnas genéricas para leer el CSV
        # Usamos col1-col50 para máxima flexibilidad
        temp_cols = ', '.join([f"col{i} VARCHAR(16777216)" for i in range(1, 51)])
        create_temp_sql = f"""
        CREATE TEMPORARY TABLE {temp_table} (
            {temp_cols}
        );
        """
        sf_exec(cur, create_temp_sql)
        
        # Extraer el path relativo del stage_path
        # stage_path viene de LIST y puede ser: raw_stage/folder/file.csv.gz o folder/file.csv.gz
        relative_path = str(stage_path)
        
        # Remover cualquier prefijo @ y nombre del stage
        if relative_path.startswith("@"):
            # Remover el @ y dividir por /
            parts = relative_path.lstrip('@').split("/")
            # Buscar el nombre del stage (RAW_STAGE) y tomar todo después
            stage_name = "RAW_STAGE"
            stage_found = False
            for i, part in enumerate(parts):
                if stage_name.upper() in part.upper() or part.upper() == stage_name.upper():
                    # Encontramos el stage, tomar todo después
                    if i < len(parts) - 1:
                        relative_path = "/".join(parts[i + 1:])
                        stage_found = True
                        break
            if not stage_found:
                # Si no encontramos el stage name, tomar todo después del primer elemento
                if len(parts) > 1:
                    relative_path = "/".join(parts[1:])
                else:
                    relative_path = ""
        
        # Remover el prefijo "raw_stage/" si existe (el path de LIST ya lo incluye)
        relative_path = relative_path.lstrip('/')
        if relative_path.lower().startswith("raw_stage/"):
            relative_path = relative_path[len("raw_stage/"):]
        relative_path = relative_path.lstrip('/')
        
        if not relative_path:
            raise ValueError(f"No se pudo extraer el path relativo de: {stage_path}")
        
        # Usar STAGE_FQN_PUT (solo el nombre del stage) ya que estamos en el contexto correcto
        # después de USE DATABASE y USE SCHEMA
        # Usar PATTERN para buscar el archivo específico (más confiable para archivos comprimidos)
        # Agregar error_on_column_count_mismatch=false para permitir archivos con menos columnas
        copy_sql = f"""
        COPY INTO {temp_table}
        FROM @{STAGE_FQN_PUT}
        PATTERN = '.*{re.escape(file_name)}'
        FILE_FORMAT = (TYPE = CSV, FIELD_DELIMITER = ',', SKIP_HEADER = 0, FIELD_OPTIONALLY_ENCLOSED_BY = '"', ERROR_ON_COLUMN_COUNT_MISMATCH = FALSE)
        ON_ERROR = 'CONTINUE'
        FORCE = TRUE;
        """
        try:
            sf_exec(cur, copy_sql)
        except Exception as copy_err:
            print(f"  ⚠️  Error en COPY INTO: {copy_err}")
            # Intentar con el path completo como alternativa
            copy_sql_alt = f"""
            COPY INTO {temp_table}
            FROM @{STAGE_FQN_PUT}/{relative_path}
            FILE_FORMAT = (TYPE = CSV, FIELD_DELIMITER = ',', SKIP_HEADER = 0, FIELD_OPTIONALLY_ENCLOSED_BY = '"', ERROR_ON_COLUMN_COUNT_MISMATCH = FALSE)
            ON_ERROR = 'CONTINUE'
            FORCE = TRUE;
            """
            sf_exec(cur, copy_sql_alt)
        
        # Verificar si se copió algo
        count_sql = f"SELECT COUNT(*) FROM {temp_table};"
        count_result = sf_exec(cur, count_sql)
        row_count = count_result[0][0] if count_result else 0
        
        if row_count == 0:
            print(f"  ⚠️  No se pudo leer ninguna fila del archivo {file_name}")
            return None
        
        # Leer solo la primera fila (que es el header)
        select_sql = f"""
        SELECT col1, col2, col3, col4, col5, col6, col7, col8, col9, col10,
               col11, col12, col13, col14, col15, col16, col17, col18, col19, col20,
               col21, col22, col23, col24, col25, col26, col27, col28, col29, col30,
               col31, col32, col33, col34, col35, col36, col37, col38, col39, col40,
               col41, col42, col43, col44, col45, col46, col47, col48, col49, col50
        FROM {temp_table} LIMIT 1;
        """
        result = sf_exec(cur, select_sql)
        
        # Limpiar tabla temporal
        try:
            sf_exec(cur, f"DROP TABLE IF EXISTS {temp_table};")
        except:
            pass
        
        if result and len(result) > 0:
            # Obtener los valores de las columnas (que son los headers)
            headers = []
            row = result[0]
            for i, val in enumerate(row, 1):
                if val is None:
                    break  # Detener cuando encontremos None
                header = str(val or f"COL{i}").strip().strip('"')
                if header:  # Solo agregar si no está vacío
                    headers.append(header)
            
            # Si no encontramos headers, usar nombres genéricos
            if not headers:
                headers = [f"COL{i}" for i in range(1, 51)]
            
            # Sanitizar nombres de columnas
            headers = [sanitize_token(h) if h else f"COL{i+1}" for i, h in enumerate(headers)]
            return headers
        
        return None
    except Exception as e:
        print(f"  ⚠️  Error al leer headers de {file_name}: {e}")
        import traceback
        traceback.print_exc()
        # Limpiar tabla temporal en caso de error
        if temp_table:
            try:
                sf_exec(cur, f"DROP TABLE IF EXISTS {temp_table};")
            except:
                pass
        return None


def create_table_from_csv(cur, table_name: str, headers: list, stage_path: str, file_name: str):
    """
    Crea una tabla en Snowflake con la estructura del CSV y carga los datos.
    
    Args:
        cur: Cursor de Snowflake
        table_name: Nombre de la tabla a crear
        headers: Lista de nombres de columnas
        stage_path: Ruta del archivo en el stage
        file_name: Nombre del archivo CSV
    """
    # Asegurar que file_name y stage_path sean strings
    file_name = str(file_name)
    stage_path = str(stage_path)
    
    # Sanitizar nombre de tabla pero preservar el case original
    table_name_sanitized = sanitize_token(table_name)
    
    # Formatear nombre de tabla completo - SIEMPRE usar comillas para preservar case
    if SF_DB != SF_DB.upper():
        full_table_name = f'"{SF_DB}"."{SF_SCHEMA}"."{table_name_sanitized}"'
    else:
        # Aunque la DB esté en mayúsculas, usar comillas para el nombre de tabla para preservar case
        full_table_name = f'{SF_DB}.{SF_SCHEMA}."{table_name_sanitized}"'
    
    # Verificar si la tabla ya existe - buscar todas las tablas y comparar case-sensitive
    check_sql = f"SHOW TABLES IN SCHEMA {SF_SCHEMA};"
    try:
        all_tables = sf_exec(cur, check_sql)
        # Comparar case-sensitive: buscar el nombre exacto (con el case original)
        existing = any(len(row) > 1 and row[1] == table_name_sanitized for row in all_tables) if all_tables else False
    except:
        # Fallback: intentar con LIKE (menos preciso para case-sensitive)
        check_sql_like = f"SHOW TABLES LIKE '{table_name_sanitized}';"
        existing_tables = sf_exec(cur, check_sql_like)
        existing = any(len(row) > 1 and row[1] == table_name_sanitized for row in existing_tables) if existing_tables else False
    
    if existing:
        print(f"  ⚠️  La tabla '{table_name_sanitized}' ya existe. Omitiendo...")
        return "skipped"
    
    # Crear columnas SQL (todas como VARCHAR para máxima compatibilidad)
    columns = []
    for i, header in enumerate(headers, 1):
        col_name = sanitize_token(header) if header else f"COL{i}"
        columns.append(f'"{col_name}" VARCHAR(16777216)')
    
    # Crear la tabla
    create_sql = f"""
    CREATE TABLE {full_table_name} (
        {', '.join(columns)}
    );
    """
    
    print(f"  📦 Creando tabla: {table_name_sanitized} ({len(headers)} columnas)")
    sf_exec(cur, create_sql)
    
    # Cargar datos desde el stage
    # Extraer el path relativo del stage
    relative_path = str(stage_path)
    
    # Remover cualquier prefijo @ y nombre del stage
    if relative_path.startswith("@"):
        # Remover el @ y dividir por /
        parts = relative_path.lstrip('@').split("/")
        # Buscar el nombre del stage (RAW_STAGE) y tomar todo después
        stage_name = "RAW_STAGE"
        stage_found = False
        for i, part in enumerate(parts):
            if stage_name.upper() in part.upper() or part.upper() == stage_name.upper():
                # Encontramos el stage, tomar todo después
                if i < len(parts) - 1:
                    relative_path = "/".join(parts[i + 1:])
                    stage_found = True
                    break
        if not stage_found:
            # Si no encontramos el stage name, tomar todo después del primer elemento
            if len(parts) > 1:
                relative_path = "/".join(parts[1:])
            else:
                relative_path = ""
    
    # Remover el prefijo "raw_stage/" si existe (el path de LIST ya lo incluye)
    relative_path = relative_path.lstrip('/')
    if relative_path.lower().startswith("raw_stage/"):
        relative_path = relative_path[len("raw_stage/"):]
    relative_path = relative_path.lstrip('/')
    
    if not relative_path:
        raise ValueError(f"No se pudo extraer el path relativo de: {stage_path}")
    
    # Usar STAGE_FQN_PUT (solo el nombre del stage) ya que estamos en el contexto correcto
    # después de USE DATABASE y USE SCHEMA
    # Agregar error_on_column_count_mismatch=false para permitir archivos con menos columnas
    copy_sql = f"""
    COPY INTO {full_table_name}
    FROM @{STAGE_FQN_PUT}/{relative_path}
    FILE_FORMAT = (TYPE = CSV, FIELD_DELIMITER = ',', SKIP_HEADER = 1, FIELD_OPTIONALLY_ENCLOSED_BY = '"', ERROR_ON_COLUMN_COUNT_MISMATCH = FALSE)
    ON_ERROR = 'CONTINUE'
    FORCE = FALSE;
    """
    
    print(f"  📥 Cargando datos desde: {file_name}")
    sf_exec(cur, copy_sql)
    
    # Verificar cuántas filas se cargaron
    count_sql = f"SELECT COUNT(*) FROM {full_table_name};"
    count_result = sf_exec(cur, count_sql)
    row_count = count_result[0][0] if count_result else 0
    
    print(f"  ✅ Tabla '{table_name_sanitized}' creada con {row_count} filas")
    return True


def process_csv_files_to_tables(cur, file_filter: list = None, folder_filter: list = None):
    """
    Procesa todos los CSV en el stage y crea tablas para cada uno.
    
    Args:
        cur: Cursor de Snowflake
        file_filter: Lista de nombres de archivos a procesar (opcional)
        folder_filter: Lista de nombres de carpetas a procesar (opcional)
    """
    # Listar archivos en el stage
    files = list_files_in_stage(cur)
    
    if not files:
        print("⚠️  No se encontraron archivos CSV en el stage.")
        return
    
    print(f"📋 Archivos encontrados en stage: {len(files)}")
    
    # Aplicar filtros
    if folder_filter:
        files = [f for f in files if any(f[2].upper() == folder.upper() or folder.upper() in f[2].upper() 
                                        for folder in folder_filter)]
    
    if file_filter:
        # Normalizar filtros: remover extensiones comunes
        normalized_filters = []
        for file in file_filter:
            normalized = str(file).upper().replace('.CSV.GZ', '').replace('.CSV', '')
            normalized_filters.append(normalized)
        
        files = [f for f in files if any(
            str(f[1]).upper().replace('.CSV.GZ', '').replace('.CSV', '') == norm_filter or 
            norm_filter in str(f[1]).upper() 
            for norm_filter in normalized_filters
        )]
    
    if not files:
        print("⚠️  No hay archivos que coincidan con los filtros especificados.")
        return
    
    print(f"📤 Archivos a procesar: {len(files)}\n")
    
    processed = 0
    skipped = 0
    errors = 0
    
    for stage_path, file_name, folder_name in files:
        try:
            # Asegurar que file_name sea string
            file_name = str(file_name)
            folder_name = str(folder_name)
            stage_path = str(stage_path)
            
            # Nombre de tabla: solo el nombre del CSV (sin extensión)
            sheet_name = file_name.replace('.csv.gz', '').replace('.csv', '')
            table_name = sheet_name
            
            print(f"🔄 Procesando: {file_name} (folder: {folder_name})")
            
            # Obtener headers del CSV
            headers = get_csv_headers_from_stage(cur, stage_path, file_name)
            
            if not headers:
                print(f"  ❌ No se pudieron leer los headers de {file_name}")
                errors += 1
                continue
            
            # Crear tabla y cargar datos
            result = create_table_from_csv(cur, table_name, headers, stage_path, file_name)
            
            if result == True:
                processed += 1
            elif result == "skipped":
                skipped += 1
            else:
                errors += 1
            
            print()
            
        except Exception as e:
            print(f"  ❌ Error procesando {file_name}: {e}")
            errors += 1
            print()
    
    # Mensaje final con resumen
    summary_parts = []
    if processed > 0:
        summary_parts.append(f"{processed} tablas creadas")
    if skipped > 0:
        summary_parts.append(f"{skipped} omitidas (ya existían)")
    if errors > 0:
        summary_parts.append(f"{errors} errores")
    
    summary = ", ".join(summary_parts) if summary_parts else "sin cambios"
    print(f"✅ Proceso completado: {summary}")


def main():
    """
    Función principal.
    Uso:
        python snowflake_csv_to_tables.py [database] [schema] [folders] [files]
    
    Args:
        database: Base de datos de Snowflake (opcional)
        schema: Schema de Snowflake (opcional, default: RAW)
        folders: Carpetas a procesar, separadas por comas (opcional, todas por defecto)
        files: Archivos CSV a procesar, separados por comas, sin extensión (opcional, todos por defecto)
    
    Ejemplos:
        python snowflake_csv_to_tables.py
        python snowflake_csv_to_tables.py POM_TEST01
        python snowflake_csv_to_tables.py POM_TEST01 RAW
        python snowflake_csv_to_tables.py POM_TEST01 RAW CIERRE_PROPIAS___7084110
        python snowflake_csv_to_tables.py POM_TEST01 RAW CIERRE_PROPIAS___7084110 Estados_Cuenta,Desgloce_Cierre
    """
    import sys
    
    # Parsear argumentos
    database = None
    schema = None
    folder_filter = None
    file_filter = None
    
    if len(sys.argv) > 1:
        database = sys.argv[1]
    if len(sys.argv) > 2:
        schema = sys.argv[2]
    if len(sys.argv) > 3:
        folder_filter = [f.strip() for f in sys.argv[3].split(",") if f.strip()]
    if len(sys.argv) > 4:
        file_filter = [c.strip() for c in sys.argv[4].split(",") if c.strip()]
    
    # Si no hay argumentos pero hay variables de entorno, usarlas
    if not database and os.getenv("SF_DATABASE"):
        database = os.getenv("SF_DATABASE")
    if not schema and os.getenv("SF_SCHEMA"):
        schema = os.getenv("SF_SCHEMA")
    
    # Conectar a Snowflake
    conn = connect_sf(database, schema)
    
    print(f"📊 Base de datos Snowflake: {SF_DB}")
    print(f"📋 Schema: {SF_SCHEMA}")
    if folder_filter:
        print(f"📁 Carpetas a procesar: {', '.join(folder_filter)}")
    if file_filter:
        print(f"📄 Archivos a procesar: {', '.join(file_filter)}")
    print()
    
    try:
        cur = conn.cursor()
        try:
            process_csv_files_to_tables(cur, file_filter, folder_filter)
            conn.commit()
        finally:
            cur.close()
    finally:
        conn.close()


if __name__ == "__main__":
    main()
