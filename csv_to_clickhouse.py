import os
import re
import csv
import gzip
import shutil
import tempfile
import uuid
import time
from datetime import datetime
from pathlib import Path

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
CH_PASSWORD = os.getenv("CH_PASSWORD", "SN6q6ihjXVj6.")
CH_DATABASE = os.getenv("CH_DATABASE", "default")
CH_TABLE = os.getenv("CH_TABLE", "")  # Tabla destino (opcional, se puede especificar por archivo)

# ============== Carpetas ==============
CSV_STAGING_DIR = os.getenv("CSV_STAGING_DIR", r"UPLOADS\POM_DROP\csv_staging")
CSV_PROCESSED_DIR = os.getenv("CSV_PROCESSED_DIR", r"UPLOADS\POM_DROP\csv_processed")
CSV_ERROR_DIR = os.getenv("CSV_ERROR_DIR", r"UPLOADS\POM_DROP\csv_error")

# Opcional: filtrar carpetas específicas (coma-separado). Si está vacío, procesa todas.
FOLDERS_FILTER = [s.strip() for s in os.getenv("FOLDERS_FILTER", "").split(",") if s.strip()]

# Opcional: filtrar CSV específicos dentro de las carpetas (coma-separado, sin extensión .csv)
CSV_FILTER = [s.strip() for s in os.getenv("CSV_FILTER", "").split(",") if s.strip()]


def ensure_dirs():
    for d in (CSV_STAGING_DIR, CSV_PROCESSED_DIR, CSV_ERROR_DIR):
        os.makedirs(d, exist_ok=True)


def sanitize_token(s: str, maxlen: int = 120) -> str:
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
    Crea una conexión a ClickHouse Cloud.
    Si la base de datos no existe, intenta crearla.
    """
    global CH_DATABASE
    
    if database:
        CH_DATABASE = database
    
    if not CH_PASSWORD:
        raise RuntimeError("Falta CH_PASSWORD (definí la variable de entorno).")
    
    # Primero intentar conectarse sin especificar la base de datos (o con "default")
    # para poder verificar/crear la base de datos si es necesario
    try:
        # Conectar primero a una base de datos que siempre existe (default o system)
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
            # La base de datos no existe, intentar crearla
            print(f"📦 La base de datos '{CH_DATABASE}' no existe. Intentando crearla...")
            try:
                create_sql = f"CREATE DATABASE IF NOT EXISTS `{CH_DATABASE}`"
                temp_client.command(create_sql)
                print(f"✅ Base de datos '{CH_DATABASE}' creada exitosamente")
            except Exception as create_err:
                # Si falla la creación, listar bases de datos disponibles
                available_dbs = list_available_databases(temp_client)
                temp_client.close()
                
                if available_dbs:
                    db_list = "\n   - ".join(available_dbs[:15])  # Mostrar hasta 15
                    if len(available_dbs) > 15:
                        db_list += f"\n   ... y {len(available_dbs) - 15} más"
                    raise RuntimeError(
                        f"❌ No se pudo crear la base de datos '{CH_DATABASE}'.\n"
                        f"Error: {create_err}\n\n"
                        f"💡 Bases de datos disponibles ({len(available_dbs)}):\n   - {db_list}\n\n"
                        f"💡 Sugerencias:\n"
                        f"   - Usa una de las bases de datos listadas arriba\n"
                        f"   - Ejemplo: python csv_to_clickhouse.py default ...\n"
                        f"   - O crea la base de datos '{CH_DATABASE}' en ClickHouse primero"
                    )
                else:
                    raise RuntimeError(
                        f"❌ No se pudo crear la base de datos '{CH_DATABASE}'.\n"
                        f"Error: {create_err}\n"
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
        print(f"✅ Conectado a ClickHouse Cloud: {CH_HOST}:{CH_PORT}")
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
                        f"   - Ejemplo: python csv_to_clickhouse.py default ...\n"
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


def list_csv_folders(folders_filter: list = None):
    """
    Lista las carpetas en CSV_STAGING_DIR.
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
    Lista los archivos CSV (sin comprimir y comprimidos) en una carpeta.
    
    Args:
        folder_path: Ruta de la carpeta
        csv_filter: Lista de nombres de CSV a filtrar (sin extensión .csv). Si None, usa CSV_FILTER
    """
    if csv_filter is None:
        csv_filter = CSV_FILTER
    
    files = []
    for name in os.listdir(folder_path):
        # Aceptar tanto .csv como .csv.gz
        if name.lower().endswith(".csv") or name.lower().endswith(".csv.gz"):
            # Filtrar si hay filtro especificado
            if csv_filter:
                csv_name = name.replace('.csv.gz', '').replace('.csv', '')
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


def get_csv_info(csv_path: str, delimiter: str = None) -> tuple[int, int]:
    """
    Obtiene el número de filas y columnas de un CSV.
    Retorna (row_count, col_count)
    """
    try:
        # Detectar delimitador si no se proporciona
        if delimiter is None:
            delimiter = detect_delimiter(csv_path)
        
        # Detectar si está comprimido
        is_gzipped = csv_path.lower().endswith('.gz')
        
        if is_gzipped:
            f = gzip.open(csv_path, 'rt', encoding='utf-8', newline='')
        else:
            f = open(csv_path, 'rt', encoding='utf-8', newline='')
        
        with f:
            reader = csv.reader(f, delimiter=delimiter)
            # Leer header
            try:
                header = next(reader)
                col_count = len(header)
            except StopIteration:
                return 0, 0
            
            # Contar filas restantes
            row_count = sum(1 for _ in reader)
        
        return row_count, col_count
    except Exception as e:
        print(f"  ⚠️  Error leyendo info del CSV: {e}")
        return 0, 0


def detect_delimiter(csv_path: str) -> str:
    """
    Detecta el delimitador del CSV usando csv.Sniffer.
    Prueba con: coma, punto y coma, tab, pipe.
    """
    is_gzipped = csv_path.lower().endswith('.gz')
    
    try:
        if is_gzipped:
            f = gzip.open(csv_path, 'rt', encoding='utf-8', newline='')
        else:
            f = open(csv_path, 'rt', encoding='utf-8', newline='')
        
        with f:
            # Leer una muestra del archivo (primeras líneas)
            sample = f.read(10240)  # Leer hasta 10KB
            if not sample:
                return ','  # Default
            
            # Usar csv.Sniffer para detectar el delimitador
            sniffer = csv.Sniffer()
            delimiters = [',', ';', '\t', '|']
            try:
                detected = sniffer.sniff(sample, delimiters=delimiters)
                return detected.delimiter
            except:
                # Si Sniffer falla, probar manualmente contando columnas en la primera línea
                first_line = sample.split('\n')[0] if '\n' in sample else sample
                max_cols = 0
                best_delimiter = ','
                
                for delim in delimiters:
                    cols = first_line.split(delim)
                    if len(cols) > max_cols:
                        max_cols = len(cols)
                        best_delimiter = delim
                
                return best_delimiter
    except Exception:
        return ','  # Default a coma si hay algún error


def make_unique_headers(headers: list) -> tuple:
    """
    Renombra columnas duplicadas agregando un sufijo numérico.
    Ejemplo: ['col1', 'col2', 'col1', 'col3'] -> ['col1', 'col2', 'col1_2', 'col3']
    
    Retorna: (headers_únicos, lista_de_renombres)
    """
    seen = {}
    unique_headers = []
    renames = []  # Lista de (original, renombrado)
    
    for header in headers:
        if header in seen:
            seen[header] += 1
            unique_header = f"{header}_{seen[header]}"
            unique_headers.append(unique_header)
            renames.append((header, unique_header))
        else:
            seen[header] = 0  # Primera ocurrencia no tiene sufijo
            unique_headers.append(header)
    
    return unique_headers, renames


def get_csv_headers(csv_path: str) -> tuple:
    """
    Obtiene los headers (nombres de columnas) de un CSV.
    Detecta automáticamente el delimitador.
    Maneja columnas duplicadas renombrándolas.
    
    Retorna: (headers_únicos, delimitador, lista_de_renombres)
    """
    try:
        # Detectar delimitador
        delimiter = detect_delimiter(csv_path)
        
        is_gzipped = csv_path.lower().endswith('.gz')
        
        if is_gzipped:
            f = gzip.open(csv_path, 'rt', encoding='utf-8', newline='')
        else:
            f = open(csv_path, 'rt', encoding='utf-8', newline='')
        
        with f:
            reader = csv.reader(f, delimiter=delimiter)
            try:
                header_row = next(reader)
                # Sanitizar nombres de columnas
                headers = [sanitize_token(h.strip().strip('"')) if h else f"col{i+1}" 
                          for i, h in enumerate(header_row)]
                # Hacer únicos los headers (renombrar duplicados)
                unique_headers, renames = make_unique_headers(headers)
                return unique_headers, delimiter, renames
            except StopIteration:
                return [], delimiter, []
    except Exception as e:
        print(f"  ⚠️  Error leyendo headers: {e}")
        return [], ',', []


def ensure_table(client, table_name: str, headers: list):
    """
    Crea la tabla en ClickHouse si no existe.
    Si existe pero tiene estructura diferente, la elimina y la recrea.
    Usa estructura flexible con columnas String para máxima compatibilidad.
    """
    # Sanitizar nombre de tabla pero preservar guiones (ClickHouse los acepta con comillas invertidas)
    table_name_sanitized = sanitize_token(table_name)
    # Usar comillas invertidas para el nombre de la tabla (ClickHouse requiere esto para caracteres especiales)
    full_table_name = f"`{CH_DATABASE}`.`{table_name_sanitized}`"
    
    # Preparar nombres de columnas esperadas
    expected_column_names = [sanitize_token(h) if h else f"col{i+1}" for i, h in enumerate(headers)]
    
    # Verificar si la tabla existe
    table_exists = False
    structure_matches = False
    
    try:
        check_sql = f"EXISTS TABLE {full_table_name}"
        result = client.query(check_sql)
        if result.result_rows[0][0] == 1:
            table_exists = True
            
            # Verificar estructura de la tabla existente
            try:
                desc_sql = f"DESCRIBE TABLE {full_table_name}"
                desc_result = client.query(desc_sql)
                existing_columns = [row[0] for row in desc_result.result_rows]
                
                # Comparar columnas (ignorar orden, solo nombres)
                if set(existing_columns) == set(expected_column_names):
                    structure_matches = True
                    print(f"  ✅ Tabla '{table_name_sanitized}' ya existe con estructura correcta")
                    return full_table_name
                else:
                    print(f"  ⚠️  Tabla '{table_name_sanitized}' existe pero con estructura diferente")
                    print(f"      Columnas existentes: {len(existing_columns)}")
                    print(f"      Columnas esperadas: {len(expected_column_names)}")
                    print(f"      Eliminando tabla para recrearla...")
                    
                    # Eliminar tabla existente
                    drop_sql = f"DROP TABLE IF EXISTS {full_table_name}"
                    client.command(drop_sql)
                    print(f"      ✅ Tabla eliminada")
            except Exception as e:
                print(f"  ⚠️  No se pudo verificar estructura de la tabla: {e}")
                # Si no podemos verificar, eliminar y recrear por seguridad
                drop_sql = f"DROP TABLE IF EXISTS {full_table_name}"
                try:
                    client.command(drop_sql)
                except:
                    pass
    except:
        pass
    
    # Crear columnas SQL (todas como String para máxima compatibilidad)
    columns = []
    for i, header in enumerate(headers, 1):
        col_name = sanitize_token(header) if header else f"col{i}"
        columns.append(f"`{col_name}` String")
    
    # Crear la tabla - usar comillas invertidas para el nombre completo
    create_sql = f"""
    CREATE TABLE IF NOT EXISTS {full_table_name} (
        {', '.join(columns)}
    ) ENGINE = MergeTree()
    ORDER BY tuple();
    """
    
    try:
        client.command(create_sql)
        if table_exists:
            print(f"  📦 Tabla '{table_name_sanitized}' recreada ({len(headers)} columnas)")
        else:
            print(f"  📦 Tabla '{table_name_sanitized}' creada ({len(headers)} columnas)")
        return full_table_name
    except Exception as e:
        print(f"  ❌ Error creando tabla '{table_name_sanitized}': {e}")
        raise


def upload_csv_to_clickhouse(client, csv_path: str, table_name: str, headers: list = None, delimiter: str = None):
    """
    Sube un archivo CSV a ClickHouse.
    """
    csv_filename = os.path.basename(csv_path)
    is_gzipped = csv_path.lower().endswith('.gz')
    
    # Obtener headers y delimitador si no se proporcionaron
    if not headers:
        headers, delimiter, renames = get_csv_headers(csv_path)
        if not headers:
            raise ValueError("No se pudieron leer los headers del CSV")
        if renames:
            print(f"    ⚠️  Columnas duplicadas renombradas: {', '.join([f'{old}→{new}' for old, new in renames])}")
    elif delimiter is None:
        delimiter = detect_delimiter(csv_path)
    
    # Asegurar que la tabla existe
    full_table_name = ensure_table(client, table_name, headers)
    
    # Preparar columnas para INSERT
    column_names = [sanitize_token(h) if h else f"col{i+1}" for i, h in enumerate(headers)]
    columns_str = ', '.join([f"`{col}`" for col in column_names])
    
    # Leer y cargar datos
    print(f"  📥 Cargando datos desde: {csv_filename} (delimitador: {repr(delimiter)})")
    
    try:
        # ClickHouse puede leer CSV directamente desde archivo local usando INSERT con formato
        # Pero para archivos remotos, necesitamos leer el archivo y hacer INSERT
        
        if is_gzipped:
            f = gzip.open(csv_path, 'rt', encoding='utf-8', newline='')
        else:
            f = open(csv_path, 'rt', encoding='utf-8', newline='')
        
        with f:
            reader = csv.reader(f, delimiter=delimiter)
            
            # Saltar header (primera fila)
            try:
                next(reader)
            except StopIteration:
                print(f"    ⚠️  El archivo está vacío o no tiene header")
                return 0
            
            # Leer datos en lotes para mejor rendimiento
            batch_size = 10000
            batch = []
            total_rows = 0
            
            for row in reader:
                if not row:  # Saltar filas vacías
                    continue
                
                # Limpiar valores (remover espacios y comillas)
                values = [str(v).strip().strip('"') if v else '' for v in row]
                
                # Asegurar que tenemos el mismo número de valores que columnas
                while len(values) < len(column_names):
                    values.append('')
                values = values[:len(column_names)]
                
                batch.append(values)
                
                # Insertar en lotes
                if len(batch) >= batch_size:
                    try:
                        client.insert(full_table_name, batch, column_names=column_names)
                        total_rows += len(batch)
                        batch = []
                    except Exception as e:
                        print(f"    ⚠️  Error en batch: {e}")
                        batch = []
            
            # Insertar el último batch
            if batch:
                client.insert(full_table_name, batch, column_names=column_names)
                total_rows += len(batch)
        
        # Verificar que los datos se insertaron correctamente
        try:
            count_result = client.query(f"SELECT COUNT(*) FROM {full_table_name}")
            actual_count = count_result.result_rows[0][0] if count_result.result_rows else 0
            print(f"  ✅ {total_rows} filas cargadas en '{table_name}' (verificado: {actual_count} filas en tabla)")
        except Exception as e:
            print(f"  ✅ {total_rows} filas cargadas en '{table_name}' (no se pudo verificar: {e})")
        
        return total_rows
        
    except Exception as e:
        print(f"  ❌ Error cargando datos: {e}")
        raise


def ingest_csv_folder(client, folder_path: str, csv_filter: list = None, target_table: str = None) -> int:
    """
    Procesa todos los CSV en una carpeta y los carga a ClickHouse.
    Retorna el número de archivos procesados exitosamente.
    
    Args:
        client: Cliente de ClickHouse
        folder_path: Ruta de la carpeta
        csv_filter: Lista de nombres de CSV a filtrar (sin extensión .csv)
        target_table: Tabla destino. Si es None, usa el nombre del archivo como nombre de tabla
    """
    folder_name = os.path.basename(folder_path)
    csv_files = list_csvs_in_folder(folder_path, csv_filter)
    
    if not csv_files:
        print(f"  ⚠️  No hay archivos CSV en {folder_name}")
        return 0
    
    ok = 0
    
    for csv_path in csv_files:
        csv_filename = os.path.basename(csv_path)
        # El nombre del archivo es el nombre de la tabla (sin extensión)
        sheet_name = csv_filename.replace('.csv.gz', '').replace('.csv', '')
        table_name = target_table if target_table else sheet_name
        
        try:
            print(f"  → Procesando: {csv_filename}")
            
            # Obtener headers y delimitador primero
            headers, delimiter, renames = get_csv_headers(csv_path)
            if not headers:
                print(f"    ❌ No se pudieron leer los headers")
                continue
            
            # Mostrar información sobre columnas renombradas si hay
            if renames:
                print(f"    ⚠️  Columnas duplicadas renombradas: {', '.join([f'{old}→{new}' for old, new in renames])}")
            
            # Obtener información del CSV usando el delimitador detectado
            row_count, col_count = get_csv_info(csv_path, delimiter)
            print(f"    📊 {row_count} filas, {col_count} columnas")
            print(f"    🔍 Delimitador detectado: {repr(delimiter)}")
            
            print(f"    📋 Columnas detectadas: {len(headers)}")
            if len(headers) <= 10:
                print(f"    📋 Nombres: {', '.join(headers)}")
            else:
                print(f"    📋 Primeras 10: {', '.join(headers[:10])}...")
            
            # Subir a ClickHouse
            upload_csv_to_clickhouse(client, csv_path, table_name, headers, delimiter)
            ok += 1
            
        except Exception as e:
            print(f"    ❌ Error procesando {csv_filename}: {e}")
    
    return ok


def main():
    """
    Función principal.
    Uso:
        python csv_to_clickhouse.py [database] [folders] [csvs] [table]
    
    Args:
        database: Base de datos de ClickHouse (opcional)
        folders: Carpetas a procesar, separadas por comas (opcional, todas por defecto)
        csvs: CSV a procesar, separados por comas, sin extensión (opcional, todos por defecto)
        table: Tabla destino (opcional, por defecto usa el nombre del archivo CSV)
    
    Ejemplos:
        python csv_to_clickhouse.py
        python csv_to_clickhouse.py default
        python csv_to_clickhouse.py default SQLSERVER_POM_Aplicaciones
        python csv_to_clickhouse.py default SQLSERVER_POM_Aplicaciones ResutadoNotificar,Bitacora
        python csv_to_clickhouse.py default SQLSERVER_POM_Aplicaciones ResutadoNotificar mi_tabla
    """
    import sys
    
    start_time = time.time()
    ensure_dirs()
    
    # Parsear argumentos
    database = None
    folders_filter = None
    csv_filter = None
    target_table = None
    
    if len(sys.argv) > 1:
        database = sys.argv[1]
    if len(sys.argv) > 2:
        folders_filter = [f.strip() for f in sys.argv[2].split(",") if f.strip()]
    if len(sys.argv) > 3:
        csv_filter = [c.strip() for c in sys.argv[3].split(",") if c.strip()]
    if len(sys.argv) > 4:
        target_table = sys.argv[4].strip()
    
    # Si no hay argumentos pero hay variables de entorno, usarlas
    if not database and os.getenv("CH_DATABASE"):
        database = os.getenv("CH_DATABASE")
    if not target_table and os.getenv("CH_TABLE"):
        target_table = os.getenv("CH_TABLE")
    
    # Conectar a ClickHouse
    client = connect_ch(database)
    
    print(f"📊 Base de datos ClickHouse: {CH_DATABASE}")
    if folders_filter:
        print(f"📁 Carpetas a procesar: {', '.join(folders_filter)}")
    if csv_filter:
        print(f"📄 CSV a procesar: {', '.join(csv_filter)}")
    if target_table:
        print(f"🎯 Tabla destino: {target_table}")
    print()
    
    try:
        folders = list_csv_folders(folders_filter)
        
        if not folders:
            elapsed_time = time.time() - start_time
            print("No hay carpetas con CSV en staging que coincidan con el filtro.")
            print()
            print("=" * 60)
            print(f"RESUMEN DE EJECUCIÓN")
            print("=" * 60)
            print(f"Carpetas procesadas: 0")
            print(f"Tiempo de ejecución: {elapsed_time:.2f} segundos")
            print("=" * 60)
            return
        
        print(f"📤 Carpetas encontradas: {len(folders)}")
        print()
        
        total_folders = 0
        total_files = 0
        folders_ok = 0
        folders_error = 0
        
        for folder_path in folders:
            folder_name = os.path.basename(folder_path)
            print(f"Procesando carpeta: {folder_name}")
            total_folders += 1
            
            try:
                ok_files = ingest_csv_folder(client, folder_path, csv_filter, target_table)
                
                if ok_files > 0:
                    move_folder(folder_path, CSV_PROCESSED_DIR)
                    print(f"OK → processed ({ok_files} archivos): {folder_name}")
                    total_files += ok_files
                    folders_ok += 1
                else:
                    move_folder(folder_path, CSV_ERROR_DIR)
                    print(f"ERROR → error (0 archivos OK): {folder_name}")
                    folders_error += 1
                    
            except Exception as e:
                move_folder(folder_path, CSV_ERROR_DIR)
                print(f"ERROR → error: {folder_name} | {e}")
                folders_error += 1
            print()
        
        elapsed_time = time.time() - start_time
        print("=" * 60)
        print(f"RESUMEN DE EJECUCIÓN")
        print("=" * 60)
        print(f"Carpetas procesadas: {total_folders}")
        print(f"Archivos cargados: {total_files}")
        print(f"Carpetas exitosas: {folders_ok}")
        print(f"Carpetas con error: {folders_error}")
        print(f"Tiempo de ejecución: {elapsed_time:.2f} segundos")
        print("=" * 60)
                
    finally:
        client.close()


if __name__ == "__main__":
    main()
