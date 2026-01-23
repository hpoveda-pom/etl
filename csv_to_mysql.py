import os
import re
import csv
import gzip
import time
from datetime import datetime

try:
    import pymysql
except ImportError:
    print("[ERROR] Error: Falta la librería pymysql")
    print("[INFO] Instálala con: pip install pymysql")
    exit(1)

# ============== MySQL config ==============
MYSQL_HOST = os.getenv("MYSQL_HOST", "localhost")
MYSQL_USER = os.getenv("MYSQL_USER", "root")
MYSQL_PASSWORD = os.getenv("MYSQL_PASSWORD", "")
MYSQL_DATABASE = os.getenv("MYSQL_DATABASE", "default")
MYSQL_PORT = int(os.getenv("MYSQL_PORT", "3306"))

# ============== Carpetas ==============
CSV_STAGING_DIR = os.getenv("CSV_STAGING_DIR", r"UPLOADS\POM_DROP\csv_staging")

# Opcional: filtrar carpetas específicas (coma-separado). Si está vacío, procesa todas.
FOLDERS_FILTER = [s.strip() for s in os.getenv("FOLDERS_FILTER", "").split(",") if s.strip()]

# Opcional: filtrar CSV específicos dentro de las carpetas (coma-separado, sin extensión .csv)
CSV_FILTER = [s.strip() for s in os.getenv("CSV_FILTER", "").split(",") if s.strip()]


def ensure_dirs():
    os.makedirs(CSV_STAGING_DIR, exist_ok=True)


def sanitize_token(s: str, maxlen: int = 120) -> str:
    s = (s or "").strip()
    s = re.sub(r"[^\w\-\.]+", "_", s, flags=re.UNICODE)
    s = re.sub(r"_+", "_", s).strip("_")
    return s[:maxlen] if s else "NA"


def connect_mysql(database: str = None):
    """
    Crea una conexión a MySQL.
    Si la base de datos no existe, la crea.
    """
    global MYSQL_DATABASE
    
    if database:
        MYSQL_DATABASE = database
    
    try:
        # Primero conectar sin especificar base de datos para poder crearla si no existe
        conn = pymysql.connect(
            host=MYSQL_HOST,
            user=MYSQL_USER,
            password=MYSQL_PASSWORD,
            port=MYSQL_PORT,
            charset='utf8mb4',
            cursorclass=pymysql.cursors.DictCursor
        )
        
        cur = conn.cursor()
        
        # Verificar si la base de datos existe
        cur.execute("SHOW DATABASES LIKE %s", (MYSQL_DATABASE,))
        db_exists = cur.fetchone() is not None
        
        if not db_exists:
            print(f"📦 Creando base de datos: {MYSQL_DATABASE}")
            # Crear base de datos con utf8mb4
            cur.execute(f"CREATE DATABASE IF NOT EXISTS `{MYSQL_DATABASE}` CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci")
            conn.commit()
            print(f"[OK] Base de datos '{MYSQL_DATABASE}' creada")
        else:
            print(f"[OK] Base de datos '{MYSQL_DATABASE}' ya existe")
        
        # Usar la base de datos
        cur.execute(f"USE `{MYSQL_DATABASE}`")
        cur.close()
        conn.close()
        
        # Reconectar especificando la base de datos
        conn = pymysql.connect(
            host=MYSQL_HOST,
            user=MYSQL_USER,
            password=MYSQL_PASSWORD,
            database=MYSQL_DATABASE,
            port=MYSQL_PORT,
            charset='utf8mb4',
            cursorclass=pymysql.cursors.DictCursor
        )
        
        print(f"[OK] Conectado a MySQL: {MYSQL_HOST}:{MYSQL_PORT}")
        print(f" Base de datos: {MYSQL_DATABASE}")
        
        return conn
    except pymysql.Error as e:
        error_msg = str(e)
        if "access denied" in error_msg.lower() or "password" in error_msg.lower():
            raise RuntimeError(
                f"[ERROR] Error de autenticación. Verifica MYSQL_USER y MYSQL_PASSWORD.\n"
                f"Error: {error_msg}"
            )
        elif "connection" in error_msg.lower() or "timeout" in error_msg.lower():
            raise RuntimeError(
                f"[ERROR] Error de conexión. Verifica MYSQL_HOST y MYSQL_PORT.\n"
                f"Error: {error_msg}"
            )
        else:
            raise RuntimeError(f"[ERROR] Error conectando a MySQL: {error_msg}")


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
        print(f"  [WARN]  Error leyendo headers: {e}")
        return [], ',', []


def ensure_table(conn, table_name: str, headers: list):
    """
    Crea la tabla en MySQL si no existe.
    Si existe pero tiene estructura diferente, la elimina y la recrea.
    Usa estructura flexible con columnas TEXT para máxima compatibilidad.
    """
    # Sanitizar nombre de tabla
    table_name_sanitized = sanitize_token(table_name)
    full_table_name = f"`{table_name_sanitized}`"
    
    # Preparar nombres de columnas esperadas
    expected_column_names = [sanitize_token(h) if h else f"col{i+1}" for i, h in enumerate(headers)]
    
    cur = conn.cursor()
    
    try:
        # Verificar si la tabla existe
        cur.execute(f"SHOW TABLES LIKE %s", (table_name_sanitized,))
        table_exists = cur.fetchone() is not None
        
        if table_exists:
            # Verificar estructura de la tabla existente
            try:
                cur.execute(f"DESCRIBE {full_table_name}")
                existing_columns = [row['Field'] for row in cur.fetchall()]
                
                # Comparar columnas (ignorar orden, solo nombres)
                if set(existing_columns) == set(expected_column_names):
                    print(f"  [OK] Tabla '{table_name_sanitized}' ya existe con estructura correcta")
                    return full_table_name
                else:
                    print(f"  [WARN]  Tabla '{table_name_sanitized}' existe pero con estructura diferente")
                    print(f"      Columnas existentes: {len(existing_columns)}")
                    print(f"      Columnas esperadas: {len(expected_column_names)}")
                    print(f"      Eliminando tabla para recrearla...")
                    
                    # Eliminar tabla existente
                    cur.execute(f"DROP TABLE IF EXISTS {full_table_name}")
                    conn.commit()
                    print(f"      [OK] Tabla eliminada")
            except Exception as e:
                print(f"  [WARN]  No se pudo verificar estructura de la tabla: {e}")
                # Si no podemos verificar, eliminar y recrear por seguridad
                cur.execute(f"DROP TABLE IF EXISTS {full_table_name}")
                conn.commit()
        
        # Crear columnas SQL (todas como TEXT para máxima compatibilidad con utf8mb4)
        columns = []
        for i, header in enumerate(headers, 1):
            col_name = sanitize_token(header) if header else f"col{i}"
            columns.append(f"`{col_name}` TEXT CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci")
        
        # Crear la tabla con utf8mb4
        create_sql = f"""
        CREATE TABLE {full_table_name} (
            {', '.join(columns)}
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
        """
        
        cur.execute(create_sql)
        conn.commit()
        
        if table_exists:
            print(f"  📦 Tabla '{table_name_sanitized}' recreada ({len(headers)} columnas)")
        else:
            print(f"  📦 Tabla '{table_name_sanitized}' creada ({len(headers)} columnas)")
        
        return full_table_name
        
    except Exception as e:
        print(f"  [ERROR] Error creando tabla '{table_name_sanitized}': {e}")
        conn.rollback()
        raise
    finally:
        cur.close()


def upload_csv_to_mysql(conn, csv_path: str, table_name: str, headers: list = None, delimiter: str = None):
    """
    Sube un archivo CSV a MySQL.
    """
    csv_filename = os.path.basename(csv_path)
    is_gzipped = csv_path.lower().endswith('.gz')
    
    # Obtener headers y delimitador si no se proporcionaron
    if not headers:
        headers, delimiter, renames = get_csv_headers(csv_path)
        if not headers:
            raise ValueError("No se pudieron leer los headers del CSV")
        if renames:
            rename_info = ', '.join([f'{old}->{new}' for old, new in renames[:5]])
            if len(renames) > 5:
                rename_info += f' ... y {len(renames) - 5} más'
            print(f"    [WARN]  Columnas duplicadas renombradas: {rename_info}")
    elif delimiter is None:
        delimiter = detect_delimiter(csv_path)
    
    # Asegurar que la tabla existe
    full_table_name = ensure_table(conn, table_name, headers)
    
    # Preparar columnas para INSERT
    column_names = [sanitize_token(h) if h else f"col{i+1}" for i, h in enumerate(headers)]
    columns_str = ', '.join([f"`{col}`" for col in column_names])
    placeholders = ', '.join(['%s'] * len(column_names))
    
    # Leer y cargar datos
    print(f"   Cargando datos desde: {csv_filename} (delimitador: {repr(delimiter)})")
    
    cur = conn.cursor()
    
    try:
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
                print(f"    [WARN]  El archivo está vacío o no tiene header")
                return 0
            
            # Leer datos en lotes para mejor rendimiento
            batch_size = 10000
            batch = []
            total_rows = 0
            
            insert_sql = f"INSERT INTO {full_table_name} ({columns_str}) VALUES ({placeholders})"
            
            for row in reader:
                if not row:  # Saltar filas vacías
                    continue
                
                # Limpiar valores (remover espacios y comillas)
                values = [str(v).strip().strip('"') if v else None for v in row]
                
                # Asegurar que tenemos el mismo número de valores que columnas
                while len(values) < len(column_names):
                    values.append(None)
                values = values[:len(column_names)]
                
                batch.append(values)
                
                # Insertar en lotes
                if len(batch) >= batch_size:
                    try:
                        cur.executemany(insert_sql, batch)
                        conn.commit()
                        total_rows += len(batch)
                        batch = []
                    except Exception as e:
                        print(f"    [WARN]  Error en batch: {e}")
                        conn.rollback()
                        batch = []
            
            # Insertar el último batch
            if batch:
                try:
                    cur.executemany(insert_sql, batch)
                    conn.commit()
                    total_rows += len(batch)
                except Exception as e:
                    print(f"    [WARN]  Error en último batch: {e}")
                    conn.rollback()
        
        # Verificar que los datos se insertaron correctamente
        try:
            cur.execute(f"SELECT COUNT(*) as cnt FROM {full_table_name}")
            result = cur.fetchone()
            actual_count = result['cnt'] if result else 0
            print(f"  [OK] {total_rows} filas cargadas en '{table_name}' (verificado: {actual_count} filas en tabla)")
        except Exception as e:
            print(f"  [OK] {total_rows} filas cargadas en '{table_name}' (no se pudo verificar: {e})")
        
        return total_rows
        
    except Exception as e:
        print(f"  [ERROR] Error cargando datos: {e}")
        conn.rollback()
        raise
    finally:
        cur.close()


def ingest_csv_folder(conn, folder_path: str, csv_filter: list = None, target_table: str = None) -> int:
    """
    Procesa todos los CSV en una carpeta y los carga a MySQL.
    Retorna el número de archivos procesados exitosamente.
    
    Args:
        conn: Conexión a MySQL
        folder_path: Ruta de la carpeta
        csv_filter: Lista de nombres de CSV a filtrar (sin extensión .csv)
        target_table: Tabla destino. Si es None, usa el nombre del archivo como nombre de tabla
    """
    folder_name = os.path.basename(folder_path)
    csv_files = list_csvs_in_folder(folder_path, csv_filter)
    
    if not csv_files:
        print(f"[WARN]  No se encontraron archivos CSV en: {folder_name}")
        return 0
    
    print(f"📁 Carpeta: {folder_name}")
    print(f" Archivos encontrados: {len(csv_files)}")
    print()
    
    ok = 0
    
    for csv_path in csv_files:
        csv_filename = os.path.basename(csv_path)
        # Nombre de tabla: solo el nombre del CSV (sin extensión)
        table_name = csv_filename.replace('.csv.gz', '').replace('.csv', '')
        
        # Si se especificó target_table, usarlo
        if target_table:
            table_name = target_table
        
        try:
            print(f" Procesando: {csv_filename} -> tabla '{table_name}'")
            upload_csv_to_mysql(conn, csv_path, table_name)
            ok += 1
            print()
        except Exception as e:
            print(f"  [ERROR] Error procesando {csv_filename}: {e}")
            print()
    
    return ok


def main():
    """
    Función principal.
    Uso:
        python csv_to_mysql.py [database] [folders] [csv_files]
    
    Args:
        database: Base de datos de MySQL (opcional)
        folders: Carpetas a procesar, separadas por comas (opcional, todas por defecto)
        csv_files: Archivos CSV a procesar, separados por comas, sin extensión (opcional, todos por defecto)
    
    Ejemplos:
        python csv_to_mysql.py
        python csv_to_mysql.py mi_base_datos
        python csv_to_mysql.py mi_base_datos SQLSERVER_POM_Aplicaciones
        python csv_to_mysql.py mi_base_datos SQLSERVER_POM_Aplicaciones Estados_Cuenta,Desgloce_Cierre
    """
    import sys
    
    start_time = time.time()
    ensure_dirs()
    
    # Parsear argumentos
    database = None
    folder_filter = None
    csv_filter = None
    
    if len(sys.argv) > 1:
        database = sys.argv[1]
    if len(sys.argv) > 2:
        folder_filter = [f.strip() for f in sys.argv[2].split(",") if f.strip()]
    if len(sys.argv) > 3:
        csv_filter = [c.strip() for c in sys.argv[3].split(",") if c.strip()]
    
    # Si no hay argumentos pero hay variables de entorno, usarlas
    if not database and os.getenv("MYSQL_DATABASE"):
        database = os.getenv("MYSQL_DATABASE")
    if not folder_filter and FOLDERS_FILTER:
        folder_filter = FOLDERS_FILTER
    if not csv_filter and CSV_FILTER:
        csv_filter = CSV_FILTER
    
    # Conectar a MySQL
    conn = connect_mysql(database)
    
    print(f" Base de datos MySQL: {MYSQL_DATABASE}")
    if folder_filter:
        print(f"📁 Carpetas a procesar: {', '.join(folder_filter)}")
    if csv_filter:
        print(f"📄 Archivos a procesar: {', '.join(csv_filter)}")
    print()
    
    try:
        # Listar carpetas
        folders = list_csv_folders(folder_filter)
        
        if not folders:
            print("[WARN]  No se encontraron carpetas para procesar.")
            return 0
        
        print(f"📂 Carpetas encontradas: {len(folders)}")
        print()
        
        total_ok = 0
        
        for folder_path in folders:
            ok = ingest_csv_folder(conn, folder_path, csv_filter)
            total_ok += ok
        
        elapsed_time = time.time() - start_time
        
        print()
        print("=" * 60)
        print(f"RESUMEN DE EJECUCIÓN")
        print("=" * 60)
        print(f"Archivos procesados: {total_ok}")
        print(f"Tiempo de ejecución: {elapsed_time:.2f} segundos")
        print("=" * 60)
        
        return 0
        
    except Exception as e:
        elapsed_time = time.time() - start_time
        print(f"[ERROR] Error: {e}")
        print()
        print("=" * 60)
        print(f"RESUMEN DE EJECUCIÓN")
        print("=" * 60)
        print(f"Archivos procesados: 0")
        print(f"Tiempo de ejecución: {elapsed_time:.2f} segundos")
        print(f"Estado: ERROR")
        print("=" * 60)
        return 1
    finally:
        conn.close()


if __name__ == "__main__":
    exit(main())
