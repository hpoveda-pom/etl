import os
import re
import csv
import gzip
import time
import uuid
from datetime import datetime

try:
    import clickhouse_connect
except ImportError:
    print("❌ Error: Falta la librería clickhouse-connect")
    print("💡 Instálala con: pip install clickhouse-connect")
    exit(1)

# ============== ClickHouse config ==============
CH_HOST = os.getenv("CH_HOST", "f4rf85ygzj.eastus2.azure.clickhouse.cloud")
CH_PORT = int(os.getenv("CH_PORT", "8443"))
CH_USER = os.getenv("CH_USER", "default")
CH_PASSWORD = os.getenv("CH_PASSWORD", "")
CH_DATABASE = os.getenv("CH_DATABASE", "default")

# ============== Carpetas ==============
CSV_STAGING_DIR = os.getenv("CSV_STAGING_DIR", r"UPLOADS\POM_DROP\csv_staging")


def sanitize_token(s: str, maxlen: int = 120) -> str:
    """Sanitiza un string para usarlo como nombre de tabla/columna en ClickHouse."""
    s = (s or "").strip()
    s = re.sub(r"[^\w\-\.]+", "_", s, flags=re.UNICODE)
    s = re.sub(r"_+", "_", s).strip("_")
    return s[:maxlen] if s else "NA"


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
                    db_list = "\n   - ".join(available_dbs[:15])
                    if len(available_dbs) > 15:
                        db_list += f"\n   ... y {len(available_dbs) - 15} más"
                    raise RuntimeError(
                        f"❌ No se pudo crear la base de datos '{CH_DATABASE}'.\n"
                        f"Error: {create_err}\n\n"
                        f"💡 Bases de datos disponibles ({len(available_dbs)}):\n   - {db_list}\n\n"
                        f"💡 Sugerencias:\n"
                        f"   - Usa una de las bases de datos listadas arriba\n"
                        f"   - Ejemplo: python clickhouse_csv_to_tables.py default ...\n"
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
                        f"   - Ejemplo: python clickhouse_csv_to_tables.py default ...\n"
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


def list_csv_files_in_directory(directory: str):
    """
    Lista todos los archivos CSV en el directorio especificado.
    Retorna lista de tuplas (file_path, file_name, folder_name).
    """
    files = []
    if not os.path.exists(directory):
        return files
    
    for root, dirs, filenames in os.walk(directory):
        for filename in filenames:
            if filename.lower().endswith('.csv.gz') or filename.lower().endswith('.csv'):
                file_path = os.path.join(root, filename)
                # Extraer folder_name (nombre de la carpeta padre)
                folder_name = os.path.basename(os.path.dirname(file_path))
                files.append((file_path, filename, folder_name))
    
    return files


def get_csv_headers_from_file(csv_path: str):
    """
    Obtiene los headers (nombres de columnas) de un CSV local.
    
    Args:
        csv_path: Ruta del archivo CSV local
    """
    try:
        is_gzipped = csv_path.lower().endswith('.csv.gz')
        
        if is_gzipped:
            f = gzip.open(csv_path, 'rt', encoding='utf-8', newline='')
        else:
            f = open(csv_path, 'rt', encoding='utf-8', newline='')
        
        with f:
            # Detectar delimitador
            sample = f.read(10240)
            f.seek(0)
            
            sniffer = csv.Sniffer()
            delimiters = [',', ';', '\t', '|']
            try:
                detected = sniffer.sniff(sample, delimiters=delimiters)
                delimiter = detected.delimiter
            except:
                delimiter = ','
            
            reader = csv.reader(f, delimiter=delimiter)
            try:
                header_row = next(reader)
                # Sanitizar nombres de columnas
                headers = [sanitize_token(h.strip().strip('"')) if h else f"col{i+1}" 
                          for i, h in enumerate(header_row)]
                # Hacer únicos los headers (renombrar duplicados)
                unique_headers, renames = make_unique_headers(headers)
                
                if renames:
                    rename_info = ', '.join([f'{old}→{new}' for old, new in renames[:5]])
                    if len(renames) > 5:
                        rename_info += f' ... y {len(renames) - 5} más'
                    print(f"  ⚠️  Columnas duplicadas renombradas: {rename_info}")
                
                print(f"  ✅ Headers leídos: {', '.join(unique_headers[:10])}{'...' if len(unique_headers) > 10 else ''}")
                return unique_headers, delimiter
            except StopIteration:
                raise RuntimeError(f"No se pudo leer ninguna fila del archivo {os.path.basename(csv_path)}")
        
    except Exception as e:
        print(f"  ⚠️  Error al leer headers: {e}")
        import traceback
        traceback.print_exc()
        raise RuntimeError(f"No se pudieron leer los headers del archivo {os.path.basename(csv_path)}")


def create_table_from_csv(client, table_name: str, headers: list, csv_path: str):
    """
    Crea una tabla en ClickHouse con la estructura del CSV y carga los datos.
    
    Args:
        client: Cliente de ClickHouse
        table_name: Nombre de la tabla a crear
        headers: Lista de nombres de columnas
        csv_path: Ruta del archivo CSV local
    """
    file_name = os.path.basename(csv_path)
    
    # Sanitizar nombre de tabla
    table_name_sanitized = sanitize_token(table_name)
    full_table_name = f"`{CH_DATABASE}`.`{table_name_sanitized}`"
    
    # Verificar si la tabla existe
    try:
        check_sql = f"EXISTS TABLE {full_table_name}"
        result = ch_exec(client, check_sql)
        existing = result[0][0] == 1 if result else False
    except:
        existing = False
    
    if existing:
        print(f"  ⚠️  La tabla '{table_name_sanitized}' ya existe. Eliminando antes de recrear...")
        # Eliminar la tabla existente antes de crearla
        drop_sql = f"DROP TABLE IF EXISTS {full_table_name}"
        try:
            ch_exec(client, drop_sql)
            print(f"  ✅ Tabla eliminada")
        except Exception as drop_err:
            print(f"  ⚠️  Error al eliminar tabla existente: {drop_err}")
            print(f"  ❌ No se pudo eliminar la tabla existente. Omitiendo creación...")
            return "skipped"
    
    # Crear columnas SQL (todas como String para máxima compatibilidad)
    columns = []
    for i, header in enumerate(headers, 1):
        col_name = sanitize_token(header) if header else f"col{i}"
        columns.append(f"`{col_name}` String")
    
    # Crear la tabla
    create_sql = f"""
    CREATE TABLE IF NOT EXISTS {full_table_name} (
        {', '.join(columns)}
    ) ENGINE = MergeTree()
    ORDER BY tuple();
    """
    
    print(f"  📦 Creando tabla: {table_name_sanitized} ({len(headers)} columnas)")
    ch_exec(client, create_sql)
    
    # Cargar datos desde el CSV
    print(f"  📥 Cargando datos desde: {file_name}")
    
    # Leer CSV y cargar datos
    is_gzipped = csv_path.lower().endswith('.csv.gz')
    headers_actual, delimiter = get_csv_headers_from_file(csv_path)
    
    # Asegurar que los headers coinciden
    if len(headers_actual) != len(headers):
        print(f"  ⚠️  Advertencia: número de columnas en headers ({len(headers_actual)}) diferente al esperado ({len(headers)})")
        headers = headers_actual
    
    column_names = [sanitize_token(h) if h else f"col{i+1}" for i, h in enumerate(headers)]
    
    # Leer y cargar datos en lotes
    batch_size = 10000
    batch = []
    total_rows = 0
    
    if is_gzipped:
        f = gzip.open(csv_path, 'rt', encoding='utf-8', newline='')
    else:
        f = open(csv_path, 'rt', encoding='utf-8', newline='')
    
    with f:
        reader = csv.reader(f, delimiter=delimiter)
        # Saltar header
        try:
            next(reader)
        except StopIteration:
            print(f"    ⚠️  El archivo está vacío o no tiene header")
            return "skipped"
        
        for row in reader:
            if not row:  # Saltar filas vacías
                continue
            
            # Limpiar valores
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
    
    # Verificar cuántas filas se cargaron
    try:
        count_sql = f"SELECT COUNT(*) FROM {full_table_name}"
        count_result = ch_exec(client, count_sql)
        row_count = count_result[0][0] if count_result else 0
        print(f"  ✅ Tabla '{table_name_sanitized}' creada con {row_count} filas")
    except:
        print(f"  ✅ Tabla '{table_name_sanitized}' creada con {total_rows} filas (no se pudo verificar)")
    
    return True


def process_csv_files_to_tables(client, file_filter: list = None, folder_filter: list = None):
    """
    Procesa todos los CSV en el directorio y crea tablas para cada uno.
    
    Args:
        client: Cliente de ClickHouse
        file_filter: Lista de nombres de archivos a procesar (opcional)
        folder_filter: Lista de nombres de carpetas a procesar (opcional)
    """
    # Listar archivos en el directorio
    files = list_csv_files_in_directory(CSV_STAGING_DIR)
    
    if not files:
        print("⚠️  No se encontraron archivos CSV en el directorio.")
        return
    
    print(f"📋 Archivos encontrados: {len(files)}")
    
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
    
    for file_path, file_name, folder_name in files:
        try:
            file_name = str(file_name)
            folder_name = str(folder_name)
            
            # Nombre de tabla: solo el nombre del CSV (sin extensión)
            sheet_name = file_name.replace('.csv.gz', '').replace('.csv', '')
            table_name = sheet_name
            
            print(f"🔄 Procesando: {file_name} (folder: {folder_name})")
            
            # Obtener headers del CSV
            headers, delimiter = get_csv_headers_from_file(file_path)
            
            if not headers:
                print(f"  ❌ No se pudieron leer los headers de {file_name}")
                errors += 1
                continue
            
            # Crear tabla y cargar datos
            result = create_table_from_csv(client, table_name, headers, file_path)
            
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
    
    return processed, skipped, errors


def main():
    """
    Función principal.
    Uso:
        python clickhouse_csv_to_tables.py [database] [folders] [files]
    
    Args:
        database: Base de datos de ClickHouse (opcional)
        folders: Carpetas a procesar, separadas por comas (opcional, todas por defecto)
        files: Archivos CSV a procesar, separados por comas, sin extensión (opcional, todos por defecto)
    
    Ejemplos:
        python clickhouse_csv_to_tables.py
        python clickhouse_csv_to_tables.py default
        python clickhouse_csv_to_tables.py default CIERRE_PROPIAS___7084110
        python clickhouse_csv_to_tables.py default CIERRE_PROPIAS___7084110 Estados_Cuenta,Desgloce_Cierre
    """
    import sys
    
    start_time = time.time()
    
    # Parsear argumentos
    database = None
    folder_filter = None
    file_filter = None
    
    if len(sys.argv) > 1:
        database = sys.argv[1]
    if len(sys.argv) > 2:
        folder_filter = [f.strip() for f in sys.argv[2].split(",") if f.strip()]
    if len(sys.argv) > 3:
        file_filter = [c.strip() for c in sys.argv[3].split(",") if c.strip()]
    
    # Si no hay argumentos pero hay variables de entorno, usarlas
    if not database and os.getenv("CH_DATABASE"):
        database = os.getenv("CH_DATABASE")
    
    # Conectar a ClickHouse
    client = connect_ch(database)
    
    print(f"📊 Base de datos ClickHouse: {CH_DATABASE}")
    if folder_filter:
        print(f"📁 Carpetas a procesar: {', '.join(folder_filter)}")
    if file_filter:
        print(f"📄 Archivos a procesar: {', '.join(file_filter)}")
    print()
    
    try:
        result = process_csv_files_to_tables(client, file_filter, folder_filter)
        
        elapsed_time = time.time() - start_time
        
        if result:
            processed, skipped, errors = result
            print()
            print("=" * 60)
            print(f"RESUMEN DE EJECUCIÓN")
            print("=" * 60)
            print(f"Tablas creadas: {processed}")
            print(f"Tablas omitidas (ya existían): {skipped}")
            print(f"Errores: {errors}")
            print(f"Tiempo de ejecución: {elapsed_time:.2f} segundos")
            print("=" * 60)
        else:
            elapsed_time = time.time() - start_time
            print()
            print("=" * 60)
            print(f"RESUMEN DE EJECUCIÓN")
            print("=" * 60)
            print(f"Tablas procesadas: 0")
            print(f"Tiempo de ejecución: {elapsed_time:.2f} segundos")
            print("=" * 60)
    finally:
        client.close()


if __name__ == "__main__":
    main()
