import os
import gzip
import shutil
import time
from pathlib import Path

# ============== Carpetas ==============
CSV_STAGING_DIR = os.getenv("CSV_STAGING_DIR", r"UPLOADS\POM_DROP\csv_staging")

# Opcional: filtrar carpetas específicas (coma-separado). Si está vacío, procesa todas.
FOLDERS_FILTER = [s.strip() for s in os.getenv("FOLDERS_FILTER", "").split(",") if s.strip()]

# Opcional: filtrar archivos CSV específicos (coma-separado, sin extensión .csv). Si está vacío, procesa todos.
CSV_FILTER = [s.strip() for s in os.getenv("CSV_FILTER", "").split(",") if s.strip()]

# Opción: eliminar CSV originales después de comprimir (por defecto: False)
DELETE_ORIGINALS = os.getenv("DELETE_ORIGINALS", "false").lower() in ("true", "1", "yes")


def compress_csv_to_gz(csv_path: str, output_gz_path: str):
    """
    Comprime un archivo CSV a CSV.gz.
    """
    with open(csv_path, 'rt', encoding='utf-8', newline='') as f_in:
        with gzip.open(output_gz_path, 'wt', encoding='utf-8', newline='') as f_out:
            shutil.copyfileobj(f_in, f_out)


def list_sqlserver_folders(base_dir: str, folders_filter: list = None):
    """
    Lista las carpetas en el directorio base.
    Si hay filtro, busca cualquier carpeta que coincida.
    Si no hay filtro, busca carpetas SQLSERVER_* por defecto.
    Retorna lista de rutas de carpetas.
    
    Args:
        base_dir: Directorio base donde buscar
        folders_filter: Lista de nombres de carpetas a filtrar (si None, usa FOLDERS_FILTER)
    """
    if folders_filter is None:
        folders_filter = FOLDERS_FILTER
    
    folders = []
    if not os.path.exists(base_dir):
        return folders
    
    for item in os.listdir(base_dir):
        folder_path = os.path.join(base_dir, item)
        if os.path.isdir(folder_path):
            folder_name = os.path.basename(folder_path)
            
            # Si hay filtro especificado, buscar cualquier carpeta que coincida
            if folders_filter:
                # Buscar coincidencias (exacta o parcial)
                if any(folder_name == f or folder_name.startswith(f + "_") or f in folder_name 
                      for f in folders_filter):
                    folders.append(folder_path)
            else:
                # Si no hay filtro, buscar solo carpetas SQLSERVER_* por defecto
                if folder_name.startswith("SQLSERVER_"):
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


def compress_csvs_in_folder(folder_path: str, csv_filter: list = None) -> tuple[int, int]:
    """
    Comprime todos los CSV en una carpeta a CSV.gz.
    Retorna (compressed_count, error_count).
    """
    csv_files = list_csvs_in_folder(folder_path, csv_filter)
    
    if not csv_files:
        folder_name = os.path.basename(folder_path)
        # Verificar si hay archivos CSV.gz ya comprimidos
        gz_files = [f for f in os.listdir(folder_path) if f.lower().endswith(".csv.gz")]
        if gz_files:
            print(f"  ℹ️  No hay archivos CSV sin comprimir en {folder_name} (ya existen {len(gz_files)} archivos .csv.gz)")
        else:
            print(f"  ⚠️  No se encontraron archivos CSV en {folder_name}")
        return 0, 0
    
    folder_name = os.path.basename(folder_path)
    compressed = 0
    errors = 0
    
    for csv_path in csv_files:
        csv_filename = os.path.basename(csv_path)
        csv_gz_path = csv_path + ".gz"
        
        # Verificar si el archivo .gz ya existe
        if os.path.exists(csv_gz_path):
            print(f"  ⚠️  {csv_filename}.gz ya existe. Omitiendo...")
            continue
        
        try:
            # Obtener tamaño original
            original_size = os.path.getsize(csv_path)
            
            # Comprimir
            compress_csv_to_gz(csv_path, csv_gz_path)
            
            # Obtener tamaño comprimido
            compressed_size = os.path.getsize(csv_gz_path)
            compression_ratio = (1 - compressed_size / original_size) * 100 if original_size > 0 else 0
            
            print(f"  ✓ {csv_filename} → {csv_filename}.gz "
                  f"({original_size:,} → {compressed_size:,} bytes, "
                  f"{compression_ratio:.1f}% compresión)")
            
            # Eliminar CSV original si está configurado
            if DELETE_ORIGINALS:
                os.remove(csv_path)
                print(f"    🗑️  CSV original eliminado")
            
            compressed += 1
            
        except Exception as e:
            print(f"  ❌ Error comprimiendo {csv_filename}: {e}")
            errors += 1
    
    return compressed, errors


def main():
    """
    Función principal.
    Uso:
        python compress_csv_to_gz.py [folders] [csvs] [delete_originals]
    
    Args:
        folders: Carpetas a procesar, separadas por comas (opcional, todas por defecto)
                 Ejemplo: SQLSERVER_POM_Aplicaciones
        csvs: CSV a procesar, separados por comas, sin extensión (opcional, todos por defecto)
              Ejemplo: ResutadoNotificar,Bitacora
        delete_originals: Si es "true", elimina los CSV originales después de comprimir (opcional, default: false)
    
    Ejemplos:
        python compress_csv_to_gz.py
        python compress_csv_to_gz.py SQLSERVER_POM_Aplicaciones
        python compress_csv_to_gz.py SQLSERVER_POM_Aplicaciones ResutadoNotificar,Bitacora
        python compress_csv_to_gz.py SQLSERVER_POM_Aplicaciones "" true
    """
    import sys
    
    start_time = time.time()
    global DELETE_ORIGINALS  # Declarar global al inicio
    
    if not os.path.exists(CSV_STAGING_DIR):
        print(f"⚠️  El directorio '{CSV_STAGING_DIR}' no existe.")
        return 1
    
    # Parsear argumentos
    folders_filter = None
    csv_filter = None
    delete_originals_arg = None
    
    # Valores booleanos reconocidos
    boolean_values = ("true", "false", "1", "0", "yes", "no")
    
    if len(sys.argv) > 1:
        folders_arg = sys.argv[1].strip()
        if folders_arg and folders_arg.lower() not in boolean_values:
            folders_filter = [f.strip() for f in folders_arg.split(",") if f.strip()]
    
    if len(sys.argv) > 2:
        csvs_arg = sys.argv[2].strip()
        # Si el segundo argumento es un valor booleano, es delete_originals (se saltó csv_filter)
        if csvs_arg.lower() in boolean_values:
            delete_originals_arg = csvs_arg.lower() in ("true", "1", "yes")
            DELETE_ORIGINALS = delete_originals_arg
        elif csvs_arg:  # Es un filtro de CSV válido
            csv_filter = [c.strip() for c in csvs_arg.split(",") if c.strip()]
    
    if len(sys.argv) > 3:
        delete_originals_arg = sys.argv[3].strip().lower() in ("true", "1", "yes")
        DELETE_ORIGINALS = delete_originals_arg
    
    # Si no hay argumentos pero hay variables de entorno, usarlas
    if not folders_filter and FOLDERS_FILTER:
        folders_filter = FOLDERS_FILTER
    if not csv_filter and CSV_FILTER:
        csv_filter = CSV_FILTER
    
    print(f"📁 Directorio base: {CSV_STAGING_DIR}")
    if folders_filter:
        print(f"📂 Carpetas a procesar: {', '.join(folders_filter)}")
    if csv_filter:
        print(f"📄 CSV a procesar: {', '.join(csv_filter)}")
    print(f"🗑️  Eliminar originales: {'Sí' if DELETE_ORIGINALS else 'No'}")
    print()
    
    # Listar carpetas
    folders = list_sqlserver_folders(CSV_STAGING_DIR, folders_filter)
    
    if not folders:
        if folders_filter:
            print(f"⚠️  No se encontraron carpetas que coincidan con el filtro: {', '.join(folders_filter)}")
        else:
            print("⚠️  No se encontraron carpetas SQLSERVER_* en el directorio.")
        return 1
    
    print(f"📋 Carpetas encontradas: {len(folders)}")
    print()
    
    total_compressed = 0
    total_errors = 0
    
    for folder_path in folders:
        folder_name = os.path.basename(folder_path)
        print(f"📦 Procesando carpeta: {folder_name}")
        
        compressed, errors = compress_csvs_in_folder(folder_path, csv_filter)
        
        total_compressed += compressed
        total_errors += errors
        
        if compressed > 0:
            print(f"  ✅ {compressed} archivos comprimidos")
        if errors > 0:
            print(f"  ❌ {errors} errores")
        print()
    
    elapsed_time = time.time() - start_time
    
    # Resumen final
    print(f"✅ Proceso completado: {total_compressed} archivos comprimidos, {total_errors} errores")
    print()
    print("=" * 60)
    print(f"RESUMEN DE EJECUCIÓN")
    print("=" * 60)
    print(f"Carpetas procesadas: {len(folders)}")
    print(f"Archivos comprimidos: {total_compressed}")
    print(f"Errores: {total_errors}")
    print(f"Tiempo de ejecución: {elapsed_time:.2f} segundos")
    print("=" * 60)
    
    return 0 if total_errors == 0 else 1


if __name__ == "__main__":
    exit(main())
