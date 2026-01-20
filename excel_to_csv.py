import os
import re
import shutil
import time
from datetime import datetime

import pandas as pd

# ============== Carpetas ==============
INBOX_DIR     = os.getenv("INBOX_DIR", r"UPLOADS\POM_DROP\inbox")
PROCESSED_DIR = os.getenv("PROCESSED_DIR", r"UPLOADS\POM_DROP\processed")
ERROR_DIR     = os.getenv("ERROR_DIR", r"UPLOADS\POM_DROP\error")
CSV_STAGING_DIR = os.getenv("CSV_STAGING_DIR", r"UPLOADS\POM_DROP\csv_staging")

# Opcional: limitar hojas por allowlist (coma-separado)
SHEETS_ALLOWLIST = [s.strip() for s in os.getenv("SHEETS_ALLOWLIST", "").split(",") if s.strip()]


def ensure_dirs():
    for d in (INBOX_DIR, PROCESSED_DIR, ERROR_DIR, CSV_STAGING_DIR):
        os.makedirs(d, exist_ok=True)


def sanitize_token(s: str, maxlen: int = 120) -> str:
    s = (s or "").strip()
    s = re.sub(r"[^\w\-\.]+", "_", s, flags=re.UNICODE)
    s = re.sub(r"_+", "_", s).strip("_")
    return s[:maxlen] if s else "NA"


def list_excels():
    files = []
    for name in os.listdir(INBOX_DIR):
        low = name.lower()
        if low.endswith(".xlsx") and not name.startswith("~$"):
            files.append(os.path.join(INBOX_DIR, name))
    return sorted(files)


def move_file(src: str, dst_dir: str, max_retries: int = 5, retry_delay: float = 0.5):
    """
    Mueve un archivo con reintentos para manejar bloqueos en Windows.
    """
    os.makedirs(dst_dir, exist_ok=True)
    name = os.path.basename(src)
    dst = os.path.join(dst_dir, name)
    if os.path.exists(dst):
        stem, ext = os.path.splitext(name)
        dst = os.path.join(dst_dir, f"{stem}_{datetime.now().strftime('%Y%m%d_%H%M%S')}{ext}")
    
    for attempt in range(max_retries):
        try:
            shutil.move(src, dst)
            return
        except PermissionError as e:
            if attempt < max_retries - 1:
                time.sleep(retry_delay)
                continue
            raise


def write_df_to_csv(df: pd.DataFrame, out_path: str):
    """Escribe un DataFrame a CSV sin comprimir."""
    df.to_csv(out_path, index=False, encoding='utf-8')


def derive_structure_from_filename(original_filename: str) -> str:
    """
    Regla base: la 'estructura' = nombre base del archivo (sin .xlsx), sanitizado.
    Ej: 'BAC_CONCILIACION_20260106' -> 'BAC_CONCILIACION_20260106'
    """
    base = os.path.splitext(original_filename)[0]
    return sanitize_token(base)


def convert_excel_to_csv(excel_path: str) -> int:
    """
    Convierte un archivo Excel a CSV (sin comprimir).
    Crea una carpeta con el nombre del Excel y dentro los CSV de cada sheet.
    Retorna el número de sheets convertidos exitosamente.
    """
    original_file = os.path.basename(excel_path)
    structure = derive_structure_from_filename(original_file)
    
    # Crear carpeta de destino para este Excel
    excel_output_dir = os.path.join(CSV_STAGING_DIR, structure)
    os.makedirs(excel_output_dir, exist_ok=True)
    
    xls = None
    try:
        xls = pd.ExcelFile(excel_path, engine="openpyxl")
        sheets = xls.sheet_names
        
        if SHEETS_ALLOWLIST:
            sheets = [s for s in sheets if s in SHEETS_ALLOWLIST]
        
        if not sheets:
            raise RuntimeError(f"Excel sin sheets válidos: {original_file}")
        
        ok = 0
        
        for sheet in sheets:
            safe_sheet = sanitize_token(sheet)
            
            # Nombre del archivo solo con el nombre del sheet (la carpeta ya identifica el Excel)
            out_name = f"{safe_sheet}.csv"
            output_path = os.path.join(excel_output_dir, out_name)
            
            try:
                df = pd.read_excel(xls, sheet_name=sheet, dtype=object)
                df = df.where(pd.notnull(df), None)
                
                write_df_to_csv(df, output_path)
                file_bytes = os.path.getsize(output_path)
                
                row_count = int(df.shape[0])
                col_count = int(df.shape[1])
                
                print(f"  → Sheet: {sheet} → {out_name} ({row_count} filas, {col_count} columnas, {file_bytes} bytes)")
                ok += 1
                
            except Exception as e:
                print(f"  ❌ Error procesando sheet '{sheet}': {e}")
        
        return ok
        
    finally:
        # Cerrar explícitamente el archivo Excel para liberar el bloqueo
        if xls is not None:
            xls.close()


def main():
    start_time = time.time()
    ensure_dirs()
    
    files = list_excels()
    if not files:
        print("No hay .xlsx en inbox.")
        elapsed_time = time.time() - start_time
        print()
        print("=" * 60)
        print(f"RESUMEN DE EJECUCIÓN")
        print("=" * 60)
        print(f"Archivos procesados: 0")
        print(f"Tiempo de ejecución: {elapsed_time:.2f} segundos")
        print("=" * 60)
        return
    
    total_files = 0
    total_sheets = 0
    files_ok = 0
    files_error = 0
    
    for excel_path in files:
        original_file = os.path.basename(excel_path)
        print(f"Procesando {original_file}")
        total_files += 1
        
        try:
            ok_sheets = convert_excel_to_csv(excel_path)
            
            if ok_sheets > 0:
                move_file(excel_path, PROCESSED_DIR)
                print(f"OK → processed ({ok_sheets} sheets): {original_file}")
                total_sheets += ok_sheets
                files_ok += 1
            else:
                move_file(excel_path, ERROR_DIR)
                print(f"ERROR → error (0 sheets OK): {original_file}")
                files_error += 1
                
        except Exception as e:
            move_file(excel_path, ERROR_DIR)
            print(f"ERROR → error: {original_file} | {e}")
            files_error += 1
    
    elapsed_time = time.time() - start_time
    print()
    print("=" * 60)
    print(f"RESUMEN DE EJECUCIÓN")
    print("=" * 60)
    print(f"Archivos Excel procesados: {total_files}")
    print(f"Hojas convertidas a CSV: {total_sheets}")
    print(f"Archivos exitosos: {files_ok}")
    print(f"Archivos con error: {files_error}")
    print(f"Tiempo de ejecución: {elapsed_time:.2f} segundos")
    print("=" * 60)


if __name__ == "__main__":
    main()
