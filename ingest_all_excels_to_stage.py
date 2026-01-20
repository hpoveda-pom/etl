import os
import re
import gzip
import shutil
import uuid
import tempfile
import time
from datetime import datetime

import pandas as pd
import snowflake.connector

# ============== Snowflake config (env vars recomendado) ==============
SF_ACCOUNT   = os.getenv("SF_ACCOUNT", "fkwugeu-qic97823")
SF_USER      = os.getenv("SF_USER", "HPOVEDAPOMCR")
SF_PASSWORD  = os.getenv("SF_PASSWORD", "ik5niBj5FiXN4px")
SF_ROLE      = os.getenv("SF_ROLE", "ACCOUNTADMIN")
SF_WH        = os.getenv("SF_WAREHOUSE", "COMPUTE_WH")
SF_DB        = os.getenv("SF_DATABASE", "POM_TEST01")
SF_SCHEMA    = os.getenv("SF_SCHEMA", "RAW")

# ============== Carpetas (UNC o montadas) ==============
INBOX_DIR     = os.getenv("INBOX_DIR", r"UPLOADS\POM_DROP\inbox")
PROCESSED_DIR = os.getenv("PROCESSED_DIR", r"UPLOADS\POM_DROP\processed")
ERROR_DIR     = os.getenv("ERROR_DIR", r"UPLOADS\POM_DROP\error")

# ============== Stage y log table ==============
STAGE_FQN = f"{SF_DB}.{SF_SCHEMA}.RAW_STAGE"
LOG_TABLE = f"{SF_DB}.{SF_SCHEMA}.INGEST_LOG"
TARGET_TABLE = f"{SF_DB}.{SF_SCHEMA}.INGEST_GENERIC_RAW"

# Opcional: limitar hojas por allowlist (coma-separado)
SHEETS_ALLOWLIST = [s.strip() for s in os.getenv("SHEETS_ALLOWLIST", "").split(",") if s.strip()]


def ensure_dirs():
    for d in (INBOX_DIR, PROCESSED_DIR, ERROR_DIR):
        os.makedirs(d, exist_ok=True)


def sanitize_token(s: str, maxlen: int = 120) -> str:
    s = (s or "").strip()
    s = re.sub(r"[^\w\-\.]+", "_", s, flags=re.UNICODE)
    s = re.sub(r"_+", "_", s).strip("_")
    return s[:maxlen] if s else "NA"


def connect_sf():
    if not SF_PASSWORD:
        raise RuntimeError("Falta SF_PASSWORD (definí la variable de entorno).")
    return snowflake.connector.connect(
        account=SF_ACCOUNT,
        user=SF_USER,
        password=SF_PASSWORD,
        role=SF_ROLE,
        warehouse=SF_WH,
        database=SF_DB,
        schema=SF_SCHEMA,
    )


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


def sf_exec(cur, sql: str):
    try:
        cur.execute(sql)
        return cur.fetchall() if cur.description else None
    except Exception as e:
        print(f"  ❌ Error SQL: {e}")
        print(f"  SQL: {sql[:200]}...")
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


def write_df_to_csv_gz(df: pd.DataFrame, out_path_gz: str):
    with gzip.open(out_path_gz, "wt", encoding="utf-8", newline="") as f:
        df.to_csv(f, index=False)


def put_to_stage(cur, local_path: str, stage_target: str):
    """
    Sube un archivo local al stage de Snowflake.
    local_path: ruta absoluta del archivo local
    stage_target: ruta en el stage, ej: @DB.SCHEMA.STAGE/prefix/file.csv.gz
    """
    # Convertir ruta de Windows a formato file:// URL para Snowflake PUT
    # En Windows, PUT requiere formato: file:///C:/path/to/file (con 3 slashes)
    abs_path = os.path.abspath(local_path)
    # Reemplazar backslashes por forward slashes
    abs_path_normalized = abs_path.replace("\\", "/")
    
    # Formato file:// URL para Windows (3 slashes después de file:)
    file_url = f"file:///{abs_path_normalized}"
    
    # Asegurarse de que stage_target no tenga barras al final y sea la ruta exacta
    # El stage_target debe ser exactamente: @STAGE/carpeta/archivo.csv.gz
    # NO usar comillas alrededor del stage_target en el comando PUT
    stage_target_clean = stage_target.rstrip('/')
    
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
    """
    # Extraer el path relativo del stage (sin el @STAGE_FQN)
    if stage_path.startswith(f"@{STAGE_FQN}/"):
        relative_path = stage_path[len(f"@{STAGE_FQN}/"):]
    else:
        relative_path = stage_path.replace(f"@{STAGE_FQN}/", "")
    
    # Limpiar el path relativo (sin barras al inicio)
    relative_path = relative_path.lstrip('/')
    
    # Usar FORCE=FALSE para evitar cargar archivos que ya fueron cargados
    # Snowflake rastrea qué archivos ya fueron procesados por COPY INTO
    copy_sql = f"""
    COPY INTO {table_name}
    FROM @{STAGE_FQN}/{relative_path}
    FILE_FORMAT = (TYPE = CSV, FIELD_DELIMITER = ',', SKIP_HEADER = 1, FIELD_OPTIONALLY_ENCLOSED_BY = '"')
    ON_ERROR = 'CONTINUE'
    FORCE = FALSE;
    """
    print(f"  → COPY INTO: @{STAGE_FQN}/{relative_path}")
    sf_exec(cur, copy_sql)


def derive_structure_from_filename(original_filename: str) -> str:
    """
    Regla base: la 'estructura' = nombre base del archivo (sin .xlsx), sanitizado.
    Ej: 'BAC_CONCILIACION_20260106' -> 'BAC_CONCILIACION_20260106'
    Si luego querés recortar la fecha, se cambia aquí.
    """
    base = os.path.splitext(original_filename)[0]
    return sanitize_token(base)


def ingest_one_excel(cur, excel_path: str, batch_id: str) -> int:
    original_file = os.path.basename(excel_path)
    structure = derive_structure_from_filename(original_file)

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
            tmp_dir = tempfile.mkdtemp(prefix="etl_excel_")

            # Nombre del archivo solo con el nombre del sheet (la carpeta ya identifica el Excel)
            out_name = f"{safe_sheet}.csv.gz"
            local_csv_gz = os.path.join(tmp_dir, out_name)

            row_count = 0
            col_count = 0

            try:
                df = pd.read_excel(xls, sheet_name=sheet, dtype=object)
                df = df.where(pd.notnull(df), None)

                row_count = int(df.shape[0])
                col_count = int(df.shape[1])

                write_df_to_csv_gz(df, local_csv_gz)
                file_bytes = os.path.getsize(local_csv_gz)

                # Path en stage: carpeta con nombre del Excel, dentro archivos con nombre del sheet
                # Estructura: @STAGE/{estructura}/{sheet}.csv.gz
                # Si el mismo archivo se procesa de nuevo, se sobrescribe en el stage
                # pero COPY INTO con FORCE=FALSE evitará duplicados en la tabla
                stage_path = f"@{STAGE_FQN}/{structure}/{out_name}"
                
                print(f"  → Sheet: {sheet} → Archivo: {out_name} en carpeta: {structure}")

                put_to_stage(cur, local_csv_gz, stage_path)
                
                # Cargar datos desde el stage a la tabla
                # FORCE=FALSE: solo carga si el archivo no fue cargado antes (evita duplicados)
                copy_from_stage_to_table(cur, stage_path, TARGET_TABLE)

                log_ingest(cur, batch_id, original_file, sheet, stage_path,
                           row_count, col_count, file_bytes, "OK", "")
                ok += 1

            except Exception as e:
                file_bytes = os.path.getsize(local_csv_gz) if os.path.exists(local_csv_gz) else 0
                log_ingest(cur, batch_id, original_file, sheet, "",
                           row_count, col_count, file_bytes, "ERROR", str(e))

            finally:
                shutil.rmtree(tmp_dir, ignore_errors=True)

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
        elapsed_time = time.time() - start_time
        print("No hay .xlsx en inbox.")
        print()
        print("=" * 60)
        print(f"RESUMEN DE EJECUCIÓN")
        print("=" * 60)
        print(f"Archivos procesados: 0")
        print(f"Tiempo de ejecución: {elapsed_time:.2f} segundos")
        print("=" * 60)
        return

    conn = connect_sf()
    try:
        cur = conn.cursor()
        try:
            total_files = 0
            total_sheets = 0
            files_ok = 0
            files_error = 0
            
            for excel_path in files:
                original_file = os.path.basename(excel_path)
                batch_id = str(uuid.uuid4())
                print(f"Procesando {original_file} (batch={batch_id})")
                total_files += 1

                try:
                    ok_sheets = ingest_one_excel(cur, excel_path, batch_id)
                    conn.commit()

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
                    conn.rollback()
                    move_file(excel_path, ERROR_DIR)
                    print(f"ERROR → error: {original_file} | {e}")
                    files_error += 1
            
            elapsed_time = time.time() - start_time
            print()
            print("=" * 60)
            print(f"RESUMEN DE EJECUCIÓN")
            print("=" * 60)
            print(f"Archivos Excel procesados: {total_files}")
            print(f"Hojas cargadas a Snowflake: {total_sheets}")
            print(f"Archivos exitosos: {files_ok}")
            print(f"Archivos con error: {files_error}")
            print(f"Tiempo de ejecución: {elapsed_time:.2f} segundos")
            print("=" * 60)

        finally:
            cur.close()
    finally:
        conn.close()


if __name__ == "__main__":
    main()
