import os
import sys
from pathlib import Path
import clickhouse_connect
from dotenv import load_dotenv

# Cargar .env desde el directorio etl/ (padre del script)
script_dir = Path(__file__).resolve().parent
parent_dir = script_dir.parent  # etl/
env_path = parent_dir / ".env"
if env_path.exists():
    load_dotenv(env_path, override=True)
else:
    # Fallback: búsqueda automática
    load_dotenv(override=True)

# =========================
# ENV CONFIG
# =========================
CH_HOST = os.getenv("CH_HOST", "localhost")
CH_PORT = int(os.getenv("CH_PORT", "8123"))
CH_USER = os.getenv("CH_USER", "default")
CH_PASSWORD = os.getenv("CH_PASSWORD", "")
CH_DATABASE = os.getenv("CH_DATABASE", "default")

# =========================
# HELPERS
# =========================
def ch_client():
    secure = (CH_PORT == 8443)
    return clickhouse_connect.get_client(
        host=CH_HOST,
        port=CH_PORT,
        username=CH_USER,
        password=CH_PASSWORD,
        database=CH_DATABASE,
        secure=secure,
        verify=False,
    )

def usage():
    print("Uso:")
    print("  python clickhouse_truncate.py nombre_base_datos nombre_tabla")
    print("")
    print("Ejemplos:")
    print("  python clickhouse_truncate.py POM_Aplicaciones jd_BI_216")
    print("  python clickhouse_truncate.py POM_Aplicaciones \"tabla con espacios\"")
    sys.exit(1)

def parse_args():
    if len(sys.argv) < 3:
        usage()
    
    db_name = sys.argv[1].strip()
    table_name = sys.argv[2].strip()
    
    if not db_name:
        raise Exception("Nombre de base de datos vacío.")
    if not table_name:
        raise Exception("Nombre de tabla vacío.")
    
    return db_name, table_name

def truncate_table(ch, db_name: str, table_name: str):
    """Trunca una tabla (elimina todos los datos pero mantiene la estructura)"""
    print(f"[INFO] Truncando tabla: `{db_name}`.`{table_name}`")
    
    try:
        # Verificar que la tabla existe
        check_sql = f"EXISTS TABLE `{db_name}`.`{table_name}`"
        result = ch.query(check_sql)
        
        if result.result_rows[0][0] == 0:
            print(f"[ERROR] La tabla `{db_name}`.`{table_name}` no existe")
            return False
        
        # Obtener información de la tabla antes de truncar
        desc_sql = f"SELECT count() FROM `{db_name}`.`{table_name}`"
        count_result = ch.query(desc_sql)
        row_count = count_result.result_rows[0][0] if count_result.result_rows else 0
        
        print(f"[INFO] Filas en la tabla antes de truncar: {row_count}")
        
        # Truncar la tabla
        ch.command(f"TRUNCATE TABLE IF EXISTS `{db_name}`.`{table_name}`")
        
        # Verificar que se truncó correctamente
        count_result_after = ch.query(desc_sql)
        row_count_after = count_result_after.result_rows[0][0] if count_result_after.result_rows else 0
        
        print(f"[OK] Tabla `{db_name}`.`{table_name}` truncada correctamente")
        print(f"[INFO] Filas en la tabla después de truncar: {row_count_after}")
        
        return True
        
    except Exception as e:
        print(f"[ERROR] No se pudo truncar la tabla: {e}")
        raise

# =========================
# MAIN
# =========================
def main():
    try:
        db_name, table_name = parse_args()
        
        ch = ch_client()
        print(f"[INFO] Conectado a ClickHouse: {CH_HOST}:{CH_PORT}")
        
        truncate_table(ch, db_name, table_name)
        
        print("\n[OK] Operación completada exitosamente")
        
    except KeyboardInterrupt:
        print("\n[CANCELADO] Operación cancelada por el usuario")
        sys.exit(1)
    except Exception as e:
        print(f"\n[ERROR] {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()
