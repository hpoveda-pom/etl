import os
import sys
import clickhouse_connect
from dotenv import load_dotenv

load_dotenv()

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
    print("  python clickhouse_drop.py DATABASE nombre_base_datos")
    print("  python clickhouse_drop.py TABLE nombre_base_datos nombre_tabla")
    print("")
    print("Ejemplos:")
    print("  python clickhouse_drop.py DATABASE POM_Aplicaciones")
    print("  python clickhouse_drop.py TABLE POM_Aplicaciones jd_BI_216")
    print("  python clickhouse_drop.py TABLE POM_Aplicaciones \"tabla con espacios\"")
    sys.exit(1)

def parse_args():
    if len(sys.argv) < 3:
        usage()
    
    action = sys.argv[1].strip().upper()
    
    if action not in ("DATABASE", "TABLE", "DB"):
        print(f"[ERROR] Acción inválida: {action}")
        print("        Debe ser 'DATABASE' o 'TABLE'")
        usage()
    
    if action in ("DATABASE", "DB"):
        if len(sys.argv) < 3:
            usage()
        db_name = sys.argv[2].strip()
        if not db_name:
            raise Exception("Nombre de base de datos vacío.")
        return ("DATABASE", db_name, None)
    
    # TABLE
    if len(sys.argv) < 4:
        print("[ERROR] Para TABLE necesitas: DATABASE y TABLE")
        usage()
    
    db_name = sys.argv[2].strip()
    table_name = sys.argv[3].strip()
    
    if not db_name:
        raise Exception("Nombre de base de datos vacío.")
    if not table_name:
        raise Exception("Nombre de tabla vacío.")
    
    return ("TABLE", db_name, table_name)

def drop_database(ch, db_name: str):
    """Elimina una base de datos completa"""
    print(f"[INFO] Eliminando base de datos: `{db_name}`")
    
    try:
        ch.command(f"DROP DATABASE IF EXISTS `{db_name}`")
        print(f"[OK] Base de datos `{db_name}` eliminada correctamente")
    except Exception as e:
        print(f"[ERROR] No se pudo eliminar la base de datos: {e}")
        raise

def drop_table(ch, db_name: str, table_name: str):
    """Elimina una tabla específica"""
    print(f"[INFO] Eliminando tabla: `{db_name}`.`{table_name}`")
    
    try:
        ch.command(f"DROP TABLE IF EXISTS `{db_name}`.`{table_name}`")
        print(f"[OK] Tabla `{db_name}`.`{table_name}` eliminada correctamente")
    except Exception as e:
        print(f"[ERROR] No se pudo eliminar la tabla: {e}")
        raise

# =========================
# MAIN
# =========================
def main():
    try:
        action, db_name, table_name = parse_args()
        
        ch = ch_client()
        print(f"[INFO] Conectado a ClickHouse: {CH_HOST}:{CH_PORT}")
        
        if action == "DATABASE":
            # Confirmar antes de eliminar base de datos
            print(f"\n[ADVERTENCIA] Estás a punto de eliminar la base de datos: `{db_name}`")
            print("              Esto eliminará TODAS las tablas y datos dentro de ella.")
            respuesta = input("              ¿Estás seguro? (escribe 'SI' para confirmar): ")
            
            if respuesta.strip().upper() != "SI":
                print("[CANCELADO] Operación cancelada por el usuario")
                return
            
            drop_database(ch, db_name)
        else:
            # TABLE
            drop_table(ch, db_name, table_name)
        
        print("\n[OK] Operación completada exitosamente")
        
    except KeyboardInterrupt:
        print("\n[CANCELADO] Operación cancelada por el usuario")
        sys.exit(1)
    except Exception as e:
        print(f"\n[ERROR] {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()
