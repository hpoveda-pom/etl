import getpass
import os
import sys

import pyodbc


def ask_text(prompt: str, default: str | None = None, allow_empty: bool = False) -> str:
    suffix = f" [{default}]" if default else ""
    value = input(f"{prompt}{suffix}: ").strip()
    if not value:
        if default is not None:
            return default
        if allow_empty:
            return ""
    return value


def ask_yes_no(prompt: str, default_yes: bool = True) -> bool:
    default_label = "S" if default_yes else "N"
    value = input(f"{prompt} (S/N) [{default_label}]: ").strip().lower()
    if not value:
        return default_yes
    return value in {"s", "si", "sí", "y", "yes"}


def pick_driver(requested_driver: str | None) -> str:
    available = list(pyodbc.drivers())
    if not available:
        raise RuntimeError("No se encontraron drivers ODBC en el sistema.")

    if requested_driver and requested_driver in available:
        return requested_driver

    preferred = [
        "ODBC Driver 18 for SQL Server",
        "ODBC Driver 17 for SQL Server",
        "ODBC Driver 13 for SQL Server",
        "SQL Server Native Client 11.0",
        "SQL Server",
    ]
    for driver in preferred:
        if driver in available:
            if requested_driver:
                print(f"[WARN] Driver '{requested_driver}' no encontrado. Usando '{driver}'.")
            return driver

    # Fallback al primer driver disponible
    if requested_driver:
        print(f"[WARN] Driver '{requested_driver}' no encontrado. Usando '{available[0]}'.")
    return available[0]


def build_connection_string(
    server: str,
    database: str,
    driver: str,
    use_windows_auth: bool,
    user: str | None = None,
    password: str | None = None,
) -> str:
    if use_windows_auth:
        return (
            f"DRIVER={{{driver}}};"
            f"SERVER={server};"
            f"DATABASE={database};"
            "Trusted_Connection=yes;"
            "TrustServerCertificate=yes;"
        )

    if not user or not password:
        raise RuntimeError("Falta usuario o password para autenticacion SQL Server.")

    return (
        f"DRIVER={{{driver}}};"
        f"SERVER={server};"
        f"DATABASE={database};"
        f"UID={user};"
        f"PWD={password};"
        "TrustServerCertificate=yes;"
    )


def main() -> int:
    print("=== Test de conexion a SQL Server ===")

    default_server = os.getenv("SQL_SERVER", "localhost")
    default_db = os.getenv("SQL_DATABASE", "master")
    default_user = os.getenv("SQL_USER", "")
    default_pass = os.getenv("SQL_PASSWORD", "")
    default_driver = os.getenv("SQL_DRIVER", "ODBC Driver 17 for SQL Server")

    server = ask_text("Host o servidor (ej: HOST\\INSTANCIA o HOST,PUERTO)", default_server)
    database = ask_text("Base de datos", default_db)
    use_windows_auth = ask_yes_no("Usar autenticacion Windows", default_yes=True)

    user = None
    password = None
    if not use_windows_auth:
        user = ask_text("Usuario SQL", default_user)
        if default_pass:
            password = getpass.getpass("Password SQL [usa env SQL_PASSWORD si dejas vacio]: ").strip()
            if not password:
                password = default_pass
        else:
            password = getpass.getpass("Password SQL: ").strip()

    driver = pick_driver(default_driver)

    try:
        conn_str = build_connection_string(
            server=server,
            database=database,
            driver=driver,
            use_windows_auth=use_windows_auth,
            user=user,
            password=password,
        )
        print(f"[INFO] Driver: {driver}")
        print(f"[INFO] Server: {server}")
        print(f"[INFO] Database: {database}")
        print(f"[INFO] Auth: {'Windows' if use_windows_auth else 'SQL Server'}")

        with pyodbc.connect(conn_str, timeout=10) as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT 1")
            cursor.fetchone()

        print("[OK] Conexion exitosa.")
        return 0
    except pyodbc.Error as exc:
        print(f"[ERROR] Fallo de conexion: {exc}")
        return 1
    except Exception as exc:
        print(f"[ERROR] Error inesperado: {exc}")
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
