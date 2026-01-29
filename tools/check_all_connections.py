#!/usr/bin/env python3
"""
Script para verificar todas las conexiones configuradas en el sistema ETL.

Verifica conexiones desde .env:
- SQL Server (con autenticación Windows o SQL Server)
- Snowflake
- ClickHouse

Autor: Herbert Poveda
Empresa: POM Cobranzas
Departamento: Business Intelligence (BI)
Fecha: 19 de enero de 2026
"""

import os
import re
import sys
from pathlib import Path
from typing import Dict, List, Tuple, Optional
from datetime import datetime

# Intentar cargar .env si existe
try:
    from dotenv import load_dotenv
    # Buscar .env en el directorio padre (etl/) primero, luego en tools/, luego búsqueda automática
    script_dir = Path(__file__).resolve().parent  # etl/tools/ (ruta absoluta)
    parent_dir = script_dir.parent                # etl/ (ruta absoluta)
    
    # Intentar primero en el directorio padre (etl/)
    parent_env = parent_dir / ".env"
    script_env = script_dir / ".env"
    
    # Debug: mostrar rutas que está buscando
    print(f"[DEBUG] Buscando .env en:")
    print(f"  - {parent_env} (existe: {parent_env.exists()})")
    print(f"  - {script_env} (existe: {script_env.exists()})")
    
    if parent_env.exists():
        env_path = str(parent_env)
        load_dotenv(env_path, override=True)
        print(f"[OK] Archivo .env cargado desde: {env_path}")
    # Si no existe, intentar en el directorio actual (tools/)
    elif script_env.exists():
        env_path = str(script_env)
        load_dotenv(env_path, override=True)
        print(f"[OK] Archivo .env cargado desde: {env_path}")
    else:
        # Si no se encuentra en ubicaciones específicas, usar load_dotenv() estándar
        # que busca desde el directorio actual hacia arriba
        result = load_dotenv(override=True)
        if result:
            print(f"[OK] Archivo .env cargado (búsqueda automática)")
        else:
            print(f"[WARN] No se encontró archivo .env")
            print(f"[DEBUG] script_dir={script_dir}")
            print(f"[DEBUG] parent_dir={parent_dir}")
except ImportError:
    print("[WARN] python-dotenv no está instalado. Las variables de entorno deben estar configuradas manualmente.")
except Exception as e:
    print(f"[WARN] Error al cargar .env: {e}")
    import traceback
    traceback.print_exc()

# ============== Imports condicionales ==============
try:
    import pyodbc
    HAS_PYODBC = True
except ImportError:
    HAS_PYODBC = False
    print("[WARN] pyodbc no está instalado. No se podrá probar SQL Server.")

try:
    import clickhouse_connect
    HAS_CLICKHOUSE = True
except ImportError:
    HAS_CLICKHOUSE = False
    print("[WARN] clickhouse-connect no está instalado. No se podrá probar ClickHouse.")

try:
    import snowflake.connector
    HAS_SNOWFLAKE = True
except ImportError:
    HAS_SNOWFLAKE = False
    print("[WARN] snowflake-connector-python no está instalado. No se podrá probar Snowflake.")

try:
    import pymysql
    HAS_MYSQL = True
    MYSQL_MODULE = "pymysql"
except ImportError:
    try:
        import mysql.connector
        HAS_MYSQL = True
        MYSQL_MODULE = "mysql.connector"
    except ImportError:
        HAS_MYSQL = False
        MYSQL_MODULE = None
        print("[WARN] pymysql o mysql-connector-python no está instalado. No se podrá probar MySQL.")


# ============== Resultados ==============
class ConnectionResult:
    def __init__(self, name: str, source: str):
        self.name = name
        self.source = source
        self.success = False
        self.error = None
        self.details = {}
        self.test_time = None
        self.databases_info = {}  # {db_name: table_count}

    def __str__(self):
        status = "[OK]" if self.success else "[ERROR]"
        result = f"{status} {self.name} ({self.source})"
        if not self.success and self.error:
            result += f"\n    Error: {self.error}"
        if self.details:
            details_str = ", ".join([f"{k}={v}" for k, v in self.details.items()])
            result += f"\n    {details_str}"
        if self.success and self.test_time:
            result += f"\n    Tiempo: {self.test_time:.2f}s"
        if self.success and self.databases_info:
            result += f"\n    Bases de datos: {len(self.databases_info)}"
            for db_name, table_count in sorted(self.databases_info.items()):
                result += f"\n      - {db_name}: {table_count} tablas"
        return result


results: List[ConnectionResult] = []


# ============== Funciones para listar bases de datos y tablas ==============
def get_mysql_databases(host: str, username: str, password: str) -> Dict[str, int]:
    """Obtiene lista de bases de datos MySQL y cuenta tablas en cada una."""
    databases_info = {}
    
    if not HAS_MYSQL:
        return databases_info
    
    try:
        if MYSQL_MODULE == "mysql.connector":
            import mysql.connector
            conn = mysql.connector.connect(
                host=host,
                user=username,
                password=password,
                connection_timeout=5
            )
        elif MYSQL_MODULE == "pymysql":
            import pymysql
            conn = pymysql.connect(
                host=host,
                user=username,
                password=password,
                connect_timeout=5
            )
        else:
            return databases_info
        
        cursor = conn.cursor()
        
        # Obtener lista de bases de datos
        cursor.execute("SHOW DATABASES")
        databases = [row[0] for row in cursor.fetchall()]
        
        # Para cada base de datos, contar tablas
        for db in databases:
            # Excluir bases de datos del sistema
            if db.lower() in ['information_schema', 'performance_schema', 'mysql', 'sys']:
                continue
            
            try:
                cursor.execute(f"USE `{db}`")
                cursor.execute("SHOW TABLES")
                tables = cursor.fetchall()
                databases_info[db] = len(tables)
            except:
                databases_info[db] = 0
        
        cursor.close()
        conn.close()
    except Exception:
        pass
    
    return databases_info


def get_sqlserver_databases(server: str, driver: str, use_windows_auth: bool, 
                           user: str = None, password: str = None) -> Dict[str, int]:
    """Obtiene lista de bases de datos SQL Server y cuenta tablas en cada una."""
    databases_info = {}
    
    if not HAS_PYODBC:
        return databases_info
    
    try:
        if use_windows_auth:
            conn_str = (
                f"DRIVER={{{driver}}};"
                f"SERVER={server};"
                "Trusted_Connection=yes;"
                "TrustServerCertificate=yes;"
            )
        else:
            conn_str = (
                f"DRIVER={{{driver}}};"
                f"SERVER={server};"
                f"UID={user};"
                f"PWD={password};"
                "TrustServerCertificate=yes;"
            )
        
        conn = pyodbc.connect(conn_str, timeout=5)
        cursor = conn.cursor()
        
        # Obtener lista de bases de datos
        cursor.execute("""
            SELECT name FROM sys.databases 
            WHERE name NOT IN ('master', 'tempdb', 'model', 'msdb')
            ORDER BY name
        """)
        databases = [row[0] for row in cursor.fetchall()]
        
        # Para cada base de datos, contar tablas
        for db in databases:
            try:
                cursor.execute(f"USE [{db}]")
                cursor.execute("""
                    SELECT COUNT(*) 
                    FROM INFORMATION_SCHEMA.TABLES 
                    WHERE TABLE_TYPE = 'BASE TABLE'
                """)
                table_count = cursor.fetchone()[0]
                databases_info[db] = table_count
            except:
                databases_info[db] = 0
        
        cursor.close()
        conn.close()
    except Exception:
        pass
    
    return databases_info


def get_snowflake_databases(account: str, user: str, password: str, 
                           role: str, warehouse: str) -> Dict[str, int]:
    """Obtiene lista de bases de datos Snowflake y cuenta tablas en cada una."""
    databases_info = {}
    
    if not HAS_SNOWFLAKE:
        return databases_info
    
    try:
        conn = snowflake.connector.connect(
            account=account,
            user=user,
            password=password,
            role=role,
            warehouse=warehouse,
            timeout=10
        )
        cursor = conn.cursor()
        
        # Obtener lista de bases de datos
        cursor.execute("SHOW DATABASES")
        databases = [row[1] for row in cursor.fetchall()]  # El nombre está en la columna 1
        
        # Para cada base de datos, contar tablas
        for db in databases:
            try:
                cursor.execute(f'USE DATABASE "{db}"')
                cursor.execute("SHOW TABLES")
                tables = cursor.fetchall()
                databases_info[db] = len(tables)
            except:
                databases_info[db] = 0
        
        cursor.close()
        conn.close()
    except Exception:
        pass
    
    return databases_info


def get_clickhouse_databases(host: str, port: int, user: str, password: str) -> Dict[str, int]:
    """Obtiene lista de bases de datos ClickHouse y cuenta tablas en cada una."""
    databases_info = {}
    
    if not HAS_CLICKHOUSE:
        return databases_info
    
    try:
        # Detectar si es HTTP (8123) o HTTPS (8443)
        # Puerto 8123 = HTTP, puerto 8443 = HTTPS
        is_secure = port == 8443
        
        client = clickhouse_connect.get_client(
            host=host,
            port=port,
            username=user,
            password=password,
            database="default",
            secure=is_secure,
            verify=is_secure  # Solo verificar SSL si es HTTPS
        )
        
        # Obtener lista de bases de datos
        result = client.query("SHOW DATABASES")
        databases = [row[0] for row in result.result_rows if row[0] not in ['system', 'information_schema', 'INFORMATION_SCHEMA']]
        
        # Para cada base de datos, contar tablas
        for db in databases:
            try:
                result = client.query(f"SELECT count() FROM system.tables WHERE database = '{db}'")
                table_count = result.result_rows[0][0] if result.result_rows else 0
                databases_info[db] = table_count
            except:
                databases_info[db] = 0
        
    except Exception:
        pass
    
    return databases_info


# ============== Funciones de prueba ==============
def test_sqlserver_python() -> ConnectionResult:
    """Prueba conexión SQL Server (Desarrollo) desde .env."""
    result = ConnectionResult("SQL Server", ".env")
    
    server = os.getenv("SQL_SERVER")
    database = os.getenv("SQL_DATABASE", "master")
    user = os.getenv("SQL_USER")
    password = os.getenv("SQL_PASSWORD")
    # Leer SQL_USE_WINDOWS_AUTH del .env (acepta "true", "yes", "1")
    use_windows_auth = os.getenv("SQL_USE_WINDOWS_AUTH", "false").lower() in ("true", "yes", "1")
    driver = os.getenv("SQL_DRIVER", "ODBC Driver 17 for SQL Server")
    
    if not server:
        result.error = "SQL_SERVER no configurado"
        return result
    
    # Mostrar tipo de autenticación en los detalles
    auth_type = "Windows" if use_windows_auth else "SQL Server"
    result.details = {"server": server, "database": database, "auth": auth_type}
    
    if not HAS_PYODBC:
        result.error = "pyodbc no está instalado"
        return result
    
    try:
        start = datetime.now()
        # Verificar driver
        drivers = pyodbc.drivers()
        if not drivers:
            result.error = "No se encontraron drivers ODBC"
            return result
        
        if driver not in drivers:
            # Buscar un driver alternativo
            preferred_drivers = [
                "ODBC Driver 18 for SQL Server",
                "ODBC Driver 17 for SQL Server",
                "ODBC Driver 13 for SQL Server",
                "SQL Server Native Client 11.0",
                "SQL Server"
            ]
            for d in preferred_drivers:
                if d in drivers:
                    driver = d
                    break
            if driver not in drivers:
                driver = drivers[0]
        
        if use_windows_auth:
            # Autenticación Windows (no requiere usuario/contraseña)
            conn_str = (
                f"DRIVER={{{driver}}};"
                f"SERVER={server};"
                f"DATABASE={database};"
                "Trusted_Connection=yes;"
                "TrustServerCertificate=yes;"
            )
        else:
            # Autenticación SQL Server (requiere usuario/contraseña)
            if not user or not password:
                result.error = "SQL_USER y SQL_PASSWORD requeridos (o definir SQL_USE_WINDOWS_AUTH=true en .env)"
                return result
            conn_str = (
                f"DRIVER={{{driver}}};"
                f"SERVER={server};"
                f"DATABASE={database};"
                f"UID={user};"
                f"PWD={password};"
                "TrustServerCertificate=yes;"
            )
            result.details["user"] = user
        
        result.details["driver"] = driver
        
        conn = pyodbc.connect(conn_str, timeout=5)
        cursor = conn.cursor()
        cursor.execute("SELECT 1")
        cursor.fetchone()
        cursor.close()
        conn.close()
        
        result.success = True
        result.test_time = (datetime.now() - start).total_seconds()
        
        # Obtener información de bases de datos
        try:
            result.databases_info = get_sqlserver_databases(server, driver, use_windows_auth, user, password)
        except:
            pass
    except Exception as e:
        result.error = str(e)
    
    return result


def test_sqlserver_prod_python() -> ConnectionResult:
    """Prueba conexión SQL Server (Producción) desde .env."""
    result = ConnectionResult("SQL Server (Producción)", ".env")
    
    server = os.getenv("SQL_SERVER_PROD")
    database = os.getenv("SQL_DATABASE", "master")
    user = os.getenv("SQL_USER_PROD")
    password = os.getenv("SQL_PASSWORD_PROD")
    driver = os.getenv("SQL_DRIVER", "ODBC Driver 17 for SQL Server")
    
    # Si no hay configuración de producción, omitir la prueba
    if not server or not user or not password:
        result.error = "SQL_SERVER_PROD, SQL_USER_PROD o SQL_PASSWORD_PROD no configurados (opcional)"
        return result
    
    result.details = {"server": server, "database": database, "auth": "SQL Server", "user": user}
    
    if not HAS_PYODBC:
        result.error = "pyodbc no está instalado"
        return result
    
    try:
        start = datetime.now()
        # Verificar driver
        drivers = pyodbc.drivers()
        if not drivers:
            result.error = "No se encontraron drivers ODBC"
            return result
        
        if driver not in drivers:
            # Buscar un driver alternativo
            preferred_drivers = [
                "ODBC Driver 18 for SQL Server",
                "ODBC Driver 17 for SQL Server",
                "ODBC Driver 13 for SQL Server",
                "SQL Server Native Client 11.0",
                "SQL Server"
            ]
            for d in preferred_drivers:
                if d in drivers:
                    driver = d
                    break
            if driver not in drivers:
                driver = drivers[0]
        
        # Producción siempre usa SQL Server Auth
        conn_str = (
            f"DRIVER={{{driver}}};"
            f"SERVER={server};"
            f"DATABASE={database};"
            f"UID={user};"
            f"PWD={password};"
            "TrustServerCertificate=yes;"
        )
        result.details["driver"] = driver
        
        conn = pyodbc.connect(conn_str, timeout=5)
        cursor = conn.cursor()
        cursor.execute("SELECT 1")
        cursor.fetchone()
        cursor.close()
        conn.close()
        
        result.success = True
        result.test_time = (datetime.now() - start).total_seconds()
        
        # Obtener información de bases de datos
        try:
            result.databases_info = get_sqlserver_databases(server, driver, False, user, password)
        except:
            pass
    except Exception as e:
        result.error = str(e)
    
    return result


def test_snowflake_python() -> ConnectionResult:
    """Prueba conexión Snowflake desde .env."""
    result = ConnectionResult("Snowflake", ".env")
    
    account = os.getenv("SF_ACCOUNT")
    user = os.getenv("SF_USER")
    password = os.getenv("SF_PASSWORD")
    role = os.getenv("SF_ROLE", "ACCOUNTADMIN")
    warehouse = os.getenv("SF_WAREHOUSE", "COMPUTE_WH")
    database = os.getenv("SF_DATABASE")
    schema = os.getenv("SF_SCHEMA", "RAW")
    
    if not account or not user or not password:
        result.error = "SF_ACCOUNT, SF_USER o SF_PASSWORD no configurados"
        return result
    
    result.details = {"account": account, "database": database or "N/A", "schema": schema, "user": user}
    
    if not HAS_SNOWFLAKE:
        result.error = "snowflake-connector-python no está instalado"
        return result
    
    try:
        start = datetime.now()
        conn = snowflake.connector.connect(
            account=account,
            user=user,
            password=password,
            role=role,
            warehouse=warehouse,
            database=database,
            schema=schema,
            timeout=10
        )
        cursor = conn.cursor()
        cursor.execute("SELECT 1")
        cursor.fetchone()
        cursor.close()
        conn.close()
        
        result.success = True
        result.test_time = (datetime.now() - start).total_seconds()
        
        # Obtener información de bases de datos
        try:
            result.databases_info = get_snowflake_databases(account, user, password, role, warehouse)
        except:
            pass
    except Exception as e:
        result.error = str(e)
    
    return result


def test_clickhouse_python() -> ConnectionResult:
    """Prueba conexión ClickHouse desde .env."""
    result = ConnectionResult("ClickHouse", ".env")
    
    host = os.getenv("CH_HOST")
    port = int(os.getenv("CH_PORT", "8443"))
    user = os.getenv("CH_USER", "default")
    password = os.getenv("CH_PASSWORD")
    database = os.getenv("CH_DATABASE", "default")
    
    if not host:
        result.error = "CH_HOST no configurado"
        return result
    
    if not password:
        result.error = "CH_PASSWORD no configurado (obligatorio)"
        return result
    
    # Detectar si es HTTP (8123) o HTTPS (8443)
    # Puerto 8123 = HTTP, puerto 8443 = HTTPS
    is_secure = port == 8443
    protocol = "HTTPS" if is_secure else "HTTP"
    
    result.details = {"host": host, "port": port, "protocol": protocol, "database": database, "user": user}
    
    if not HAS_CLICKHOUSE:
        result.error = "clickhouse-connect no está instalado"
        return result
    
    try:
        start = datetime.now()
        # Suprimir warnings de ClickHouse durante la conexión
        import warnings
        with warnings.catch_warnings():
            warnings.filterwarnings("ignore", category=UserWarning)
            warnings.filterwarnings("ignore", message=".*Http Driver.*")
            
            client = clickhouse_connect.get_client(
                host=host,
                port=port,
                username=user,
                password=password,
                database=database,
                secure=is_secure,
                verify=is_secure  # Solo verificar SSL si es HTTPS
            )
            result_query = client.query("SELECT 1")
        
        result.success = True
        result.test_time = (datetime.now() - start).total_seconds()
        
        # Obtener información de bases de datos
        try:
            result.databases_info = get_clickhouse_databases(host, port, user, password)
        except:
            pass
    except Exception as e:
        # Limpiar el mensaje de error si es muy largo
        error_msg = str(e)
        if "Unexpected Http Driver Exception" in error_msg:
            result.error = f"Error de conexión ClickHouse (puerto {port}, protocolo {protocol})"
        else:
            result.error = error_msg
    
    return result


# ============== Función principal ==============
def main():
    print("=" * 70)
    print("VERIFICACIÓN DE CONEXIONES - Sistema ETL POM")
    print("=" * 70)
    print()
    
    # Mostrar configuración desde .env
    sql_use_windows_auth = os.getenv("SQL_USE_WINDOWS_AUTH", "false").lower() in ("true", "yes", "1")
    sql_server = os.getenv("SQL_SERVER", "No configurado")
    print(f"[INFO] Configuración desde .env:")
    print(f"       SQL_SERVER: {sql_server}")
    print(f"       SQL_USE_WINDOWS_AUTH: {sql_use_windows_auth} ({'Windows Auth' if sql_use_windows_auth else 'SQL Server Auth'})")
    print()
    
    # Probar conexiones desde .env
    print("[INFO] Probando conexiones desde .env...")
    print()
    
    # SQL Server (Desarrollo)
    sqlserver_result = test_sqlserver_python()
    results.append(sqlserver_result)
    
    # SQL Server (Producción)
    sqlserver_prod_result = test_sqlserver_prod_python()
    results.append(sqlserver_prod_result)
    
    # Snowflake
    snowflake_result = test_snowflake_python()
    results.append(snowflake_result)
    
    # ClickHouse
    clickhouse_result = test_clickhouse_python()
    results.append(clickhouse_result)
    
    # Mostrar resultados
    print("=" * 70)
    print("RESULTADOS")
    print("=" * 70)
    print()
    
    successful = 0
    failed = 0
    
    for result in results:
        print(result)
        print()
        if result.success:
            successful += 1
        else:
            failed += 1
    
    # Resumen
    print("=" * 70)
    print(f"Resumen: {successful} exitosas, {failed} fallidas de {len(results)} conexiones")
    print("=" * 70)
    
    return 0 if failed == 0 else 1


if __name__ == "__main__":
    sys.exit(main())
