#!/usr/bin/env python3
"""
ETL que ejecuta pipelines de Phoenix.
Conecta a la DB Phoenix (MySQL), consulta la config del pipeline,
ejecuta el reporte en el origen e inserta en el destino (MySQL o ClickHouse).

Origen soportado: MySQL, ClickHouse.
Destino soportado: MySQL, ClickHouse.

Configuración: archivo .env en etl/ con PHOENIX_DB_HOST, PHOENIX_DB_USER, etc.
Si Phoenix está en otro servidor: PHOENIX_DB_HOST=phoenix.pomcr.local

Uso:
  python phoenix_pipeline_run.py 1           # Por PipelinesId
  python phoenix_pipeline_run.py --id 561    # Por ReportsId
  python phoenix_pipeline_run.py --list      # Listar pipelines activos
"""
import os
import sys
import re
from pathlib import Path
from datetime import datetime

# Cargar .env desde etl/ o tools/
try:
    from dotenv import load_dotenv
    script_dir = Path(__file__).resolve().parent
    for env_path in [script_dir.parent / ".env", script_dir / ".env"]:
        if env_path.exists():
            load_dotenv(env_path, override=True)
            break
    else:
        load_dotenv(override=True)
except ImportError:
    pass

try:
    import pymysql
except ImportError:
    print("[ERROR] Falta pymysql. Instala: pip install pymysql")
    sys.exit(1)

# ClickHouse opcional (solo si el destino es ClickHouse)
try:
    import clickhouse_connect
    HAS_CLICKHOUSE = True
except ImportError:
    HAS_CLICKHOUSE = False

# ============== Phoenix MySQL config ==============
PHOENIX_HOST = os.getenv("PHOENIX_DB_HOST", os.getenv("MYSQL_HOST", "localhost"))
PHOENIX_PORT = int(os.getenv("PHOENIX_DB_PORT", os.getenv("MYSQL_PORT", "3306")))
PHOENIX_USER = os.getenv("PHOENIX_DB_USER", os.getenv("MYSQL_USER", "root"))
PHOENIX_PASS = os.getenv("PHOENIX_DB_PASS", os.getenv("MYSQL_PASSWORD", ""))
PHOENIX_DB = os.getenv("PHOENIX_DB_NAME", "phoenix")


def connect_phoenix():
    """Conecta a la base de datos Phoenix."""
    try:
        return pymysql.connect(
            host=PHOENIX_HOST,
            port=PHOENIX_PORT,
            user=PHOENIX_USER,
            password=PHOENIX_PASS,
            database=PHOENIX_DB,
            charset="utf8mb4",
            cursorclass=pymysql.cursors.DictCursor,
        )
    except pymysql.err.OperationalError as e:
        if "Connection refused" in str(e) or "2003" in str(e):
            raise RuntimeError(
                f"No se pudo conectar a Phoenix MySQL en {PHOENIX_HOST}:{PHOENIX_PORT}. "
                f"Configura PHOENIX_DB_HOST en .env (ej: PHOENIX_DB_HOST=phoenix.pomcr.local). "
                f"Error: {e}"
            ) from e
        raise


def get_connection(conn_phoenix, connection_id):
    """Obtiene los datos de una conexión desde la tabla connections."""
    with conn_phoenix.cursor() as cur:
        cur.execute(
            "SELECT ConnectionId, Title, Connector, Hostname, Port, Username, Password, ServiceName, `Schema` "
            "FROM connections WHERE ConnectionId = %s AND Status = 1",
            (connection_id,),
        )
        return cur.fetchone()


def get_pipeline_by_id(conn_phoenix, pipelines_id):
    """Obtiene pipeline + reporte + conexiones por PipelinesId."""
    q = """
    SELECT p.PipelinesId, p.ReportsId, p.ConnSource, p.TableSource, p.SchemaSource,
           p.SchemaCreate, p.TableCreate, p.TableTruncate, p.TimeStamp, p.RecordsAlert,
           r.Title, r.ConnectionId, r.Query,
           src.Hostname AS src_host, src.Port AS src_port, src.Username AS src_user,
           src.Password AS src_pass, src.ServiceName AS src_service, src.`Schema` AS src_schema, src.Connector AS src_connector,
           dst.Hostname AS dst_host, dst.Port AS dst_port, dst.Username AS dst_user,
           dst.Password AS dst_pass, dst.ServiceName AS dst_service, dst.`Schema` AS dst_schema, dst.Connector AS dst_connector
    FROM pipelines p
    INNER JOIN reports r ON r.ReportsId = p.ReportsId
    LEFT JOIN connections src ON src.ConnectionId = r.ConnectionId
    LEFT JOIN connections dst ON dst.ConnectionId = p.ConnSource
    WHERE p.PipelinesId = %s AND p.Status = 1 AND r.Status = 1
    """
    with conn_phoenix.cursor() as cur:
        cur.execute(q, (pipelines_id,))
        return cur.fetchone()


def get_pipeline_by_reports_id(conn_phoenix, reports_id):
    """Obtiene pipeline + reporte por ReportsId."""
    q = """
    SELECT p.PipelinesId, p.ReportsId, p.ConnSource, p.TableSource, p.SchemaSource,
           p.SchemaCreate, p.TableCreate, p.TableTruncate, p.TimeStamp, p.RecordsAlert,
           r.Title, r.ConnectionId, r.Query,
           src.Hostname AS src_host, src.Port AS src_port, src.Username AS src_user,
           src.Password AS src_pass, src.ServiceName AS src_service, src.`Schema` AS src_schema, src.Connector AS src_connector,
           dst.Hostname AS dst_host, dst.Port AS dst_port, dst.Username AS dst_user,
           dst.Password AS dst_pass, dst.ServiceName AS dst_service, dst.`Schema` AS dst_schema, dst.Connector AS dst_connector
    FROM pipelines p
    INNER JOIN reports r ON r.ReportsId = p.ReportsId
    LEFT JOIN connections src ON src.ConnectionId = r.ConnectionId
    LEFT JOIN connections dst ON dst.ConnectionId = p.ConnSource
    WHERE p.ReportsId = %s AND p.Status = 1 AND r.Status = 1
    """
    with conn_phoenix.cursor() as cur:
        cur.execute(q, (reports_id,))
        return cur.fetchone()


def list_pipelines(conn_phoenix):
    """Lista pipelines activos."""
    q = """
    SELECT p.PipelinesId, p.ReportsId, b.Title AS ReportTitle, c.Title AS DestTitle
    FROM pipelines p
    INNER JOIN reports b ON b.ReportsId = p.ReportsId
    LEFT JOIN connections c ON c.ConnectionId = p.ConnSource
    WHERE p.Status = 1 AND b.Status = 1
    ORDER BY b.Title
    """
    with conn_phoenix.cursor() as cur:
        cur.execute(q)
        return cur.fetchall()


def get_db_from_conn(row, prefix):
    """Obtiene database/schema de una fila de conexión."""
    schema = row.get(f"{prefix}_schema") or ""
    service = row.get(f"{prefix}_service") or ""
    return (schema or service or "default").strip()


def get_port_from_conn(row, prefix, default_mysql=3306, default_ch=8443):
    """Obtiene puerto. Si está vacío, usa default según connector."""
    port = row.get(f"{prefix}_port")
    connector = (row.get(f"{prefix}_connector") or "").lower()
    if port and str(port).strip():
        try:
            return int(port)
        except (ValueError, TypeError):
            pass
    return default_ch if "clickhouse" in connector else default_mysql


def snake_case(text):
    """Convierte título a snake_case."""
    if not text:
        return ""
    text = re.sub(r"[^\w\s]", "", str(text))
    text = re.sub(r"\s+", " ", text).strip()
    return re.sub(r"\s+", "_", text).lower()


def _host(pipe, prefix):
    h = pipe.get(f"{prefix}_host") or ""
    return h.strip() if h else "localhost"


def fetch_source_data(pipe):
    """Ejecuta la query del reporte en el origen y devuelve (headers, rows)."""
    connector = (pipe.get("src_connector") or "mysqli").lower()
    host = _host(pipe, "src")
    port = get_port_from_conn(pipe, "src")
    user = pipe.get("src_user") or "root"
    password = pipe.get("src_pass") or ""
    database = get_db_from_conn(pipe, "src") or "default"
    query = (pipe.get("Query") or "").strip()

    if not query:
        raise ValueError("El reporte no tiene Query definida")

    if "clickhouse" in connector:
        if not HAS_CLICKHOUSE:
            raise RuntimeError("Destino ClickHouse: instala clickhouse-connect (pip install clickhouse-connect)")
        client = clickhouse_connect.get_client(
            host=host, port=port, username=user, password=password,
            database=database, secure=(port == 8443), verify=False
        )
        result = client.query(query)
        headers = [col[0] for col in result.column_names] if result.column_names else []
        rows = [list(r) for r in result.result_rows] if result.result_rows else []
        return headers, rows

    # MySQL
    conn = pymysql.connect(
        host=host, port=port, user=user, password=password, database=database,
        charset="utf8mb4", cursorclass=pymysql.cursors.DictCursor
    )
    try:
        with conn.cursor() as cur:
            cur.execute(query)
            rows = cur.fetchall()
        headers = list(rows[0].keys()) if rows else []
        return headers, [list(r.values()) for r in rows]
    finally:
        conn.close()


def insert_to_mysql(pipe, headers, rows, table_name):
    """Inserta datos en MySQL/MariaDB."""
    db = (pipe.get("SchemaSource") or "").strip() or get_db_from_conn(pipe, "dst") or "default"
    conn = pymysql.connect(
        host=_host(pipe, "dst"),
        port=get_port_from_conn(pipe, "dst"),
        user=pipe.get("dst_user") or "root",
        password=pipe.get("dst_pass") or "",
        database=db,
        charset="utf8mb4",
    )
    try:
        truncate = pipe.get("TableTruncate", 1)
        add_ts = pipe.get("TimeStamp", 0)

        with conn.cursor() as cur:
            if truncate:
                try:
                    cur.execute(f"TRUNCATE TABLE `{table_name}`")
                    conn.commit()
                except pymysql.Error:
                    conn.rollback()
            cols = ", ".join(f"`{h}`" for h in headers)
            if add_ts:
                cols += ", created_at, updated_at"
            placeholders = ", ".join(["%s"] * (len(headers) + (2 if add_ts else 0)))
            ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            insert_sql = f"INSERT INTO `{table_name}` ({cols}) VALUES ({placeholders})"
            for row in rows:
                r = list(row)
                if add_ts:
                    r.extend([ts, ts])
                cur.execute(insert_sql, r)
            conn.commit()
            return len(rows)
    finally:
        conn.close()


def insert_to_clickhouse(pipe, headers, rows, table_name):
    """Inserta datos en ClickHouse."""
    if not HAS_CLICKHOUSE:
        raise RuntimeError("Instala clickhouse-connect: pip install clickhouse-connect")
    db = (pipe.get("SchemaSource") or "").strip() or get_db_from_conn(pipe, "dst") or "default"
    client = clickhouse_connect.get_client(
        host=_host(pipe, "dst"),
        port=get_port_from_conn(pipe, "dst"),
        username=pipe.get("dst_user") or "default",
        password=pipe.get("dst_pass") or "",
        database=db,
        secure=True,
        verify=False,
    )
    client.insert(table_name, rows, column_names=headers)
    return len(rows)


def update_last_execution(conn_phoenix, reports_id):
    """Actualiza LastExecution en pipelines."""
    with conn_phoenix.cursor() as cur:
        cur.execute(
            "UPDATE pipelines SET LastExecution = NOW() WHERE ReportsId = %s",
            (reports_id,),
        )
        conn_phoenix.commit()


def run_pipeline(pipelines_id=None, reports_id=None):
    """Ejecuta un pipeline."""
    conn = connect_phoenix()
    try:
        if pipelines_id:
            pipe = get_pipeline_by_id(conn, pipelines_id)
        elif reports_id:
            pipe = get_pipeline_by_reports_id(conn, reports_id)
        else:
            print("[ERROR] Indica PipelinesId o ReportsId")
            return 1

        if not pipe:
            print("[ERROR] Pipeline no encontrado o inactivo")
            return 1

        reports_id = pipe["ReportsId"]
        table_name = pipe.get("TableSource") or snake_case(f"{reports_id} {pipe.get('Title', '')}")
        print(f"[Pipeline {pipe['PipelinesId']}] Reporte {reports_id} -> {table_name}")

        t0 = datetime.now()
        headers, rows = fetch_source_data(pipe)
        t1 = datetime.now()
        print(f"  Consulta origen: {len(rows)} filas en {(t1-t0).total_seconds():.2f}s")

        if not rows:
            print("[AVISO] No hay datos para insertar")
            update_last_execution(conn, reports_id)
            return 0

        connector = (pipe.get("dst_connector") or "mysqli").lower()
        t2 = datetime.now()
        if "clickhouse" in connector:
            insert_to_clickhouse(pipe, headers, rows, table_name)
        else:
            insert_to_mysql(pipe, headers, rows, table_name)
        t3 = datetime.now()
        print(f"  Insertado: {(t3-t2).total_seconds():.2f}s")

        update_last_execution(conn, reports_id)
        print(f"[OK] Pipeline completado en {(t3-t0).total_seconds():.2f}s")
        return 0
    except Exception as e:
        print(f"[ERROR] {e}")
        return 1
    finally:
        conn.close()


def main():
    pipelines_id = None
    reports_id = None

    args = sys.argv[1:]
    if "--list" in args:
        conn = connect_phoenix()
        for p in list_pipelines(conn):
            print(f"  {p['PipelinesId']}: Reporte {p['ReportsId']} - {p['ReportTitle']} -> {p.get('DestTitle') or 'N/A'}")
        conn.close()
        return 0
    if "--id" in args:
        i = args.index("--id")
        if i + 1 < len(args):
            reports_id = int(args[i + 1])
    if not reports_id and args:
        try:
            pipelines_id = int(args[0])
        except (ValueError, IndexError):
            pass

    return run_pipeline(pipelines_id=pipelines_id, reports_id=reports_id)


if __name__ == "__main__":
    sys.exit(main())
