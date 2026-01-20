# Suite ETL - Procesamiento de Datos

Suite completa de herramientas ETL para procesar datos desde múltiples fuentes (SQL Server, Excel) hacia diferentes destinos (CSV, ClickHouse, Snowflake).

---

## Información del Proyecto

- **Autor**: Herbert Poveda
- **Empresa**: POM Cobranzas
- **Departamento**: Business Intelligence (BI)
- **Fecha**: 19 de enero de 2026
- **Versión**: 1.0.0

---

## Tabla de Contenidos

- [Descripción General](#descripción-general)
- [Requisitos](#requisitos)
- [Instalación](#instalación)
- [Estructura de Carpetas](#estructura-de-carpetas)
- [Variables de Entorno](#variables-de-entorno)
- [Scripts ETL](#scripts-etl)
  - [sqlserver_to_csv.py](#1-sqlserver_to_csvpy)
  - [excel_to_csv.py](#2-excel_to_csvpy)
  - [compress_csv_to_gz.py](#3-compress_csv_to_gzpy)
  - [csv_to_snowflake.py](#4-csv_to_snowflakepy)
  - [csv_to_clickhouse.py](#5-csv_to_clickhousepy)
  - [ingest_all_excels_to_stage.py](#6-ingest_all_excels_to_stagepy)
  - [snowflake_csv_to_tables.py](#7-snowflake_csv_to_tablespy)
- [Flujos de Trabajo Comunes](#flujos-de-trabajo-comunes)
- [Troubleshooting](#troubleshooting)
- [Changelog](#changelog)

---

## Descripción General

Esta suite ETL proporciona herramientas para:

- **Extracción**: Exportar datos desde SQL Server y Excel
- **Transformación**: Convertir formatos, comprimir archivos, filtrar datos
- **Carga**: Importar datos a ClickHouse y Snowflake

### Características Principales

- Exportación masiva de tablas SQL Server a CSV
- Conversión de archivos Excel a CSV
- Compresión de CSV a formato GZ
- Carga automática a Snowflake y ClickHouse
- Filtrado flexible por tablas, carpetas y archivos
- Manejo robusto de errores con reintentos
- Logging detallado de operaciones
- Sanitización automática de nombres

---

## Requisitos

### Requisitos Generales

- **Python 3.7+**
- **Sistema Operativo**: Windows (principalmente), Linux/Mac compatible

### Dependencias Python

```bash
pip install pandas pyodbc openpyxl clickhouse-connect snowflake-connector-python
```

O instalar todas de una vez:

```bash
pip install -r requirements.txt
```

### Requisitos Adicionales por Script

| Script | Requisitos Adicionales |
|--------|----------------------|
| `sqlserver_to_csv.py` | Driver ODBC para SQL Server |
| `excel_to_csv.py` | openpyxl (incluido en dependencias) |
| `csv_to_snowflake.py` | Cuenta de Snowflake |
| `csv_to_clickhouse.py` | Cuenta de ClickHouse Cloud |
| `ingest_all_excels_to_stage.py` | Cuenta de Snowflake |

### Drivers ODBC para SQL Server

Necesitas uno de los siguientes drivers instalados:
- ODBC Driver 17 for SQL Server (recomendado)
- ODBC Driver 13 for SQL Server
- SQL Server Native Client 11.0
- SQL Server

---

## Instalación

1. **Clonar o descargar el repositorio**

2. **Instalar dependencias**:
```bash
pip install pandas pyodbc openpyxl clickhouse-connect snowflake-connector-python
```

3. **Configurar variables de entorno** (ver sección [Variables de Entorno](#variables-de-entorno))

4. **Crear estructura de carpetas** (se crean automáticamente, pero puedes crearlas manualmente):
```
UPLOADS/
└── POM_DROP/
    ├── inbox/          # Archivos Excel a procesar
    ├── processed/      # Archivos procesados exitosamente
    ├── error/          # Archivos con errores
    ├── csv_staging/    # CSV generados (intermedio)
    ├── csv_processed/  # CSV procesados exitosamente
    └── csv_error/      # CSV con errores
```

---

## Estructura de Carpetas

El sistema utiliza la siguiente estructura de carpetas (configurable mediante variables de entorno):

```
UPLOADS/POM_DROP/
├── inbox/              # Archivos Excel (.xlsx) a procesar
├── processed/          # Archivos Excel procesados exitosamente
├── error/              # Archivos Excel con errores
├── csv_staging/        # CSV generados (punto intermedio)
│   ├── SQLSERVER_*     # Carpetas con CSV de SQL Server
│   └── [nombre_excel] # Carpetas con CSV de Excel
├── csv_processed/      # Carpetas de CSV procesadas exitosamente
└── csv_error/          # Carpetas de CSV con errores
```

### Flujo de Archivos

1. **Excel → CSV**: `inbox/` → `csv_staging/` → `processed/` o `error/`
2. **SQL Server → CSV**: `csv_staging/SQLSERVER_*/` → (procesamiento) → `csv_processed/` o `csv_error/`
3. **CSV → Cloud**: `csv_staging/` → (carga) → `csv_processed/` o `csv_error/`

---

## Variables de Entorno

### Variables Comunes (Carpetas)

| Variable | Descripción | Valor por Defecto |
|----------|-------------|-------------------|
| `INBOX_DIR` | Carpeta de archivos Excel a procesar | `UPLOADS\POM_DROP\inbox` |
| `PROCESSED_DIR` | Carpeta de archivos procesados | `UPLOADS\POM_DROP\processed` |
| `ERROR_DIR` | Carpeta de archivos con errores | `UPLOADS\POM_DROP\error` |
| `CSV_STAGING_DIR` | Carpeta de CSV intermedios | `UPLOADS\POM_DROP\csv_staging` |
| `CSV_PROCESSED_DIR` | Carpeta de CSV procesados | `UPLOADS\POM_DROP\csv_processed` |
| `CSV_ERROR_DIR` | Carpeta de CSV con errores | `UPLOADS\POM_DROP\csv_error` |

### Variables de SQL Server

| Variable | Descripción | Valor por Defecto |
|----------|-------------|-------------------|
| `SQL_SERVER` | Servidor SQL Server | `SRV-DESA\SQLEXPRESS` |
| `SQL_DATABASE` | Nombre de la base de datos | *(requerido)* |
| `SQL_USER` | Usuario SQL (opcional, para autenticación SQL) | *(vacío - usa Windows)* |
| `SQL_PASSWORD` | Contraseña SQL (opcional) | *(vacío - usa Windows)* |
| `SQL_DRIVER` | Driver ODBC a usar | `ODBC Driver 17 for SQL Server` |
| `TABLES_FILTER` | Tablas a exportar (separadas por coma) | *(vacío - todas)* |

### Variables de Snowflake

| Variable | Descripción | Valor por Defecto |
|----------|-------------|-------------------|
| `SF_ACCOUNT` | Cuenta de Snowflake | `fkwugeu-qic97823` |
| `SF_USER` | Usuario de Snowflake | `HPOVEDAPOMCR` |
| `SF_PASSWORD` | Contraseña de Snowflake | *(requerido)* |
| `SF_ROLE` | Rol de Snowflake | `ACCOUNTADMIN` |
| `SF_WAREHOUSE` | Warehouse de Snowflake | `COMPUTE_WH` |
| `SF_DATABASE` | Base de datos de Snowflake | `POM_TEST01` |
| `SF_SCHEMA` | Schema de Snowflake | `RAW` |

### Variables de ClickHouse

| Variable | Descripción | Valor por Defecto |
|----------|-------------|-------------------|
| `CH_HOST` | Host de ClickHouse Cloud | `f4rf85ygzj.eastus2.azure.clickhouse.cloud` |
| `CH_PORT` | Puerto de ClickHouse | `8443` |
| `CH_USER` | Usuario de ClickHouse | `default` |
| `CH_PASSWORD` | Contraseña de ClickHouse | *(requerido)* |
| `CH_DATABASE` | Base de datos de ClickHouse | `default` |
| `CH_TABLE` | Tabla destino (opcional) | *(vacío)* |

### Variables de Filtrado

| Variable | Descripción | Valor por Defecto |
|----------|-------------|-------------------|
| `FOLDERS_FILTER` | Carpetas a procesar (separadas por coma) | *(vacío - todas)* |
| `CSV_FILTER` | CSV a procesar (separadas por coma, sin extensión) | *(vacío - todos)* |
| `SHEETS_ALLOWLIST` | Hojas de Excel a procesar (separadas por coma) | *(vacío - todas)* |
| `DELETE_ORIGINALS` | Eliminar CSV originales después de comprimir (`true`/`false`) | `false` |

### Configuración en Windows (PowerShell)

```powershell
# SQL Server
$env:SQL_SERVER="SRV-DESA\SQLEXPRESS"
$env:SQL_DATABASE="MiBaseDeDatos"
$env:SQL_USER="usuario"  # Opcional
$env:SQL_PASSWORD="contraseña"  # Opcional

# Snowflake
$env:SF_ACCOUNT="fkwugeu-qic97823"
$env:SF_USER="HPOVEDAPOMCR"
$env:SF_PASSWORD="tu_contraseña"
$env:SF_DATABASE="POM_TEST01"
$env:SF_SCHEMA="RAW"

# ClickHouse
$env:CH_HOST="f4rf85ygzj.eastus2.azure.clickhouse.cloud"
$env:CH_USER="default"
$env:CH_PASSWORD="tu_contraseña"
$env:CH_DATABASE="default"

# Carpetas
$env:CSV_STAGING_DIR="UPLOADS\POM_DROP\csv_staging"
$env:INBOX_DIR="UPLOADS\POM_DROP\inbox"
```

### Configuración en Windows (CMD)

```cmd
set SQL_SERVER=SRV-DESA\SQLEXPRESS
set SQL_DATABASE=MiBaseDeDatos
set SF_PASSWORD=tu_contraseña
set CH_PASSWORD=tu_contraseña
```

### Configuración en Linux/Mac

```bash
export SQL_SERVER="SRV-DESA\\SQLEXPRESS"
export SQL_DATABASE="MiBaseDeDatos"
export SF_PASSWORD="tu_contraseña"
export CH_PASSWORD="tu_contraseña"
```

---

## Scripts ETL

### 1. sqlserver_to_csv.py

**Versión**: 1.0.0  
**Descripción**: Exporta tablas de SQL Server a archivos CSV.

**Requisitos**:
- Driver ODBC para SQL Server
- Acceso a SQL Server (Windows Auth o SQL Auth)

**Variables de Entorno**:
- `SQL_SERVER`, `SQL_DATABASE`, `SQL_USER`, `SQL_PASSWORD`, `SQL_DRIVER`
- `CSV_STAGING_DIR`
- `TABLES_FILTER` (opcional)

**Configuración en el Código**:
```python
EXCLUDED_TABLE_PREFIXES = ["TMP_"]  # Excluir tablas que empiecen con "TMP_"
```

**Uso**:

```bash
# Exportar todas las tablas de una base de datos
python sqlserver_to_csv.py MiBaseDeDatos

# Exportar tablas específicas
python sqlserver_to_csv.py MiBaseDeDatos Tabla1,Tabla2,Tabla3

# Usando variables de entorno
$env:SQL_DATABASE="MiBaseDeDatos"
python sqlserver_to_csv.py
```

**Características**:
- Autenticación Windows (por defecto) o SQL Server
- Detección automática de drivers ODBC
- Exclusión de tablas por prefijos
- Filtrado de tablas específicas
- Reintentos automáticos en caso de pérdida de conexión
- Sanitización automática de nombres de archivos

**Salida**: Archivos CSV en `CSV_STAGING_DIR/SQLSERVER_[nombre_base_datos]/`

---

### 2. excel_to_csv.py

**Versión**: 1.0.0  
**Descripción**: Convierte archivos Excel (.xlsx) a CSV. Cada hoja del Excel se convierte en un archivo CSV separado.

**Requisitos**:
- `openpyxl` (incluido en dependencias)

**Variables de Entorno**:
- `INBOX_DIR`, `PROCESSED_DIR`, `ERROR_DIR`
- `CSV_STAGING_DIR`
- `SHEETS_ALLOWLIST` (opcional)

**Uso**:

```bash
# Procesar todos los Excel en inbox
python excel_to_csv.py

# Filtrar hojas específicas
$env:SHEETS_ALLOWLIST="Hoja1,Hoja2"
python excel_to_csv.py
```

**Características**:
- Procesa todos los archivos `.xlsx` en `INBOX_DIR`
- Cada hoja del Excel se convierte en un CSV separado
- Crea una carpeta con el nombre del Excel (sanitizado)
- Mueve archivos a `processed/` o `error/` según resultado
- Manejo de archivos bloqueados con reintentos

**Salida**: 
- Archivos CSV en `CSV_STAGING_DIR/[nombre_excel]/[hoja].csv`
- Excel original movido a `processed/` o `error/`

---

### 3. compress_csv_to_gz.py

**Versión**: 1.0.0  
**Descripción**: Comprime archivos CSV a formato `.csv.gz` para reducir el tamaño.

**Requisitos**: Ninguno adicional

**Variables de Entorno**:
- `CSV_STAGING_DIR`
- `FOLDERS_FILTER` (opcional)
- `CSV_FILTER` (opcional)
- `DELETE_ORIGINALS` (opcional, `true`/`false`)

**Uso**:

```bash
# Comprimir todos los CSV en carpetas SQLSERVER_*
python compress_csv_to_gz.py

# Comprimir CSV de una carpeta específica
python compress_csv_to_gz.py SQLSERVER_POM_Aplicaciones

# Comprimir CSV específicos
python compress_csv_to_gz.py SQLSERVER_POM_Aplicaciones ResutadoNotificar,Bitacora

# Comprimir y eliminar CSV originales (forma corta - omitiendo filtro de CSV)
python compress_csv_to_gz.py SQLSERVER_POM_Aplicaciones true

# Comprimir y eliminar CSV originales (forma explícita)
python compress_csv_to_gz.py SQLSERVER_POM_Aplicaciones "" true

# Comprimir CSV específicos y eliminar originales
python compress_csv_to_gz.py SQLSERVER_POM_Aplicaciones ResutadoNotificar,Bitacora true
```

**Características**:
- Comprime CSV a formato GZ (compatible con Snowflake y ClickHouse)
- Muestra ratio de compresión
- Opción para eliminar CSV originales después de comprimir exitosamente
- Filtrado por carpetas y archivos
- Detecta automáticamente si el segundo argumento es un valor booleano (permite omitir el filtro de CSV)
- Mensajes informativos cuando no hay archivos CSV para comprimir o ya están comprimidos
- Omite archivos que ya tienen su versión `.csv.gz` (evita duplicados)

**Notas**:
- Si un archivo ya tiene su versión `.csv.gz`, no se procesa ni se elimina el original
- Los CSV originales solo se eliminan después de comprimirse exitosamente
- Puedes omitir el filtro de CSV pasando directamente `true` como segundo argumento

**Salida**: Archivos `.csv.gz` en las mismas carpetas (o CSV originales eliminados si `DELETE_ORIGINALS=true`)

---

### 4. csv_to_snowflake.py

**Versión**: 1.0.0  
**Descripción**: Carga archivos CSV (o CSV.gz) desde `csv_staging` a Snowflake.

**Requisitos**:
- Cuenta de Snowflake
- `snowflake-connector-python`

**Variables de Entorno**:
- `SF_ACCOUNT`, `SF_USER`, `SF_PASSWORD`, `SF_ROLE`, `SF_WAREHOUSE`
- `SF_DATABASE`, `SF_SCHEMA`
- `CSV_STAGING_DIR`, `CSV_PROCESSED_DIR`, `CSV_ERROR_DIR`
- `FOLDERS_FILTER`, `CSV_FILTER` (opcional)

**Uso**:

```bash
# Cargar todos los CSV a Snowflake
python csv_to_snowflake.py

# Especificar base de datos y schema
python csv_to_snowflake.py POM_TEST01 RAW

# Filtrar por carpeta
python csv_to_snowflake.py POM_TEST01 RAW SQLSERVER_POM_Aplicaciones

# Filtrar por carpeta y CSV específicos
python csv_to_snowflake.py POM_TEST01 RAW SQLSERVER_POM_Aplicaciones ResutadoNotificar,Bitacora

# Especificar tabla destino personalizada
python csv_to_snowflake.py POM_TEST01 RAW SQLSERVER_POM_Aplicaciones ResutadoNotificar "POM_TEST01.RAW.MI_TABLA"
```

**Características**:
- Crea automáticamente stage `RAW_STAGE` si no existe
- Crea tablas `INGEST_LOG` e `INGEST_GENERIC_RAW` en schema RAW
- Comprime CSV a `.gz` automáticamente antes de subir
- Registra operaciones en `INGEST_LOG`
- Carga datos a tabla genérica o tabla específica
- Mueve carpetas procesadas a `csv_processed/` o `csv_error/`

**Tablas Creadas**:
- `RAW_STAGE`: Stage para almacenar archivos
- `INGEST_LOG`: Log de operaciones de carga
- `INGEST_GENERIC_RAW`: Tabla genérica con estructura flexible (col1-col50)

**Salida**: 
- Datos cargados en Snowflake
- Carpetas movidas a `csv_processed/` o `csv_error/`

---

### 5. csv_to_clickhouse.py

**Versión**: 1.0.0  
**Descripción**: Carga archivos CSV (o CSV.gz) desde `csv_staging` a ClickHouse Cloud.

**Requisitos**:
- Cuenta de ClickHouse Cloud
- `clickhouse-connect`

**Variables de Entorno**:
- `CH_HOST`, `CH_PORT`, `CH_USER`, `CH_PASSWORD`, `CH_DATABASE`
- `CH_TABLE` (opcional)
- `CSV_STAGING_DIR`, `CSV_PROCESSED_DIR`, `CSV_ERROR_DIR`
- `FOLDERS_FILTER`, `CSV_FILTER` (opcional)

**Uso**:

```bash
# Cargar todos los CSV a ClickHouse
python csv_to_clickhouse.py

# Especificar base de datos
python csv_to_clickhouse.py default

# Filtrar por carpeta
python csv_to_clickhouse.py default SQLSERVER_POM_Aplicaciones

# Filtrar por carpeta y CSV específicos
python csv_to_clickhouse.py default SQLSERVER_POM_Aplicaciones ResutadoNotificar,Bitacora

# Especificar tabla destino
python csv_to_clickhouse.py default SQLSERVER_POM_Aplicaciones ResutadoNotificar mi_tabla
```

**Características**:
- Crea tablas automáticamente con estructura del CSV
- Detecta delimitador automáticamente (coma, punto y coma, tab, pipe)
- Maneja columnas duplicadas renombrándolas
- Soporta CSV comprimidos (.csv.gz)
- Sanitiza nombres de columnas y tablas
- Mueve carpetas procesadas a `csv_processed/` o `csv_error/`

**Salida**: 
- Tablas creadas en ClickHouse con los datos cargados
- Carpetas movidas a `csv_processed/` o `csv_error/`

---

### 6. ingest_all_excels_to_stage.py

**Versión**: 1.0.0  
**Descripción**: Procesa archivos Excel directamente desde `inbox` y los carga a Snowflake en un solo paso.

**Requisitos**:
- Cuenta de Snowflake
- `snowflake-connector-python`
- `openpyxl`

**Variables de Entorno**:
- `SF_ACCOUNT`, `SF_USER`, `SF_PASSWORD`, `SF_ROLE`, `SF_WAREHOUSE`
- `SF_DATABASE`, `SF_SCHEMA`
- `INBOX_DIR`, `PROCESSED_DIR`, `ERROR_DIR`
- `SHEETS_ALLOWLIST` (opcional)

**Uso**:

```bash
# Procesar todos los Excel en inbox
python ingest_all_excels_to_stage.py

# Filtrar hojas específicas
$env:SHEETS_ALLOWLIST="Hoja1,Hoja2"
python ingest_all_excels_to_stage.py
```

**Características**:
- Lee Excel desde `inbox/`
- Convierte cada hoja a CSV comprimido (.csv.gz)
- Sube directamente al stage de Snowflake
- Carga datos a `INGEST_GENERIC_RAW`
- Registra operaciones en `INGEST_LOG`
- Mueve Excel a `processed/` o `error/` según resultado

**Salida**: 
- Datos cargados en Snowflake
- Excel movido a `processed/` o `error/`

---

### 7. snowflake_csv_to_tables.py

**Versión**: 1.0.0  
**Descripción**: Crea tablas individuales en Snowflake desde archivos CSV que ya están en el stage.

**Requisitos**:
- Cuenta de Snowflake
- Archivos CSV ya cargados en el stage `RAW_STAGE`

**Variables de Entorno**:
- `SF_ACCOUNT`, `SF_USER`, `SF_PASSWORD`, `SF_ROLE`, `SF_WAREHOUSE`
- `SF_DATABASE`, `SF_SCHEMA`

**Uso**:

```bash
# Crear tablas desde todos los CSV en el stage
python snowflake_csv_to_tables.py

# Especificar base de datos y schema
python snowflake_csv_to_tables.py POM_TEST01 RAW

# Filtrar por carpeta en el stage
python snowflake_csv_to_tables.py POM_TEST01 RAW CIERRE_PROPIAS___7084110

# Filtrar por carpeta y archivos específicos
python snowflake_csv_to_tables.py POM_TEST01 RAW CIERRE_PROPIAS___7084110 Estados_Cuenta,Desgloce_Cierre
```

**Características**:
- Lee archivos CSV desde el stage de Snowflake
- Crea una tabla por cada archivo CSV
- El nombre de la tabla es el nombre del archivo (sin extensión)
- Omite tablas que ya existen
- Detecta headers automáticamente
- Carga datos desde el stage a la tabla

**Salida**: Tablas creadas en Snowflake con los datos de los CSV

---

## Flujos de Trabajo Comunes

### Flujo 1: SQL Server → CSV → Snowflake

```bash
# 1. Exportar tablas de SQL Server a CSV
python sqlserver_to_csv.py MiBaseDeDatos

# 2. (Opcional) Comprimir CSV
python compress_csv_to_gz.py

# 3. Cargar CSV a Snowflake
python csv_to_snowflake.py POM_TEST01 RAW

# 4. (Opcional) Crear tablas individuales desde el stage
python snowflake_csv_to_tables.py POM_TEST01 RAW
```

### Flujo 2: Excel → CSV → ClickHouse

```bash
# 1. Convertir Excel a CSV
python excel_to_csv.py

# 2. (Opcional) Comprimir CSV
python compress_csv_to_gz.py

# 3. Cargar CSV a ClickHouse
python csv_to_clickhouse.py default
```

### Flujo 3: Excel → Snowflake (Directo)

```bash
# Procesar Excel directamente a Snowflake
python ingest_all_excels_to_stage.py
```

### Flujo 4: Procesamiento Completo Automatizado

```bash
# Script batch (ejemplo para Windows)
@echo off
echo Exportando SQL Server...
python sqlserver_to_csv.py MiBaseDeDatos
echo Comprimiendo CSV...
python compress_csv_to_gz.py
echo Cargando a Snowflake...
python csv_to_snowflake.py POM_TEST01 RAW
echo Creando tablas individuales...
python snowflake_csv_to_tables.py POM_TEST01 RAW
echo Proceso completado!
```

---

## Troubleshooting

### Error: "No se encontró un driver ODBC compatible"

**Solución**: Instala uno de los drivers ODBC para SQL Server:
- [ODBC Driver 17 for SQL Server](https://docs.microsoft.com/en-us/sql/connect/odbc/download-odbc-driver-for-sql-server)
- [SQL Server Native Client](https://docs.microsoft.com/en-us/sql/relational-databases/native-client/applications/installing-sql-server-native-client)

### Error: "Error de autenticación" en SQL Server

**Solución**: 
- Verifica que tengas permisos en SQL Server
- Si usas autenticación SQL, verifica `SQL_USER` y `SQL_PASSWORD`
- Si usas autenticación Windows, verifica que tu usuario tenga acceso

### Error: "La base de datos no existe" en Snowflake

**Solución**:
- Verifica el nombre exacto de la base de datos (case-sensitive)
- Verifica que tengas permisos para acceder a la base de datos
- El script intentará crear la base de datos si no existe (requiere permisos)

### Error: "No se pudieron leer los headers" en ClickHouse

**Solución**:
- Verifica que el CSV tenga headers en la primera fila
- Verifica que el delimitador sea correcto (coma, punto y coma, etc.)
- Verifica que el archivo no esté corrupto

### Error: "PermissionError" al mover archivos

**Solución**:
- Verifica que el archivo no esté abierto en otro programa
- Verifica permisos de escritura en las carpetas destino
- El script intenta automáticamente con reintentos

### Archivos no se procesan

**Solución**:
- Verifica que los archivos estén en las carpetas correctas (`inbox/`, `csv_staging/`)
- Verifica filtros (`FOLDERS_FILTER`, `CSV_FILTER`, `SHEETS_ALLOWLIST`)
- Verifica que los archivos tengan las extensiones correctas (`.xlsx`, `.csv`, `.csv.gz`)

---

## Notas Adicionales

### Sanitización de Nombres

Todos los scripts sanitizan automáticamente los nombres de:
- Archivos CSV
- Nombres de tablas
- Nombres de columnas
- Nombres de carpetas

Caracteres especiales se reemplazan por guiones bajos (`_`).

### Encoding

Todos los archivos CSV se generan con encoding **UTF-8** para soportar caracteres especiales.

### Manejo de NULL/NaN

Los valores NULL/NaN se manejan correctamente:
- En CSV: se convierten a cadenas vacías o se mantienen como NULL según el destino
- En Snowflake: se manejan como NULL
- En ClickHouse: se manejan como cadenas vacías o NULL según el tipo de columna

### Logging

Los scripts proporcionan información detallada:
- Operaciones exitosas
- Advertencias
- Errores
- Estadísticas (filas, columnas, tamaño de archivos)

---

## Changelog

### Versión 1.0.0 - 19 de enero de 2026

**Scripts incluidos**:
- `sqlserver_to_csv.py` v1.0.0
- `excel_to_csv.py` v1.0.0
- `compress_csv_to_gz.py` v1.0.0
- `csv_to_snowflake.py` v1.0.0
- `csv_to_clickhouse.py` v1.0.0
- `ingest_all_excels_to_stage.py` v1.0.0
- `snowflake_csv_to_tables.py` v1.0.0

**Características iniciales**:
- Exportación de tablas SQL Server a CSV
- Conversión de Excel a CSV
- Compresión de CSV a formato GZ
- Carga automática a Snowflake y ClickHouse
- Filtrado flexible por tablas, carpetas y archivos
- Manejo robusto de errores con reintentos
- Logging detallado de operaciones
- Sanitización automática de nombres
- Exclusión de tablas por prefijos en SQL Server

---

## Contribuciones

Las contribuciones son bienvenidas. Por favor:

1. Fork el proyecto
2. Crea una rama para tu feature (`git checkout -b feature/AmazingFeature`)
3. Commit tus cambios (`git commit -m 'Add some AmazingFeature'`)
4. Push a la rama (`git push origin feature/AmazingFeature`)
5. Abre un Pull Request

---

## Licencia

Este proyecto está bajo la Licencia MIT - ver el archivo LICENSE para más detalles.

---

## Contacto y Soporte

**Autor**: Herbert Poveda  
**Empresa**: POM Cobranzas  
**Departamento**: Business Intelligence (BI)  
**Fecha**: 19 de enero de 2026

Para preguntas o problemas:
- Abre un issue en el repositorio
- Revisa la sección [Troubleshooting](#troubleshooting)
- Verifica las variables de entorno y configuración

---

**Desarrollado por Business Intelligence (BI) - POM Cobranzas**
