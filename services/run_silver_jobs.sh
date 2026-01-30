#!/bin/bash

cd /home/hpoveda/etl || exit 1

python silver/sqlserver_to_clickhouse_silver.py POM_Aplicaciones POM_Aplicaciones "*" 0 reset
python silver/sqlserver_to_clickhouse_silver.py POM_Reportes POM_Reportes "*" 0 reset
python silver/sqlserver_to_clickhouse_silver.py POM_PJ POM_PJ "*" 0 reset
python silver/sqlserver_to_clickhouse_silver.py POM_Historico POM_Historico "*" 0 reset
python silver/sqlserver_to_clickhouse_silver.py POM_Buro POM_Buro "*" 0 reset
python silver/sqlserver_to_clickhouse_silver.py Reporteria Reporteria "*" 0 reset
