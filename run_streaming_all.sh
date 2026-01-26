#!/bin/bash
set -euo pipefail

BASE="/home/hpoveda/etl"
PY="/usr/bin/python3"
SCRIPT="$BASE/sqlserver_to_clickhouse_streaming.py"

LOCKFILE="/tmp/sqlserver_to_clickhouse_streaming.lock"
LOGDIR="$BASE/logs"

mkdir -p "$LOGDIR"

exec 200>$LOCKFILE
flock -n 200 || exit 0

cd "$BASE"

echo "===== $(date) START =====" >> "$LOGDIR/runner.log"

$PY "$SCRIPT" POM_Aplicaciones POM_Aplicaciones --prod >> "$LOGDIR/POM_Aplicaciones.log" 2>&1
$PY "$SCRIPT" POM_Reportes     POM_Reportes     --prod >> "$LOGDIR/POM_Reportes.log" 2>&1
$PY "$SCRIPT" Reporteria       Reporteria       --prod >> "$LOGDIR/Reporteria.log" 2>&1
$PY "$SCRIPT" POM_PJ           POM_PJ           --prod >> "$LOGDIR/POM_PJ.log" 2>&1
$PY "$SCRIPT" POM_Buro         POM_Buro         --prod >> "$LOGDIR/POM_Buro.log" 2>&1
$PY "$SCRIPT" POM_Historico    POM_Historico    --prod >> "$LOGDIR/POM_Historico.log" 2>&1

echo "===== $(date) END =====" >> "$LOGDIR/runner.log"
