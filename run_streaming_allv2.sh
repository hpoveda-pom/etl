#!/bin/bash
set -euo pipefail

export TZ="America/Costa_Rica"

BASE="/home/hpoveda/etl"
PY="/usr/bin/python3"
SCRIPT="$BASE/sqlserver_to_clickhouse_streaming.py"

LOCKFILE="/tmp/sqlserver_to_clickhouse_streaming.lock"
LOGDIR="$BASE/logs"

mkdir -p "$LOGDIR"

exec 200>$LOCKFILE
flock -n 200 || exit 0

cd "$BASE"

log_runner() {
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] $1" >> "$LOGDIR/runner.log"
}

run_streaming() {
    local db_name=$1
    local start_time=$(date +%s)
    log_runner "Iniciando streaming: $db_name"

    # Pausa opcional entre bases para bajar presión de merges
    # (puede ajustarse por env var o fijo)
    local sleep_between_dbs="${SLEEP_BETWEEN_DBS:-15}"

    $PY "$SCRIPT" "$db_name" "$db_name" --prod >> "$LOGDIR/${db_name}.log" 2>&1
    local exit_code=$?

    local end_time=$(date +%s)
    local duration=$((end_time - start_time))
    local minutes=$((duration / 60))
    local seconds=$((duration % 60))

    if [ $exit_code -eq 0 ]; then
        log_runner "✓ Completado: $db_name | Duración: ${minutes}m ${seconds}s"
    else
        log_runner "✗ Error en: $db_name | Duración: ${minutes}m ${seconds}s | Exit code: $exit_code"
    fi

    sleep "$sleep_between_dbs"
    return $exit_code
}

SCRIPT_START_TIME=$(date +%s)
START_DATE=$(date '+%Y-%m-%d %H:%M:%S')
log_runner "=========================================="
log_runner "INICIO DE EJECUCIÓN COMPLETA"
log_runner "Fecha/Hora: $START_DATE"
log_runner "=========================================="

run_streaming "POM_Aplicaciones"
run_streaming "POM_Reportes"
run_streaming "Reporteria"
run_streaming "POM_PJ"
run_streaming "POM_Buro"
run_streaming "POM_Historico"

SCRIPT_END_TIME=$(date +%s)
END_DATE=$(date '+%Y-%m-%d %H:%M:%S')
TOTAL_DURATION=$((SCRIPT_END_TIME - SCRIPT_START_TIME))
TOTAL_MINUTES=$((TOTAL_DURATION / 60))
TOTAL_SECONDS=$((TOTAL_DURATION % 60))

log_runner "=========================================="
log_runner "FIN DE EJECUCIÓN COMPLETA"
log_runner "Fecha/Hora: $END_DATE"
log_runner "Tiempo total: ${TOTAL_MINUTES}m ${TOTAL_SECONDS}s"
log_runner "=========================================="
log_runner ""
