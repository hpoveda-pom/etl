#!/bin/bash
set -euo pipefail

export TZ="America/Costa_Rica"

BASE="/home/hpoveda/etl"
PY="/usr/bin/python3"
SCRIPT="$BASE/streaming/sqlserver_to_clickhouse_streamingv4.py"

LOCKFILE="/tmp/sqlserver_to_clickhouse_streamingv4.lock"
LOGDIR="$BASE/logs"
PIDDIR="/tmp/streaming_v4_pids"

# Verificar que el directorio base existe
if [ ! -d "$BASE" ]; then
    echo "❌ ERROR: Directorio base no existe: $BASE"
    echo "   Edita el script y cambia la variable BASE con la ruta correcta"
    exit 1
fi

# Verificar que Python existe
if [ ! -f "$PY" ]; then
    echo "❌ ERROR: Python no encontrado en: $PY"
    echo "   Verifica la ruta con: which python3"
    echo "   Edita el script y cambia la variable PY con la ruta correcta"
    exit 1
fi

# Verificar que el script de streaming existe
if [ ! -f "$SCRIPT" ]; then
    echo "❌ ERROR: Script de streaming no encontrado: $SCRIPT"
    exit 1
fi

mkdir -p "$LOGDIR"
mkdir -p "$PIDDIR"

exec 200>$LOCKFILE
if ! flock -n 200; then
    echo "⚠️  Otro proceso está ejecutando el script. Espera o verifica con: lsof $LOCKFILE"
    exit 1
fi

cd "$BASE" || {
    echo "❌ ERROR: No se puede cambiar al directorio: $BASE"
    exit 1
}

log_runner() {
    local message="[$(date '+%Y-%m-%d %H:%M:%S')] $1"
    echo "$message" >> "$LOGDIR/runner_v4.log"
    echo "$message"  # También mostrar en consola
}

# Función para iniciar streaming como servicio continuo
start_streaming_service() {
    local db_name=$1
    local pid_file="$PIDDIR/${db_name}.pid"
    local log_file="$LOGDIR/${db_name}_v4.log"
    
    # Verificar si ya está corriendo
    if [ -f "$pid_file" ]; then
        local old_pid=$(cat "$pid_file")
        if ps -p "$old_pid" > /dev/null 2>&1; then
            log_runner "⚠ Servicio ya corriendo: $db_name (PID: $old_pid)"
            return 0
        else
            # PID file existe pero proceso no, limpiar
            rm -f "$pid_file"
        fi
    fi
    
    log_runner "▶ Iniciando servicio streaming v4: $db_name"
    
    # Ejecutar en background y guardar PID
    nohup $PY "$SCRIPT" "$db_name" "$db_name" --prod --poll-interval 10 >> "$log_file" 2>&1 &
    local pid=$!
    echo "$pid" > "$pid_file"
    
    # Esperar un momento para verificar que inició correctamente
    sleep 2
    if ps -p "$pid" > /dev/null 2>&1; then
        log_runner "✓ Servicio iniciado: $db_name (PID: $pid)"
        return 0
    else
        log_runner "✗ Error iniciando servicio: $db_name"
        rm -f "$pid_file"
        return 1
    fi
}

# Función para detener un servicio
stop_streaming_service() {
    local db_name=$1
    local pid_file="$PIDDIR/${db_name}.pid"
    
    if [ -f "$pid_file" ]; then
        local pid=$(cat "$pid_file")
        if ps -p "$pid" > /dev/null 2>&1; then
            log_runner "⏹ Deteniendo servicio: $db_name (PID: $pid)"
            kill "$pid" 2>/dev/null || true
            sleep 1
            # Si aún está corriendo, forzar kill
            if ps -p "$pid" > /dev/null 2>&1; then
                kill -9 "$pid" 2>/dev/null || true
            fi
            rm -f "$pid_file"
            log_runner "✓ Servicio detenido: $db_name"
        else
            rm -f "$pid_file"
        fi
    fi
}

# Función para verificar estado de servicios
check_services_status() {
    log_runner "📊 Estado de servicios:"
    for db_name in "POM_Aplicaciones" "POM_Reportes" "Reporteria" "POM_PJ" "POM_Buro" "POM_Historico"; do
        local pid_file="$PIDDIR/${db_name}.pid"
        if [ -f "$pid_file" ]; then
            local pid=$(cat "$pid_file")
            if ps -p "$pid" > /dev/null 2>&1; then
                log_runner "  ✓ $db_name: CORRIENDO (PID: $pid)"
            else
                log_runner "  ✗ $db_name: DETENIDO (PID file existe pero proceso no)"
                rm -f "$pid_file"
            fi
        else
            log_runner "  ○ $db_name: NO INICIADO"
        fi
    done
}

# Función para reiniciar un servicio
restart_streaming_service() {
    local db_name=$1
    stop_streaming_service "$db_name"
    sleep 2
    start_streaming_service "$db_name"
}

# Manejo de argumentos
ACTION="${1:-start}"

case "$ACTION" in
    start)
        SCRIPT_START_TIME=$(date +%s)
        START_DATE=$(date '+%Y-%m-%d %H:%M:%S')
        log_runner "=========================================="
        log_runner "INICIO DE SERVICIOS STREAMING V4"
        log_runner "Fecha/Hora: $START_DATE"
        log_runner "=========================================="
        
        start_streaming_service "POM_Aplicaciones"
        sleep 5
        
        start_streaming_service "POM_Reportes"
        sleep 5
        
        start_streaming_service "Reporteria"
        sleep 5
        
        start_streaming_service "POM_PJ"
        sleep 5
        
        start_streaming_service "POM_Buro"
        sleep 5
        
        start_streaming_service "POM_Historico"
        
        sleep 5
        check_services_status
        
        log_runner "=========================================="
        log_runner "SERVICIOS INICIADOS"
        log_runner "=========================================="
        log_runner ""
        ;;
    
    stop)
        log_runner "=========================================="
        log_runner "DETENIENDO SERVICIOS STREAMING V4"
        log_runner "=========================================="
        
        stop_streaming_service "POM_Aplicaciones"
        stop_streaming_service "POM_Reportes"
        stop_streaming_service "Reporteria"
        stop_streaming_service "POM_PJ"
        stop_streaming_service "POM_Buro"
        stop_streaming_service "POM_Historico"
        
        log_runner "=========================================="
        log_runner "SERVICIOS DETENIDOS"
        log_runner "=========================================="
        ;;
    
    restart)
        log_runner "=========================================="
        log_runner "REINICIANDO SERVICIOS STREAMING V4"
        log_runner "=========================================="
        
        restart_streaming_service "POM_Aplicaciones"
        sleep 3
        restart_streaming_service "POM_Reportes"
        sleep 3
        restart_streaming_service "Reporteria"
        sleep 3
        restart_streaming_service "POM_PJ"
        sleep 3
        restart_streaming_service "POM_Buro"
        sleep 3
        restart_streaming_service "POM_Historico"
        
        sleep 5
        check_services_status
        
        log_runner "=========================================="
        log_runner "SERVICIOS REINICIADOS"
        log_runner "=========================================="
        ;;
    
    status)
        echo ""
        echo "=========================================="
        echo "ESTADO DE SERVICIOS STREAMING V4"
        echo "=========================================="
        check_services_status
        echo "=========================================="
        echo ""
        echo "📁 Logs disponibles en: $LOGDIR/"
        echo "   - runner_v4.log (log del gestor)"
        echo "   - [BaseDatos]_v4.log (log de cada servicio)"
        echo ""
        echo "💡 Ver logs en tiempo real:"
        echo "   tail -f $LOGDIR/runner_v4.log"
        ;;
    
    *)
        echo "Uso: $0 {start|stop|restart|status}"
        echo ""
        echo "Comandos:"
        echo "  start   - Inicia todos los servicios streaming v4"
        echo "  stop    - Detiene todos los servicios streaming v4"
        echo "  restart - Reinicia todos los servicios streaming v4"
        echo "  status  - Muestra el estado de todos los servicios"
        exit 1
        ;;
esac
