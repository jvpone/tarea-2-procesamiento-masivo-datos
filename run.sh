#!/usr/bin/env bash
# Tarea 2 — corre todo de punta a punta sobre una máquina limpia.
#
#     ./run.sh             # todo: entorno → contenedores → carga → vivo (+D.1) → verificación → preguntas → D.2
#     ./run.sh <paso>      # un solo paso: carga | vivo | verificar | preguntas | fallas
#
# Requisitos: Docker (con `docker compose`, ~4 GB de RAM para Docker) y Python ≥ 3.10.
# En Windows se corre desde Git Bash (o WSL). Ver README.md.
set -euo pipefail
cd "$(dirname "$0")"
PASO="${1:-todo}"
export PYTHONIOENCODING=utf-8 PYTHONUTF8=1

# --- Python en ambiente virtual ----------------------------------------------
if command -v python3 >/dev/null 2>&1 && python3 -c 'import sys; assert sys.version_info >= (3, 10)' 2>/dev/null; then
  PY_SIS=python3
else
  PY_SIS=python
fi
if [ ! -d .venv ]; then
  echo "== creando .venv con $($PY_SIS --version)"
  "$PY_SIS" -m venv .venv
fi
if [ -x .venv/bin/python ]; then PY=.venv/bin/python; else PY=.venv/Scripts/python.exe; fi
"$PY" -m pip install -q --disable-pip-version-check -r requirements.txt

# --- Contenedores (construye la imagen de HBase si falta) ---------------------
echo "== docker compose up -d --wait"
docker compose up -d --build --wait

mkdir -p resultados
REGISTRO=resultados/duracion_run.txt
[ "$PASO" = todo ] && : > "$REGISTRO"

correr() {   # correr <nombre> <script.py> [args]
  local nombre=$1; shift
  echo; echo "================ $nombre ================"
  local t0=$SECONDS
  "$PY" -u "$@"
  echo "$nombre: $((SECONDS - t0)) s" | tee -a "$REGISTRO"
}

case "$PASO" in
  carga)     correr "B.1 carga histórica" carga_historica.py ;;
  vivo)      correr "B.2+D.1 hora en vivo" vivo.py ;;
  verificar) correr "B.3 verificación" verificar.py ;;
  preguntas) correr "C preguntas" preguntas.py ;;
  fallas)    correr "D.2 reinicio de Redis" fallas_redis.py ;;
  todo)
    T0=$SECONDS
    correr "B.1 carga histórica" carga_historica.py
    correr "B.2+D.1 hora en vivo" vivo.py
    correr "B.3 verificación" verificar.py
    correr "C preguntas" preguntas.py
    correr "D.2 reinicio de Redis" fallas_redis.py
    correr "B.3 verificación final" verificar.py
    echo "TOTAL: $((SECONDS - T0)) s" | tee -a "$REGISTRO"
    ;;
  *) echo "paso desconocido: $PASO (carga | vivo | verificar | preguntas | fallas | todo)"; exit 2 ;;
esac
