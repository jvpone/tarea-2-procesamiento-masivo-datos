"""B.2 + D.1 — La hora en vivo, de punta a punta, con el consumidor de ingesta muriendo a mitad de camino.

1. Deja Redis limpio: borra el stream y el tablero, crea los grupos `ingesta` y `tablero` (id 0).
2. Lanza `tablero-1`, `ingesta-1` y el productor (7.200 eventos a 20×, ~3 min).
3. D.1: `ingesta-1` se lanza con --morir-tras-lote: hace el put de ese lote en HBase y
   muere con os._exit(1) ANTES del XACK. Se muestra qué quedó en XPENDING y a nombre de quién.
4. Se lanza `ingesta-2`, que reclama esas entradas con XAUTOCLAIM, las reprocesa y sigue hasta el fin.
5. Se muestra qué quedó en la celda de una fila reprocesada (versiones).

    python vivo.py                 # con la falla D.1 (lo que corre run.sh)
    python vivo.py --sin-falla     # la hora en vivo sin matar a nadie
"""
import argparse
import json
import os
import subprocess
import sys
import time
from datetime import datetime

from comun import (GRUPO_INGESTA, GRUPO_TABLERO, RAIZ, RESULTADOS, STREAM, TZ_CL, Bitacora, conectar_hbase,
                   conectar_redis)

ap = argparse.ArgumentParser()
ap.add_argument("--sin-falla", action="store_true")
ap.add_argument("--morir-tras-lote", type=int, default=15)
a = ap.parse_args()

log = Bitacora("b2_d1_vivo.txt")
r = conectar_redis()
ENV = {**os.environ, "PYTHONIOENCODING": "utf-8"}


def lanzar(*args):
    return subprocess.Popen([sys.executable, "-u", *args], cwd=RAIZ, env=ENV)


def hora(ms_):
    return datetime.fromtimestamp(ms_ / 1000, TZ_CL).strftime("%H:%M:%S.%f")[:-3]


# 1. Redis limpio
borrar = [STREAM, f"{STREAM}:fin", "ranking:flujo", "tablero:max_ts"]
for patron in ("ultimo:*", "ventana:*", "tipo:*"):
    borrar += list(r.scan_iter(patron))
r.delete(*borrar)
for g in (GRUPO_INGESTA, GRUPO_TABLERO):
    r.xgroup_create(STREAM, g, id="0", mkstream=True)
log(f"Redis limpio · stream {STREAM} con grupos {GRUPO_INGESTA} y {GRUPO_TABLERO} (desde el id 0)")

# 2. Consumidores primero, productor después
t0 = time.perf_counter()
tablero = lanzar("tablero.py", "--consumidor", "tablero-1")
extra = [] if a.sin_falla else ["--morir-tras-lote", str(a.morir_tras_lote)]
ingesta1 = lanzar("ingesta.py", "--consumidor", "ingesta-1", *extra)
time.sleep(1)
productor = lanzar("productor.py")

# 3. D.1
procesos = [productor, tablero, ingesta1]
if not a.sin_falla:
    codigo = ingesta1.wait()
    log(f"\n=== D.1 · ingesta-1 terminó con código {codigo} (os._exit(1) entre el put y el XACK) ===")
    muerto = json.loads((RESULTADOS / "d1_lote_muerto.json").read_text(encoding="utf-8"))
    resumen = r.xpending(STREAM, GRUPO_INGESTA)
    log(f"XPENDING {STREAM} {GRUPO_INGESTA} → {resumen['pending']} entradas pendientes · "
        f"rango {resumen['min']} … {resumen['max']} · por consumidor: "
        f"{ {c['name']: c['pending'] for c in resumen['consumers']} }")
    detalle = r.xpending_range(STREAM, GRUPO_INGESTA, min="-", max="+", count=1000)
    for d in detalle[:3]:
        log(f"   id {d['message_id']} · consumidor {d['consumer']} · idle {d['time_since_delivered']} ms · "
            f"entregas {d['times_delivered']}")
    if len(detalle) > 3:
        log(f"   … ({len(detalle)} en total)")
    log(f"el lote que murió tenía {len(muerto['ids'])} entradas: ¿son exactamente las pendientes? "
        f"{sorted(d['message_id'] for d in detalle) == sorted(muerto['ids'])}")
    xinfo = {g["name"]: g for g in r.xinfo_groups(STREAM)}
    log(f"mientras tanto el grupo {GRUPO_TABLERO} sigue solo (grupos independientes): "
        f"entries-read={xinfo[GRUPO_TABLERO]['entries-read']} · pendientes={xinfo[GRUPO_TABLERO]['pending']}")
    time.sleep(2.5)    # que las entradas lleven > 2 s sin confirmar (min-idle del XAUTOCLAIM)
    log("se lanza ingesta-2: al arrancar hace XAUTOCLAIM (min-idle 2000 ms) sobre lo que dejó ingesta-1")
    ingesta2 = lanzar("ingesta.py", "--consumidor", "ingesta-2")
    procesos = [productor, tablero, ingesta2]

for p in procesos:
    p.wait()
dt = time.perf_counter() - t0
log(f"\nHora en vivo terminada en {dt:.1f} s")
for g in r.xinfo_groups(STREAM):
    log(f"grupo {g['name']}: entries-read={g['entries-read']} · lag={g['lag']} · pendientes={g['pending']} · "
        f"último entregado {g['last-delivered-id']}")
for g in (GRUPO_INGESTA, GRUPO_TABLERO):
    for c in r.xinfo_consumers(STREAM, g):
        log(f"   {g}/{c['name']}: pendientes={c['pending']}")

if not a.sin_falla:
    log("\n=== D.1 · ¿qué quedó en HBase para una fila del lote reprocesado? ===")
    conn = conectar_hbase()
    for clave in muerto["claves"][:2]:
        celdas = conn.table("lecturas").cells(clave.encode(), b"m:valor", versions=5, include_timestamp=True)
        log(f"lecturas['{clave}'] m:valor → " +
            ", ".join(f"{v.decode()} @ {hora(ts)} (escritura)" for v, ts in celdas))
    log("Mismo row key → la segunda escritura NO crea otra fila: agrega una versión a la misma celda "
        "(VERSIONS => 3). Un get/scan devuelve solo la más nueva, con el mismo valor.")
log.cerrar()
