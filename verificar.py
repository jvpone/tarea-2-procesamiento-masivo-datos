"""B.3 — Verificación: ¿quedó todo donde debía, ni más ni menos?

Imprime: eventos producidos · entradas procesadas y XPENDING de cada grupo ·
filas de cada tabla de HBase · el tablero de Redis; y lo compara con los
números de control del enunciado (y con lo que se espera del generador).
Si algo no calza lo dice, con la explicación, y termina con código 1.

    python verificar.py
"""
import sys
from collections import Counter

from comun import (CONTROL, GRUPO_INGESTA, GRUPO_TABLERO, N_VIVO, STREAM, Bitacora, conectar_hbase,
                   conectar_redis, contar, en_vivo, historico, ms)

log = Bitacora("b3_verificacion.txt")
r, conn = conectar_redis(), conectar_hbase()
fallas = []


def chequear(ok, texto, explicacion=""):
    log(("  OK   " if ok else "  FALLA ") + texto)
    if not ok:
        fallas.append(texto)
        if explicacion:
            log("        → " + explicacion)


# --- Lo que dicen los datos (generador con la semilla de la tarea) -----------
hist, vivo = historico(), en_vivo()
for nombre, evs in (("hist", hist), ("vivo", vivo)):
    eventos = len(evs)
    repetidos = eventos - len({tuple(sorted(e.items())) for e in evs})
    claves = len({(e["estacion_id"], e["timestamp"]) for e in evs})
    c = CONTROL[nombre]
    chequear((eventos, repetidos, claves) == (c["eventos"], c["repetidos"], c["claves"]),
             f"generador {nombre}: {eventos:,} eventos · {repetidos:,} repetidos · {claves:,} claves distintas "
             f"(control {c['eventos']:,} · {c['repetidos']:,} · {c['claves']:,})",
             "la semilla/t0 no son los de la tarea")
todos = hist + vivo
choques = Counter((e["estacion_id"], e["timestamp"]) for e in {tuple(sorted(e.items())): e for e in todos}.values())
n_choques = sum(1 for v in choques.values() if v > 1)
log(f"  871.200 − 853.953 = {871_200 - 853_953:,} = {CONTROL['total']['repetidos']:,} repetidos tal cual + "
    f"{n_choques} pares con la misma (estación, ms) y distinto valor")

# --- Redis: el stream y los grupos -------------------------------------------
log("\n[Redis] stream y grupos")
xs = r.xinfo_stream(STREAM)
producidos = int(r.get(f"{STREAM}:fin") or 0)
chequear(xs["entries-added"] == N_VIVO and producidos == N_VIVO,
         f"producidos: {xs['entries-added']:,} XADD al stream (marca de fin del productor: {producidos:,}) · "
         f"XLEN hoy = {xs['length']:,} (MAXLEN ~ no recortó nada: {xs['length'] == xs['entries-added']})",
         "el productor no terminó o el stream se borró/recortó")
for g in r.xinfo_groups(STREAM):
    pend = r.xpending(STREAM, g["name"])["pending"]
    chequear(g["entries-read"] == N_VIVO and g["lag"] == 0 and pend == 0,
             f"grupo {g['name']}: entradas procesadas (entries-read) {g['entries-read']:,} · lag {g['lag']} · "
             f"XPENDING {pend}",
             "quedaron entradas sin confirmar: un consumidor murió y nadie las reclamó")
grupos = {g["name"] for g in r.xinfo_groups(STREAM)}
chequear({GRUPO_INGESTA, GRUPO_TABLERO} <= grupos, f"existen los dos grupos independientes: {sorted(grupos)}")

# --- HBase -------------------------------------------------------------------
log("\n[HBase] filas por tabla")
esperado = CONTROL["total"]["claves"]
for tabla in ("lecturas", "lecturas_tipo"):
    n = contar(tabla, conn)
    chequear(n == esperado, f"{tabla}: {n:,} filas (control {esperado:,} = {CONTROL['hist']['claves']:,} histórico "
                            f"+ {CONTROL['vivo']['claves']:,} en vivo)",
             "faltan filas: se perdieron eventos (¿XACK antes del put?)" if n < esperado else
             "sobran filas: la row key no es idempotente (¿usa la hora de llegada?)")

# --- Tablero -----------------------------------------------------------------
log("\n[Redis] tablero")
ultimo_esp = {}
for e in vivo:                       # último por MEDICIÓN; empate en el ms → el último que llegó
    if e["estacion_id"] not in ultimo_esp or ms(e["timestamp"]) >= ultimo_esp[e["estacion_id"]][0]:
        ultimo_esp[e["estacion_id"]] = (ms(e["timestamp"]), e["valor"], e["tipo_sensor"])
claves_ultimo = sorted(r.scan_iter("ultimo:*"))
chequear(len(claves_ultimo) == 16, f"ultimo:*: {len(claves_ultimo)} hashes (16 estaciones)")
mal = [k for k in claves_ultimo
       if (lambda h, e: (int(h["ts"]), float(h["valor"]), h["tipo"]) != e)(r.hgetall(k), ultimo_esp[k[7:]])]
chequear(not mal, "ultimo:* = el último por MEDICIÓN de cada estación (comparado con el generador)",
         f"no calzan: {mal}")
flujo = {k: v for k, v in ultimo_esp.items() if v[2] == "flujo_vehicular"}
rk = dict(r.zrange("ranking:flujo", 0, -1, withscores=True))
chequear(rk == {k: v[1] for k, v in flujo.items()}, f"ranking:flujo: {len(rk)} estaciones de flujo con su último valor")
max_ts = int(r.get("tablero:max_ts"))
chequear(max_ts == max(ms(e["timestamp"]) for e in vivo), f"tablero:max_ts = medición más reciente del stream")
ok_v = True
for est in flujo:
    esperado_v = {ms(e["timestamp"]) for e in vivo if e["estacion_id"] == est and ms(e["timestamp"]) > max_ts - 600_000}
    scores = {int(s) for _, s in r.zrange(f"ventana:{est}", 0, -1, withscores=True)}
    ok_v &= scores == esperado_v
chequear(ok_v, f"ventana:*: {len(list(r.scan_iter('ventana:*')))} zsets, cada uno con exactamente las mediciones "
               f"de los últimos 10 min (score > max_ts − 600.000 ms)")
tipos = {k: r.scard(k) for k in r.scan_iter("tipo:*")}
chequear(tipos == {"tipo:flujo_vehicular": 8, "tipo:calidad_aire": 4, "tipo:ocupacion_estacionamiento": 4},
         f"tipo:*: {tipos}")

log("\n" + ("VERIFICACIÓN OK: todo calza con los números de control" if not fallas
            else f"VERIFICACIÓN CON {len(fallas)} DIFERENCIAS (ver arriba)"))
log.cerrar()
sys.exit(1 if fallas else 0)
