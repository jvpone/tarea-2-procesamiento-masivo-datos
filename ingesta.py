"""B.2 — Consumidor `ingesta`: del stream a HBase (las dos tablas), en lotes.

    XREADGROUP (lote)  →  put en `lecturas` + put en `lecturas_tipo`  →  XACK

    python ingesta.py --consumidor ingesta-1
    python ingesta.py --consumidor ingesta-1 --morir-tras-lote 15    # Parte D.1: muere entre put y XACK

Idempotente: la row key sale de (estación, timestamp de medición), así que
reprocesar un lote (o un evento repetido) escribe sobre las mismas filas.
"""
import argparse
import json
import os

from comun import (GRUPO_INGESTA, RESULTADOS, STREAM, clave_lecturas, clave_tipo, cols_lecturas, cols_tipo,
                   conectar_hbase, conectar_redis)
from consumo import asegurar_grupo, bucle

ap = argparse.ArgumentParser()
ap.add_argument("--stream", default=STREAM)
ap.add_argument("--grupo", default=GRUPO_INGESTA)
ap.add_argument("--consumidor", default="ingesta-1")
ap.add_argument("--tablas", default="lecturas,lecturas_tipo")
ap.add_argument("--count", type=int, default=200)
ap.add_argument("--pausa", type=float, default=0.5, help="espera entre lotes para juntar más eventos")
ap.add_argument("--morir-tras-lote", type=int, default=0, help="os._exit(1) después del put de ese lote, antes del XACK")
ap.add_argument("--reclamar-ms", type=int, default=2000)
a = ap.parse_args()

r = conectar_redis()
conn = conectar_hbase()
t_est, t_tipo = (conn.table(t) for t in a.tablas.split(","))
asegurar_grupo(r, a.stream, a.grupo)
lotes = 0


def procesar(entradas, origen):
    global lotes
    lotes += 1
    b1, b2 = t_est.batch(), t_tipo.batch()
    for _, ev in entradas:
        b1.put(clave_lecturas(ev), cols_lecturas(ev))
        b2.put(clave_tipo(ev), cols_tipo(ev))
    b1.send()            # primera escritura (tabla por estación)
    b2.send()            # segunda escritura (tabla por tipo). Si esta falla, no hay XACK: el lote se reintenta entero
    if a.morir_tras_lote and lotes == a.morir_tras_lote:
        ids = [i for i, _ in entradas]
        (RESULTADOS / "d1_lote_muerto.json").write_text(json.dumps(
            {"consumidor": a.consumidor, "lote": lotes, "ids": ids,
             "claves": [clave_lecturas(ev).decode() for _, ev in entradas]}, ensure_ascii=False, indent=1),
            encoding="utf-8")
        print(f"[{a.consumidor}] *** lote {lotes}: {len(entradas)} entradas YA escritas en HBase "
              f"({ids[0]} … {ids[-1]}); muero ANTES del XACK (os._exit(1)) ***", flush=True)
        os._exit(1)


print(f"[{a.consumidor}] leyendo {a.stream} en el grupo {a.grupo} → HBase {a.tablas}", flush=True)
n = bucle(r, a.stream, a.grupo, a.consumidor, procesar, count=a.count, pausa=a.pausa, reclamar_ms=a.reclamar_ms,
          log=lambda *x: print(*x, flush=True))
print(f"[{a.consumidor}] fin: {n} entradas escritas en HBase y confirmadas en {lotes} lotes", flush=True)
