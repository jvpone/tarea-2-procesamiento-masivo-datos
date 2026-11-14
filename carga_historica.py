"""B.1 — Carga histórica: los 864.000 eventos de los 5 días van DIRECTO a HBase.

Sin pasar por Redis (es historia que ya existe). Crea desde cero las tablas
`lecturas` y `lecturas_tipo`, pre-divididas en regiones, y escribe con `batch`
(un viaje a HBase cada BATCH filas, no un `put` por fila).

    python carga_historica.py
"""
import time

from comun import (Bitacora, SPLITS_LECTURAS, SPLITS_TIPO, clave_lecturas, clave_tipo, cols_lecturas,
                   cols_tipo, conectar_hbase, contar, crear_tabla, historico, CONTROL)

BATCH = 5_000

log = Bitacora("b1_carga_historica.txt")
conn = conectar_hbase()

for nombre, splits in (("lecturas", SPLITS_LECTURAS), ("lecturas_tipo", SPLITS_TIPO)):
    regiones = crear_tabla(nombre, splits, conn=conn)
    log(f"tabla {nombre}: {len(regiones)} regiones · cortes {splits}")

t = time.perf_counter()
hist = historico()
log(f"generados {len(hist):,} eventos en {time.perf_counter() - t:.1f} s "
    f"({hist[0]['timestamp']} → {hist[-1]['timestamp']})")

for nombre, fclave, fcols in (("lecturas", clave_lecturas, cols_lecturas),
                              ("lecturas_tipo", clave_tipo, cols_tipo)):
    t = time.perf_counter()
    with conn.table(nombre).batch(batch_size=BATCH) as b:
        for ev in hist:
            b.put(fclave(ev), fcols(ev))
    dt = time.perf_counter() - t
    filas = contar(nombre, conn)
    log(f"{nombre}: {len(hist):,} put en lotes de {BATCH:,} → {dt:.1f} s · {len(hist) / dt:,.0f} put/s · "
        f"{filas:,} filas (control: {CONTROL['hist']['claves']:,}) · "
        f"{len(hist) - filas:,} put cayeron sobre una fila existente")
    assert filas == CONTROL["hist"]["claves"], f"{nombre}: {filas} filas ≠ {CONTROL['hist']['claves']}"

log("Carga histórica OK")
log.cerrar()
