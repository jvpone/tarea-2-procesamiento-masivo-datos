"""B.2 — Productor: la hora en vivo (7.200 eventos) al stream `sensores:t2` con XADD, a 20×.

    python productor.py                       # la hora completa, un evento cada 25 ms
    python productor.py --desde 0 --hasta 3000 --stream d2:sensores   # (lo usa la Parte D.2)

MAXLEN ~ 20.000: ver README §MAXLEN. Al terminar escribe `<stream>:fin` = total
producido, que es la señal para que los consumidores terminen.
"""
import argparse
import itertools
import time

import redis

from comun import MAXLEN, N_HIST, N_VIVO, PAUSA_VIVO, SEMILLA, STREAM, T0, conectar_redis
from generador import eventos

ap = argparse.ArgumentParser()
ap.add_argument("--stream", default=STREAM)
ap.add_argument("--desde", type=int, default=0, help="índice dentro de la hora en vivo")
ap.add_argument("--hasta", type=int, default=N_VIVO)
ap.add_argument("--pausa", type=float, default=PAUSA_VIVO)
ap.add_argument("--maxlen", type=int, default=MAXLEN)
ap.add_argument("--sin-fin", action="store_true", help="no marcar el fin (habrá otra tanda después)")
a = ap.parse_args()

r = conectar_redis()
vivo = itertools.islice(eventos(T0, semilla=SEMILLA), N_HIST + a.desde, N_HIST + a.hasta)
print(f"[productor] {a.hasta - a.desde} eventos → {a.stream} (MAXLEN ~ {a.maxlen}, uno cada {a.pausa * 1000:.0f} ms)",
      flush=True)

inicio = time.monotonic()
for k, ev in enumerate(vivo):
    n = a.desde + k
    while True:
        try:
            r.xadd(a.stream, {**ev, "n": n}, maxlen=a.maxlen, approximate=True)
            break
        except (redis.ConnectionError, redis.BusyLoadingError, redis.TimeoutError):
            time.sleep(0.2)                    # Redis reiniciando (D.2): el evento se reintenta, no se salta
    if (n + 1) % 1000 == 0:
        print(f"[productor] {n + 1} eventos producidos", flush=True)
    time.sleep(max(0.0, inicio + (k + 1) * a.pausa - time.monotonic()))   # sin deriva

if not a.sin_fin:
    r.set(f"{a.stream}:fin", a.hasta)
print(f"[productor] listo: {a.hasta - a.desde} eventos en {time.monotonic() - inicio:.1f} s", flush=True)
