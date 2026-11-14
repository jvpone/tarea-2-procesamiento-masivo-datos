"""El bucle común a los dos consumidores (ingesta y tablero).

    XREADGROUP (lote)  →  procesar(lote)  →  XACK   (el XACK SIEMPRE después)

Además:
  * al arrancar, recupera lo que este mismo consumidor dejó pendiente (id 0);
  * reclama con XAUTOCLAIM lo que otro consumidor del grupo dejó pendiente más de
    `reclamar_ms` (así se rescata un lote de un consumidor muerto, Parte D.1);
  * sobrevive a un reinicio de Redis (reintenta la conexión y, si el grupo
    desapareció porque Redis no persistía nada, lo vuelve a crear: Parte D.2);
  * termina solo cuando el productor marcó el fin, el grupo no tiene nada sin
    entregar (lag 0) y no queda nada pendiente.
"""
import time

import redis


def _esperar_redis(r, log):
    while True:
        try:
            r.ping()
            return
        except (redis.ConnectionError, redis.BusyLoadingError, redis.TimeoutError):
            time.sleep(0.5)


def asegurar_grupo(r, stream, grupo, log=print):
    try:
        r.xgroup_create(stream, grupo, id="0", mkstream=True)
        log(f"grupo {grupo} creado sobre {stream} (desde el id 0)")
        return True
    except redis.ResponseError as e:
        if "BUSYGROUP" not in str(e):
            raise
        return False


def info_grupo(r, stream, grupo):
    for g in r.xinfo_groups(stream):
        if g["name"] == grupo:
            return g
    return None


def bucle(r, stream, grupo, consumidor, procesar, *, count=200, block_ms=1000, pausa=0.0,
          reclamar_ms=2000, log=print):
    """Consume `stream` como `consumidor` del `grupo` hasta el fin. Devuelve cuántas entradas confirmó."""
    confirmadas, clave_fin = 0, f"{stream}:fin"

    def procesar_y_ack(entradas, origen):
        nonlocal confirmadas
        entradas = [(i, c) for i, c in entradas if c]          # XAUTOCLAIM puede traer ids ya recortados
        if not entradas:
            return 0
        procesar(entradas, origen)                              # 1) escribir (HBase / Redis)
        confirmadas += r.xack(stream, grupo, *[i for i, _ in entradas])   # 2) recién ahora, XACK
        return len(entradas)

    def reclamar():
        """XAUTOCLAIM: toma lo que otro consumidor del grupo dejó sin confirmar más de reclamar_ms."""
        _, reclamadas, *_ = r.xautoclaim(stream, grupo, consumidor, min_idle_time=reclamar_ms,
                                         start_id="0-0", count=1000)
        if reclamadas:
            n = procesar_y_ack(reclamadas, "XAUTOCLAIM")
            log(f"[{consumidor}] reclamó con XAUTOCLAIM {len(reclamadas)} entradas pendientes de otro "
                f"consumidor → {n} reprocesadas y confirmadas")
        return len(reclamadas)

    siguiente = "0"            # primero: lo propio que quedó pendiente (si este nombre ya existía)
    ultimo_reclamo = 0.0       # y enseguida (y cada 10 s) lo que otros dejaron pendiente
    while True:
        try:
            if siguiente == ">" and time.monotonic() - ultimo_reclamo > 10:
                ultimo_reclamo = time.monotonic()
                if reclamar():
                    continue
            if siguiente != ">":
                res = r.xreadgroup(grupo, consumidor, {stream: siguiente}, count=count)
                entradas = res[0][1] if res else []
                if not entradas:
                    siguiente = ">"
                    continue
                procesar_y_ack(entradas, "pendiente-propio")
                siguiente = entradas[-1][0]
                continue

            res = r.xreadgroup(grupo, consumidor, {stream: ">"}, count=count, block=block_ms)
            if res and res[0][1]:
                procesar_y_ack(res[0][1], "nuevo")
                if pausa:
                    time.sleep(pausa)      # deja juntar un lote más grande (menos viajes a HBase)
                continue

            # Nada nuevo: ¿alguien del grupo dejó entradas pendientes hace rato?
            if reclamar():
                continue

            if r.exists(clave_fin):
                g = info_grupo(r, stream, grupo)
                if g and g["pending"] == 0 and (g.get("lag") in (0, None)):
                    return confirmadas
        except (redis.ConnectionError, redis.BusyLoadingError, redis.TimeoutError) as e:
            log(f"[{consumidor}] Redis no responde ({type(e).__name__}): reintentando…")
            _esperar_redis(r, log)
        except redis.ResponseError as e:
            if "NOGROUP" in str(e):
                log(f"[{consumidor}] ¡el grupo {grupo} ya no existe (Redis perdió su estado)! se recrea desde 0")
                asegurar_grupo(r, stream, grupo, log)
                siguiente = ">"
            else:
                raise
