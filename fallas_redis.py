"""D.2 — Redis se reinicia con el pipeline a medio camino. Dos configuraciones:

    aof               la elegida (docker-compose.yml): AOF + appendfsync everysec, volumen t2-redis
    sin_persistencia  la contraria (docker-compose.sin-persistencia.yml): sin RDB ni AOF

Para cada una, con un stream y tablas PROPIOS del experimento (d2:sensores,
d2_lecturas, d2_lecturas_tipo, tablero con prefijo d2:) para no tocar lo de la Parte B:

 1. productor + tablero + ingesta sobre la hora en vivo;
 2. a los ~2.000 eventos la ingesta se cae (se mata el proceso): el productor sigue hasta 3.000,
    así que ~1.000 eventos están SOLO en Redis (la "ventana de pérdida");
 3. un consumidor "sonda" toma 50 entradas sin confirmarlas (para que haya pendientes);
 4. foto de Redis → docker compose restart redis → foto de Redis;
 5. se reanuda todo (productor desde el 3.000) y al final se cuenta qué llegó a HBase.

    python fallas_redis.py
"""
import os
import subprocess
import sys
import time

import redis

from comun import RAIZ, Bitacora, N_VIVO, conectar_hbase, conectar_redis, contar, crear_tabla, en_vivo

STREAM = "d2:sensores"
P = "d2:"
TABLAS = "d2_lecturas,d2_lecturas_tipo"
CORTE_INGESTA, CORTE_PRODUCTOR, SONDA = 2_000, 3_000, 50
COMPOSE = {"aof": ["-f", "docker-compose.yml"],
           "sin_persistencia": ["-f", "docker-compose.yml", "-f", "docker-compose.sin-persistencia.yml"]}
ENV = {**os.environ, "PYTHONIOENCODING": "utf-8"}
log = Bitacora("d2_redis_reinicio.txt")


def compose(cfg, *args):
    return subprocess.run(["docker", "compose", *COMPOSE[cfg], *args], cwd=RAIZ, check=True,
                          capture_output=True, text=True)


def lanzar(*args):
    return subprocess.Popen([sys.executable, "-u", *args], cwd=RAIZ, env=ENV)


def matar(p):
    """kill -9 al proceso. En Windows el python.exe de un venv es un lanzador que crea un proceso hijo:
    hay que matar el árbol entero (taskkill /T), si no el consumidor real sigue vivo."""
    if sys.platform == "win32":
        subprocess.run(["taskkill", "/F", "/T", "/PID", str(p.pid)], capture_output=True)
    else:
        p.kill()
    p.wait()


def esperar_redis():
    while True:
        try:
            r = conectar_redis()
            r.ping()
            return r
        except (redis.ConnectionError, redis.BusyLoadingError):
            time.sleep(0.3)


def foto(r):
    f = {"XLEN": r.xlen(STREAM) if r.exists(STREAM) else "(no existe)"}
    try:
        for g in r.xinfo_groups(STREAM):
            cons = {c["name"]: c["pending"] for c in r.xinfo_consumers(STREAM, g["name"])}
            f[f"grupo {g['name']}"] = f"entries-read={g['entries-read']} pendientes={g['pending']} {cons}"
    except redis.ResponseError:
        pass
    if not any(k.startswith("grupo") for k in f):
        f["grupos"] = "(ninguno)"
    f["ultimo:*"] = len(list(r.scan_iter(f"{P}ultimo:*")))
    vent = list(r.scan_iter(f"{P}ventana:*"))
    f["ventana:*"] = f"{len(vent)} zsets, {sum(r.zcard(k) for k in vent)} elementos"
    f["ranking:flujo"] = r.zcard(f"{P}ranking:flujo")
    f["tipo:*"] = len(list(r.scan_iter(f"{P}tipo:*")))
    return f


def experimento(cfg):
    log(f"\n{'=' * 78}\nConfiguración: {cfg}   ({' '.join(COMPOSE[cfg])})\n{'=' * 78}")
    compose(cfg, "up", "-d", "--wait", "redis")
    r = esperar_redis()
    log("config de Redis:", {k: v for k, v in r.config_get("*").items() if k in ("appendonly", "appendfsync", "save", "dir")})
    viejas = [STREAM, f"{STREAM}:fin"] + list(r.scan_iter(f"{P}*"))
    r.delete(*viejas)
    conn = conectar_hbase()
    for t in TABLAS.split(","):
        crear_tabla(t, versiones=1, conn=conn)
    for g in ("ingesta", "tablero"):
        r.xgroup_create(STREAM, g, id="0", mkstream=True)

    tablero = lanzar("tablero.py", "--stream", STREAM, "--prefijo", P)
    ingesta = lanzar("ingesta.py", "--stream", STREAM, "--tablas", TABLAS, "--consumidor", "ingesta-1")
    productor = lanzar("productor.py", "--stream", STREAM, "--hasta", str(CORTE_PRODUCTOR), "--sin-fin")
    while (next(g for g in r.xinfo_groups(STREAM) if g["name"] == "ingesta")["entries-read"] or 0) < CORTE_INGESTA:
        time.sleep(0.2)
    matar(ingesta)                      # la ingesta se cae: lo que siga llegando queda solo en Redis
    productor.wait()
    time.sleep(1)
    matar(tablero)
    r.xreadgroup("ingesta", "sonda", {STREAM: ">"}, count=SONDA)      # 50 pendientes "en vuelo"
    conn = conectar_hbase()             # Thrift corta las conexiones ociosas (~60 s): una nueva
    en_hbase = contar(TABLAS.split(",")[0], conn)
    log(f"ingesta-1 muerta a los ~{CORTE_INGESTA} eventos; productor llegó a {CORTE_PRODUCTOR}; "
        f"la sonda tomó {SONDA} sin XACK. En HBase: {en_hbase} filas.")
    g = next(g for g in r.xinfo_groups(STREAM) if g["name"] == "ingesta")
    ventana = r.xlen(STREAM) - (g["entries-read"] - g["pending"])
    log(f"VENTANA DE PÉRDIDA en este instante: {ventana} entradas están en Redis y todavía NO en HBase "
        f"({g['pending']} pendientes + {g['lag']} sin entregar al grupo ingesta)")
    antes = foto(r)
    log("ANTES del reinicio:")
    for k, v in antes.items():
        log(f"   {k:<22} {v}")

    t = time.perf_counter()
    compose(cfg, "restart", "redis")
    r = esperar_redis()
    log(f"\n$ docker compose restart redis   ({time.perf_counter() - t:.1f} s hasta responder PING)")
    despues = foto(r)
    log("DESPUÉS del reinicio:")
    for k, v in despues.items():
        log(f"   {k:<22} {v}   {'(igual)' if antes.get(k) == v else '(CAMBIÓ)'}")

    # Se reanuda: ingesta-2 recupera pendientes (XAUTOCLAIM) o recrea el grupo; el productor sigue desde 3.000
    tablero = lanzar("tablero.py", "--stream", STREAM, "--prefijo", P, "--consumidor", "tablero-2")
    ingesta = lanzar("ingesta.py", "--stream", STREAM, "--tablas", TABLAS, "--consumidor", "ingesta-2")
    productor = lanzar("productor.py", "--stream", STREAM, "--desde", str(CORTE_PRODUCTOR))
    for p in (productor, ingesta, tablero):
        p.wait()
    conn = conectar_hbase()
    filas = contar(TABLAS.split(",")[0], conn)
    vivo = en_vivo()
    esperadas = {(e["estacion_id"], e["timestamp"]) for e in vivo}
    en_tabla = {tuple(k.decode().split("#")) for k, _ in conn.table(TABLAS.split(",")[0]).scan(
        filter="FirstKeyOnlyFilter() AND KeyOnlyFilter()")}
    faltan = esperadas - en_tabla
    idx = sorted(i for i, e in enumerate(vivo) if (e["estacion_id"], e["timestamp"]) in faltan)
    log(f"\nAl terminar la hora: d2_lecturas tiene {filas:,} filas de {len(esperadas):,} esperadas → "
        f"{'NADA perdido' if not faltan else f'{len(faltan):,} lecturas PERDIDAS'}")
    if idx:
        log(f"   eventos perdidos (índice en la hora en vivo): {idx[0]} … {idx[-1]} "
            f"({len(idx)} eventos, {sum(1 for i in idx if i < CORTE_INGESTA - 200)} de ellos antes del corte de la ingesta)")
    ult = len(list(r.scan_iter(f"{P}ultimo:*")))
    log(f"   tablero al final: {ult} ultimo:* " + ("(sobrevivió al reinicio y siguió actualizándose)"
        if despues["ultimo:*"] else "(se reconstruyó solo con lo que llegó después del reinicio)"))
    return {"antes": antes, "despues": despues, "ventana": ventana, "perdidas": len(faltan)}


res = {cfg: experimento(cfg) for cfg in ("aof", "sin_persistencia")}

log(f"\n{'=' * 78}\nResumen\n{'=' * 78}")
log(f"{'':<22} {'AOF everysec (elegida)':<42} {'sin persistencia':<42}")
for k in res["aof"]["antes"]:
    a = res["aof"]["despues"].get(k, "(no existe)")
    b = res["sin_persistencia"]["despues"].get(k, "(no existe)")
    log(f"{k:<22} {str(a)[:41]:<42} {str(b)[:41]:<42}")
log(f"{'ventana de pérdida':<22} {res['aof']['ventana']:<42} {res['sin_persistencia']['ventana']:<42}")
log(f"{'lecturas perdidas':<22} {res['aof']['perdidas']:<42} {res['sin_persistencia']['perdidas']:<42}")

# Se deja Redis como estaba: la configuración elegida (recarga su AOF del volumen) y sin las claves del experimento
compose("aof", "up", "-d", "--wait", "redis")
r = esperar_redis()
r.delete(*([STREAM, f"{STREAM}:fin"] + list(r.scan_iter(f"{P}*"))))
conn = conectar_hbase()
for t in TABLAS.split(","):
    conn.delete_table(t, disable=True)
log(f"\nRedis vuelve a la configuración elegida; el estado de la Parte B sigue ahí: "
    f"{len(list(r.scan_iter('ultimo:*')))} ultimo:*, stream sensores:t2 con {r.xlen('sensores:t2')} entradas.")
log.cerrar()
