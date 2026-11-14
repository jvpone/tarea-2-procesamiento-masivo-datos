"""B.2 — Consumidor `tablero`: mantiene en Redis el estado "de ahora".

Al terminar la hora en vivo existen (revisables con redis-cli):
    ultimo:<estacion>    hash  {valor, ts (medición, ms), tipo}
    ranking:flujo        zset  estacion → último valor (solo flujo vehicular)
    ventana:<estacion>   zset  score = ts de medición (ms), miembro "<ts>:<valor>"; solo flujo,
                               recortado a los últimos 10 minutos de MEDICIÓN
    tipo:<tipo>          set   estaciones de cada tipo (el catálogo)
    tablero:max_ts       la medición más reciente vista en el stream (ancla de las ventanas)

"Último" = último por MEDICIÓN: un script Lua (atómico en Redis) compara el ts
del evento con el ts guardado en ultimo:<estacion> y solo actualiza si no es
más viejo. Una lectura tardía no hace retroceder el tablero.

    python tablero.py --consumidor tablero-1
"""
import argparse

from comun import GRUPO_TABLERO, STREAM, conectar_redis, ms
from consumo import asegurar_grupo, bucle

VENTANA_MS = 10 * 60 * 1000

LUA = """
-- KEYS: 1 ultimo:<est>  2 ranking:flujo  3 ventana:<est>  4 tipo:<tipo>  5 tablero:max_ts
-- ARGV: 1 estacion  2 tipo  3 valor  4 ts_ms  5 es_flujo(1/0)  6 ventana_ms
redis.call('SADD', KEYS[4], ARGV[1])
local ts = tonumber(ARGV[4])
local prev = redis.call('HGET', KEYS[1], 'ts')
local actualizo = 0
-- ">=": ante dos lecturas del MISMO milisegundo gana la última escrita (igual que HBase)
if (not prev) or ts >= tonumber(prev) then
  redis.call('HSET', KEYS[1], 'valor', ARGV[3], 'ts', ARGV[4], 'tipo', ARGV[2])
  if ARGV[5] == '1' then redis.call('ZADD', KEYS[2], ARGV[3], ARGV[1]) end
  actualizo = 1
end
local maxts = tonumber(redis.call('GET', KEYS[5]) or '0')
if ts > maxts then redis.call('SET', KEYS[5], ARGV[4]); maxts = ts end
if ARGV[5] == '1' then
  -- una sola lectura por milisegundo (un repetido no suma dos veces al promedio)
  redis.call('ZREMRANGEBYSCORE', KEYS[3], ARGV[4], ARGV[4])
  redis.call('ZADD', KEYS[3], ARGV[4], ARGV[4] .. ':' .. ARGV[3])
  -- recorte: solo (max_ts - 10 min, max_ts]
  redis.call('ZREMRANGEBYSCORE', KEYS[3], '-inf', maxts - tonumber(ARGV[6]))
end
return actualizo
"""

ap = argparse.ArgumentParser()
ap.add_argument("--stream", default=STREAM)
ap.add_argument("--grupo", default=GRUPO_TABLERO)
ap.add_argument("--consumidor", default="tablero-1")
ap.add_argument("--prefijo", default="", help="prefijo de las claves del tablero (D.2 usa 'd2:')")
ap.add_argument("--pausa", type=float, default=0.1)
a = ap.parse_args()

r = conectar_redis()
script = r.register_script(LUA)
asegurar_grupo(r, a.stream, a.grupo)
P = a.prefijo
stats = {"eventos": 0, "ignorados_por_tardios": 0}


def procesar(entradas, origen):
    pipe = r.pipeline(transaction=False)
    for _, ev in entradas:
        est, tipo = ev["estacion_id"], ev["tipo_sensor"]
        script(keys=[f"{P}ultimo:{est}", f"{P}ranking:flujo", f"{P}ventana:{est}", f"{P}tipo:{tipo}",
                     f"{P}tablero:max_ts"],
               args=[est, tipo, ev["valor"], ms(ev["timestamp"]), int(tipo == "flujo_vehicular"), VENTANA_MS],
               client=pipe)
    res = pipe.execute()
    stats["eventos"] += len(res)
    stats["ignorados_por_tardios"] += res.count(0)


print(f"[{a.consumidor}] leyendo {a.stream} en el grupo {a.grupo} → tablero en Redis (prefijo '{P}')", flush=True)
n = bucle(r, a.stream, a.grupo, a.consumidor, procesar, pausa=a.pausa, log=lambda *x: print(*x, flush=True))

# Recorte final: cada ventana se recorta cuando SU estación recibe un evento; al cerrar
# la hora se recortan todas contra la última medición del stream.
max_ts = int(r.get(f"{P}tablero:max_ts") or 0)
for k in r.scan_iter(f"{P}ventana:*"):
    r.zremrangebyscore(k, "-inf", max_ts - VENTANA_MS)
print(f"[{a.consumidor}] fin: {n} entradas confirmadas · {stats['ignorados_por_tardios']} no movieron "
      f"ultimo:* por ser más viejas que lo ya guardado (tardías/repetidas)", flush=True)
