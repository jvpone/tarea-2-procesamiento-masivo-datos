"""Parte C — Las seis preguntas, cada una desde el motor que le corresponde, con tiempos medidos.

Cada consulta se corre 6 veces: la primera calienta cachés y se descarta; se
reporta la MEDIANA de las otras 5. Escribe resultados/preguntas.md (lo que se
cita en el informe) y resultados/tiempos.json.

    python preguntas.py            # las seis
    python preguntas.py 1 3        # solo algunas
"""
import json
import platform
import statistics
import subprocess
import sys
import time
import urllib.request
from collections import defaultdict, deque
from datetime import datetime

from comun import (ESTACIONES, RAIZ, RESULTADOS, SPLITS_LECTURAS, STREAM, TZ_CL, clave_est_ts, clave_salt,
                   clave_ts_est, conectar_hbase, conectar_redis, contar, crear_tabla, en_contenedor, en_vivo,
                   fmt_t, hbase_shell, iso, medir, ms, salt)

r, conn = conectar_redis(), conectar_hbase()
lecturas, lecturas_tipo = conn.table("lecturas"), conn.table("lecturas_tipo")
MD = []                 # el informe en markdown
TIEMPOS = {}


def md(*lineas):
    for l in lineas:
        print(l, flush=True)
        MD.append(l)


def tabla_md(cabecera, filas):
    md("| " + " | ".join(cabecera) + " |", "|" + "---|" * len(cabecera))
    for f in filas:
        md("| " + " | ".join(str(x) for x in f) + " |")
    md("")


def tiempo(nombre, mediana, primera, corridas):
    TIEMPOS[nombre] = {"mediana_s": mediana, "primera_s": primera, "corridas_s": corridas}
    md(f"- **{nombre}**: mediana {fmt_t(mediana)} (5 corridas: {', '.join(fmt_t(c) for c in corridas)}; "
       f"la primera, descartada: {fmt_t(primera)})")


def hhmmss(ms_):
    return datetime.fromtimestamp(ms_ / 1000, TZ_CL).strftime("%d-%m %H:%M:%S.%f")[:-3]


def hardware():
    cpu = platform.processor()
    try:
        if sys.platform == "win32":
            cpu = subprocess.run(["powershell", "-NoProfile", "-Command", "(Get-CimInstance Win32_Processor).Name"],
                                 capture_output=True, text=True).stdout.strip() or cpu
        elif sys.platform == "darwin":
            cpu = subprocess.run(["sysctl", "-n", "machdep.cpu.brand_string"], capture_output=True, text=True).stdout.strip()
        else:
            cpu = next(l.split(":", 1)[1].strip() for l in open("/proc/cpuinfo") if l.startswith("model name"))
    except Exception:
        pass
    d = subprocess.run(["docker", "info", "--format", "{{.NCPU}} CPU · {{.MemTotal}} B RAM · {{.ServerVersion}}"],
                       capture_output=True, text=True).stdout.strip()
    try:
        ncpu, mem, ver = d.split(" · ")
        d = f"{ncpu} · {int(mem.split()[0]) / 2**30:.1f} GB RAM para Docker · Docker {ver}"
    except ValueError:
        pass
    return f"{cpu} · {platform.system()} {platform.release()} · Python {platform.python_version()} · {d}"


FLUJO = [e for e, t in ESTACIONES if t == "flujo_vehicular"]
TODAS = [e for e, _ in ESTACIONES]


# =============================================================================
def p1():
    md("## P1 · \"Ahora\" — Redis contra HBase", "")

    def desde_redis():
        pipe = r.pipeline(transaction=False)
        for est in TODAS:
            pipe.hgetall(f"ultimo:{est}")
        pipe.zrevrange("ranking:flujo", 0, 4, withscores=True)
        *ult, top = pipe.execute()
        return dict(zip(TODAS, ult)), top

    def desde_hbase():
        # "Lo último" con clave estacion#timestamp: scan INVERSO desde el final del prefijo de la estación,
        # limit=1. La primera fila que aparece es la de mayor timestamp de MEDICIÓN.
        ult = {}
        for est in TODAS:
            k, v = next(lecturas.scan(row_start=f"{est}#~".encode(), row_stop=f"{est}#".encode(),
                                      reverse=True, limit=1))
            ult[est] = {"valor": v[b"m:valor"].decode(), "ts": ms(k.decode().split("#")[1]), "tipo": v[b"m:tipo"].decode()}
        top = sorted(((e, float(u["valor"])) for e, u in ult.items() if u["tipo"] == "flujo_vehicular"),
                     key=lambda x: -x[1])[:5]
        return ult, top

    def hbase_ingenuo():
        # Lo que NO hay que hacer: recorrer toda la historia de cada estación y quedarse con la última fila.
        ult = {}
        for est in TODAS:
            for k, v in lecturas.scan(row_prefix=f"{est}#".encode(), batch_size=5000):
                pass
            ult[est] = (k, v)
        return ult

    def hbase_hacia_adelante():
        # Variante: si se sabe una cota inferior de "ahora" (acá, 1 h antes de la última medición), un scan
        # HACIA ADELANTE de esa última hora por estación, quedándose con la última fila.
        desde = iso(int(r.get("tablero:max_ts")) - 3_600_000)
        ult = {}
        for est in TODAS:
            for k, v in lecturas.scan(row_start=f"{est}#{desde}".encode(), row_stop=f"{est}#~".encode(),
                                      batch_size=1000):
                pass
            ult[est] = (k.decode().split("#")[1], v[b"m:valor"].decode())
        return ult

    (ult_r, top_r), t_r, p_r, c_r = medir(desde_redis)
    (ult_h, top_h), t_h, p_h, c_h = medir(desde_hbase)
    ult_f, t_f, p_f, c_f = medir(hbase_hacia_adelante)
    _, t_i, p_i, c_i = medir(hbase_ingenuo, corridas=5)
    assert all(ult_f[e] == (iso(ult_h[e]["ts"]), ult_h[e]["valor"]) for e in TODAS)

    filas = []
    for est in TODAS:
        a, b = ult_r[est], ult_h[est]
        filas.append([est, a["tipo"], a["valor"], hhmmss(int(a["ts"])), b["valor"], hhmmss(b["ts"]),
                      "✓" if (float(a["valor"]), int(a["ts"])) == (float(b["valor"]), b["ts"]) else "✗"])
    tabla_md(["estación", "tipo", "valor (Redis)", "ts medición (Redis)", "valor (HBase)", "ts medición (HBase)",
              "="], filas)
    md("Top 5 de flujo vehicular (último valor):", "")
    tabla_md(["#", "Redis `ZREVRANGE ranking:flujo 0 4`", "HBase (16 scans inversos + sort)"],
             [[i + 1, f"{a} = {s:g}", f"{e} = {v:g}"] for i, ((a, s), (e, v)) in enumerate(zip(top_r, top_h))])
    tiempo("P1 Redis (16 HGETALL + ZREVRANGE, en un pipeline)", t_r, p_r, c_r)
    tiempo("P1 HBase (16 scans inversos limit=1 + ordenar en Python)", t_h, p_h, c_h)
    tiempo("P1 HBase variante: scan hacia adelante de la última hora por estación", t_f, p_f, c_f)
    tiempo("P1 HBase ingenuo (recorrer toda la historia de cada estación)", t_i, p_i, c_i)
    md(f"", f"HBase / Redis = **{t_h / t_r:,.0f}×** con scans inversos, {t_f / t_r:,.0f}× con la variante hacia "
           f"adelante y {t_i / t_r:,.0f}× recorriendo toda la historia (las cuatro dan el mismo resultado).", "",
       "Cómo sacar \"lo último\" de HBase con `estacion#timestamp`: (1) **scan inverso** desde el final del "
       "prefijo de la estación con `limit=1` — no necesita saber nada, pero el scan inverso es caro cuando los "
       "datos ya están en HFiles (HBase tiene que retroceder bloque a bloque: medido ~40 ms por estación después "
       "del `flush`, ~6 ms cuando la hora en vivo aún estaba en la MemStore); (2) si se conoce una cota de "
       "\"ahora\", un scan **hacia adelante** de esa ventana; (3) en diseño, una tabla con clave "
       "`estacion#(10^13 − ms)` (Lab 07) deja la última lectura primera y la consulta es un scan hacia adelante "
       "con `limit=1`, al precio de otra escritura por evento. Ninguna se acerca a Redis.", "")

    # ¿Qué estación cambia según el reloj? Último por LLEGADA = la última entrada del stream (ID de Redis).
    por_llegada = {}
    for sid, ev in r.xrange(STREAM):
        por_llegada[ev["estacion_id"]] = (sid, ev)
    distintas = [e for e in TODAS
                 if (float(por_llegada[e][1]["valor"]), ms(por_llegada[e][1]["timestamp"]))
                 != (float(ult_r[e]["valor"]), int(ult_r[e]["ts"]))]
    md("**Último por medición vs. último por llegada.** Por llegada = la última entrada del stream con esa "
       "estación (el ID de Redis es la hora de llegada); por medición = `ultimo:<estacion>`.", "")
    filas = []
    for e in distintas:
        sid, ev = por_llegada[e]
        llegada_ms = int(sid.split("-")[0])
        filas.append([e, "por llegada (última entrada del stream)", ev["valor"], ev["timestamp"],
                      f"ID {sid}"])
        # la entrada del stream que corresponde al último por medición
        sid_m = next(s for s, x in r.xrange(STREAM) if x["estacion_id"] == e
                     and ms(x["timestamp"]) == int(ult_r[e]["ts"]) and x["valor"] == ult_r[e]["valor"])
        filas.append([e, "por medición (`ultimo:" + e + "`)", ult_r[e]["valor"], iso(int(ult_r[e]["ts"])),
                      f"ID {sid_m}"])
    tabla_md(["estación", "criterio", "valor", "timestamp de medición", "llegó al stream"], filas)
    md(f"Estaciones donde los dos criterios difieren: **{', '.join(distintas) or 'ninguna'}**. La última entrada "
       f"que llegó de esa estación es una lectura **tardía**: se midió antes que otra que ya había llegado. "
       f"Un tablero 'por llegada' retrocedería; el script Lua de `tablero.py` la ignora porque su ts es menor.", "")
    md("**Interpretación.** Las dos respuestas coinciden fila a fila, pero Redis responde en "
       f"{fmt_t(t_r)} con un solo viaje (pipeline) porque el estado 'de ahora' ya está precalculado, mientras que "
       f"HBase necesita 16 scans (uno por estación) y ordenar en el cliente. Un dashboard que refresca cada "
       f"segundo para muchos usuarios lee de Redis; HBase se reserva para la historia.", "")
    return distintas


# =============================================================================
def p2():
    md("## P2 · Ventana de 10 minutos — Redis", "")
    VENT = 600_000

    def ventanas():
        ancla = int(r.get("tablero:max_ts"))           # la medición más reciente del stream
        pipe = r.pipeline(transaction=False)
        for est in FLUJO:
            pipe.zrangebyscore(f"ventana:{est}", f"({ancla - VENT}", "+inf")
        res = pipe.execute()
        out = {}
        for est, miembros in zip(FLUJO, res):
            vals = [float(m.split(":")[1]) for m in miembros]
            out[est] = (len(vals), sum(vals) / len(vals) if vals else None)
        return ancla, out

    (ancla, prom), t, p, c = medir(ventanas)
    md(f"Ancla = `tablero:max_ts` = {iso(ancla)} → ventana ({iso(ancla - VENT)}, {iso(ancla)}].", "")
    tabla_md(["estación", "lecturas en la ventana", "promedio de flujo (veh/min)"],
             [[e, n, f"{v:.2f}"] for e, (n, v) in sorted(prom.items(), key=lambda x: -x[1][1])])
    tiempo("P2 Redis (8 ZRANGEBYSCORE en un pipeline + promedio)", t, p, c)

    # Control: lo mismo calculado desde el generador (NO es la respuesta, solo valida la de Redis)
    vivo = en_vivo()
    ok = True
    for est in FLUJO:
        vistos = {}
        for e in vivo:
            if e["estacion_id"] == est and ms(e["timestamp"]) > ancla - VENT:
                vistos[e["timestamp"]] = e["valor"]
        esperado = sum(vistos.values()) / len(vistos)
        ok &= abs(esperado - prom[est][1]) < 1e-9
    md(f"Control contra el generador (dedup por timestamp): {'calza' if ok else 'NO calza'}.", "")

    # Memoria: con recorte vs. sin recorte
    mem_v = {est: r.memory_usage(f"ventana:{est}") for est in FLUJO}
    enc = r.object("encoding", f"ventana:{FLUJO[0]}")
    pipe = r.pipeline(transaction=False)
    for sid, ev in r.xrange(STREAM):
        if ev["tipo_sensor"] == "flujo_vehicular":
            pipe.zadd(f"tmp:sin_recorte:{ev['estacion_id']}", {f"{ms(ev['timestamp'])}:{ev['valor']}": ms(ev["timestamp"])})
    pipe.execute()
    mem_h = {est: r.memory_usage(f"tmp:sin_recorte:{est}") for est in FLUJO}
    n_h = {est: r.zcard(f"tmp:sin_recorte:{est}") for est in FLUJO}
    enc_h = r.object("encoding", f"tmp:sin_recorte:{FLUJO[0]}")
    # 5 días de una estación (sacados de HBase) en un zset, para medir y no solo extrapolar
    pipe = r.pipeline(transaction=False)
    for k, v in lecturas.scan(row_prefix=b"CC-001#", batch_size=5000):
        t_ms = ms(k.decode().split("#")[1])
        pipe.zadd("tmp:sin_recorte_5d:CC-001", {f"{t_ms}:{v[b'm:valor'].decode()}": t_ms})
    pipe.execute()
    mem_5d, n_5d = r.memory_usage("tmp:sin_recorte_5d:CC-001"), r.zcard("tmp:sin_recorte_5d:CC-001")
    r.delete(*[f"tmp:sin_recorte:{e}" for e in FLUJO], "tmp:sin_recorte_5d:CC-001")
    nv = sum(r.zcard(f"ventana:{e}") for e in FLUJO)
    tabla_md(["zset", "elementos", "MEMORY USAGE", "bytes/elemento", "codificación"], [
        ["8 × `ventana:*` (10 min, recortadas)", nv, f"{sum(mem_v.values()):,} B", f"{sum(mem_v.values()) / nv:.0f}", enc],
        ["8 × sin recortar (la hora entera)", sum(n_h.values()), f"{sum(mem_h.values()):,} B",
         f"{sum(mem_h.values()) / sum(n_h.values()):.0f}", enc_h],
        ["CC-001 sin recortar, 5 días + 1 h (desde HBase)", n_5d, f"{mem_5d:,} B", f"{mem_5d / n_5d:.0f}", "skiplist"],
    ])
    por_dia = mem_5d / n_5d * (172_800 / 16) * len(FLUJO)
    md(f"Sin recorte, la memoria crece lineal con el tiempo: ~{por_dia / 2**20:.1f} MB por día para las 8 "
       f"estaciones de flujo, ~{por_dia * 365 / 2**30:.2f} GB al año, sin límite. Con el recorte queda acotada "
       f"(~{sum(mem_v.values()) / 2**10:.0f} KB) para siempre. Ojo con el salto de codificación: hasta 128 "
       f"elementos Redis guarda el zset como *listpack* (compacto); la hora entera ya pasa a *skiplist* "
       f"(índice + hash), varias veces más bytes por elemento.", "")
    md("**¿Por qué un sorted set con el ts como score?** Porque Redis lo mantiene ordenado por tiempo de medición: "
       "recortar es `ZREMRANGEBYSCORE -inf <ancla-10min>` y leer la ventana es `ZRANGEBYSCORE`, ambos O(log n + m), "
       "y una lectura tardía se inserta en su lugar (por medición), no al final como en una lista.", "")
    md(f"**Interpretación.** El promedio móvil sale en {fmt_t(t)} leyendo solo lo que está en la ventana; "
       f"el recorte es lo que permite que esta estructura viva en RAM indefinidamente.", "")


# =============================================================================
def p3():
    md("## P3 · Un rango — HBase", "")
    ini, fin = "CC-001#2026-11-04T08:00:00.000-03:00", "CC-001#2026-11-04T10:00:00.000-03:00"

    def cuartos(filas):
        g = defaultdict(list)
        for k, v in filas:
            ts = datetime.fromisoformat(k.decode().split("#")[1])
            g[ts.replace(minute=ts.minute // 15 * 15, second=0, microsecond=0)].append(float(v[b"m:valor"]))
        return len(filas), {q: (len(v), sum(v) / len(v)) for q, v in sorted(g.items())}

    def por_rango():
        return cuartos(list(lecturas.scan(row_start=ini.encode(), row_stop=fin.encode())))

    def tabla_entera():
        # recorrido de TODA la tabla, con el filtro evaluado en el RegionServer (sobre la row key)
        f = "RowFilter(=, 'regexstring:^CC-001#2026-11-04T0[89]:')"
        return cuartos(list(lecturas.scan(filter=f, batch_size=5000)))

    (n, q), t_r, p_r, c_r = medir(por_rango)
    (n2, q2), t_f, p_f, c_f = medir(tabla_entera)
    md(f"Scan por rango `[{ini}, {fin})` → **{n} lecturas**. Recorrido entero + `RowFilter` → {n2} lecturas "
       f"({'mismo resultado' if (n, q) == (n2, q2) else 'DISTINTO'}).", "")
    tabla_md(["cuarto de hora", "lecturas", "promedio (veh/min)"],
             [[k.strftime("%H:%M") + "–" + (k.replace(minute=(k.minute + 15) % 60, hour=k.hour + (k.minute == 45))).strftime("%H:%M"),
               c, f"{v:.2f}"] for k, (c, v) in q.items()])
    tiempo("P3 HBase scan por rango", t_r, p_r, c_r)
    tiempo("P3 HBase tabla entera + RowFilter", t_f, p_f, c_f)
    total = contar("lecturas", conn)
    md("", f"El recorrido completo es **{t_f / t_r:,.0f}× más lento**: lee {total:,} filas para devolver {n} "
           f"({n / total:.4%}). El scan por rango salta directo a la región y la posición de `CC-001#2026-11-04T08` "
           f"(la row key es el índice) y lee solo esas {n} filas.", "")
    md(f"**Con cinco años** (365× más filas): el scan por rango tarda lo mismo (depende de las filas del rango, no "
       f"del tamaño de la tabla), y el recorrido crece lineal: del orden de {t_f * 365 / 60:,.0f} min en esta "
       f"máquina — en un cluster se paraleliza entre RegionServers, pero sigue leyendo todo.", "")


# =============================================================================
def p4():
    md("## P4 · Dos patrones de acceso — catálogo en Redis + HBase", "")
    D, H = "2026-11-05T07:00:00.000-03:00", "2026-11-05T09:00:00.000-03:00"

    def agregar(pares):
        """pares: (estacion, ts_iso, valor) en orden de ts por estación → promedio, máximo, hora del 1er máximo."""
        g = defaultdict(list)
        for est, ts, v in pares:
            g[est].append((ts, v))
        out = {}
        for est, xs in sorted(g.items()):
            vmax = max(v for _, v in xs)
            out[est] = (len(xs), sum(v for _, v in xs) / len(xs), vmax, next(ts for ts, v in xs if v == vmax))
        return out

    def a_por_estacion():
        estaciones = sorted(r.smembers("tipo:calidad_aire"))           # el catálogo, de Redis
        pares = []
        for est in estaciones:
            for k, v in lecturas.scan(row_start=f"{est}#{D}".encode(), row_stop=f"{est}#{H}".encode()):
                pares.append((est, k.decode().split("#")[1], float(v[b"m:valor"])))
        return agregar(pares)

    def b_por_tipo():
        pares = [(v[b"m:estacion"].decode(), k.decode().split("#")[1], float(v[b"m:valor"]))
                 for k, v in lecturas_tipo.scan(row_start=f"calidad_aire#{D}".encode(),
                                                row_stop=f"calidad_aire#{H}".encode())]
        return agregar(pares)

    ra, t_a, p_a, c_a = medir(a_por_estacion)
    rb, t_b, p_b, c_b = medir(b_por_tipo)
    md(f"Rango `[05-nov 07:00, 09:00)`. Estaciones de calidad del aire según `SMEMBERS tipo:calidad_aire`: "
       f"{', '.join(sorted(r.smembers('tipo:calidad_aire')))}.", "")
    tabla_md(["estación", "lecturas", "PM2.5 promedio", "PM2.5 máximo", "hora del máximo (1ª vez)"],
             [[e, n, f"{p:.2f}", f"{m:g}", ts[11:23]] for e, (n, p, m, ts) in ra.items()])
    md(f"(a) y (b) dan exactamente lo mismo: {ra == rb}.", "")
    tiempo("P4 (a) Redis SMEMBERS + 4 scans en `lecturas`", t_a, p_a, c_a)
    tiempo("P4 (b) 1 scan en `lecturas_tipo`", t_b, p_b, c_b)

    # Lo que cuesta la segunda tabla
    print(hbase_shell("flush 'lecturas'\nflush 'lecturas_tipo'\n"))
    time.sleep(3)
    du = {t: int(en_contenedor(f"du -sk /data/hbase/data/default/{t} | cut -f1").strip() or 0)
          for t in ("lecturas", "lecturas_tipo")}
    carga = (RESULTADOS / "b1_carga_historica.txt").read_text(encoding="utf-8") \
        if (RESULTADOS / "b1_carga_historica.txt").exists() else ""
    md("", "**Lo que cuesta `lecturas_tipo`:**", "")
    tabla_md(["", "lecturas", "lecturas_tipo"], [
        ["put por evento", "1", "1 más (el doble de escrituras: 871.200 put extra)"],
        ["disco (HFiles tras flush, `du`)", f"{du['lecturas'] / 1024:.1f} MB", f"{du['lecturas_tipo'] / 1024:.1f} MB"],
        ["carga histórica", *[l.split("→")[1].split("·")[0].strip() for l in carga.splitlines()
                              if l.startswith(("lecturas:", "lecturas_tipo:"))][:2]] if carga.count("→") >= 2
        else ["carga histórica", "ver b1", "ver b1"],
    ])
    md(f"La segunda tabla ocupa **{du['lecturas_tipo'] / max(du['lecturas'], 1):.2f}× lo que la primera** (su clave es "
       f"más larga y HBase la repite en cada celda): el disco total pasa de {du['lecturas'] / 1024:.0f} MB a "
       f"{(du['lecturas'] + du['lecturas_tipo']) / 1024:.0f} MB "
       f"({(du['lecturas'] + du['lecturas_tipo']) / max(du['lecturas'], 1):.1f}×), y en un cluster con HDFS "
       f"(réplica 3) eso se triplica. Las escrituras se duplican exactas. En la ingesta en vivo cada lote hace dos "
       f"`batch.send()`; si el segundo falla, el proceso no llega al `XACK`: el lote queda pendiente y se "
       f"reintenta entero, y como ambas claves son idempotentes reescribir la primera tabla no duplica nada. "
       f"Lo que sí existe es una ventana en que `lecturas` tiene el evento y `lecturas_tipo` todavía no "
       f"(HBase solo es atómico por fila, no entre tablas).", "")
    md(f"**Interpretación.** Con 4 estaciones por tipo, (a) son 4 scans cortos y (b) 1 scan: "
       f"{fmt_t(t_a)} vs {fmt_t(t_b)} ({t_a / t_b:.1f}×). Acá la segunda tabla **no vale la pena**: duplica "
       f"escrituras y disco para ahorrar milisegundos. Valdría con cientos/miles de estaciones por tipo (un scan "
       f"en vez de miles) o si la consulta por tipo fuera la más frecuente del sistema.", "")


# =============================================================================
def p5():
    md("## P5 · Hotspotting y salting — HBase", "")
    vivo = en_vivo()
    disenos = [
        ("p5_est_ts", clave_est_ts, SPLITS_LECTURAS, "estacion#ts"),
        ("p5_ts_est", clave_ts_est, ["2026-11-03", "2026-11-04", "2026-11-05", "2026-11-06", "2026-11-07"],
         "ts#estacion (por día)"),
        ("p5_salt", clave_salt, ["1", "2", "3"], "salt#ts#estacion (por salt)"),
    ]
    for nombre, _, splits, _ in disenos:
        crear_tabla(nombre, splits, versiones=1, conn=conn)

    def escrituras_por_region(tabla):
        m = json.load(urllib.request.urlopen(
            "http://localhost:16010/jmx?qry=Hadoop:service=HBase,name=RegionServer,sub=Regions"))["beans"][0]
        return {(rg["start_key"].decode() or "(inicio)"):
                m.get(f"Namespace_default_table_{tabla}_region_{rg['name'].decode().split('.')[-2]}_metric_writeRequestCount", 0)
                for rg in conn.table(tabla).regions()}

    time.sleep(12)
    antes = {n: escrituras_por_region(n) for n, *_ in disenos}
    for nombre, fclave, _, _ in disenos:
        with conn.table(nombre).batch(batch_size=500) as b:
            for ev in vivo:
                b.put(fclave(ev), {b"m:valor": str(ev["valor"]).encode()})
    time.sleep(25)                      # JMX publica cada ~10 s
    despues = {n: escrituras_por_region(n) for n, *_ in disenos}
    md(f"La hora en vivo ({len(vivo):,} put) escrita en tres tablas. Salt de cada estación (crc32 mod 4): "
       + ", ".join(f"{e}→{salt(e)}" for e in TODAS) + ".", "")
    md("Escrituras recibidas por región (`writeRequestCount` de JMX, después − antes):", "")
    filas = []
    for nombre, _, _, desc in disenos:
        d = {k: despues[nombre][k] - antes[nombre][k] for k in antes[nombre]}
        tot = sum(d.values()) or 1
        filas.append([f"`{desc}`", " · ".join(f"[{k}] {v:,} ({v / tot:.0%})" for k, v in d.items())])
    tabla_md(["diseño", "escrituras por región (clave de inicio de la región)"], filas)

    A, B = "2026-11-07T00:10:00.000-03:00", "2026-11-07T00:20:00.000-03:00"

    def q_est_ts():
        t = conn.table("p5_est_ts")
        return sum(len(list(t.scan(row_start=f"{e}#{A}".encode(), row_stop=f"{e}#{B}".encode()))) for e in TODAS)

    def q_ts_est():
        return len(list(conn.table("p5_ts_est").scan(row_start=A.encode(), row_stop=B.encode())))

    def q_salt():
        t = conn.table("p5_salt")
        return sum(len(list(t.scan(row_start=f"{s}#{A}".encode(), row_stop=f"{s}#{B}".encode()))) for s in range(4))

    res = [medir(q_est_ts), medir(q_ts_est), medir(q_salt)]
    md(f"\"Todas las lecturas de todas las estaciones entre 00:10 y 00:20 del 07-nov\": "
       f"{', '.join(str(x[0]) for x in res)} filas (deben ser iguales).", "")
    for (n, t, p, c), etiqueta in zip(res, ["P5 estacion#ts (16 rangos)", "P5 ts#estacion (1 rango)",
                                            "P5 salt#ts#estacion (4 rangos)"]):
        tiempo(etiqueta, t, p, c)
    md("", "**Interpretación.** `ts#estacion` concentra el 100 % de las escrituras de la hora en vivo en la última "
           "región (la de \"hoy\"; las tardías de antes de medianoche caen en la anterior): con N servidores "
           "trabaja uno. `estacion#ts` y el salt reparten (el salt, tan parejo como reparta crc32 a 16 "
           "estaciones). Para leer un intervalo de todas las estaciones, `ts#estacion` es un rango, el salt 4 y "
           "`estacion#ts` 16, pero en milisegundos las tres son comparables con este volumen. **Elegimos "
           "`estacion#ts`** (la de `lecturas`): reparte la escritura y su consulta típica (una estación en un rango) "
           "es un solo scan. Cambiaríamos a salt si la consulta dominante fuera \"todo el sistema en un intervalo\" "
           "con muchísimas estaciones (16 rangos → miles), y a `ts#estacion` solo con escritura por lotes "
           "históricos (sin flujo continuo que genere hotspot) o en una sola máquina.", "")


# =============================================================================
def p6():
    UMBRAL, VENT = 50.0, 600_000
    md("## P6 · La propia — \"¿Desde cuándo está sobre la norma?\"", "")
    md(f"Pregunta: *¿qué estaciones de calidad del aire están **ahora** sobre {UMBRAL:g} µg/m³ de PM2.5 (referencia: "
       f"la norma diaria chilena de PM2.5) y **desde cuándo** llevan el episodio sin interrupción?* Un episodio "
       f"sigue mientras el promedio móvil de 10 minutos de medición se mantiene ≥ {UMBRAL:g} (con un umbral sobre "
       f"lecturas sueltas, el ruido cortaría el episodio a cada rato).", "")
    md("**Reparto.** Redis decide el **qué**: el catálogo `tipo:calidad_aire` y el `ultimo:<est>` dicen qué "
       "estaciones están en episodio ahora y desde qué medición mirar. HBase decide el **cuándo**: un scan "
       "**inverso** sobre `lecturas` (`estacion#timestamp`) que parte en la medición actual y retrocede, con un "
       "promedio móvil de 10 min, hasta el primer tramo bajo el umbral. El stream solo tiene la última hora: si el "
       "episodio empezó antes, Redis no puede saberlo.", "")

    def consulta():
        estaciones = sorted(r.smembers("tipo:calidad_aire"))               # Redis: el qué
        pipe = r.pipeline(transaction=False)
        for est in estaciones:
            pipe.hgetall(f"ultimo:{est}")
        out = []
        for est, h in zip(estaciones, pipe.execute()):
            v0, ts0 = float(h["valor"]), int(h["ts"])
            if v0 < UMBRAL:
                out.append((est, v0, ts0, None, 0, "bajo la norma ahora"))
                continue
            dq, suma, leidas, desde = deque(), 0.0, 0, None                 # HBase: el cuándo
            for k, v in lecturas.scan(row_start=f"{est}#{iso(ts0)}".encode(), row_stop=f"{est}#".encode(),
                                      reverse=True, batch_size=2000, columns=[b"m:valor"]):
                leidas += 1
                t_ms, x = ms(k.decode().split("#")[1]), float(v[b"m:valor"])
                dq.append((t_ms, x))
                suma += x
                while dq[0][0] - t_ms > VENT:
                    suma -= dq.popleft()[1]
                if dq[0][0] - t_ms >= VENT - 10_000 and suma / len(dq) < UMBRAL:
                    desde = dq[0][0]                 # el último tramo de 10 min bajo la norma termina acá
                    break
            out.append((est, v0, ts0, desde, leidas, "en episodio"))
        return out

    res, t, p, c = medir(consulta)
    inicio_stream = int(r.xrange(STREAM, count=1)[0][0].split("-")[0])
    filas = []
    for est, v0, ts0, desde, leidas, estado in res:
        if estado != "en episodio":
            filas.append([est, f"{v0:g}", estado, "—", "—", "—"])
            continue
        if desde is None:
            primera = next(lecturas.scan(row_prefix=f"{est}#".encode(), limit=1))[0].decode().split("#")[1]
            filas.append([est, f"{v0:g}", "en episodio", f"desde antes del inicio de la historia ({primera[:16]})",
                          f"> {(ts0 - ms(primera)) / 3_600_000:,.0f} h", f"{leidas:,} (toda su historia)"])
        else:
            dur = (ts0 - desde) / 60_000
            filas.append([est, f"{v0:g}", "en episodio", iso(desde)[:23].replace("T", " "),
                          f"{dur:,.0f} min" + (" (empezó antes del stream)" if desde < inicio_stream else ""),
                          f"{leidas:,}"])
    tabla_md(["estación", "PM2.5 ahora (Redis)", "estado", "episodio desde (HBase)", "duración", "filas leídas en HBase"],
             filas)
    tiempo("P6 Redis (catálogo + últimos) + HBase (scans inversos con promedio móvil)", t, p, c)
    md("", "**Interpretación.** El costo en HBase lo decide la respuesta, no la tabla: un episodio corto lee unos "
           "cientos de filas, uno que dura toda la historia recorre la estación completa (~54 mil filas), y aun así es "
           "un scan contiguo gracias a la row key. Hacerlo solo con Redis exigiría guardar días de lecturas en RAM "
           "(lo que P2 mostró que no escala); hacerlo solo con HBase obligaría a reconstruir \"el ahora\" con scans "
           "inversos en cada consulta en vez de leer cuatro hashes.", "")


# =============================================================================
if __name__ == "__main__":
    elegidas = sys.argv[1:] or ["1", "2", "3", "4", "5", "6"]
    hw = hardware()
    md("# Parte C — Resultados de las preguntas", "",
       f"Medido el {datetime.now(TZ_CL).strftime('%Y-%m-%d %H:%M')} en: {hw}. "
       "HBase 2.5.15 standalone (un solo proceso) y Redis 7 en Docker, en la misma máquina.", "",
       "Tiempo = mediana de 5 corridas, descartando una primera corrida de calentamiento.", "")
    for q in elegidas:
        globals()[f"p{q}"]()
    sufijo = "" if len(elegidas) == 6 else "_" + "_".join(elegidas)
    (RESULTADOS / f"preguntas{sufijo}.md").write_text("\n".join(MD) + "\n", encoding="utf-8")
    (RESULTADOS / f"tiempos{sufijo}.json").write_text(json.dumps({"hardware": hw, "tiempos": TIEMPOS}, indent=1,
                                                                 ensure_ascii=False), encoding="utf-8")
