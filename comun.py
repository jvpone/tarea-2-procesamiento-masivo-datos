"""Constantes y utilidades compartidas por todos los scripts de la Tarea 2.

Todo lo que define "qué datos" y "qué claves" vive acá, para que la carga
histórica, los consumidores, la verificación y las preguntas usen exactamente
las mismas funciones de row key.
"""
import itertools
import os
import statistics
import subprocess
import sys
import time
import zlib
from datetime import datetime, timedelta, timezone
from pathlib import Path

import happybase
import redis

from generador import ESTACIONES, eventos

RAIZ = Path(__file__).resolve().parent
RESULTADOS = RAIZ / "resultados"
RESULTADOS.mkdir(exist_ok=True)

# --- Parámetros de la tarea (enunciado §2) ----------------------------------
SEMILLA = 2026                                   # ojo: el default del generador es 2509
TZ_CL = timezone(timedelta(hours=-3))            # noviembre en Chile: UTC−3 (horario de verano)
T0 = datetime(2026, 11, 2, tzinfo=TZ_CL)         # 2026-11-02T00:00:00-03:00
N_HIST = 864_000                                 # 5 días × 172.800
N_VIVO = 7_200                                   # la hora siguiente
PAUSA_VIVO = 0.025                               # 20× el real: un evento cada 25 ms

# Números de control (enunciado §2)
CONTROL = {
    "hist": {"eventos": 864_000, "repetidos": 17_090, "claves": 846_906},
    "vivo": {"eventos": 7_200, "repetidos": 153, "claves": 7_047},
    "total": {"eventos": 871_200, "repetidos": 17_243, "claves": 853_953},
}

TIPO_DE = dict(ESTACIONES)
COMUNAS = {"CC": "Concepción", "TAL": "Talcahuano", "HUA": "Hualpén", "SPP": "San Pedro de la Paz",
           "CHI": "Chiguayante", "PEN": "Penco", "COR": "Coronel"}


def comuna(estacion):
    return COMUNAS[estacion.split("-")[0]]


def todos_los_eventos():
    """Los 871.200 eventos de la tarea (histórico + hora en vivo), siempre los mismos."""
    return list(itertools.islice(eventos(T0, semilla=SEMILLA), N_HIST + N_VIVO))


def historico():
    return list(itertools.islice(eventos(T0, semilla=SEMILLA), N_HIST))


def en_vivo():
    return list(itertools.islice(eventos(T0, semilla=SEMILLA), N_HIST, N_HIST + N_VIVO))


def ms(ts_iso):
    """Timestamp ISO (con offset) → milisegundos desde 1970 (el reloj de MEDICIÓN)."""
    return int(datetime.fromisoformat(ts_iso).timestamp() * 1000)


def iso(ms_):
    return datetime.fromtimestamp(ms_ / 1000, TZ_CL).isoformat(timespec="milliseconds")


# --- Redis ------------------------------------------------------------------
REDIS_HOST = os.environ.get("REDIS_HOST", "127.0.0.1")
REDIS_PORT = int(os.environ.get("REDIS_PORT", 6379))
STREAM = "sensores:t2"
GRUPO_INGESTA = "ingesta"
GRUPO_TABLERO = "tablero"
MAXLEN = 20_000            # ~ (aproximado): ver README §MAXLEN
CLAVE_FIN = "t2:productor:fin"     # el productor la escribe al terminar (con el total producido)


def conectar_redis(**kw):
    return redis.Redis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=True, **kw)


# --- HBase ------------------------------------------------------------------
HBASE_HOST = os.environ.get("HBASE_HOST", "127.0.0.1")
HBASE_PORT = int(os.environ.get("HBASE_PORT", 9090))
FAMILIA = "m"
VERSIONES = 3              # para poder MIRAR las versiones de una celda reprocesada (D.1)
URL_JMX = "http://localhost:16010/jmx?qry=Hadoop:service=HBase,name=RegionServer,sub=Regions"

# Cortes de región (justificados en el README §Regiones).
SPLITS_LECTURAS = ["CC-005", "HUA", "SPP-002"]
SPLITS_TIPO = ["flujo_vehicular", "flujo_vehicular#2026-11-04T12", "ocupacion_estacionamiento"]


def conectar_hbase():
    return happybase.Connection(HBASE_HOST, HBASE_PORT, timeout=120_000)


# Row keys. TODAS salen de la estación y del timestamp de MEDICIÓN del evento,
# nunca de cuándo llegó: un repetido o una tardía cae siempre en la misma fila.
def clave_lecturas(ev):
    """lecturas: estacion#timestamp  →  CC-001#2026-11-04T08:00:00.000-03:00"""
    return f"{ev['estacion_id']}#{ev['timestamp']}".encode()


def clave_tipo(ev):
    """lecturas_tipo: tipo#timestamp#estacion  →  calidad_aire#2026-11-05T07:00:00.000-03:00#CC-004"""
    return f"{ev['tipo_sensor']}#{ev['timestamp']}#{ev['estacion_id']}".encode()


def cols_lecturas(ev):
    return {b"m:valor": str(ev["valor"]).encode(), b"m:tipo": ev["tipo_sensor"].encode()}


def cols_tipo(ev):
    return {b"m:valor": str(ev["valor"]).encode(), b"m:estacion": ev["estacion_id"].encode()}


# P5: tres diseños de clave para la misma hora en vivo
def salt(estacion):
    """hash estable (crc32; el hash() de Python cambia entre procesos) mod 4."""
    return zlib.crc32(estacion.encode()) % 4


def clave_est_ts(ev):
    return f"{ev['estacion_id']}#{ev['timestamp']}".encode()


def clave_ts_est(ev):
    return f"{ev['timestamp']}#{ev['estacion_id']}".encode()


def clave_salt(ev):
    return f"{salt(ev['estacion_id'])}#{ev['timestamp']}#{ev['estacion_id']}".encode()


def hbase_shell(comandos):
    """Corre comandos en el `hbase shell` del contenedor (lo que happybase no sabe: SPLITS, flush...)."""
    p = subprocess.run(["docker", "compose", "exec", "-T", "hbase", "hbase", "shell", "-n"],
                       input=comandos, capture_output=True, text=True, cwd=RAIZ, encoding="utf-8",
                       errors="replace")
    ruido = ("SLF4J", "Took ", "HBase Shell", "Use \"help\"", "For Reference", "Version ", "hbase:0")
    return "\n".join(l for l in (p.stdout + p.stderr).splitlines() if l.strip() and not l.startswith(ruido))


def en_contenedor(cmd, servicio="hbase"):
    return subprocess.run(["docker", "compose", "exec", "-T", servicio, "sh", "-c", cmd],
                          capture_output=True, text=True, cwd=RAIZ).stdout


def crear_tabla(nombre, splits=None, versiones=VERSIONES, conn=None):
    """Borra (si existe) y crea una tabla pre-dividida en regiones."""
    conn = conn or conectar_hbase()
    if nombre.encode() in conn.tables():
        conn.delete_table(nombre, disable=True)
    corte = f", SPLITS => {splits!r}" if splits else ""
    salida = hbase_shell(f"create '{nombre}', {{NAME => '{FAMILIA}', VERSIONS => {versiones}}}{corte}\n")
    if nombre.encode() not in conectar_hbase().tables():
        sys.exit(f"No se pudo crear la tabla {nombre}:\n{salida}")
    return conn.table(nombre).regions()


def contar(tabla, conn=None, **scan_kw):
    """COUNT(*) a la HBase: recorrer, trayendo solo la primera clave de cada fila (Lab 07, 6.4)."""
    conn = conn or conectar_hbase()
    return sum(1 for _ in conn.table(tabla).scan(filter="FirstKeyOnlyFilter() AND KeyOnlyFilter()",
                                                 batch_size=5000, **scan_kw))


# --- Medición ---------------------------------------------------------------
def medir(fn, corridas=5):
    """Corre fn() 1 + `corridas` veces: la primera calienta cachés y se descarta.
    Devuelve (resultado de la última corrida, mediana en s, primera en s, lista de tiempos)."""
    tiempos, res = [], None
    for _ in range(corridas + 1):
        t = time.perf_counter()
        res = fn()
        tiempos.append(time.perf_counter() - t)
    return res, statistics.median(tiempos[1:]), tiempos[0], tiempos[1:]


def fmt_t(s):
    return f"{s * 1000:,.1f} ms" if s < 1 else f"{s:,.2f} s"


class Bitacora:
    """Imprime y, a la vez, guarda en resultados/<nombre> (lo que se entrega como evidencia)."""

    def __init__(self, nombre):
        self.ruta = RESULTADOS / nombre
        self.f = open(self.ruta, "w", encoding="utf-8")

    def __call__(self, *a):
        texto = " ".join(str(x) for x in a)
        print(texto, flush=True)
        self.f.write(texto + "\n")
        self.f.flush()

    def cerrar(self):
        self.f.close()
