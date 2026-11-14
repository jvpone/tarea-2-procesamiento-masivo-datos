# Tarea 2 — Redis + HBase: del stream a la historia

**Procesamiento Masivo de Datos · ELE051-B / EIN102B · USM 2026-2 · Paralelo 701**

Pipeline completo de sensores del Gran Concepción: la **historia** (5 días, 864.000 eventos) va directo a
**HBase**; la **hora en vivo** (7.200 eventos) entra a un **stream de Redis** (`sensores:t2`) y dos consumer
groups independientes la bajan a HBase (`ingesta`) y mantienen el tablero "de ahora" en Redis (`tablero`).
Encima: verificación contra los números de control, las seis preguntas con tiempos medidos y los dos
experimentos de fallas.

El informe (PDF, ≤ 5 páginas) está en [`informe/informe.pdf`](informe/informe.pdf).

---

## 1. Cómo reproducir desde cero

**Requisitos:** Docker con `docker compose` (≥ 4 GB de RAM asignados a Docker, ~3 GB de disco),
Python ≥ 3.10 y `bash` (en Windows: Git Bash o WSL). Nada más: el script crea el ambiente virtual.

```bash
git clone <este repositorio> && cd <carpeta>
docker compose build          # una vez por máquina: descarga HBase (~340 MB). Es lo único que usa la red
docker compose up -d --wait   # levanta Redis y HBase y espera sus healthchecks
./run.sh                      # todo, de punta a punta
```

`./run.sh` hace, en orden y sin pasos a mano:

| paso | script | qué hace |
|---|---|---|
| entorno | `run.sh` | crea `.venv`, instala `requirements.txt`, `docker compose up -d --build --wait` (no hace falta haberlo levantado antes) |
| B.1 | `carga_historica.py` | crea `lecturas` y `lecturas_tipo` **desde cero**, pre-divididas, y carga los 864.000 eventos con `batch` |
| B.2 + D.1 | `vivo.py` | limpia Redis, crea los grupos, lanza `tablero-1`, `ingesta-1` y el `productor` (20×). `ingesta-1` **muere a propósito** entre el put y el XACK de un lote; se muestra XPENDING y `ingesta-2` lo recupera con XAUTOCLAIM |
| B.3 | `verificar.py` | producidos · procesados y XPENDING por grupo · filas de cada tabla · tablero, contra los números de control |
| C | `preguntas.py` | las 6 preguntas, mediana de 5 corridas (+1 de calentamiento descartada) |
| D.2 | `fallas_redis.py` | `docker compose restart redis` a mitad de camino, con AOF (elegida) y sin persistencia |
| B.3 | `verificar.py` | de nuevo, para mostrar que D.2 no tocó el estado de la Parte B |

Cada paso se puede correr solo: `./run.sh carga | vivo | verificar | preguntas | fallas`.
Todo lo que imprime queda también en [`resultados/`](resultados/) (es la evidencia que cita el informe).

**Cuánto tarda** (medido con `docker compose down -v` + sin `.venv`, imagen ya construida; ver
[`resultados/duracion_run.txt`](resultados/duracion_run.txt)):

| paso | tiempo |
|---|---|
| entorno: `.venv` + `pip install` + `docker compose up --wait` (HBase tarda ~40 s en estar *healthy*) | ~70 s |
| B.1 carga histórica (generar 864.000 eventos + 2 tablas × 864.000 put + contar) | 47 s |
| B.2 + D.1 hora en vivo (180 s de productor a 20× + recuperación) | 185 s |
| B.3 verificación | 21 s |
| C preguntas (6 × (1+5) corridas, incluye ~40 s esperando las métricas JMX en P5) | 131 s |
| D.2 reinicio de Redis (dos configuraciones × hora en vivo) | 407 s |
| B.3 verificación final | 20 s |
| **total `./run.sh`** | **14 min 20 s** |

La primera vez hay que sumar `docker compose build` (descarga ~340 MB de HBase; 2-5 min según la red).
Medido en un AMD Ryzen 7 5700X3D (8 núcleos/16 hilos), 32 GB RAM (15,6 GB para Docker), Windows 10 +
Docker Desktop 26 (WSL2), disco SSD.

Apagar: `docker compose down` (los datos quedan en los volúmenes `t2-redis` y `t2-hbase`);
`docker compose down -v` borra también los volúmenes.

### Estructura

```
docker-compose.yml               Redis 7 (AOF everysec, volumen t2-redis) + HBase 2.5.15 (volumen t2-hbase), con healthchecks
docker-compose.sin-persistencia.yml  la configuración contraria de Redis, solo para D.2
Dockerfile                       HBase standalone + Thrift (el del Lab 07, sin cambios)
generador.py                     el generador del Lab 06, sin cambios (se llama con semilla=2026)
comun.py                         parámetros de la tarea, row keys, conexión, medición
carga_historica.py               B.1
productor.py  ingesta.py  tablero.py  consumo.py   B.2 (consumo.py = el bucle XREADGROUP → procesar → XACK)
vivo.py                          orquesta la hora en vivo + el experimento D.1
verificar.py                     B.3
preguntas.py                     Parte C
fallas_redis.py                  D.2
run.sh                           todo lo anterior, en orden
resultados/                      salidas de la última corrida (texto/markdown; ningún dato crudo)
informe/                         informe.html (fuente) → informe.pdf
```

**Sin datos en el repositorio:** los 871.200 eventos se generan en cada corrida con
`eventos(t0=2026-11-02T00:00-03:00, semilla=2026)`. El `.gitignore` excluye `.venv`, parquets, CSV,
dumps de Redis y cualquier `.env`. No hay claves ni secretos: Redis y HBase corren locales y sin
autenticación (el VPS del curso no se usa).

---

## 2. Decisiones

### 2.1 Row keys

Todas las claves salen de **la estación y el timestamp de medición** del evento — nunca del ID del stream
ni de la hora de escritura. Por eso son **idempotentes**: un evento repetido, una lectura tardía o un lote
reprocesado después de una caída escriben exactamente la misma fila (el `put` agrega una versión a la misma
celda en vez de crear una fila nueva). Los timestamps son ISO 8601 con milisegundos y el mismo offset
(`-03:00`, noviembre en Chile) en todas partes: así el orden de bytes de la clave es el orden temporal.

| tabla | row key | ejemplo | rápida | lenta |
|---|---|---|---|---|
| `lecturas` | `estacion#timestamp` | `CC-001#2026-11-04T08:00:00.000-03:00` | una estación en un rango de tiempo (1 scan contiguo); lo último de una estación (scan inverso, `limit=1`) | todo el sistema en un instante (16 rangos); cualquier filtro por valor o por tipo (recorre la tabla) |
| `lecturas_tipo` | `tipo#timestamp#estacion` | `calidad_aire#2026-11-05T07:00:00.000-03:00#CC-004` | un tipo de sensor en un rango de tiempo (P4: 1 scan) | una estación sola (hay que leer su tipo entero y filtrar) |

La estación va **al final** en `lecturas_tipo` solo para desempatar dos estaciones del mismo tipo que midan
en el mismo milisegundo. Columnas: familia `m`, `m:valor` y `m:tipo` (en `lecturas`) / `m:estacion`
(en `lecturas_tipo`). `VERSIONS => 3` para poder **mirar** en D.1 qué quedó en la celda reprocesada.

Con esta clave, los 864.000 eventos dan **846.906 filas**: 17.090 repetidos tal cual + 4 pares que chocan
en (estación, ms) con distinto valor caen sobre una fila existente (HBase se queda con el último escrito).

### 2.2 Regiones (SPLITS)

`happybase` no sabe pre-dividir, así que las tablas se crean con `hbase shell` (`comun.crear_tabla`).

- **`lecturas`: `['CC-005', 'HUA', 'SPP-002']` → 4 regiones de 4 estaciones cada una.** El generador elige
  la estación de cada evento uniformemente, así que cada estación aporta ~1/16 del volumen y 4 estaciones
  por región reparten parejo (los cortes del Lab 07, `COR/PEN/TAL`, dejan 6 estaciones en la primera
  región y 3 en cada una de las dos últimas). Medido en P5: las escrituras de la hora en vivo se reparten 25/24/25/26 %.
- **`lecturas_tipo`: `['flujo_vehicular', 'flujo_vehicular#2026-11-04T12', 'ocupacion_estacionamiento']`.**
  Por tipo hay 8 estaciones de flujo y 4 de cada otro tipo (50/25/25 % del volumen). Un corte por tipo
  dejaría la región de flujo con el doble que las demás, así que flujo se corta además a la mitad de la
  historia (mediodía del 04-nov). Limitación conocida (y discutida en P4/P5): dentro de un tipo la clave es
  monótona en el tiempo, así que la escritura en vivo cae en la última región de cada tipo.
- P5: `ts#estacion` pre-dividida por día (`2026-11-03 … 2026-11-07`) y `salt#ts#estacion` por salt (`1,2,3`).

### 2.3 MAXLEN del stream: `MAXLEN ~ 20000`

- La hora entera son 7.200 entradas: con 20.000 el stream **nunca recorta nada en una corrida** (la
  verificación lo comprueba: `XLEN = entries-added`), y la Parte D puede releer todo.
- **Lag tolerado:** un consumidor puede atrasarse hasta ~20.000 entradas antes de que se recorte algo que no
  leyó: **~8 minutos a 20×** (40 ev/s) o **~2,8 horas al ritmo real** (2 ev/s). Es mucho más que lo que tarda
  en volver a levantarse un consumidor caído.
- **Memoria:** ~20.000 × ~100 B ≈ 2 MB. Barato.
- `~` (aproximado) deja que Redis recorte por nodos enteros del radix tree (más barato que exacto); el
  stream puede quedar unas decenas de entradas por sobre 20.000.
- **¿Qué pasa con una entrada recortada que nadie leyó?** Se pierde para ese grupo **sin aviso**: `XREADGROUP >`
  simplemente sigue desde la siguiente que exista y el grupo nunca se entera. Si estaba **pendiente** (entregada
  y sin XACK), su ID sigue en el PEL pero el contenido ya no: `XAUTOCLAIM` la devuelve en la lista de IDs
  borrados (nuestro `consumo.py` descarta las entradas sin campos). Para la ingesta eso es un hueco en HBase,
  por eso el MAXLEN tiene que ser mayor que el peor atraso esperable de la ingesta.

### 2.4 Persistencia de Redis: AOF con `appendfsync everysec`, sin RDB

En `docker-compose.yml`: `redis-server --appendonly yes --appendfsync everysec --save ""`, con volumen con
nombre `t2-redis:/data`.

- **Por qué persistir:** en este pipeline Redis no es solo un caché. El stream es el **buffer** entre el
  productor y HBase: lo que está en el stream y todavía no en HBase (la *ventana de pérdida*) solo existe en
  Redis. Y los consumer groups (posición de cada grupo + pendientes) son la memoria de "qué falta procesar".
  Sin persistencia, un reinicio borra todo eso: D.2 lo mide (976 lecturas perdidas).
- **Por qué AOF y no RDB:** RDB guarda fotos cada N segundos/cambios; entre una foto y la siguiente se
  pierde todo lo escrito (minutos). AOF registra cada escritura.
- **Por qué `everysec`:** pérdida máxima ≈ 1 segundo de escrituras ante una caída de la **máquina**
  (40 eventos a 20×, 2 al ritmo real), con un fsync por segundo en un hilo aparte (sin costo visible a
  40 ev/s). `always` (fsync por comando) daría pérdida 0 pero limita el throughput al del disco; aquí
  sería viable, pero 1 s de eventos de sensores es una pérdida aceptable y el estándar de la industria.
  Un reinicio ordenado (`docker compose restart`, SIGTERM) no pierde nada: Redis hace fsync al apagarse.
- **Por qué `--save ""`:** un solo mecanismo. Con RDB y AOF juntos, al arrancar manda el AOF igual, y las
  fotos RDB solo agregarían I/O.

### 2.5 "Último" = último por medición

`tablero.py` aplica cada evento con un **script Lua** (atómico dentro de Redis): lee `ultimo:<est>.ts` y
solo actualiza el hash (y `ranking:flujo`) si el ts del evento es **≥** el guardado. Una lectura tardía
(ts menor) no hace retroceder el tablero; un repetido escribe lo mismo; ante dos lecturas distintas del
mismo milisegundo gana la última escrita (igual que HBase). Por ser atómico, ni dos consumidores del grupo
`tablero` en paralelo podrían intercalar una lectura y una escritura. En la hora en vivo, 242 eventos no
movieron `ultimo:*` por ser más viejos que lo ya guardado.

`ventana:<est>`: score = ts de medición (ms), miembro `"<ts>:<valor>"`. Antes del `ZADD` se borra lo que
hubiera en ese mismo ms (una lectura por ms, como en HBase), y después se recorta todo lo que tenga
score ≤ `tablero:max_ts − 600.000` (la medición más reciente del stream menos 10 min). Al cerrar la hora se
recortan todas las ventanas contra la última medición.

### 2.6 XACK después del put, y recuperación

`consumo.py` implementa el ciclo para ambos consumidores: `XREADGROUP` (lote) → escribir → `XACK`. La
ingesta hace dos `batch.send()` (una por tabla) y recién después confirma: si se cae antes, el lote queda
pendiente y se reprocesa entero (idempotente). Al arrancar, cada consumidor relee lo suyo pendiente (id `0`)
y hace `XAUTOCLAIM` de lo que otros dejaron más de 2 s sin confirmar (y lo repite cada 10 s). Si Redis
se reinicia, reintenta la conexión; si el grupo desapareció (Redis sin persistencia), lo recrea.

### 2.7 Otras

- **20×:** el productor duerme hasta `inicio + (k+1)·25 ms` (sin deriva acumulada): la hora tarda 180,0 s.
- **Lotes de la ingesta:** `pausa` de 0,5 s entre lecturas para juntar ~20 eventos por lote (un viaje a
  HBase por lote, no por evento).
- **Salt:** `crc32(estacion) mod 4` (el `hash()` de Python cambia entre procesos).

---

## 3. Resultados (resumen)

Todos los números de control calzan (`resultados/b3_verificacion.txt`): 7.200 producidos, los dos grupos con
`entries-read = 7.200`, lag 0 y XPENDING 0, y **853.953 filas** en `lecturas` y en `lecturas_tipo`
(846.906 del histórico + 7.047 en vivo; 871.200 − 853.953 = 17.243 repetidos + 4 choques de clave).

| | resultado | tiempo (mediana de 5) |
|---|---|---|
| B.1 | 864.000 put por tabla en lotes de 5.000 | 11,2 s y 10,3 s (≈ 80.000 put/s) |
| P1 | "ahora": Redis vs HBase — mismo resultado; difiere por reloj: **TAL-001** | Redis 0,8 ms · HBase 76–671 ms |
| P2 | promedio 10 min por estación de flujo desde `ventana:*`; 20 KB recortado vs ~9 MB/día sin recortar | 2,0 ms |
| P3 | CC-001 04-nov 08–10 h: 891 lecturas; scan por rango vs tabla entera | 8,7 ms vs 553 ms (64×) |
| P4 | PM2.5 05-nov 07–09 h, 4 estaciones: 4 rangos vs 1 scan | 31,7 ms vs 26,3 ms |
| P5 | `ts#estacion`: 100 % de las escrituras en una región; lectura 16/1/4 rangos | 48 / 7,6 / 15 ms |
| P6 | episodios sobre 50 µg/m³: CC-004 toda la historia (> 121 h), CHI-001 desde 23:09 del día anterior | 2,55 s |
| D.1 | `ingesta-1` muere con 20 entradas pendientes; `ingesta-2` las reclama; HBase exacto | — |
| D.2 | AOF: sobrevive todo, 0 perdidas · sin persistencia: se pierde todo, **976 lecturas** perdidas | — |

Detalle completo: [`resultados/preguntas.md`](resultados/preguntas.md) y el informe.

---

## 4. Qué no funcionó (y cómo se resolvió)

- **El XAUTOCLAIM llegaba tarde.** La primera versión de `consumo.py` solo reclamaba pendientes ajenos
  cuando no llegaba nada nuevo; con el productor escribiendo cada 25 ms, `ingesta-2` recuperó el lote de
  `ingesta-1` recién al terminar la hora (~3 min de ventana). Ahora reclama al arrancar y cada 10 s.
- **`kill` no mataba al consumidor en Windows.** El `python.exe` de un `.venv` en Windows es un lanzador que
  crea un proceso hijo: `Popen.kill()` mataba al lanzador y el consumidor real seguía leyendo. D.2 mata el
  árbol (`taskkill /F /T`); en Linux/macOS es un `kill -9` normal. (D.1 no tiene el problema: el consumidor
  se mata a sí mismo con `os._exit(1)`.)
- **La conexión Thrift se cortaba.** El servidor Thrift de HBase cierra las conexiones ociosas (~60 s):
  después de esperar al productor, el conteo fallaba con `TSocket read 0 bytes`. Se abre una conexión nueva
  después de cada espera larga.
- **`entries-read` es `nil`** en un grupo recién creado: comparar con un entero fallaba.
- **La primera "pregunta propia" no servía.** "¿Hace cuánto no se veía un flujo tan alto?" respondía
  siempre "hace segundos": el generador es un proceso estacionario (vuelve a su nivel base, sin ciclo diario),
  y eso ya lo sabe el stream, sin HBase. Se cambió a "¿desde cuándo está sobre la norma de PM2.5?", donde la
  respuesta sí puede estar antes de la hora en vivo (y lo está: CHI-001 empezó 50 min antes del stream).
- **El filtro por valor de HBase no se usó:** los valores se guardan como texto y `SingleColumnValueFilter`
  compara bytes (`"9.5" > "100"`), como mostró el Lab 07. Las consultas por valor se resuelven en el cliente
  después de un scan por rango.
- **Lo que no está resuelto:** la escritura doble de la ingesta no es atómica entre tablas (HBase solo
  garantiza atomicidad por fila); hay una ventana de milisegundos en que `lecturas` tiene un evento y
  `lecturas_tipo` no. Se acota con el reintento del lote, no se elimina.
- **La escala:** todo corre en un HBase *standalone* (HMaster, RegionServer y ZooKeeper en un proceso) en un
  solo PC: las regiones "se reparten" pero las atiende el mismo servidor. Los tiempos sirven para comparar
  órdenes de magnitud entre diseños, no como throughput de un cluster (ver informe §5).

---

## Créditos

`generador.py` (Lab 06) y `Dockerfile` (Lab 07) son del repositorio de laboratorios del curso
([diegoalrv/ein102b](https://github.com/diegoalrv/ein102b)), reutilizados sin cambios como permite el
enunciado. El resto del código es propio de esta tarea.
