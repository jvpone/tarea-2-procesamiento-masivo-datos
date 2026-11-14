# Parte C — Resultados de las preguntas

Medido el 2026-11-13 23:19 en: AMD Ryzen 7 5700X3D 8-Core Processor · Windows 10 · Python 3.12.10 · 16 CPU · 15.6 GB RAM para Docker · Docker 26.1.1. HBase 2.5.15 standalone (un solo proceso) y Redis 7 en Docker, en la misma máquina.

Tiempo = mediana de 5 corridas, descartando una primera corrida de calentamiento.

## P1 · "Ahora" — Redis contra HBase

| estación | tipo | valor (Redis) | ts medición (Redis) | valor (HBase) | ts medición (HBase) | = |
|---|---|---|---|---|---|---|
| CC-001 | flujo_vehicular | 33.2 | 07-11 00:59:58.000 | 33.2 | 07-11 00:59:58.000 | ✓ |
| CC-002 | flujo_vehicular | 62.7 | 07-11 00:59:54.000 | 62.7 | 07-11 00:59:54.000 | ✓ |
| CC-003 | ocupacion_estacionamiento | 42.6 | 07-11 00:59:57.500 | 42.6 | 07-11 00:59:57.500 | ✓ |
| CC-004 | calidad_aire | 73.4 | 07-11 00:59:53.000 | 73.4 | 07-11 00:59:53.000 | ✓ |
| CC-005 | flujo_vehicular | 19.1 | 07-11 00:59:52.500 | 19.1 | 07-11 00:59:52.500 | ✓ |
| TAL-001 | flujo_vehicular | 41.0 | 07-11 00:59:40.500 | 41.0 | 07-11 00:59:40.500 | ✓ |
| TAL-002 | ocupacion_estacionamiento | 60.0 | 07-11 00:59:50.000 | 60.0 | 07-11 00:59:50.000 | ✓ |
| TAL-003 | calidad_aire | 45.0 | 07-11 00:59:58.500 | 45.0 | 07-11 00:59:58.500 | ✓ |
| HUA-001 | flujo_vehicular | 90.1 | 07-11 00:59:51.000 | 90.1 | 07-11 00:59:51.000 | ✓ |
| HUA-002 | flujo_vehicular | 97.1 | 07-11 00:59:59.000 | 97.1 | 07-11 00:59:59.000 | ✓ |
| SPP-001 | ocupacion_estacionamiento | 66.0 | 07-11 00:59:56.000 | 66.0 | 07-11 00:59:56.000 | ✓ |
| SPP-002 | flujo_vehicular | 98.0 | 07-11 00:59:59.500 | 98.0 | 07-11 00:59:59.500 | ✓ |
| CHI-001 | calidad_aire | 67.8 | 07-11 00:59:56.500 | 67.8 | 07-11 00:59:56.500 | ✓ |
| PEN-001 | flujo_vehicular | 68.6 | 07-11 00:59:47.000 | 68.6 | 07-11 00:59:47.000 | ✓ |
| COR-001 | calidad_aire | 19.4 | 07-11 00:59:43.500 | 19.4 | 07-11 00:59:43.500 | ✓ |
| COR-002 | ocupacion_estacionamiento | 62.8 | 07-11 00:59:55.500 | 62.8 | 07-11 00:59:55.500 | ✓ |

Top 5 de flujo vehicular (último valor):

| # | Redis `ZREVRANGE ranking:flujo 0 4` | HBase (16 scans inversos + sort) |
|---|---|---|
| 1 | SPP-002 = 98 | SPP-002 = 98 |
| 2 | HUA-002 = 97.1 | HUA-002 = 97.1 |
| 3 | HUA-001 = 90.1 | HUA-001 = 90.1 |
| 4 | PEN-001 = 68.6 | PEN-001 = 68.6 |
| 5 | CC-002 = 62.7 | CC-002 = 62.7 |

- **P1 Redis (16 HGETALL + ZREVRANGE, en un pipeline)**: mediana 0.8 ms (5 corridas: 0.9 ms, 0.8 ms, 0.9 ms, 0.8 ms, 0.8 ms; la primera, descartada: 5.8 ms)
- **P1 HBase (16 scans inversos limit=1 + ordenar en Python)**: mediana 670.8 ms (5 corridas: 836.1 ms, 845.0 ms, 670.8 ms, 619.9 ms, 618.6 ms; la primera, descartada: 841.7 ms)
- **P1 HBase variante: scan hacia adelante de la última hora por estación**: mediana 75.5 ms (5 corridas: 81.4 ms, 75.5 ms, 73.4 ms, 73.8 ms, 79.6 ms; la primera, descartada: 80.1 ms)
- **P1 HBase ingenuo (recorrer toda la historia de cada estación)**: mediana 4.25 s (5 corridas: 4.10 s, 4.86 s, 4.25 s, 4.12 s, 4.95 s; la primera, descartada: 4.06 s)

HBase / Redis = **806×** con scans inversos, 91× con la variante hacia adelante y 5,106× recorriendo toda la historia (las cuatro dan el mismo resultado).

Cómo sacar "lo último" de HBase con `estacion#timestamp`: (1) **scan inverso** desde el final del prefijo de la estación con `limit=1` — no necesita saber nada, pero el scan inverso es caro cuando los datos ya están en HFiles (HBase tiene que retroceder bloque a bloque: medido ~40 ms por estación después del `flush`, ~6 ms cuando la hora en vivo aún estaba en la MemStore); (2) si se conoce una cota de "ahora", un scan **hacia adelante** de esa ventana; (3) en diseño, una tabla con clave `estacion#(10^13 − ms)` (Lab 07) deja la última lectura primera y la consulta es un scan hacia adelante con `limit=1`, al precio de otra escritura por evento. Ninguna se acerca a Redis.

**Último por medición vs. último por llegada.** Por llegada = la última entrada del stream con esa estación (el ID de Redis es la hora de llegada); por medición = `ultimo:<estacion>`.

| estación | criterio | valor | timestamp de medición | llegó al stream |
|---|---|---|---|---|
| TAL-001 | por llegada (última entrada del stream) | 48.9 | 2026-11-07T00:58:48.335-03:00 | ID 1794621984740-0 |
| TAL-001 | por medición (`ultimo:TAL-001`) | 41.0 | 2026-11-07T00:59:40.500-03:00 | ID 1794621984606-0 |

Estaciones donde los dos criterios difieren: **TAL-001**. La última entrada que llegó de esa estación es una lectura **tardía**: se midió antes que otra que ya había llegado. Un tablero 'por llegada' retrocedería; el script Lua de `tablero.py` la ignora porque su ts es menor.

**Interpretación.** Las dos respuestas coinciden fila a fila, pero Redis responde en 0.8 ms con un solo viaje (pipeline) porque el estado 'de ahora' ya está precalculado, mientras que HBase necesita 16 scans (uno por estación) y ordenar en el cliente. Un dashboard que refresca cada segundo para muchos usuarios lee de Redis; HBase se reserva para la historia.

## P2 · Ventana de 10 minutos — Redis

Ancla = `tablero:max_ts` = 2026-11-07T00:59:59.500-03:00 → ventana (2026-11-07T00:49:59.500-03:00, 2026-11-07T00:59:59.500-03:00].

| estación | lecturas en la ventana | promedio de flujo (veh/min) |
|---|---|---|
| SPP-002 | 75 | 93.13 |
| HUA-001 | 62 | 92.08 |
| PEN-001 | 86 | 87.34 |
| HUA-002 | 67 | 76.50 |
| CC-002 | 67 | 58.80 |
| TAL-001 | 74 | 34.45 |
| CC-001 | 75 | 20.89 |
| CC-005 | 70 | 18.67 |

- **P2 Redis (8 ZRANGEBYSCORE en un pipeline + promedio)**: mediana 2.0 ms (5 corridas: 2.1 ms, 2.0 ms, 2.0 ms, 2.0 ms, 2.0 ms; la primera, descartada: 2.1 ms)
Control contra el generador (dedup por timestamp): calza.

| zset | elementos | MEMORY USAGE | bytes/elemento | codificación |
|---|---|---|---|---|
| 8 × `ventana:*` (10 min, recortadas) | 576 | 19,944 B | 35 | listpack |
| 8 × sin recortar (la hora entera) | 3503 | 379,922 B | 108 | skiplist |
| CC-001 sin recortar, 5 días + 1 h (desde HBase) | 53656 | 5,761,929 B | 107 | skiplist |

Sin recorte, la memoria crece lineal con el tiempo: ~8.8 MB por día para las 8 estaciones de flujo, ~3.15 GB al año, sin límite. Con el recorte queda acotada (~19 KB) para siempre. Ojo con el salto de codificación: hasta 128 elementos Redis guarda el zset como *listpack* (compacto); la hora entera ya pasa a *skiplist* (índice + hash), varias veces más bytes por elemento.

**¿Por qué un sorted set con el ts como score?** Porque Redis lo mantiene ordenado por tiempo de medición: recortar es `ZREMRANGEBYSCORE -inf <ancla-10min>` y leer la ventana es `ZRANGEBYSCORE`, ambos O(log n + m), y una lectura tardía se inserta en su lugar (por medición), no al final como en una lista.

**Interpretación.** El promedio móvil sale en 2.0 ms leyendo solo lo que está en la ventana; el recorte es lo que permite que esta estructura viva en RAM indefinidamente.

## P3 · Un rango — HBase

Scan por rango `[CC-001#2026-11-04T08:00:00.000-03:00, CC-001#2026-11-04T10:00:00.000-03:00)` → **891 lecturas**. Recorrido entero + `RowFilter` → 891 lecturas (mismo resultado).

| cuarto de hora | lecturas | promedio (veh/min) |
|---|---|---|
| 08:00–08:15 | 112 | 15.61 |
| 08:15–08:30 | 125 | 25.67 |
| 08:30–08:45 | 97 | 17.13 |
| 08:45–09:00 | 104 | 25.87 |
| 09:00–09:15 | 116 | 24.40 |
| 09:15–09:30 | 91 | 18.10 |
| 09:30–09:45 | 128 | 19.24 |
| 09:45–10:00 | 118 | 17.49 |

- **P3 HBase scan por rango**: mediana 8.7 ms (5 corridas: 9.0 ms, 8.8 ms, 8.6 ms, 8.7 ms, 8.7 ms; la primera, descartada: 9.0 ms)
- **P3 HBase tabla entera + RowFilter**: mediana 553.2 ms (5 corridas: 542.3 ms, 559.8 ms, 540.2 ms, 555.5 ms, 553.2 ms; la primera, descartada: 557.5 ms)

El recorrido completo es **63× más lento**: lee 853,953 filas para devolver 891 (0.1043%). El scan por rango salta directo a la región y la posición de `CC-001#2026-11-04T08` (la row key es el índice) y lee solo esas 891 filas.

**Con cinco años** (365× más filas): el scan por rango tarda lo mismo (depende de las filas del rango, no del tamaño de la tabla), y el recorrido crece lineal: del orden de 3 min en esta máquina — en un cluster se paraleliza entre RegionServers, pero sigue leyendo todo.

## P4 · Dos patrones de acceso — catálogo en Redis + HBase

Rango `[05-nov 07:00, 09:00)`. Estaciones de calidad del aire según `SMEMBERS tipo:calidad_aire`: CC-004, CHI-001, COR-001, TAL-003.

| estación | lecturas | PM2.5 promedio | PM2.5 máximo | hora del máximo (1ª vez) |
|---|---|---|---|---|
| CC-004 | 877 | 68.09 | 80 | 07:22:15.000 |
| CHI-001 | 904 | 62.18 | 80 | 07:07:14.500 |
| COR-001 | 890 | 22.78 | 52.3 | 08:18:32.500 |
| TAL-003 | 895 | 48.14 | 76.5 | 07:27:43.500 |

(a) y (b) dan exactamente lo mismo: True.

- **P4 (a) Redis SMEMBERS + 4 scans en `lecturas`**: mediana 31.7 ms (5 corridas: 36.1 ms, 31.0 ms, 31.7 ms, 31.1 ms, 44.7 ms; la primera, descartada: 36.8 ms)
- **P4 (b) 1 scan en `lecturas_tipo`**: mediana 26.3 ms (5 corridas: 26.2 ms, 24.8 ms, 30.6 ms, 26.3 ms, 32.6 ms; la primera, descartada: 26.5 ms)

**Lo que cuesta `lecturas_tipo`:**

|  | lecturas | lecturas_tipo |
|---|---|---|
| put por evento | 1 | 1 más (el doble de escrituras: 871.200 put extra) |
| disco (HFiles tras flush, `du`) | 122.6 MB | 146.2 MB |
| carga histórica | 11.2 s | 10.3 s |

La segunda tabla ocupa **1.19× lo que la primera** (su clave es más larga y HBase la repite en cada celda): el disco total pasa de 123 MB a 269 MB (2.2×), y en un cluster con HDFS (réplica 3) eso se triplica. Las escrituras se duplican exactas. En la ingesta en vivo cada lote hace dos `batch.send()`; si el segundo falla, el proceso no llega al `XACK`: el lote queda pendiente y se reintenta entero, y como ambas claves son idempotentes reescribir la primera tabla no duplica nada. Lo que sí existe es una ventana en que `lecturas` tiene el evento y `lecturas_tipo` todavía no (HBase solo es atómico por fila, no entre tablas).

**Interpretación.** Con 4 estaciones por tipo, (a) son 4 scans cortos y (b) 1 scan: 31.7 ms vs 26.3 ms (1.2×). Acá la segunda tabla **no vale la pena**: duplica escrituras y disco para ahorrar milisegundos. Valdría con cientos/miles de estaciones por tipo (un scan en vez de miles) o si la consulta por tipo fuera la más frecuente del sistema.

## P5 · Hotspotting y salting — HBase

La hora en vivo (7,200 put) escrita en tres tablas. Salt de cada estación (crc32 mod 4): CC-001→2, CC-002→0, CC-003→2, CC-004→1, CC-005→3, TAL-001→1, TAL-002→3, TAL-003→1, HUA-001→3, HUA-002→1, SPP-001→2, SPP-002→0, CHI-001→1, PEN-001→0, COR-001→2, COR-002→0.

Escrituras recibidas por región (`writeRequestCount` de JMX, después − antes):

| diseño | escrituras por región (clave de inicio de la región) |
|---|---|
| `estacion#ts` | [(inicio)] 1,761 (25%) · [CC-005] 1,708 (24%) · [HUA] 1,732 (25%) · [SPP-002] 1,847 (26%) |
| `ts#estacion (por día)` | [(inicio)] 0 (0%) · [2026-11-03] 0 (0%) · [2026-11-04] 0 (0%) · [2026-11-05] 0 (0%) · [2026-11-06] 5 (0%) · [2026-11-07] 7,043 (100%) |
| `salt#ts#estacion (por salt)` | [(inicio)] 1,818 (26%) · [1] 2,219 (31%) · [2] 1,731 (25%) · [3] 1,280 (18%) |

"Todas las lecturas de todas las estaciones entre 00:10 y 00:20 del 07-nov": 1180, 1180, 1180 filas (deben ser iguales).

- **P5 estacion#ts (16 rangos)**: mediana 48.2 ms (5 corridas: 47.4 ms, 48.2 ms, 48.3 ms, 48.3 ms, 47.6 ms; la primera, descartada: 47.2 ms)
- **P5 ts#estacion (1 rango)**: mediana 7.6 ms (5 corridas: 7.5 ms, 8.1 ms, 14.2 ms, 7.6 ms, 7.4 ms; la primera, descartada: 8.1 ms)
- **P5 salt#ts#estacion (4 rangos)**: mediana 15.4 ms (5 corridas: 15.5 ms, 14.8 ms, 15.7 ms, 15.1 ms, 15.4 ms; la primera, descartada: 15.1 ms)

**Interpretación.** `ts#estacion` concentra el 100 % de las escrituras de la hora en vivo en la última región (la de "hoy"; las tardías de antes de medianoche caen en la anterior): con N servidores trabaja uno. `estacion#ts` y el salt reparten (el salt, tan parejo como reparta crc32 a 16 estaciones). Para leer un intervalo de todas las estaciones, `ts#estacion` es un rango, el salt 4 y `estacion#ts` 16, pero en milisegundos las tres son comparables con este volumen. **Elegimos `estacion#ts`** (la de `lecturas`): reparte la escritura y su consulta típica (una estación en un rango) es un solo scan. Cambiaríamos a salt si la consulta dominante fuera "todo el sistema en un intervalo" con muchísimas estaciones (16 rangos → miles), y a `ts#estacion` solo con escritura por lotes históricos (sin flujo continuo que genere hotspot) o en una sola máquina.

## P6 · La propia — "¿Desde cuándo está sobre la norma?"

Pregunta: *¿qué estaciones de calidad del aire están **ahora** sobre 50 µg/m³ de PM2.5 (referencia: la norma diaria chilena de PM2.5) y **desde cuándo** llevan el episodio sin interrupción?* Un episodio sigue mientras el promedio móvil de 10 minutos de medición se mantiene ≥ 50 (con un umbral sobre lecturas sueltas, el ruido cortaría el episodio a cada rato).

**Reparto.** Redis decide el **qué**: el catálogo `tipo:calidad_aire` y el `ultimo:<est>` dicen qué estaciones están en episodio ahora y desde qué medición mirar. HBase decide el **cuándo**: un scan **inverso** sobre `lecturas` (`estacion#timestamp`) que parte en la medición actual y retrocede, con un promedio móvil de 10 min, hasta el primer tramo bajo el umbral. El stream solo tiene la última hora: si el episodio empezó antes, Redis no puede saberlo.

| estación | PM2.5 ahora (Redis) | estado | episodio desde (HBase) | duración | filas leídas en HBase |
|---|---|---|---|---|---|
| CC-004 | 73.4 | en episodio | desde antes del inicio de la historia (2026-11-01T23:58) | > 121 h | 53,415 (toda su historia) |
| CHI-001 | 67.8 | en episodio | 2026-11-06 23:09:49.687 | 110 min (empezó antes del stream) | 827 |
| COR-001 | 19.4 | bajo la norma ahora | — | — | — |
| TAL-003 | 45 | bajo la norma ahora | — | — | — |

- **P6 Redis (catálogo + últimos) + HBase (scans inversos con promedio móvil)**: mediana 2.55 s (5 corridas: 2.65 s, 2.42 s, 2.40 s, 2.55 s, 2.55 s; la primera, descartada: 2.90 s)

**Interpretación.** El costo en HBase lo decide la respuesta, no la tabla: un episodio corto lee unos cientos de filas, uno que dura toda la historia recorre la estación completa (~54 mil filas), y aun así es un scan contiguo gracias a la row key. Hacerlo solo con Redis exigiría guardar días de lecturas en RAM (lo que P2 mostró que no escala); hacerlo solo con HBase obligaría a reconstruir "el ahora" con scans inversos en cada consulta en vez de leer cuatro hashes.

