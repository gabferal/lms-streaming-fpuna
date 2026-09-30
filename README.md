# LMS Streaming Analytics — Proyecto Integrador

Pipeline de análisis en tiempo real para un **Learning Management System (LMS)**.  
Ingesta eventos de actividad de estudiantes desde Apache Kafka, los procesa con Apache Beam
y produce métricas de engagement por ventana de 5 minutos.

Desarrollado como Proyecto Integrador del curso de **Streaming de Datos** — FPUNA Maestría en IA.

---

## Arquitectura

```
Fuente de eventos
  (src/producer.py — confluent_kafka)
        │
        ▼  lms-events (Kafka, 1 partición)
┌────────────────────────────────────────┐
│  src/pipeline.py                       │
│                                        │
│  read_kafka_batch()  ←─ confluent_kafka│
│        │                               │
│        ▼  beam.Create(messages)        │
│  Apache Beam (DirectRunner, batch)     │
│                                        │
│  ValidateAndParse ─────────────────────┼──► lms-dlq  (confluent_kafka)
│  AssignEventTime  (event_time → ts)    │
│  FixedWindows(5 min)                   │
│  KeyByStudent                          │
│  Dedup (stateful, SetStateSpec)        │
│  FilterEngagement                      │
│  CombinePerKey (count + score)         │
│  FormatMetric  (window-aware)          │
│  IdempotentFileSink ───────────────────┼──► output/metrics.jsonl
│        │                               │
│        ▼  KafkaSinkFn (confluent_kafka)│
└────────────────────────────────────────┘
        │
        ▼  lms-metrics (Kafka, 4 particiones)
Consumidor de métricas
  (src/consumer.py)
```

### Componentes

| Módulo | Descripción |
|--------|-------------|
| `src/events.py` | Schema de evento, parsing y validación |
| `src/transforms.py` | Todos los `DoFn` y la PTransform compuesta (sin Kafka) |
| `src/pipeline.py` | Lector Python + pipeline Beam batch + escritores Python |
| `src/producer.py` | Replay de eventos con escenarios inyectados |
| `src/consumer.py` | Display de métricas en terminal |
| `src/config.py` | Constantes de configuración (env vars) |

### Escenarios demostrativos

El productor inyecta automáticamente tres escenarios al reproducir `data/events.jsonl`:

1. **Duplicado** — `evt-a001` enviado dos veces (misma `event_id`). El pipeline lo cuenta una sola vez gracias a `DeduplicateFn` con estado Beam.
2. **Fuera de orden** — `evt-c004` tiene `event_time` en la ventana [13:00–13:05] pero llega durante la ventana [13:05–13:10]. El pipeline lo asigna a la ventana correcta por `event_time`.
3. **Tardío (late)** — un evento con `event_time` muy antiguo enviado al final. Si supera `allowed_lateness`, se descarta silenciosamente.

---

## Inicio rápido

### Requisitos

- Docker ≥ 24 y Docker Compose v2

### Con Docker Compose

```bash
# 1. Levantar Kafka + pipeline
docker compose up -d

# 2. Esperar que el pipeline arranque (~15 s)
docker compose logs pipeline --tail=5

# 3. Enviar eventos (escenarios inyectados automáticamente)
docker compose run --rm producer

# 4. Esperar ~30 s; el pipeline detecta los mensajes y procesa
sleep 30

# 5. Ver métricas en archivo
cat output/metrics.jsonl | python -m json.tool

# 6. Ver métricas en tiempo real desde Kafka
docker compose run --rm consumer

# 7. UI de Kafka (opcional)
open http://localhost:8080

# 8. Detener todo
docker compose down -v
```

### Desarrollo local (sin Docker)

```bash
# Instalar dependencias
uv sync --extra dev

# Correr tests (no requiere Kafka)
uv run pytest tests/ -v

# Variables de entorno (con Kafka local)
export KAFKA_BOOTSTRAP_SERVERS=localhost:9092
uv run python -m src.pipeline    # terminal 1
uv run python -m src.producer    # terminal 2
uv run python -m src.consumer    # terminal 3
```

---

## Tests

```bash
uv run pytest tests/ -v
```

### Cobertura

| Archivo | Clases de test | Tests |
|---------|---------------|-------|
| `tests/test_events.py` | 3 | 26 |
| `tests/test_pipeline.py` | 9 | 20 |
| **Total** | **12** | **46** |

Los tests de pipeline usan `TestPipeline` / `TestStream` — **no requieren Kafka**.

---

## Variables de entorno

| Variable | Por defecto | Descripción |
|----------|-------------|-------------|
| `KAFKA_BOOTSTRAP_SERVERS` | `localhost:9092` | Brokers Kafka |
| `INPUT_TOPIC` | `lms-events` | Topic de entrada |
| `OUTPUT_TOPIC` | `lms-metrics` | Topic de salida de métricas |
| `DLQ_TOPIC` | `lms-dlq` | Dead Letter Queue |
| `WINDOW_SECONDS` | `300` | Tamaño de ventana (segundos) |
| `ALLOWED_LATENESS_SECONDS` | `120` | Tolerancia de retraso (segundos) |
| `EARLY_TRIGGER_SECONDS` | `30` | Trigger especulativo (0 = desactivado) |
| `OUTPUT_DIR` | `output` | Directorio para `metrics.jsonl` |
| `DATA_FILE` | `data/events.jsonl` | Archivo de eventos para el productor |
| `REPLAY_SPEED` | `1.0` | Factor de velocidad del replay (100 = demo rápido) |

---

## Estructura del proyecto

```
lms-streaming-fpuna/
├── data/
│   └── events.jsonl          # 15 eventos de muestra con escenarios
├── output/                   # Métricas escritas por el pipeline (git-ignored)
├── src/
│   ├── config.py
│   ├── consumer.py
│   ├── events.py
│   ├── pipeline.py
│   ├── producer.py
│   └── transforms.py
├── tests/
│   ├── test_events.py
│   └── test_pipeline.py
├── docker-compose.yml
├── Dockerfile
├── pyproject.toml
├── DOCUMENTO_TECNICO.md
└── README.md
```

---

## Formato de métricas

Cada registro en `output/metrics.jsonl` y en el topic `lms-metrics`:

```json
{
  "student_id":   "s-alice",
  "window_start": "2026-09-30T13:00:00+00:00",
  "window_end":   "2026-09-30T13:05:00+00:00",
  "event_count":  4,
  "total_score":  170,
  "avg_score":    42.5
}
```

---

## Criterios de evaluación cubiertos

| Criterio | Implementación |
|----------|---------------|
| Ingesta Kafka | `read_kafka_batch()` (confluent_kafka Python) + `KafkaSinkFn` |
| Procesamiento Beam | `DirectRunner` en modo batch (micro-poll loop) |
| Ventanas temporales | `FixedWindows(300s)` + `allowed_lateness=120s` |
| Tiempo de evento vs. procesamiento | `AssignEventTimeFn` con `event_time` del payload |
| Watermarks | `AfterWatermark` trigger; en batch el watermark avanza a +∞ al final del input |
| Estado y deduplicación | `SetStateSpec` + `TimerSpec(WATERMARK)` en `DeduplicateFn` |
| Idempotencia del sink | UPSERT por `student_id\|window_start` en `IdempotentFileSinkFn` |
| DLQ | Side output `"dlq"` + `KafkaSinkFn` al topic `lms-dlq` |
| Tests | 46 tests con `TestPipeline` / `TestStream` |
| Reproducibilidad | Docker Compose con Kafka KRaft (sin Zookeeper) |

---

## Notas de implementación — Problemas encontrados

Esta sección documenta los obstáculos técnicos que surgieron durante el desarrollo
y las decisiones de diseño que los resolvieron.

---

### Problema 1: Watermark bloqueado por particiones vacías

**Síntoma**  
El pipeline corría indefinidamente sin generar `output/metrics.jsonl`.
Los logs mostraban que el consumidor leía mensajes de algunas particiones pero nunca
disparaba ninguna ventana.

**Causa raíz**  
El topic `lms-events` tenía **4 particiones**. El productor usa `confluent_kafka` con
claves de mensaje (`student_id`), lo que hace un hash consistente de la clave a una
partición. Con 15 eventos y 4 particiones, los mensajes cayeron únicamente en las
particiones 2 y 3; las particiones 0 y 1 quedaron **completamente vacías**.

El DirectRunner de Apache Beam calcula el watermark global como el **mínimo** de los
watermarks de todas las particiones. Una partición vacía no tiene eventos → su watermark
es −∞ → el watermark global se queda en −∞ → `FixedWindows(300s)` con trigger
`AfterWatermark` nunca dispara.

Evidencia en los logs:
```
# Particiones 0 y 1: siempre offset 0 (sin mensajes)
"Seeking to offset 0 for partition lms-events-0"  ← se repite cada 2 s
"Seeking to offset 0 for partition lms-events-1"  ← se repite cada 2 s

# Particiones 2 y 3: offset avanza (mensajes consumidos)
"Seeking to offset 12 for partition lms-events-2"
"Seeking to offset 5  for partition lms-events-3"
```

**Solución aplicada**  
Reducir `lms-events` a **1 partición** en `docker-compose.yml` (sección `kafka-init`):
```yaml
--topic lms-events --partitions 1 --replication-factor 1
```
Con 1 partición, todos los mensajes van a `lms-events-0`, el watermark avanza con los
timestamps de los eventos y las ventanas cierran correctamente.

> **Aprendizaje de diseño**: en producción, si se requieren múltiples particiones para
> escalar el throughput, el productor debe garantizar que todas las particiones reciban
> datos (p. ej., usando round-robin sin clave, o claves que distribuyan uniformemente).
> Alternativamente, se puede configurar el runner para que avance el watermark de las
> particiones inactivas usando un "idle watermark" configurable en KafkaIO.

---

### Problema 2: KafkaIO cross-language (Java) no entrega elementos al pipeline Python con DirectRunner

**Síntoma**  
Después de resolver el problema de particiones (todos los mensajes llegaban a la
partición 0, offset avanzaba hasta 17), el pipeline seguía sin generar output. Los logs
mostraban el consumer leyendo indefinidamente en offset 17 (el final del topic), pero
**nunca aparecían logs de los transforms de Python** (`[sink] INSERT ...`, `[Validate]`,
etc.).

**Causa raíz**  
`ReadFromKafka` de Apache Beam es una **transformación cross-language**: la parte de
lectura corre en la JVM (Java expansion service) mediante el protocolo de portabilidad
de Beam. El elemento leído por Java debe cruzar la frontera Java→Python para que el
pipeline Python lo procese.

Este mecanismo está diseñado para runners de producción como **Apache Flink** o
**Google Cloud Dataflow**, que tienen un modelo de ejecución distribuida donde los
workers Java y Python se coordinan por red. El **DirectRunner de Python**, sin embargo,
corre todo en un solo proceso. En modo streaming, el SDF (Splittable DoFn) Java del
KafkaIO entra en un loop infinito de polling (el comportamiento correcto para un source
unbounded) y **nunca le cede el control al Python runner** para que éste ejecute los
transforms downstream. Los elementos quedan atrapados en el buffer del bundle Java.

El `AfterProcessingTime(30)` (early trigger especulativo) tampoco disparaba, porque el
timer de processing time requiere que el runner Python ejecute un bundle — algo que
nunca ocurre mientras el bundle de KafkaIO Java monopoliza el executor.

**Solución aplicada**  
Reemplazar `ReadFromKafka` y `WriteToKafka` (cross-language Java) con implementaciones
nativas Python usando `confluent_kafka` directamente:

```python
# Antes (Java cross-language — no funciona con DirectRunner en streaming):
from apache_beam.io.kafka import ReadFromKafka, WriteToKafka
raw = pipeline | ReadFromKafka(consumer_config=..., topics=[INPUT_TOPIC])

# Después (Python nativo — funciona con DirectRunner):
messages = read_kafka_batch(bootstrap, topic, group_id)   # confluent_kafka
raw = pipeline | beam.Create(messages)                    # bounded PCollection
```

El pipeline pasa de **streaming** a **batch** (micro-poll loop):

1. `read_kafka_batch()` lee con `confluent_kafka` todos los mensajes disponibles.
2. `beam.Create(messages)` crea una PCollection acotada.
3. El DirectRunner procesa el lote y avanza el watermark a +∞ al final del input.
4. Todas las `FixedWindows` cierran y `AfterWatermark` dispara → métricas emitidas.
5. El loop duerme 10 s y vuelve a leer (para capturar eventos futuros).

**Transforms de Beam sin cambios**: `ValidateAndParseFn`, `AssignEventTimeFn`,
`FixedWindows`, `DeduplicateFn`, `FilterEngagementFn`, `CountAndScoreCombineFn`,
`FormatMetricFn` e `IdempotentFileSinkFn` no requirieron ninguna modificación.
La semántica de ventanas de tiempo de evento y el manejo de watermarks son idénticos.

> **Cuándo usar KafkaIO cross-language (Java)**: en pipelines desplegados en Apache
> Flink o Google Dataflow, donde el runner distribuido coordina workers Java y Python.
> Para desarrollo y pruebas locales con el DirectRunner, `confluent_kafka` puro Python
> es la opción correcta y sin fricciones.

---

### Resumen de cambios por problema

| Problema | Archivo modificado | Cambio |
|----------|--------------------|--------|
| Particiones vacías | `docker-compose.yml` | `lms-events`: 4 → **1 partición** |
| KafkaIO cross-language | `src/pipeline.py` | `ReadFromKafka` → `read_kafka_batch()` + `beam.Create` |
| KafkaIO cross-language | `src/pipeline.py` | `WriteToKafka` → `KafkaSinkFn` (confluent_kafka DoFn) |
| KafkaIO cross-language | `src/pipeline.py` | `streaming=True` → batch mode (sin flag) |
| Demo lento | `docker-compose.yml` | `REPLAY_SPEED`: 1.0 → **100** |
