# LMS Streaming Analytics — Proyecto Integrador

Pipeline de análisis en tiempo real para un **Learning Management System (LMS)**.  
Ingesta eventos de actividad de estudiantes desde Apache Kafka, los procesa con Apache Beam
y produce métricas de engagement por ventana de 5 minutos.

Desarrollado como Proyecto Integrador del curso de **Streaming de Datos** — FPUNA Maestría en IA.

---

## Arquitectura

```
Fuente de eventos
  (src/producer.py)
        │
        ▼  lms-events (Kafka, 4 particiones)
┌────────────────────┐
│  Apache Beam       │  DirectRunner (streaming)
│                    │
│  ValidateAndParse ─┼──► lms-dlq (DLQ topic)
│  AssignEventTime   │
│  FixedWindows(5m)  │
│  KeyByStudent      │
│  Dedup (stateful)  │
│  FilterEngagement  │
│  CombinePerKey     │
│  FormatMetric      │
│  IdempotentSink ───┼──► output/metrics.jsonl
└────────────────────┘
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
| `src/pipeline.py` | Pipeline completo con KafkaIO |
| `src/producer.py` | Replay de eventos con escenarios inyectados |
| `src/consumer.py` | Display de métricas en terminal |
| `src/config.py` | Constantes de configuración (env vars) |

### Escenarios demostrativos

El productor inyecta automáticamente tres escenarios al reproducir `data/events.jsonl`:

1. **Duplicado** — `evt-a001` enviado dos veces (misma `event_id`). El pipeline lo cuenta una sola vez.
2. **Fuera de orden** — `evt-c004` tiene `event_time` en la ventana [13:00–13:05] pero llega durante la ventana [13:05–13:10]. El pipeline lo asigna a la ventana correcta por `event_time`.
3. **Tardío (late)** — un evento con `event_time` muy antiguo enviado al final. Si supera `allowed_lateness`, se descarta silenciosamente.

---

## Inicio rápido

### Requisitos

- Docker ≥ 24 y Docker Compose v2
- (Desarrollo local) Python 3.12 + `default-jre-headless` + [uv](https://docs.astral.sh/uv/)

### Con Docker Compose

```bash
# 1. Levantar Kafka + pipeline
docker compose up -d

# 2. Esperar que Kafka esté healthy (~20s)
docker compose ps

# 3. Enviar eventos (escenarios inyectados automáticamente)
docker compose run --rm producer

# 4. Ver métricas en tiempo real
docker compose run --rm consumer

# 5. Ver métricas en archivo
cat output/metrics.jsonl | python -m json.tool

# 6. UI de Kafka (opcional)
open http://localhost:8080

# 7. Detener todo
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
| `OUTPUT_DIR` | `output` | Directorio para `metrics.jsonl` |
| `DATA_FILE` | `data/events.jsonl` | Archivo de eventos para el productor |
| `REPLAY_SPEED` | `1.0` | Factor de velocidad del replay (>1 = más rápido) |

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
  "event_count":  3,
  "total_score":  85,
  "avg_score":    28.33
}
```

---

## Criterios de evaluación cubiertos

| Criterio | Implementación |
|----------|---------------|
| Ingesta Kafka | `ReadFromKafka` (KafkaIO cross-language) + `WriteToKafka` |
| Procesamiento Beam | `DirectRunner` en modo streaming |
| Ventanas temporales | `FixedWindows(300s)` + `allowed_lateness=120s` |
| Tiempo de evento vs. procesamiento | `AssignEventTimeFn` con `event_time` del payload |
| Watermarks | `AfterWatermark` trigger con early/late firing |
| Estado y deduplicación | `SetStateSpec` + `TimerSpec(WATERMARK)` en `DeduplicateFn` |
| Idempotencia del sink | UPSERT por `student_id|window_start` en `IdempotentFileSinkFn` |
| DLQ | Side output `"dlq"` + `WriteToKafka` al topic `lms-dlq` |
| Tests | 44 tests con `TestPipeline` / `TestStream` |
| Reproducibilidad | Docker Compose con Kafka KRaft (sin Zookeeper) |
