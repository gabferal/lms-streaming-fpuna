# Documento Técnico — LMS Streaming Analytics

**Proyecto Integrador — Streaming de Datos**  
**FPUNA Maestría en Inteligencia Artificial**  
**Autor:** Gabriel Álvarez (ec.gabrielalvarez@gmail.com)

---

## 1. Arquitectura del sistema

### 1.1 Diagrama de componentes

```
┌──────────────────────────────────────────────────────────────────────┐
│                          Docker Compose                              │
│                                                                      │
│  ┌─────────────┐    lms-events     ┌──────────────────────────────┐ │
│  │  Producer   │──────────────────►│      Apache Beam Pipeline    │ │
│  │(src/producer│   (4 particiones) │       (DirectRunner)         │ │
│  │    .py)     │                   │                              │ │
│  └─────────────┘                   │  ValidateAndParseFn          │ │
│        │                           │       │                      │ │
│  data/ │                           │       ├── valid ──►          │ │
│  events│                           │       └── dlq ──────────────►├─┼──► lms-dlq
│  .jsonl│                           │                              │ │
│        │                           │  AssignEventTimeFn           │ │
│  Escenarios                        │       │                      │ │
│  inyectados:                       │  WindowInto(5 min)           │ │
│  · Duplicado                       │       │                      │ │
│  · Fuera de orden                  │  KeyByStudentFn              │ │
│  · Tardío (late)                   │       │                      │ │
│                                    │  DeduplicateFn (stateful)    │ │
│                                    │       │                      │ │
│  ┌─────────────┐                   │  FilterEngagement            │ │
│  │  Consumer   │◄──────────────────│       │                      │ │
│  │(src/consumer│   lms-metrics     │  CombinePerKey               │ │
│  │    .py)     │   (4 particiones) │       │                      │ │
│  └─────────────┘                   │  FormatMetricFn              │ │
│                                    │       │                      │ │
│  ┌─────────────┐                   │  IdempotentFileSinkFn        │ │
│  │  Kafka UI   │     Introspección │       │                      │ │
│  │:8080        │◄──────────────────┤       ▼                      │ │
│  └─────────────┘                   └──────────────────────────────┘ │
│                                            │                         │
│                                            ▼                         │
│                                    output/metrics.jsonl              │
└──────────────────────────────────────────────────────────────────────┘
```

### 1.2 Flujo de datos

| Paso | Componente | Descripción |
|------|-----------|-------------|
| 1 | `src/producer.py` | Lee `data/events.jsonl`, aplica `REPLAY_SPEED`, inyecta escenarios (duplicado, fuera de orden, tardío) y publica en `lms-events`. |
| 2 | `ReadFromKafka` | Lee mensajes como tuplas `(key_bytes, value_bytes)` usando KafkaIO cross-language. |
| 3 | `ValidateAndParseFn` | Valida esquema JSON, campos requeridos, tipos de evento y timestamps. Eventos inválidos van al DLQ. |
| 4 | `AssignEventTimeFn` | Extrae `event_time` del payload y crea un `TimestampedValue`. Esto activa el procesamiento por **tiempo de evento** (no de llegada). |
| 5 | `WindowInto(FixedWindows(300s))` | Agrupa en ventanas fijas de 5 minutos. |
| 6 | `KeyByStudentFn` | Convierte cada evento en `(student_id, event_dict)`. |
| 7 | `DeduplicateFn` | Filtra duplicados per-estudiante per-ventana usando estado persistente. |
| 8 | `FilterEngagement` | Descarta `LOGIN` y `LOGOUT`; solo `QUIZ_SUBMIT`, `VIDEO_COMPLETE`, `PAGE_VIEW` se agregan. |
| 9 | `CombinePerKey(CountAndScoreCombineFn)` | Acumula `(event_count, total_score)` por estudiante por ventana. |
| 10 | `FormatMetricFn` | Produce un `dict` con `student_id`, `window_start`, `window_end`, `event_count`, `total_score`, `avg_score`. |
| 11 | `IdempotentFileSinkFn` | Escribe en `output/metrics.jsonl` con semántica UPSERT. |
| 12 | `WriteToKafka` | Publica las métricas en `lms-metrics` y los eventos DLQ en `lms-dlq`. |

---

## 2. Contrato de eventos

### 2.1 Esquema de evento (schema_version 1.0)

```json
{
  "schema_version":  "1.0",
  "event_id":        "evt-a001",
  "student_id":      "s-alice",
  "course_id":       "course-ml-2026",
  "event_type":      "QUIZ_SUBMIT",
  "event_time":      "2026-09-30T13:00:05Z",
  "arrival_time":    "2026-09-30T13:00:06Z",
  "payload": {
    "resource_id":       "quiz-ch3",
    "score":             85,
    "duration_seconds":  240,
    "attempt":           1
  }
}
```

| Campo | Tipo | Requerido | Descripción |
|-------|------|-----------|-------------|
| `schema_version` | string | No | Versión del esquema; default "1.0" |
| `event_id` | string | **Sí** | Identificador único de la ocurrencia (UUID-like) |
| `student_id` | string | **Sí** | Clave de agrupamiento y Kafka key |
| `course_id` | string | **Sí** | Identificador del curso |
| `event_type` | enum | **Sí** | `QUIZ_SUBMIT`, `VIDEO_COMPLETE`, `PAGE_VIEW`, `LOGIN`, `LOGOUT` |
| `event_time` | ISO-8601 UTC | **Sí** | Cuándo ocurrió el evento (tiempo de dominio) |
| `arrival_time` | ISO-8601 UTC | **Sí** | Cuándo llegó a la capa de ingesta |
| `payload` | object | No | Datos específicos del dominio |

### 2.2 Tipos de evento y agregación

| Tipo | Descripción | ¿Agrega? |
|------|-------------|----------|
| `QUIZ_SUBMIT` | Envío de quiz con puntaje | ✅ |
| `VIDEO_COMPLETE` | Visualización completa de video | ✅ |
| `PAGE_VIEW` | Vista de página de contenido | ✅ |
| `LOGIN` | Inicio de sesión | ❌ |
| `LOGOUT` | Cierre de sesión | ❌ |

### 2.3 Formato de métrica de salida

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

## 3. Decisiones de diseño

### 3.1 Tiempo de evento vs. tiempo de procesamiento

El pipeline usa **tiempo de evento** (`event_time` del payload), no el tiempo en que Kafka recibe el mensaje. Esto es crítico para:

- Asignar correctamente los eventos a ventanas aunque lleguen con retraso.
- Producir métricas históricamente correctas (ej. un quiz enviado a las 13:04 pertenece a la ventana de las 13:00–13:05 aunque se ingeste a las 13:06).

La conversión se realiza en `AssignEventTimeFn` mediante `TimestampedValue`:

```python
et = parse_utc(element["event_time"])
yield TimestampedValue(element, et.timestamp())
```

### 3.2 Política de ventanas y triggers

```python
beam.WindowInto(
    FixedWindows(300),                          # ventanas fijas de 5 minutos
    trigger=AfterWatermark(
        early=AfterProcessingTime(30),          # emisión especulativa c/30s (producción)
        late=AfterCount(1),                     # refinamiento por evento tardío
    ),
    accumulation_mode=AccumulationMode.ACCUMULATING,
    allowed_lateness=Duration(seconds=120),     # tolerancia de 2 minutos
)
```

**Por qué ACCUMULATING:** Cada pane tardío emite el TOTAL acumulado de la ventana, no solo el delta. Esto hace que el `IdempotentFileSinkFn` siempre reciba el estado completo y actualice el registro con el valor correcto, sin necesidad de sumar incrementos.

**Por qué `allowed_lateness=120s`:** Un sistema LMS puede tener retrasos de red de hasta 2 minutos. Eventos que lleguen dentro de ese plazo después del cierre de la ventana se aceptan y refinan la métrica.

**Nota sobre tests:** El trigger `AfterProcessingTime(30)` requiere un reloj de tiempo real que PrismRunner (runner por defecto en Beam ≥2.74) no provee en `TestPipeline`. Por esto, los tests usan `early_trigger_seconds=0` para desactivar el early trigger, manteniendo el comportamiento de producción intacto.

### 3.3 Deduplicación stateful

El `DeduplicateFn` usa estado persistente de Beam para rastrear los `event_id` vistos por cada estudiante dentro de la ventana:

```python
SEEN_IDS = SetStateSpec("seen_ids", StrUtf8Coder())
EXPIRY   = TimerSpec("expiry", TimeDomain.WATERMARK)

def process(self, element, seen_ids, window, expiry):
    student_id, event = element
    existing = set(seen_ids.read())
    if event["event_id"] not in existing:
        seen_ids.add(event["event_id"])
        expiry.set(window.end)   # liberar estado al cierre de ventana
        yield element
```

**Claves del diseño:**
- Estado es **per-key** (`student_id`) y **per-window**: dos estudiantes pueden tener el mismo `event_id` sin conflicto.
- El timer `WATERMARK` limpia el estado cuando la ventana cierra, evitando acumulación ilimitada de memoria.
- Es idempotente: si el mismo mensaje llega N veces, solo el primero pasa.

### 3.4 Idempotencia del sink

`IdempotentFileSinkFn` implementa semántica UPSERT usando un diccionario en memoria clave → registro:

```
clave = f"{student_id}|{window_start}"
```

En cada escritura, el diccionario completo se vuelca al disco atómicamente via `tmp + os.replace()`. Si el pipeline recibe el mismo pane varias veces (at-least-once), el resultado final en `metrics.jsonl` siempre tendrá exactamente **un** registro por estudiante por ventana con el último valor (last-write-wins).

**Esta construcción convierte la semántica at-least-once del pipeline en efectivamente exactly-once para el sink.**

---

## 4. Garantías de entrega

| Semántica | ¿Dónde aplica? |
|-----------|----------------|
| **At-least-once** | Kafka → Beam (sin `EXACTLY_ONCE_SEMANTICS` en DirectRunner) |
| **Exactamente una vez (efectivo)** | Sink de archivo: UPSERT por clave `student_id\|window_start` |
| **At-least-once** | Beam → Kafka (`WriteToKafka`, sin transacciones habilitadas) |

El pipeline NO implementa transacciones Kafka end-to-end. Para exactly-once completo en producción se requeriría `EXACTLY_ONCE_SEMANTICS=true` en `WriteToKafka` y un runner transaccional (Flink, Dataflow).

---

## 5. Gestión de estado: límites y consideraciones

### 5.1 Tamaño del estado

El `SetStateSpec` de `DeduplicateFn` almacena strings de `event_id`. Dado que:
- Los `event_id` son strings cortos (~12-36 bytes)
- El estado se limpia al cierre de la ventana (timer WATERMARK)
- Hay un timer por estudiante por ventana

El estado total es O(E × S) donde E = eventos únicos por ventana y S = número de estudiantes activos. Para 10.000 estudiantes con 100 eventos únicos/ventana: ~1 MB de estado en memoria — manejable para DirectRunner.

### 5.2 Riesgo de estado no liberado

Si el watermark nunca avanza (fuente estancada), el timer `WATERMARK` no dispara y el estado crece indefinidamente. Mitigación en producción: configurar idle watermark avance en la fuente Kafka.

### 5.3 Desbordamiento de ventana

Con ventanas de 5 minutos y `ACCUMULATING`, si llegan muchos panes tardíos, el `IdempotentFileSinkFn` reescribe el archivo completo en cada pane. Para N estudiantes y P panes: O(N × P) escrituras. Mitigación: limitar `allowed_lateness` y usar un sink en base de datos real en producción.

---

## 6. Dead Letter Queue (DLQ)

Eventos que no pasan la validación van al side output `"dlq"` y de ahí al topic `lms-dlq`:

```json
{
  "raw":    "{ \"event_id\": \"bad\", ... }",
  "reason": "missing_fields:['student_id']"
}
```

Razones posibles:
- `parse_error:...` — JSON malformado
- `not_a_json_object` — JSON array o primitivo
- `missing_fields:[...]` — campos requeridos ausentes
- `invalid_event_type:WATCH_VIDEO` — tipo no reconocido
- `invalid_event_time:...` — timestamp no parseble
- `schema_error:...` — error de construcción del dataclass

---

## 7. Escenarios de prueba

### 7.1 Suite de tests (46 tests)

| Archivo | Clases | Tests | Descripción |
|---------|--------|-------|-------------|
| `tests/test_events.py` | 3 | 26 | Parsing, validación, factory |
| `tests/test_pipeline.py` | 9 | 20 | Transforms Beam con TestPipeline/TestStream |
| **Total** | **12** | **46** | |

> **Resultado:** 45 passed, 1 xfailed (PrismRunner bug con `allowed_lateness` en TestStream).

### 7.2 Escenarios demostrativos (`data/events.jsonl`)

| Línea | Event ID | Estudiante | Tipo | Escenario |
|-------|----------|-----------|------|-----------|
| 1 | `evt-a001` | s-alice | QUIZ_SUBMIT | Normal (score=85) |
| 2 | `evt-a001` | s-alice | QUIZ_SUBMIT | **DUPLICADO**: misma event_id → deduplicado |
| 3-9 | varios | s-bob, s-carlos | varios | Normal, ventana [13:00-13:05] |
| 10-12 | varios | todos | varios | Normal, ventana [13:05-13:10] |
| 13 | `evt-c004` | s-carlos | QUIZ_SUBMIT | **FUERA DE ORDEN**: event_time=13:04:30 (ventana 1), arrival=13:06:30 (durante ventana 2) |
| 14 | `evt-a005` | s-alice | LOGIN | No agrega (LOGIN excluido) |
| 15 | `evt-bad1` | *(ausente)* | QUIZ_SUBMIT | **DLQ**: falta `student_id` |

### 7.3 Reproducibilidad

```bash
# Limpia el estado anterior
docker compose down -v
rm -f output/metrics.jsonl

# Levanta desde cero
docker compose up -d
docker compose run --rm producer
cat output/metrics.jsonl
```

La secuencia produce siempre el mismo resultado determinístico gracias a:
- Datos fijos en `data/events.jsonl`
- Kafka KRaft con retención corta (2h)
- Sink idempotente (UPSERT)

---

## 8. Stack tecnológico

| Componente | Tecnología | Versión |
|-----------|-----------|---------|
| Ingesta de eventos | Apache Kafka (KRaft, sin Zookeeper) | Bitnami 3.8 |
| Motor de streaming | Apache Beam | ≥2.74.0 |
| Runner | DirectRunner → PrismRunner | (incluido en SDK) |
| Lenguaje | Python | 3.12 |
| KafkaIO | Cross-language expansion service (Java) | auto-start |
| Empaquetado | uv | latest |
| Contenedores | Docker Compose v2 | - |
| UI de Kafka | kafka-ui | latest |

---

## 9. Limitaciones conocidas

| Limitación | Impacto | Mitigación |
|-----------|---------|-----------|
| DirectRunner no provee exactly-once end-to-end en Kafka | Duplicados en `lms-metrics` posibles bajo reinicios | Usar Flink/Dataflow + transacciones Kafka en producción |
| PrismRunner no descarta eventos post-`allowed_lateness` en TestStream | Test `test_late_event_beyond_tolerance_dropped` marcado xfail | El `WindowInto` está correctamente configurado; la limitación es del runner de test |
| `IdempotentFileSinkFn` recarga el archivo en cada elemento | I/O costoso con muchos estudiantes | Usar un sink en base de datos (Cassandra, BigQuery) en producción |
| `AfterProcessingTime(30s)` desactivado en tests | Las emisiones especulativas no se testean | Probado manualmente con Kafka real |
