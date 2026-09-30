# Guía de Presentación — LMS Streaming Analytics

**Trabajo Práctico Final · Streaming de Datos · FPUNA MIA**  
Duración sugerida: 15–20 minutos · Formato: demo grabada en pantalla + narración

---

## 0. Antes de grabar

```bash
# Limpieza de estado anterior
docker compose down -v
rm -f output/metrics.jsonl

# Levantar todo
docker compose up --build -d
```

Esperar ~20 segundos a que Kafka y el topic `lms-events` estén listos antes de arrancar el producer.

---

## 1. Apertura — Motivación y contexto (≈ 2 min)

**Lo que decís en cámara / narración:**

> "El punto de partida de este proyecto no es un ejercicio académico abstracto. Soy el desarrollador y operador de **Arandu**, un sistema de gestión de aprendizaje multi-tenant que hoy está en producción con cientos de estudiantes reales.
>
> Cada vez que un estudiante accede a un contenido, envía una evaluación o abandona a mitad de un módulo, Arandu registra ese evento. Pero hoy esos eventos se procesan en batch: una vez por día, o incluso una vez por semana. Eso significa que si un estudiante está acumulando señales de riesgo —abandono progresivo, scores que caen, actividad que desaparece— lo sabemos demasiado tarde para intervenir.
>
> Esto también es el núcleo de mi tesis de maestría: detección temprana de riesgo académico. Hoy trabajo con datos históricos de cohortes cerradas. Pero la pregunta que quiero poder responder es: ¿puedo detectar ese riesgo *mientras está ocurriendo*?
>
> Este proyecto es la respuesta técnica a esa pregunta: una arquitectura de streaming que toma eventos de un LMS en tiempo real, los procesa con ventanas de tiempo, garantiza exactamente-una-vez en la escritura de métricas, y produce indicadores accionables por estudiante a medida que los eventos llegan."

**Puntos clave a enfatizar:**
- El problema es real, no inventado para la materia
- La brecha entre procesamiento batch y tiempo real tiene consecuencias pedagógicas concretas
- La arquitectura propuesta escala desde la demo hasta Arandu en producción

---

## 2. Arquitectura — 3 minutos

Abrir el diagrama del `README.md` o el `DOCUMENTO_TECNICO.md` (Sección 1.1) y recorrerlo:

```
Producer  →  Kafka (lms-events)  →  Pipeline Beam  →  Kafka (lms-metrics)
                                         ↓
                                  output/metrics.jsonl
```

**Puntos a mencionar en el recorrido:**

| Componente | Qué decís |
|---|---|
| Producer | "Simula los eventos que en producción vendrían de Arandu: quiz_submit, video_watch, login, logout" |
| Kafka KRaft | "Broker sin Zookeeper — operacionalmente más simple para deploy en la tesis" |
| `read_kafka_batch()` | "Lee todos los mensajes disponibles con Python confluent_kafka y los convierte en input acotado para Beam" |
| `FixedWindows(300s)` | "Ventanas de 5 minutos de tiempo de evento — el evento lleva su timestamp del LMS, no del servidor" |
| `DeduplicateFn` | "Si Arandu reintenta un envío por red caída, el pipeline no duplica la métrica del estudiante" |
| `IdempotentFileSinkFn` | "UPSERT por (student_id, window_start): re-ejecutar el pipeline da el mismo resultado" |
| DLQ | "Eventos malformados van a un topic separado para revisión; el pipeline no aborta" |

---

## 3. Demo en vivo — el corazón de la presentación (≈ 8 min)

### 3.1 · Arrancar el producer y verificar eventos en Kafka

```bash
# Ver logs del producer
docker logs lms-producer --tail 30
```

Mostrar la salida: `Done — 15 events sent`. Señalar que los eventos tienen timestamps reales de "esta mañana" (tiempo de evento, no de procesamiento).

```bash
# Confirmar que llegaron al broker
docker exec kafka /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 \
  --topic lms-events --from-beginning \
  --max-messages 5 \
  --timeout-ms 5000
```

Mostrar un evento en pantalla y señalar los campos: `student_id`, `event_type`, `timestamp`, `score`.

### 3.2 · Ejecutar el pipeline

```bash
docker logs lms-pipeline -f --tail 40
```

Señalar en los logs:
- `"read N messages from lms-events"` → Beam recibe el input acotado
- Ausencia de errores de validación (salvo los LOG/OUT esperados que van a DLQ)
- `"Pipeline complete"` → el watermark avanzó a +∞, las ventanas se cerraron

### 3.3 · Mostrar el resultado

```bash
cat output/metrics.jsonl | python3 -m json.tool
```

Recorrer la salida y señalar:
- **2 ventanas de 5 minutos** corresponden al span temporal de los eventos del producer
- **s-alice en ventana 1 tiene `event_count: 4`** (no 5) → el duplicado fue descartado por `DeduplicateFn`
- **s-bob en ventana 1 tiene `total_score: 0`** → sus eventos eran LOGIN/LOGOUT, filtrados como sin puntaje; el estudiante aparece en la métrica pero con score cero (señal de presencia sin actividad evaluable)
- **s-carlos** tiene el mayor `avg_score` → candidato a "sin riesgo" en la ventana

### 3.4 · Mostrar el topic de salida en Kafka

```bash
docker exec kafka /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 \
  --topic lms-metrics --from-beginning \
  --timeout-ms 5000
```

"Las mismas métricas también fluyen al topic `lms-metrics`, desde donde un dashboard o un servicio de alertas de Arandu podría consumirlas."

---

## 4. Conceptos académicos cubiertos — 3 minutos

Hacer referencia explícita a los temas de la materia mientras mostrás el código (`src/transforms.py`):

**Ventanas y watermarks (Tarea 2):**

> "La clave de `FixedWindows(300s)` es que el cierre de la ventana está gobernado por el watermark, que avanza según los timestamps de los eventos mismos —tiempo de evento, no tiempo de llegada al sistema. Aquí usamos DirectRunner en batch: al agotar el input, el watermark salta a +∞ y todas las ventanas cierran. En producción con Flink se usaría streaming real y el watermark avanzaría progresivamente."

**Estado, deduplicación e idempotencia (Tarea 3):**

> "`DeduplicateFn` mantiene un `SetState` de event_ids dentro de cada ventana. El estado persiste mientras la ventana está abierta, pero no se filtra entre ventanas —un mismo event_id en dos ventanas distintas es válido. El `TimerSpec(WATERMARK)` limpia el estado al cerrar la ventana para no acumular memoria indefinidamente.
>
> `IdempotentFileSinkFn` implementa UPSERT: si el pipeline falla y se re-ejecuta, el resultado final en `metrics.jsonl` es idéntico. Eso es exactly-once semántico en la escritura, aunque el runner sea at-least-once."

---

## 5. Cierre — trabajo futuro y conexión con Arandu (≈ 2 min)

> "Esta arquitectura es deliberadamente cercana a lo que podría desplegarse en producción con Arandu. Las diferencias serían:
>
> 1. **Runner**: reemplazar DirectRunner por Apache Flink para streaming real y escalabilidad horizontal
> 2. **Fuente de eventos**: Arandu ya genera eventos internamente; se trataría de añadir un productor Kafka en el servicio de actividad del estudiante
> 3. **Destino**: en lugar del archivo `metrics.jsonl`, las métricas alimentarían el módulo de riesgo de Arandu directamente —el mismo componente que hoy usa reglas heurísticas sobre datos batch
>
> En el contexto de mi tesis, este pipeline representa el camino hacia la detección en tiempo real: en lugar de esperar el cierre del semestre para ver quién reprobó, el sistema señalaría en cada ventana de 5 minutos los estudiantes cuyo score promedio cayó por debajo de un umbral, con semanas de anticipación para intervenir."

---

## Checklist antes de subir el video

- [ ] `output/metrics.jsonl` tiene 6 líneas (2 ventanas × 3 estudiantes)
- [ ] Se mencionaron explícitamente: watermark, FixedWindows, DeduplicateFn, SetState, idempotencia, DLQ
- [ ] Se explicó el vínculo con Arandu en la apertura y en el cierre
- [ ] Se mostró el topic `lms-metrics` en Kafka (no solo el archivo)
- [ ] Se mencionó la diferencia entre tiempo de evento y tiempo de procesamiento
- [ ] Duración total ≤ 20 minutos

---

## Referencia rápida de comandos

```bash
# Estado del stack
docker compose ps

# Limpiar y reiniciar demo
docker compose down -v && rm -f output/metrics.jsonl && docker compose up --build -d

# Ver último run del pipeline
docker logs lms-pipeline --tail 60

# Inspeccionar DLQ
docker exec kafka /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 \
  --topic lms-dlq --from-beginning --timeout-ms 3000
```
