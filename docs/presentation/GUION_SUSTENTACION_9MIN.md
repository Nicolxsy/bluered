# Guion maestro de sustentación — BlueRed (9:15 min)

Equipo: **Joel Pérez** (20242020017) · **Nicolás Sierra** (20222020197) · **Sergio Osorio** (20241020073).
Archivo de diapositivas: `docs/presentation/BlueRed_Sustentacion.pptx` (las notas completas de cada diapositiva están en el panel de notas del orador).
Cifras medidas el 2026-11-24 (evidencia en `docs/presentation/evidence/`).

## 1. Cronograma

| # | Diapositiva | Tiempo | Orador | Duración |
|---|---|---|---|---|
| 1 | Portada y KPIs | 0:00–0:25 | Joel | 25 s |
| 2 | Proceso: del diseño UML al código | 0:25–1:20 | Joel | 55 s |
| 3 | PDF vs. código: Clean Architecture, SOLID y GoF | 1:20–2:30 | Joel | 70 s |
| 4 | PDF vs. código: persistencia políglota | 2:30–3:35 | Nicolás | 65 s |
| 5 | PDF vs. código: estados y swimlanes P2P | 3:35–4:30 | Nicolás | 55 s |
| 6 | PDF vs. código: componentes, nodos y protocolo | 4:30–5:15 | Nicolás | 45 s |
| 7 | Demostración en vivo | 5:15–8:15 | Sergio | 180 s (≈30 s de contexto + 2:30 de demo) |
| 8 | Evidencia cuantitativa: SDP y calidad | 8:15–8:35 | Joel | 20 s |
| 9 | Trazabilidad y ajustes técnicos | 8:35–8:55 | Nicolás | 20 s |
| 10 | Conclusiones y preguntas | 8:55–9:15 | Sergio | 20 s |
| 11 | Respaldo Plan B (solo si falla la demo) | — | Sergio | — |

Cifras para tener a mano: 236 pruebas pasando · 10/10 contratos import-linter · Domain Core I = 0.00 (cliente Ca=5, servidor Ca=4) · mypy strict sin errores en 60 archivos · ruff limpio · 6 contenedores sanos · TTL de sesiones 35–36 s (máx. 45 s) · `EXPLAIN ANALYZE` 0.076 ms (5 filas) y 0.322 ms (100 000 filas sintéticas con ROLLBACK).

## 2. Preparación antes de empezar el cronómetro

```bash
docker compose ps                  # 6 servicios (healthy): postgres, redis, bluered-server, peer-alice, peer-bob, peer-carol
docker compose restart peer-alice peer-bob peer-carol   # solo si Redis está vacío o los peers aparecen AWAY/OFFLINE
docker compose exec -T redis redis-cli --scan --pattern 'active_sessions:*'   # debe listar 3 claves
```

Abrir tres pestañas: Alice `http://localhost:8001`, Bob `http://localhost:8002`, Carol `http://localhost:8003`. En cada una, `Sesión y Presencia` debe mostrar **ONLINE** (si dice AWAY, pulsar `ONLINE`).

Opcional (solo para mostrar cola y pausa con archivo grande; borrar después):

```bash
head -c 20000000 /dev/urandom > docker/samples/alice/demo_20mb.bin   # luego pulsar «Reindexar» en Alice → Carpetas Compartidas
rm docker/samples/alice/demo_20mb.bin
```

## 3. Demo en vivo (2:30 min dentro de la ventana de Sergio)

**Paso 1 — Presencia + Bloom (0:50)**
1. Carol (`:8003`) → *Búsqueda y Reseñas* → buscar `quantum_xyz` → «Sin coincidencias»; el servidor descarta a Alice y Bob con sus filtros de Bloom (`GET /api/search?q=quantum_xyz`).
2. Carol → buscar `arquitectura` → resultado de Alice (`arquitectura_bluered_guia.txt`, 1303 B).
3. Alice (`:8001`) → *Sesión y Presencia* → `Actualizar`: sube *Aciertos Bloom*.

**Paso 2 — Descarga P2P + FairShare + reanudación (1:10)**
1. Carol → *Descargar P2P* sobre `arquitectura_bluered_guia.txt` (`POST /api/transfers/download`) → pestaña *Transferencias y Colas*: estado, slots (`Subidas activas: X / Y`; Alice y Carol con `MAX_UPLOAD_SLOTS=1`).
2. Con el archivo de ~20 MB: lanzar la descarga desde Carol y Bob casi a la vez → la segunda queda en `Cola #1`; en Carol, `Pausar` y `Reanudar` (continúa desde el offset).
3. Sin archivo grande (los de muestra pesan 520–1303 B y terminan en < 1 ms): mostrar las pruebas E2E reales:

```bash
uv run pytest -v tests/e2e/test_p2p_transfers.py
# cas18: 3 peers reales saturando max_upload_slots=1 bajo FairShareQueuePolicy
# cas22: corte de socket TCP, archivo .part y reanudación desde offset con SHA-256 verificado
```

**Paso 3 — Reseña ACID + Chat P2P (0:50)**
1. Carol → *Reseñas* sobre `arquitectura_bluered_guia.txt` → 4 ★ → publicar; luego 5 ★ → publicar de nuevo: sigue **1** reseña (`UNIQUE(file_hash, author_id)`), promedio 5.0.
2. Alice → *Comunidades y Chat* → *Chat Privado P2P Directo*: `#pm-peer-id` = Peer ID de Bob (en `Sesión y Presencia` de Bob), `#pm-peer-ip` = `peer-bob` (no `127.0.0.1`), puerto `2235`, mensaje «Hola Bob directo por TCP P2P» → aparece `(entregado)` y llega a Bob en tiempo real.

**Si algo falla:** diapositiva 11 (capturas reales de Alice y Carol y las 4 pruebas E2E de transferencia).

## 4. Cinco preguntas difíciles y respuestas respaldadas por evidencia

**1. ¿Por qué dos bases de datos y no solo PostgreSQL?**
Las cargas son asimétricas. Sesiones y heartbeats cambian cada pocos segundos y deben expirar solos: en Redis cada clave `active_sessions:*` lleva TTL (medimos 35–36 s, tope 45 s), sin bloqueo de filas. Reseñas y catálogo exigen transacciones: `file_reviews` tiene `UNIQUE(file_hash, author_id)` y `CHECK(rating 1..5)`, y el catálogo guarda promedio desnormalizado. Nuestro `EXPLAIN ANALYZE` usa el índice de la clave primaria (0.076 ms con 5 filas; 0.322 ms con 100 000 filas sintéticas en una transacción revertida).

**2. ¿El filtro de Bloom no da falsos positivos? ¿Es realmente O(1)?**
Sí puede dar falsos positivos, nunca falsos negativos (hay una prueba: `test_bloom_determinism_and_zero_false_negatives`). El costo de un falso positivo es un broadcast innecesario a un peer, que lo descarta en RAM (`Rechazos O(1)`) sin tocar SQLite. Parámetros reales: m = 8192 bits, k = 5. Ojo con la precisión: el Bloom es O(1); la consulta por hash en PostgreSQL es un índice B-tree, O(log n), aunque el promedio de reputación sí se lee ya calculado.

**3. ¿Qué pasa si el servidor central se cae?**
Las transferencias ya iniciadas siguen, porque el plano de datos es TCP directo entre peers. Las búsquedas, la presencia y las reseñas dependen del servidor. El cliente reconecta y se reautentica automáticamente (pruebas `test_ws_coordinator_auto_reauth_on_session_expired_and_reconnect` y `…failed_reauth_sets_presence_offline`). El servidor central es un punto único de fallo y no lo hemos replicado; el PDF habla de Redis Cluster y nosotros usamos una sola instancia.

**4. El PDF habla de NAT traversal con UPnP, STUN y UDP. ¿Qué implementaron realmente?**
Estrategias: conexión directa, UPnP, hole punching TCP señalizado por el servidor y relevo, compuestas con el patrón Strategy. En el arranque (`main.py`) la cadena cableada es directo → hole punching TCP → relevo; UPnP está implementado y probado pero no compuesto, y no hay STUN ni UDP. En Docker todos los peers comparten una red puente, así que casi siempre basta la conexión directa; el recorrido de NAT se verifica con pruebas unitarias, no con routers reales.

**5. ¿Qué quedó fuera o difiere del diseño?**
De los 24 casos del PDF: 22 completos, Cas_05 parcial (sin categorización por temas explícita) y Cas_06 (descubrir intereses comunes) sin implementar. Diferencias: control plane `ws://` en Docker (PDF: WSS), una sola instancia de Redis, esquema más simple que la Fig. 55 (sin MIME/malware en `file_catalog`), y la matriz de trazabilidad usa una numeración distinta a la del PDF en Cas_02–06, 13 y 19 (el handshake, Cas_19 del PDF, está implementado dentro de la descarga y la subida). Todo está detallado en `docs/presentation/PDF_VS_CODE_AUDIT.md`.

## 5. Reproducir los números

```bash
uv run ruff check .
uv run mypy --strict shared server client
uv run lint-imports
uv run python scripts/audit_sdp.py
uv run pytest -v
uv run --with python-pptx --with pillow python scripts/build_presentation.py
```
