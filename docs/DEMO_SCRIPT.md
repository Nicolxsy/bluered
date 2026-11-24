# Guion de Demostración en Vivo (5 Minutos) — BlueRed

Este guion está diseñado para ejecutarse frente al evaluador sobre el entorno **Docker Compose** ya configurado (`docker-compose.yml`), usando **3 pestañas del navegador** sin levantar infraestructura adicional.

---

## Preparación Previa (Antes de iniciar el cronómetro)

1. Confirmar que los 6 contenedores están saludables (`postgres`, `redis`, `bluered-server`, `peer-alice`, `peer-bob`, `peer-carol`):
   ```bash
   docker compose ps
   ```
2. Abrir **3 pestañas** en el navegador:
   - **Pestaña 1 — Alice (`http://localhost:8001`):** `PEER_TCP_PORT=2234`, `PEER_ADVERTISED_HOST=peer-alice`, `MAX_UPLOAD_SLOTS=1`, carpeta `/data/shared` (`./docker/samples/alice`: `arquitectura_bluered_guia.txt` 1,303 B y `redes_p2p_protocolo.md` 606 B).
   - **Pestaña 2 — Bob (`http://localhost:8002`):** `PEER_TCP_PORT=2235`, `PEER_ADVERTISED_HOST=peer-bob`, `MAX_UPLOAD_SLOTS=2`, carpeta `/data/shared` (`./docker/samples/bob`: `algoritmos_bloom_filter.md` 539 B y `clean_architecture_notas.txt` 520 B).
   - **Pestaña 3 — Carol (`http://localhost:8003`):** `PEER_TCP_PORT=2236`, `PEER_ADVERTISED_HOST=peer-carol`, `MAX_UPLOAD_SLOTS=1`, sin carpeta compartida inicial.
3. Verificar en las 3 pestañas la barra lateral (`WS local en tiempo real`) y en **Sesión y Presencia** (`#tab-session`) que `#session-presence-pill` muestre **`ONLINE`** (gracias a `AUTO_LOGIN=1`). Si hace falta refrescar, pulsar `#btn-refresh-session` (`Actualizar`).
4. *(Opcional, solo si se desea demostrar pausa/cola visualmente con un archivo de varios MB en el Momento 3)*: copiar un archivo temporal de ~20 MB dentro de `./docker/samples/alice/` antes de empezar y pulsar `#btn-reindex` en la pestaña **Carpetas Compartidas** de Alice (`8001`). Ver la nota en el Momento 3.

---

## Cronograma de 5 Minutos

### Momento 1 (0:00 – 0:45) — Filtro de Bloom ($O(1)$ en RAM) (`Cas_09`, `Cas_11`, `Cas_12`)
**Objetivo:** Demostrar descarte en $O(1)$ sin golpear SQLite ni propagar búsquedas inútiles, frente a aciertos reales con telemetría en vivo.

1. **Pestaña 1 (Alice, `8001`) y Pestaña 2 (Bob, `8002`) → Tab `Sesión y Presencia` (`[data-tab="session"]`):**
   - Señalar el panel **Telemetría del Nodo y Filtro de Bloom**:
     - `#bloom-size-bits` (*Tamaño Bloom m*), `#bloom-hash-k` (*Funciones hash k*), `#bloom-rejects` (*Rechazos O(1) en RAM*), `#bloom-hits` (*Aciertos Bloom*) y `#bloom-sqlite` (*Consultas SQLite*).
     - Anotar el valor inicial de `#bloom-sqlite` en Alice y Bob (p. ej. `0`).
2. **Pestaña 3 (Carol, `8003`) → Tab `Búsqueda y Reseñas` (`[data-tab="search"]`):**
   - Escribir un término inexistente (ej. `quantum_xyz`) en `#input-search-query` y pulsar `#btn-search-submit` (`Buscar en la red`).
   - **Flujo interno:** `GET /api/search?q=quantum_xyz` → WS `SEARCH_QUERY` → `SearchCoordinatorService.initiate_search` usa `BloomFilterRoutingStrategy`, que evalúa los Bloom filters en Redis (`active_sessions:*`) y descarta a Alice y Bob en $O(1)$ sin enviarles `SEARCH_BROADCAST`.
   - **Observación esperada:** `#search-results-tbody` muestra `Sin coincidencias para "quantum_xyz"`. Al pulsar `#btn-refresh-session` en Alice y Bob, `#bloom-sqlite` **no se incrementa**.
3. **Pestaña 3 (Carol, `8003`) → nueva búsqueda positiva:**
   - Buscar `arquitectura` en `#form-search`.
   - **Flujo interno:** El servidor enruta `SEARCH_BROADCAST` solo a Alice (cuyo Bloom contiene el token `arquitectura`), donde `BloomFilterRepositoryDecorator.search_local` valida en RAM e invoca `SqliteFileRepository.search_local`.
   - **Observación esperada:** En Alice (`8001`), pulsar `#btn-refresh-session` → `#bloom-hits` y `#bloom-sqlite` suben a `1`. *(Si un término coincide por falso positivo con otro peer, ese peer incrementa `#bloom-rejects` en RAM sin tocar SQLite).*

---

### Momento 2 (0:45 – 1:30) — Búsqueda Enriquecida con Calificación Promedio (`Cas_11`, `Cas_12`, `Cas_15`, `Cas_16`)
**Objetivo:** Mostrar que los resultados federados consolidan en lote las calificaciones ACID desde PostgreSQL (`file_catalog`).

1. **Pestaña 3 (Carol, `8003`) → Tab `Búsqueda y Reseñas` (`#tab-search`):**
   - Con los resultados de `arquitectura` (o buscando `bloom` para ver el archivo de Bob `algoritmos_bloom_filter.md`), señalar la tabla `#search-results-tbody`:
     - Nombre del archivo y SHA-256 truncado.
     - Peer origen (`alice peer-alice:2234` o `bob peer-bob:2235`).
     - Tamaño exacto (`1.3 KB` / `539 B`).
     - Columna **Reputación**: muestra `Sin reseñas (0)` antes de reseñar, o `X.X ★ (N)` cuando ya existen reseñas en `file_catalog` (enriquecimiento batch en `SearchCoordinatorService.enrich_hits` → `PostgresFileReviewRepository.get_reputation_summaries`).
2. **Exploración P2P directa (`Cas_16`):**
   - Hacer clic en `Explorar Usuario` (`[data-browse-peer]`) en la fila de Alice.
   - **Flujo interno:** `POST /api/peers/browse` → conexión TCP directa a `peer-alice:2234` con framing binario `PeerOp.BROWSE_REQ` / `BROWSE_RESP`.
   - **Observación esperada:** Se abre el modal `#modal-browse` (`#browse-files-tbody`) listando ambos archivos compartidos por Alice (`arquitectura_bluered_guia.txt` y `redes_p2p_protocolo.md`). Cerrar el modal con `Cerrar` (`[data-close-modal="modal-browse"]`) o iniciar descarga directamente desde allí.

---

### Momento 3 (1:30 – 2:30) — Descarga P2P Directa, Cola `FairShare` y Reanudación tras Pausa/Corte (`Cas_17`, `Cas_18`, `Cas_21`, `Cas_22`)
**Objetivo:** Demostrar transferencia TCP directa peer-to-peer, verificación SHA-256, política `FairShareQueuePolicy` y arquitectura de pausa/reanudación con checkpoints en SQLite.

1. **Pestaña 3 (Carol, `8003`):**
   - En `#search-results-tbody` (o `#browse-files-tbody`), hacer clic en **`Descargar P2P`** (`[data-download-item]`) sobre `arquitectura_bluered_guia.txt` de Alice.
   - **Flujo interno:** El frontend llama a `POST /api/transfers/download` (`conflict_policy: "keep_both"`, `wait: false`) y ejecuta `switchTab("transfers")` automáticamente hacia **Transferencias y Colas** (`#tab-transfers`).
2. **Observar Tab 4 (`Transferencias y Colas`, `#tab-transfers`):**
   - Mostrar `#transfers-slots-label` (`Subidas activas: X / Y`, configurado con `MAX_UPLOAD_SLOTS=1` en Alice y Carol, `2` en Bob).
   - Mostrar la tarjeta `.transfer-row` en `#transfers-list`: estado (`.pill`), barra de progreso `.progress-fill`, indicador `Cola #N` cuando `queue_position != null`, y los botones `[data-tx-action="pause|resume|cancel"]` (`Pausar`, `Reanudar`, `Cancelar`), actualizados en tiempo real mediante el canal `/ws/ui` (`snapshot`, `transfer_state_changed`, `transfer_progress`).
   - Mostrar también en Tab 6 (`Biblioteca Personal`) de Carol que los archivos completados se registran automáticamente (`remember_downloaded_file` / `GET /api/library`).
3. **Nota técnica honesta para el presentador (tamaño de archivos de muestra):**
   - Los archivos incluidos en `./docker/samples/alice/` (`1,303 B` y `606 B`) y `./docker/samples/bob/` (`539 B` y `520 B`) caben en **1 solo frame TCP (`< 1 ms`)**, por lo que pasan inmediatamente a `COMPLETED` en la interfaz y no dejan ventana manual para hacer clic en `Pausar` ni saturan el slot de subida en vivo.
   - **Cómo demostrar el flujo completo de `FairShare` y Pausa/Reanudación con rigor:**
     - *Opción A (en vivo en el navegador, si se preparó el archivo temporal de ~20 MB en `./docker/samples/alice/`):* lanzar la descarga desde Carol (`8003`) y Bob (`8002`) casi simultáneamente → Alice (`MAX_UPLOAD_SLOTS=1`) atiende una y marca `Cola #1` (`FairShareQueuePolicy`) en la segunda; en Carol pulsar **`Pausar`** (`POST /api/transfers/{task_id}/pause` → guarda `DownloadCheckpoint` en SQLite) y luego **`Reanudar`** (`POST /api/transfers/{task_id}/resume` → reabre desde `offset` en `PeerUploadListener.handle_download`).
     - *Opción B (sin modificar `./docker/samples/`, respaldando la UI con evidencia E2E real):* explicar la arquitectura de `DownloadCheckpoint` + `FairShareQueuePolicy` sobre la tarjeta `.transfer-row` de Tab 4 y mostrar la ejecución/resultado de los tests E2E con sockets TCP reales y transferencia multi-chunk de `1.5 MB`:
       - `tests/e2e/test_p2p_transfers.py::test_e2e_cas18_upload_slot_queueing_with_three_real_peers` (3 peers reales saturando `max_upload_slots=1` bajo `FairShareQueuePolicy`).
       - `tests/e2e/test_p2p_transfers.py::test_e2e_cas22_tcp_socket_cut_and_resume_from_offset` (corte abrupto de socket TCP en disco, archivo `.part` y reanudación exacta desde `offset` verificando SHA-256).

---

### Momento 4 (2:30 – 3:20) — Chat Privado P2P Directo, Bloqueo (403) y Cola Offline (`Cas_23`)
**Objetivo:** Demostrar mensajería directa peer-to-peer sin pasar por el servidor central, control de bloqueo HTTP 403 y persistencia offline en SQLite.

1. **Obtener UUID de Bob:**
   - En **Pestaña 2 (Bob, `8002`) → Tab `Sesión y Presencia`**, copiar el valor de **`#meta-peer-id`**.
2. **Enviar mensaje P2P directo desde Alice:**
   - Ir a **Pestaña 1 (Alice, `8001`) → Tab `Comunidades y Chat` (`[data-tab="community"]`)**, sección **Chat Privado P2P Directo** (`#form-private-chat`):
     - `#pm-peer-id`: pegar el UUID de Bob.
     - `#pm-peer-username`: `bob`.
     - `#pm-peer-ip`: **cambiar `127.0.0.1` por `peer-bob`** *(importante: `index.html` inicializa `value="127.0.0.1"` para ejecución local, pero en Docker Compose la red interna resuelve el nombre del servicio `peer-bob`)*.
     - `#pm-peer-port`: `2235` (valor por defecto).
     - `#pm-content`: escribir `"Hola Bob directo por TCP P2P"` y pulsar **`Enviar P2P`**.
   - **Flujo interno:** `POST /api/chat/private` → `PrivateChatService.send_message` → conexión TCP directa a `peer-bob:2235` (`PeerOp.PRIVATE_CHAT_MSG` / `PRIVATE_CHAT_ACK`).
   - **Observación esperada:** Aparece en `#private-messages-log` con `(entregado)` y se refleja en tiempo real en la Pestaña 2 de Bob vía `/ws/ui` (`private_message`).
3. **Demostrar Bloqueo (HTTP 403):**
   - Copiar el `#meta-peer-id` de Alice (`8001`). En **Pestaña 2 (Bob, `8002`) → Tab `Comunidades y Chat`**, pegarlo en `#pm-peer-id` y pulsar **`Bloquear`** (`#btn-block-peer` → `POST /api/peers/{peer_id}/block`). Aparece el chip en `#blocked-peers-bar`.
   - En Alice (`8001`), intentar enviar otro mensaje a Bob → `PrivateChatService` recibe `PeerBlockedError` y la API responde **HTTP 403 Forbidden** (toast de error en Alice).
   - En Bob (`8002`), pulsar `×` en el chip de `#blocked-peers-bar` (`[data-unblock-peer]`) para desbloquear a Alice antes de continuar.
4. **Cola Offline SQLite (`pending_private_messages`):**
   - Explicar (o verificar en `test_e2e_cas23_private_chat_blocklist_and_offline_delivery` / `test_main_build_client_app_presence_offline_and_pending_pm_flush`) que si el destinatario está inalcanzable o en estado `OFFLINE`, `PrivateChatService.send_message` persiste el mensaje en `pending_private_messages` (SQLite WAL) mostrándolo como `(en cola offline)`, y al volver `ONLINE` el evento `PresenceUpdateMessage` en `main.py::_handle_presence` ejecuta `deliver_pending_messages()` automáticamente.

---

### Momento 5 (3:20 – 4:15) — Comunidades, Debates con Respuestas (`Cas_01`–`Cas_04`) y Salas de Chat (`Cas_05`, `Cas_06`)
**Objetivo:** Demostrar persistencia comunitaria en PostgreSQL y sincronización en vivo entre los 3 nodos.

1. **Crear Comunidad (`Cas_01`) en Pestaña 1 (Alice, `8001`):**
   - En Tab `Comunidades y Chat` → `#form-community`: ingresar `#comm-name` = `"Sistemas Distribuidos"`, `#comm-desc` = `"Curaduría P2P y arquitectura"` y pulsar **`Crear comunidad`** (`POST /api/communities` → WS `COMMUNITY_CREATE` → PostgreSQL `communities`).
2. **Publicar Hilo de Debate (`Cas_03`) en Pestaña 1 (Alice, `8001`):**
   - Verificar que `#debate-comm-id` tiene seleccionada `"Sistemas Distribuidos"`.
   - En `#form-debate`: llenar `#debate-title` = `"Estrategias Bloom vs Broadcast"` y `#debate-body` = `"¿Qué tasa de falsos positivos usamos en BlueRed?"`, pulsar **`Publicar debate`** (`POST /api/communities/{community_id}/debates` → WS `DEBATE_POST`).
3. **Responder al Debate desde otro nodo (`Cas_04`) en Pestaña 2 (Bob, `8002`):**
   - Cambiar o abrir Tab `Comunidades y Chat` en Bob (`8002`), seleccionar `"Sistemas Distribuidos"` en `#communities-list` (`[data-select-community]`) o `#debate-comm-id`.
   - En el hilo recién sincronizado dentro de `#debates-list`, usar el formulario `.debate-reply-form` (`input[name='body']`) y pulsar **`Responder`** (`POST /api/debates/{post_id}/replies` → WS `DEBATE_REPLY` → `PostgresCommunityRepository.create_debate_post` con `parent_post_id`).
   - **Observación esperada:** El contador pasa a `1 respuestas` y la respuesta aparece anidada en Alice (`8001`), Bob (`8002`) y Carol (`8003`).
4. **Salas de Chat (`Cas_05`, `Cas_06`) en las 3 pestañas:**
   - En `#form-room-join`, dejar `#room-join-id` en **`general`** (sembrada por `server/migrations/001_init.sql`) y pulsar **`Unirse`** (`POST /api/rooms/general/join`) en Alice, Bob y Carol.
   - Desde Carol (`8003`), escribir en `#room-msg-input` y pulsar **`Enviar`** (`POST /api/rooms/general/messages` → WS `ROOM_MESSAGE`).
   - **Observación esperada:** El mensaje aparece instantáneamente en `#room-messages-log` de las 3 pestañas vía `/ws/ui` (`room_message`).

---

### Momento 6 (4:15 – 5:00) — Reseña ACID y Actualización de Reputación (`Cas_14`, `Cas_15`, `Cas_24`)
**Objetivo:** Demostrar la restricción `UNIQUE(file_hash, author_id)` en PostgreSQL (`UPSERT` ACID) y el impacto inmediato en las búsquedas enriquecidas.

1. **Publicar Reseña (`Cas_14`) desde Pestaña 3 (Carol, `8003`):**
   - Ir a Tab `Búsqueda y Reseñas` y pulsar **`Reseñas`** (`[data-open-reviews]`) en `arquitectura_bluered_guia.txt` *(o desde Tab `Biblioteca Personal` → `#library-tbody` → botón **`Reseñar`**)*.
   - Se abre `#modal-reviews`: seleccionar **4 estrellas** en `#star-picker .star-btn[data-rating="4"]`, escribir un comentario en `#review-comment` y pulsar **`Publicar o actualizar reseña`** (`POST /api/reviews/{file_hash}` → WS `REVIEW_PUBLISH` → `FileReputationService.publish_review` → PostgreSQL `INSERT ... ON CONFLICT (file_hash, author_id) DO UPDATE`).
   - **Observación esperada:** `#reviews-summary` muestra `Promedio: 4.00 ★ (1 reseñas)` y `#reviews-list` lista la reseña de Carol.
2. **Editar / Actualizar Reseña existente (`Cas_24`):**
   - Sin salir de `#modal-reviews` (mismo usuario Carol, mismo `file_hash`), seleccionar **5 estrellas** (`[data-rating="5"]`), escribir `"Actualización tras validar integridad SHA-256"` y volver a pulsar **`Publicar o actualizar reseña`**.
   - **Observación esperada:** Gracias a `UNIQUE(file_hash, author_id)` en PostgreSQL, **no se crea una segunda reseña**: `#reviews-summary` actualiza a `Promedio: 5.00 ★ (1 reseñas)` y reemplaza el comentario en `#reviews-list`.
3. **Verificar impacto en Búsqueda Enriquecida (`Cas_15` + `Cas_12`):**
   - Cerrar `#modal-reviews` (`[data-close-modal="modal-reviews"]`).
   - En **Pestaña 2 (Bob, `8002`) → Tab `Búsqueda y Reseñas`**, buscar `arquitectura` en `#form-search`.
   - **Observación esperada:** La fila de `arquitectura_bluered_guia.txt` ahora muestra **`5.0 ★ (1)`** en la columna Reputación, demostrando que `file_catalog` se actualizó de forma atómica en PostgreSQL y se propaga en lote a toda la red.
