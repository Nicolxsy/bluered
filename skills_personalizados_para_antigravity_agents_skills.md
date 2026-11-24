# Skills Personalizados de BlueRed (Con Integración Real de Context7, MCPs y TDD)

Cada skill en `.agents/skills/<nombre>/SKILL.md` cumple un papel operativo real y **todos se invocan explícitamente** a lo largo del Playbook de Super Prompts.

## Matriz de Activación de Skills por Fase (Cero Skills de Adorno)

| Skill | F0: Bootstrap & Contratos | F1: Dominio & Interfaces | F2+F3 (`/swarm`): Server + Client CP | F4: Data Plane P2P & NAT | F5 (`/swarm`): UI + Docker | F6 (`/teamwork`): Auditoría Final |
| :--- | :---: | :---: | :---: | :---: | :---: | :---: |
| **`context7-docs-first`** | **Sí** (`pydantic`, `import-linter`) | Opcional | **Sí** (`fastapi`, `asyncpg`, `redis`, `aiosqlite`, `websockets`) | **Sí** (`asyncio`, `websockets`) | **Sí** (`fastapi`, `websockets`) | — |
| **`p2p-contract-sync`** | **Sí** (diseño inicial) | **Sí** (alineación) | **Sí** (consumo WSS) | **Sí** (framing binario TCP) | **Sí** (eventos UI) | **Sí** (auditoría) |
| **`clean-arch-solid-auditor`** | **Sí** (`import-linter`, `audit_sdp.py`) | **Sí** ($I = 0.00$) | **Sí** (DIP / Decorator / Strategy) | **Sí** (OCP NAT / Observer / LSP) | **Sí** (UI sin lógica negocio) | **Sí** (tabla $C_a, C_e, I$) |
| **`polyglot-db-guardian`** | **Sí** (`docker-compose`) | — | **Sí** (DDL SQL + Redis TTL + SQLite WAL + MCPs) | **Sí** (checkpoints SQLite) | **Sí** (init DB en Docker) | **Sí** (`EXPLAIN` + `TTL` en vivo) |
| **`tdd-evidence-loop`** | **Sí** | **Sí** | **Sí** | **Sí** (E2E sockets reales) | **Sí** (smoke Docker) | **Sí** (cobertura total) |

---

## Skill 1: `.agents/skills/context7-docs-first/SKILL.md`

```markdown
---
name: context7-docs-first
description: Obliga a consultar la documentación vigente de librerías Python mediante el servidor MCP Context7 antes de escribir código con FastAPI, Pydantic v2, asyncpg, redis.asyncio, aiosqlite, websockets, pytest-asyncio o import-linter. Evita APIs deprecadas o alucinadas por Hydrogen.
---

# Context7 Documentation-First Protocol

Antes de escribir o refactorizar código que dependa de librerías externas en Python, ejecuta SIEMPRE este flujo:

## 1. Librerías Cubiertas
- `pydantic` (v2: `BaseModel`, `TypeAdapter`, discriminadores `Annotated[..., Field(discriminator="type")]`, `model_validate_json`).
- `fastapi` (WebSockets, lifespan, inyección de dependencias `Depends`, `StaticFiles`).
- `asyncpg` (pools, transacciones `async with conn.transaction()`, arrays `ANY($1::char(64)[])`).
- `redis` (`redis.asyncio`: pipelines, `hset`, `expire`, bytes crudos para Bloom Filters).
- `aiosqlite` (WAL mode, conexiones asíncronas, cursores).
- `websockets` (cliente asyncio moderno `websockets.asyncio.client`, reconexión y timeouts).
- `pytest-asyncio` (configuración `asyncio_mode = "auto"`, fixtures asíncronas).
- `import-linter` (sintaxis de contratos `forbidden` y `layers` en `pyproject.toml`).

## 2. Procedimiento Obligatorio
1. Identifica las librerías externas que vas a usar en la tarea actual.
2. Invoca el MCP `context7` (`resolve-library-id` seguido de `query-docs` / `get-library-docs`) con una consulta concreta sobre la API exacta que vas a implementar (ej. `"FastAPI lifespan and WebSocket disconnect handling"` o `"redis.asyncio pipeline hset expire"`).
3. Aplica estrictamente la sintaxis confirmada en la documentación recibida (evita sintaxis legada de Pydantic v1, `aioredis` viejo o eventos `@app.on_event` deprecados en FastAPI).
4. En tu resumen de ejecución, indica en una línea qué librerías verificaste con `context7`.
```

---

## Skill 2: `.agents/skills/clean-arch-solid-auditor/SKILL.md`

```markdown
---
name: clean-arch-solid-auditor
description: Audita Clean Architecture, principios SOLID y el Principio de Dependencias Estables (SDP) calculando Ca, Ce e Inestabilidad I en shared/, server/ y client/. Activar al diseñar módulos, cerrar fases o realizar auditorías.
---

# Clean Architecture, SOLID & SDP Auditor

Ejecuta esta auditoría sistemática antes de aprobar cualquier fase:

## 1. Fronteras de Capas (DIP)
1. Ejecuta `uv run lint-imports` y `uv run mypy --strict shared server client`.
2. Confirma que:
   - `domain/` usa únicamente la librería estándar de Python.
   - `application/` solo recibe contratos de `contracts/` en `__init__`.
   - `presentation/` y `infrastructure/` no contienen reglas de negocio (cálculos de `avg_rating`, transiciones de `TransferTask`, políticas de cola ni validaciones de reseñas).

## 2. Patrones GoF y SOLID
- **SRP:** `DiscoveryAndReviewService` y `TransferOrchestrator` separados; en servidor, `PresenceAndSessionService`, `SearchCoordinatorService`, `FileReputationService`, `AuthService` y `CommunityAndChatService` con responsabilidades disjuntas.
- **OCP:** `INatTraversalStrategy`, `ISearchRoutingStrategy` e `ITransferQueuePolicy` extensibles sin `if/elif` sobre clases concretas en los orquestadores.
- **Decorator:** `BloomFilterRepositoryDecorator` implementa `IFileIndexRepository` y envuelve a `SqliteFileRepository`.
- **Observer:** `ITransferEventListener` desacopla `TransferOrchestrator` de la UI web local.
- **LSP:** `LocalPeer` y `RemotePeer` sustituibles como `PeerNode`; `FifoQueuePolicy` y `FairShareQueuePolicy` intercambiables.

## 3. Métrica Matemática SDP
Ejecuta `uv run python scripts/audit_sdp.py` para calcular $C_a$, $C_e$ e inestabilidad:
$$I = \frac{C_e}{C_a + C_e}$$

### Criterios de Aprobación
- `Domain Core` (cliente y servidor) DEBE tener $I = 0.00$.
- Toda dependencia interna $A \to B$ debe respetar $I(B) \le I(A)$.
- Si aparece cualquier violación, corrígela antes de avanzar.
```

---

## Skill 3: `.agents/skills/p2p-contract-sync/SKILL.md`

```markdown
---
name: p2p-contract-sync
description: Mantiene sincronizados los protocolos IControlPlane (JSON/WebSockets) e IPeerTransport (framing binario TCP) en shared/bluered_contracts/ antes de tocar server/ o client/. Obligatorio en tareas de red o al trabajar con /swarm.
---

# P2P Contract Synchronization Protocol

BlueRed separa estrictamente el Plano de Control y el Plano de Datos. **Regla crítica para `/swarm`**: ningún subagente puede alterar `shared/bluered_contracts/` a espaldas del otro; los contratos se congelan primero y se prueban de forma independiente.

## 1. Plano de Control (`shared/bluered_contracts/control_plane.py`)
Modelos `Pydantic v2` discriminados por `type` con `protocol_version: str = "1.0"`:
- Auth y Presencia (`Cas_07`, `Cas_08`): `AUTH_REGISTER`, `AUTH_LOGIN`, `AUTH_OK`, `AUTH_ERROR`, `HEARTBEAT`, `PRESENCE_UPDATE`, `BLOOM_SYNC`.
- Búsqueda Federada (`Cas_11`, `Cas_12`): `SEARCH_QUERY`, `SEARCH_BROADCAST`, `SEARCH_HIT_REPORT`, `SEARCH_RESULTS`.
- Reseñas Centralizadas (`Cas_14`, `Cas_15`, `Cas_24`): `REVIEW_PUBLISH`, `REVIEW_ACK`, `REVIEWS_FETCH`, `REVIEWS_LIST`.
- Comunidad y Chat (`Cas_01`–`Cas_06`, `Cas_13`): `COMMUNITY_CREATE`, `COMMUNITY_LIST`, `DEBATE_POST`, `DEBATE_REPLY`, `LIBRARY_SAVE`, `LIBRARY_LIST`, `ROOM_LIST`, `ROOM_JOIN`, `ROOM_MESSAGE`.
- Señalización NAT (`Cas_20`): `NAT_PUNCH_REQUEST`, `NAT_PUNCH_COORDINATE`, `NAT_RELAY_REQUEST`.

## 2. Plano de Datos (`shared/bluered_contracts/peer_framing.py`)
Framing binario determinista sobre TCP:
- `4 bytes` (`uint32` big-endian): `header_len`.
- `4 bytes` (`uint32` big-endian): `payload_len`.
- `header_len bytes`: JSON UTF-8 (`op`, `request_id`, `file_hash`, `offset`, `chunk_index`, `chunk_sha256`, `status`, `queue_position`, `protocol_version`).
- `payload_len bytes`: datos binarios crudos (0 bytes en frames de control P2P).
- Validación estricta de tamaño máximo de header y payload para evitar desbordamiento de memoria.

## 3. Checklist de Sincronización
1. Verificar sintaxis `Pydantic v2` vía `context7-docs-first`.
2. Actualizar `shared/bluered_contracts/`.
3. Correr tests de contrato (incluyendo lecturas fragmentadas byte a byte en TCP).
4. Actualizar productores y consumidores en `server/` y `client/`.
```

---

## Skill 4: `.agents/skills/polyglot-db-guardian/SKILL.md`

```markdown
---
name: polyglot-db-guardian
description: Protege la Persistencia Políglota (Redis con TTL en RAM, PostgreSQL ACID en disco y SQLite WAL en cliente) y exige verificación en vivo con los MCPs de postgres y redis.
---

# Polyglot Persistence Guardian

## 1. Invariantes de Redis (RAM — Estado Volátil)
- `active_sessions:{user_id}`: SIEMPRE escribir con `EX=45` en pipeline atómica.
- `active_search_routes:{search_token}`: SIEMPRE escribir con `EX=60`.
- `peer_bloom_filters:{user_id}`: almacenar `bitset_payload` en bytes crudos + `hash_functions_k` + `updated_at`; limpiar en desconexión o expirar junto con la sesión.
- **Verificación MCP:** Cuando Docker esté levantado, usa el MCP `redis` para inspeccionar una sesión activa y confirmar `TTL > 0` (nunca `-1`).

## 2. Invariantes de PostgreSQL (Disco — ACID)
- `file_catalog`: PK `file_hash CHAR(64)` (SHA-256 en minúsculas), con `review_count`, `rating_sum` y `avg_rating` desnormalizados.
- `file_reviews`: `UNIQUE (file_hash, author_id)` y `CHECK (rating BETWEEN 1 AND 5)`.
- Toda publicación/edición de reseña (`Cas_14`, `Cas_24`) DEBE correr en una sola transacción ACID que cree el registro en `file_catalog` si no existe, haga `INSERT ... ON CONFLICT (file_hash, author_id) DO UPDATE`, y recalcule `review_count`, `rating_sum` y `avg_rating` de forma atómica con bloqueo de fila sobre `file_catalog`.
- Enriquecimiento de búsqueda (`get_summaries_by_hashes`): una sola consulta por lote `WHERE file_hash = ANY($1::char(64)[])` leyendo únicamente `file_catalog` (sin `JOIN` a `file_reviews`).
- **Verificación MCP:** Usa el MCP `postgres` para validar constraints y correr `EXPLAIN ANALYZE` confirmando *Index Scan* sobre `file_catalog_pkey`.

## 3. Invariantes de SQLite (Cliente Local)
- Activar `PRAGMA journal_mode=WAL;` y `PRAGMA foreign_keys=ON;`.
- Persistir `mtime` y `size_bytes` para omitir re-hashing de archivos sin cambios (`Cas_10`).
```

---

## Skill 5: `.agents/skills/tdd-evidence-loop/SKILL.md`

```markdown
---
name: tdd-evidence-loop
description: Impone desarrollo guiado por pruebas (TDD) y verificación basada en evidencia real (unitarios, integración contra Docker y E2E P2P con sockets TCP reales) antes de dar cualquier fase por terminada.
---

# TDD & Evidence-Based Verification Loop

## 1. Ciclo Obligatorio
1. Escribe tests cubriendo precondiciones, escenario primario, secundarios y excepcionales de cada `Cas_XX` asignado.
2. Verifica fallo inicial (*Red*).
3. Implementa código limpio cumpliendo SOLID (*Green*).
4. Refactoriza y re-ejecuta todo (*Refactor*).

## 2. Regla de Mocks vs. Infraestructura Real
- `tests/unit/`: fakes en memoria implementando `contracts/` (velocidad en milisegundos).
- `tests/integration/`: **PROHIBIDO mockear** PostgreSQL, Redis o SQLite. Ejecutar contra contenedores reales de `docker-compose.yml` y SQLite en directorio temporal.
- `tests/e2e/`: levantar sockets TCP reales en `127.0.0.1` y servidor FastAPI/WebSocket real transfiriendo archivos binarios reales ($\ge 1\text{ MB}$), verificando SHA-256, cola de espera real (`max_upload_slots=1`) y corte/reanudación de socket TCP (`Cas_22`).

## 3. Evidencia Obligatoria al Cerrar
Ninguna tarea se cierra sin pegar en la respuesta la salida real y verde de:
- `uv run ruff check .`
- `uv run mypy --strict shared server client`
- `uv run lint-imports`
- `uv run pytest`
- Lista de `Cas_XX` verificados y librerías consultadas en `context7`.
```