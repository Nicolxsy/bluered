# Setup de Google Antigravity CLI para BlueRed (Google AI Pro + Orchestrator + Swarm)

Esta guía configura **Antigravity CLI (`agy`)** específicamente para tu suscripción **Google AI Pro**, maximizando tu cuota al delegar la escritura de código a **Gemini 4 Hydrogen** bajo la planificación de **Gemini 4 Neon** (**`Orchestrator (Neon)`**) y explotando **`/swarm`** y **`/teamwork`** gracias a los contratos desacoplados de **BlueRed**.

---

## 1. Estrategia de Modelos y Modos en Google AI Pro (`agy`)

En **Google AI Pro** tienes acceso a **Gemini 4 Neon**, **Gemini 4 Hydrogen** y **Orchestrator (Neon)** (sin depender de Carbon, que es exclusivo de Ultra). La regla de eficiencia es: **Neon piensa y audita; Hydrogen escribe el código y ejecuta pruebas**.

Usa `/model` dentro de `agy` para seleccionar el modo correcto en cada etapa:

| Fase del Playbook | Selección en `/model` | Comando Nativo `agy` | Quién hace qué (Eficiencia Pro) |
| :--- | :--- | :--- | :--- |
| **Fase 0 y 1** (Monorepo, Contratos, Dominio) | **`Orchestrator (Neon)`** | `/plan` $\to$ `Ctrl+r` | Neon diseña contratos/invariantes y revisa el plan; **Hydrogen** genera archivos, `pyproject.toml`, entidades y corre `pytest`. |
| **Fase 2 + 3 en Paralelo** (Servidor Central + Cliente Control Plane) | **`Orchestrator (Neon)`** | **`/swarm`** | **Paralelismo real:** una vez congelado `shared/`, un subagente construye `server/` y otro construye `client/` (índice + Bloom + WSS) al mismo tiempo sin pisarse archivos. |
| **Fase 4** (Plano de Datos P2P, Sockets TCP, Colas, NAT, Resume) | **`Orchestrator (Neon)`** *(sube a `Gemini 4 Neon` con Thinking: High solo si hay un bug duro de sockets)* | `/plan` | Neon valida la concurrencia `asyncio`, framing TCP y la Máquina de Estados (Fig. 56); **Hydrogen** implementa y ejecuta los tests E2E reales. |
| **Fase 5** (UI Web Local + Docker Multi-Peer) | **`Orchestrator (Neon)`** | **`/swarm`** | Subagente 1 construye la SPA local (`presentation/`); Subagente 2 construye `Dockerfile`s, `docker-compose.yml` y el test smoke E2E. |
| **Fase 6** (Auditoría SOLID/SDP + Trazabilidad + Demo) | **`Orchestrator (Neon)`** *(o `Gemini 4 Neon` para revisión final)* | **`/teamwork`** | Revisión colaborativa multi-rol (Arquitecto SDP + Auditor DB + Ingeniero QA) con artefactos auditables en `docs/`. |

### Funciones Clave de Antigravity CLI que Vamos a Usar
* **`Orchestrator (Neon)`**: Cambia automáticamente entre Neon (planificación/revisión) y Hydrogen (código/tests) dentro de la misma sesión. Si Hydrogen se bloquea en un test o error de tipado, devuelve el control a Neon automáticamente.
* **`/plan` + `Ctrl+r`**: Antes de modificar código en fases críticas, revisa el plan en la pantalla de *Artifact Review* (`Ctrl+r`) y aprueba solo cuando respete los paquetes UML.
* **`/swarm`**: Distribuye tareas independientes en subagentes paralelos (*fan-out*) y luego consolida e integra (*fan-in*). Funciona impecablemente aquí porque `server/` y `client/` solo dependen de `shared/bluered_contracts/`.
* **`/teamwork`**: Ejecuta auditorías y revisiones estructuradas entre roles especializados sobre el código ya construido.
* **`/compact`**: Ejecútalo tras hacer `git commit` al cierre de cada fase para mantener la ventana de contexto ligera y evitar desperdicio de tokens.

---

## 2. Stack Tecnológico Concreto (100% Python + Docker)

* **Gestor de workspace:** `uv` (Python 3.12+).
* **Servidor Central (`server/`):** `FastAPI`, `uvicorn`, `websockets`, `asyncpg`, `redis` (`redis.asyncio`), `pydantic` v2, `argon2-cffi`, `PyJWT`.
* **Cliente Peer (`client/`):** `asyncio` (sockets TCP crudos), `websockets`, `aiosqlite`, Filtro de Bloom determinista en Python puro, `miniupnpc` (opcional con degradación controlada), `FastAPI` + UI web local ligera (`presentation/static/`).
* **Contratos compartidos (`shared/`):** Modelos `Pydantic v2` (`control_plane.py`), framing binario TCP (`peer_framing.py`) y `bloom.py`.
* **Calidad automatizada:** `pytest`, `pytest-asyncio`, `ruff`, `mypy --strict`, `import-linter`.
* **Infraestructura local:** `Docker Compose` (`postgres:16-alpine`, `redis:7-alpine`, `bluered-server`, `peer-alice`, `peer-bob`, `peer-carol`).

---

## 3. Estructura Objetivo del Monorepo

```text
bluered/
├── AGENTS.md                          # Reglas globales (sección 4.1)
├── pyproject.toml                     # Workspace uv + ruff + mypy + import-linter
├── docker-compose.yml                 # Postgres + Redis + Server + 3 Peers demo
├── docs/
│   ├── arquitectura_bluered.md        # Resumen de Cas_01..Cas_24 y figuras
│   ├── TRACEABILITY_MATRIX.md         # Generado en Fase 6
│   └── DEMO_SCRIPT.md                 # Generado en Fase 6
├── .agents/
│   ├── mcp_config.json                # MCPs: context7, postgres, redis (sección 5)
│   └── skills/
│       ├── context7-docs-first/SKILL.md
│       ├── clean-arch-solid-auditor/SKILL.md
│       ├── p2p-contract-sync/SKILL.md
│       ├── polyglot-db-guardian/SKILL.md
│       └── tdd-evidence-loop/SKILL.md
├── shared/
│   └── bluered_contracts/
│       ├── control_plane.py           # Mensajes JSON WSS (IControlPlane)
│       ├── peer_framing.py            # Protocolo binario TCP (IPeerTransport)
│       └── bloom.py                   # Bloom Filter compartido
├── server/
│   ├── AGENTS.md                      # Reglas del Servidor (sección 4.2)
│   ├── migrations/                    # DDL SQL PostgreSQL (Fig. 55 + comunidades)
│   └── bluered_server/
│       ├── domain/
│       ├── contracts/
│       ├── application/
│       ├── infrastructure/
│       └── main.py                    # Composition Root
├── client/
│   ├── AGENTS.md                      # Reglas del Cliente (sección 4.3)
│   └── bluered_client/
│       ├── domain/
│       ├── contracts/
│       ├── application/
│       ├── infrastructure/
│       ├── presentation/              # UI web local por nodo (FastAPI + SPA)
│       └── main.py                    # Composition Root
├── tests/
│   ├── unit/
│   ├── integration/
│   └── e2e/
└── scripts/
    └── audit_sdp.py                   # Métricas Ca, Ce e Inestabilidad I
```

---

## 4. Archivos `AGENTS.md` Jerárquicos (Reglas que Obligan a Usar Skills y Context7)

### 4.1 Archivo raíz: `/AGENTS.md`

```markdown
# AGENTS.md — Reglas Globales de BlueRed

## Identidad y Modelo Operativo
BlueRed es un sistema P2P híbrido en Python 3.12+ (`asyncio`) bajo **Clean Architecture (Hexagonal)** y **SOLID**.
Cuando trabajes bajo Orchestrator (Neon + Hydrogen) o `/swarm`, el planificador define contratos y el ejecutor (Hydrogen) implementa código y corre verificaciones.

## Protocolo Obligatorio de Skills y MCPs (PROHIBIDO SALTARSE)
1. **Docs-First con Context7:** Antes de escribir código que importe `fastapi`, `pydantic`, `asyncpg`, `redis`, `aiosqlite`, `websockets`, `pytest-asyncio` o `import-linter`, activa el skill `context7-docs-first` y consulta la documentación vigente vía el servidor MCP `context7`. Nunca asumas firmas de memoria.
2. **Activación Explícita de Skills:** Cada tarea DEBE cargar los skills pertinentes de `.agents/skills/` (`context7-docs-first`, `p2p-contract-sync`, `polyglot-db-guardian`, `clean-arch-solid-auditor`, `tdd-evidence-loop`) y declarar al inicio qué skills activó.
3. **Verificación en Vivo con MCPs de DB:** Al trabajar con migraciones, repositorios o auditorías, inspecciona el estado real en Docker mediante los MCPs `postgres` y `redis`.

## Reglas Arquitectónicas Innegociables (DIP & SDP)
- `domain/`: SOLO librería estándar de Python. Cero imports de `application`, `infrastructure`, `presentation` o librerías de terceros.
- `contracts/`: Interfaces (`Protocol` / `ABC`). Solo puede importar desde `domain/`.
- `application/`: Orquestadores de casos de uso (`Cas_01`–`Cas_24`). Solo importa `domain/` y `contracts/`. Inyección de dependencias estricta por constructor.
- `infrastructure/`: Implementaciones concretas (drivers, sockets, repositorios).
- `main.py`: Único *Composition Root* permitido.
- Nombres fieles al diseño UML: `PeerNode`, `LocalPeer`, `RemotePeer`, `SharedFile`, `FileReview`, `TransferTask`, `TransferOrchestrator`, `DiscoveryAndReviewService`, `BloomFilterRepositoryDecorator`, `FairShareQueuePolicy`, `PresenceAndSessionService`, `SearchCoordinatorService`, `FileReputationService`.

## Puerta de Calidad (No negociable antes de cerrar tarea)
Todo cambio debe pasar:
- `uv run ruff check .`
- `uv run mypy --strict shared server client`
- `uv run lint-imports`
- `uv run pytest`
```

### 4.2 Archivo del servidor: `/server/AGENTS.md`

```markdown
# AGENTS.md — Reglas del Servidor Central (Control Plane)

Activa obligatoriamente `polyglot-db-guardian` y `context7-docs-first` al modificar este subproyecto.

## Persistencia Políglota Estricta (Figura 55)
1. **Redis (RAM — Volátil con TTL obligatorio):**
   - `active_sessions:{user_id}`: TTL estricto de 45s renovado por heartbeats cada 15s (`Cas_08`).
   - `peer_bloom_filters:{user_id}`: bytes del BitArray + `hash_functions_k`.
   - `active_search_routes:{search_token}`: TTL estricto de 60s.
   - PROHIBIDO almacenar presencia o rutas de búsqueda en PostgreSQL.
2. **PostgreSQL (Disco — Transaccional ACID):**
   - `users`: contraseñas con Argon2, contador de intentos fallidos y bloqueo (`Cas_07`).
   - `file_catalog`: PK `file_hash CHAR(64)` (SHA-256), `review_count`, `rating_sum`, `avg_rating` desnormalizados para lecturas $O(1)$ sin `JOIN` ni `AVG()` en búsquedas.
   - `file_reviews`: `UNIQUE(file_hash, author_id)` y `CHECK (rating BETWEEN 1 AND 5)`. Permitir `UPSERT` (editar reseña previa según `Cas_14` y `Cas_24`).
   - Tablas de comunidad (`communities`, `chat_rooms`, `debate_posts`, `personal_library`) para `Cas_01`–`Cas_06` y `Cas_13`.
3. **Control Plane Puro:**
   - Jamás guardar archivos binarios de usuarios en el servidor.
   - El ruteo de búsquedas SIEMPRE pasa por `ISearchRoutingStrategy`.
```

### 4.3 Archivo del cliente: `/client/AGENTS.md`

```markdown
# AGENTS.md — Reglas del Cliente Peer (Data Plane + Índice Local)

Activa obligatoriamente `clean-arch-solid-auditor`, `p2p-contract-sync` y `context7-docs-first` al modificar este subproyecto.

## Invariantes del Nodo Simétrico
1. **Simetría P2P:** El nodo ejecuta concurrentemente `TransferOrchestrator` (descargas salientes, `Cas_17`/`Cas_22`) y `PeerUploadListener` (subidas entrantes `Cas_21`, browse `Cas_16`, chat privado `Cas_23`).
2. **Swimlanes (Figura 57):** El nodo local verifica espacio en disco y colisiones de nombre; el Peer Remoto (emisor) arbitra `slots` de subida y encola en `FairShareQueuePolicy` (`Cas_18`).
3. **Máquina de Estados (Figura 56):** `TransferTask` valida transiciones entre `QUEUED`, `HANDSHAKING`, `TRANSFERRING`, `PAUSED`, `RESUMING`, `FAILED`, `COMPLETED`, `CANCELLED`. Cancelar borra archivos `.part`.
4. **Decorator Bloom:** `BloomFilterRepositoryDecorator` envuelve `SqliteFileRepository` implementando `IFileIndexRepository`, filtrando búsquedas negativas en $O(1)$ sin tocar SQLite.
5. **Estrategia NAT (OCP):** `INatTraversalStrategy` encapsula `DirectSocketNatStrategy`, `UpnpNatStrategy`, `TcpHolePunchingStrategy` y `RelayNatStrategy`.
```

---

## 5. Configuración de Servidores MCP (`.agents/mcp_config.json`)

```json
{
  "mcpServers": {
    "context7": {
      "command": "npx",
      "args": ["-y", "@upstash/context7-mcp"]
    },
    "postgres": {
      "command": "npx",
      "args": [
        "-y",
        "@modelcontextprotocol/server-postgres",
        "postgresql://bluered:bluered_dev@localhost:5432/bluered"
      ]
    },
    "redis": {
      "command": "npx",
      "args": [
        "-y",
        "@modelcontextprotocol/server-redis",
        "redis://localhost:6379/0"
      ]
    }
  }
}
```

> **Verificación rápida antes de arrancar:** Abre `agy` en la raíz del proyecto y confirma que detecta tus 5 skills en `.agents/skills/` y los 3 servidores MCP (`context7`, `postgres`, `redis`).