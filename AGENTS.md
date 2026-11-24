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
