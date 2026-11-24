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
