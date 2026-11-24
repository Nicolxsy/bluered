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
