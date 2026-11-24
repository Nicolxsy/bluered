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
