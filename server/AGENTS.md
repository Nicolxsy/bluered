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
