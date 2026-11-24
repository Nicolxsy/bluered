# Auditoría PDF de diseño vs. código — BlueRed

Fuente de diseño: `docs/ArquitecturaDiseñoBlueRed_DiseñoDinámico_Modular_Despliegue.pdf` (52 páginas, 24 casos de uso, Figuras 1–60).
Evidencia cruda de ejecución: `docs/presentation/evidence/` (ruff, mypy, lint-imports, audit_sdp, pytest, redis, postgres, texto del PDF).
Fecha de medición: 2026-11-24, sobre el árbol de trabajo actual.

## 1. Puerta de calidad medida

| Verificación | Resultado real |
|---|---|
| `ruff check .` | All checks passed |
| `mypy --strict shared server client` | Success: no issues found in 60 source files |
| `lint-imports` | 10 contratos kept, 0 broken |
| `scripts/audit_sdp.py` | 0 violaciones (Domain Core I = 0.00 en cliente y servidor) |
| `pytest -v` | **236 passed**, 0 failed |

Nota de honestidad: la primera corrida de hoy tuvo 1 error de ruff (UP012 en un test) y 3 tests fallando
(2 tests con defectos propios: un `DownloadCheckpoint` sin `updated_at` y una condición de carrera con `sleep(0)` en
`test_ws_coordinator`; 1 smoke Docker porque los contenedores llevaban ~10 h con sesiones expiradas en Redis).
Se corrigieron los tests (no la lógica de producto) y se reiniciaron los peers; la segunda corrida quedó en verde.

## 2. Cumplimiento por bloque de casos de uso

**Hallazgo principal de numeración.** La numeración del PDF NO coincide con la de `docs/TRACEABILITY_MATRIX.md`
en los casos 02–06, 13 y 19. La matriz declara `Cas_19` como "ausente", pero en el PDF `Cas_19` es
*Negociar conexión handshake* (incluido en Cas_17 y Cas_21), y ese comportamiento **sí existe** en código
(`PeerOp.HANDSHAKE_REQ/HANDSHAKE_ACK` en `tcp_peer_transport.py::_open_and_handshake` y
`tcp_upload_server.py`), simplemente rotulado bajo `Cas_21`/`Cas_17`.

| Bloque (PDF) | Casos PDF | Estado en código |
|---|---|---|
| Comunidad (Cas_01–06) | 01 Crear comunidad, 02 Compartir conocimiento, 03 Generar debates, 04 Preservar conocimiento, 05 Tratar temáticas, 06 Descubrir intereses comunes | 01 ✔, 02/03 ✔ (debates y respuestas, `DebatePost`), 04 ✔ (biblioteca personal; la matriz la rotula `Cas_13`), 05 parcial (comunidades y salas, sin categorización por temas explícita), 06 **no implementado** como flujo dedicado (formar un espacio nuevo a partir de coincidencia de intereses) |
| Cliente–Servidor (Cas_07–15) | 07 Sesión, 08 Presencia, 09 Carpetas, 10 Indexar, 11 Buscar, 12 Transmitir búsqueda, 13 Salas, 14 Reseña, 15 Consultar reseñas | 07–12 ✔, 13 ✔ (salas; la matriz las rotula `Cas_05`/`Cas_06`), 14 ✔, 15 ✔ |
| Cliente–Cliente (Cas_16–24) | 16 Explorar, 17 Descargar, 18 Cola, 19 Handshake, 20 NAT, 21 Subida, 22 Reanudar, 23 Chat privado, 24 Reseñar | 16–18 ✔, 19 ✔ (implementado, rotulado bajo 21), 20 ✔, 21–23 ✔, 24 ✔ (UPSERT ACID) |

Conclusión: de los 24 casos del PDF, 22 están implementados de extremo a extremo, 1 parcial (Cas_05) y 1 no implementado (Cas_06).
La matriz de trazabilidad usa una numeración interna distinta y debe alinearse con el PDF (deuda documental).

## 3. Cumplimiento de Figuras 53–60

| Figura | Contenido | Resultado |
|---|---|---|
| 53 Clases cliente | `PeerNode` → `LocalPeer`/`RemotePeer`, `SharedFile`, `FileReview`, `TransferTask`, orquestadores | ✔ nombres fieles (`domain/peers.py`, `domain/files.py`, `domain/transfer.py`) |
| 54 Clases servidor | ISP: `ISessionStore`, `IFileReviewRepository`, `ISearchRoutingStrategy` | ✔ `server/.../contracts/ports.py`; estrategias `FullBroadcast`, `BloomFilter`, `ShardedRoom` |
| 55 Persistencia | Redis (sesiones TTL, Bloom) + PostgreSQL (catálogo y reseñas) | ✔ con matices (ver §4: Redis único, tablas extra) |
| 56 Estados | Semilla, Descargando, Pausado, Falla, Reanudando, Cancelada, Completado | ✔ con adaptación de nombres/estados (ver §4) |
| 57 Swimlanes | Slots y cola en el carril del Peer Remoto; NAT solo como contingencia | ✔ `PeerUploadListener._acquire_upload_slot`; NAT en `CompositeNatTraversalStrategy` |
| 58 Paquetes | Presentation / Application / Domain / Infrastructure | ✔ verificado con `lint-imports` (10 contratos) y `audit_sdp.py` |
| 59 Componentes | `IControlPlane` (WSS) vs `IPeerTransport` (TCP) | ✔ separación real; transporte WS sin TLS (ver §4) |
| 60 Nodos | Plano de control centralizado vs plano de datos TCP directo | ✔ 6 contenedores Docker (postgres, redis, bluered-server, alice, bob, carol) |

## 4. Desviaciones y adaptaciones técnicas (con justificación)

1. **Numeración de casos (PDF vs matriz).** Ver §2. Justificación: la matriz se escribió desde el código; se debe renumerar. Impacto: documental, no funcional.
2. **Cas_06 (descubrir intereses comunes) sin implementar; Cas_05 parcial.** Quedaron fuera del alcance de la entrega; el resto del bloque de comunidad sí funciona.
3. **Estados de transferencia.** El PDF define Semilla/Descargando/Pausado/Falla/Reanudando/Cancelada/Completado. El código (`TransferState`) usa `QUEUED`, `HANDSHAKING`, `TRANSFERRING`, `PAUSED`, `RESUMING`, `FAILED`, `COMPLETED`, `CANCELLED`: se separó explícitamente `HANDSHAKING` y se validan transiciones con la tabla `VALID_TRANSITIONS`.
4. **NAT traversal.** El PDF menciona UPnP/IGD, STUN y hole punching TCP/UDP. En código: `DirectSocketNatStrategy`, `UpnpNatStrategy`, `TcpHolePunchingStrategy` (TCP, señalizado por el servidor), `RelayNatStrategy` y `CompositeNatTraversalStrategy`. En `main.py` la cadena cableada es **Direct → TcpHolePunching → Relay**; `UpnpNatStrategy` existe y está probada, pero no se compone en producción. No hay STUN ni hole punching UDP. En Docker todos los peers comparten una red puente, por lo que casi siempre resuelve la conexión directa. El nombre del contrato es `INatTraversalStrategy` (el PDF dice `INatStrategy`).
5. **Transporte del plano de control.** El PDF indica WSS; el código usa `ws://` (sin TLS) porque corre en red interna Docker. Producción requeriría TLS terminado en un proxy.
6. **Redis.** El PDF dice "Redis Cluster"; se usa una sola instancia `redis:7-alpine`. Claves reales: `active_sessions:<id>` (TTL observado 35–36 s, máximo 45 s) y `peer_bloom_filters:<id>` (tipo hash con `size_bits`, `bitset_payload` de 1024 bytes, `hash_functions_k`, `updated_at`; TTL observado 32–45 s). El Bloom se guarda como hash, no como cadena.
7. **PostgreSQL: dónde viven las restricciones.** `UNIQUE(file_hash, author_id)` (`file_reviews_file_hash_author_id_key`) y `CHECK (rating >= 1 AND rating <= 5)` están en `file_reviews`, no en `file_catalog`. `file_catalog` guarda el promedio desnormalizado (`review_count`, `rating_sum`, `avg_rating`) con sus propios CHECK. Hay tablas adicionales no dibujadas en el PDF: `users`, `communities`, `community_members`, `debate_posts`, `chat_rooms`, `chat_room_members`, `personal_library` (soportan Cas_01–06 y 13).
8. **Complejidad de la búsqueda por hash.** El PDF habla de lectura O(1). La consulta `WHERE file_hash = ANY(...)` usa el índice B-tree de la clave primaria: complejidad O(log n), medida en 0.076 ms con 5 filas y 0.322 ms con 100 000 filas sintéticas (en una transacción con ROLLBACK, sin persistir). El O(1) real está en el filtro de Bloom y en el promedio desnormalizado.
9. **Protocolo P2P.** Trama binaria con prefijo `!II` (`struct.Struct("!II")` en `shared/bluered_contracts/peer_framing.py`) y verificación SHA-256 por `Sha256IntegrityVerifier`.
10. **Persistencia local del cliente.** SQLite en modo WAL (`SqliteFileRepository`), con checkpoints de descarga y cola de mensajes privados offline; el PDF solo habla de "SQLite / Sistema de archivos".
11. **Comentarios heredados con numeración antigua** en `client/.../presentation/schemas.py` y `app.py` (documentado en la matriz).
12. **Esquema simplificado respecto a la Fig. 55.** El PDF dibuja `users` con `reputation_score`, `is_banned`, `last_login`; el código tiene `failed_login_attempts` y `locked_until` (bloqueo por intentos, Cas_07). `file_catalog` del PDF incluye tamaño canónico, MIME, `is_flagged_malware` y `first_seen_at`; el real guarda solo `file_hash` y agregados de reputación. La sesión en Redis (`active_sessions`) guarda `user_id, username, ip_address, tcp_port, last_heartbeat_at, ttl_seconds`, sin `nat_type` ni `gateway_node_id`. Justificación: se implementó solo lo que exigen los 24 casos.
13. **Muestras de la demo.** Los archivos de `docker/samples/` pesan 520–1303 B y se transfieren en un solo frame; para mostrar pausa y cola en vivo hace falta un archivo de ~20 MB, o apoyarse en los tests E2E de 1.5 MB.

## 5. Resumen ejecutivo

- Código verde en toda la puerta de calidad (236 tests, 10/10 contratos de importación, 0 violaciones SDP).
- Cumplimiento funcional: 22/24 casos completos, 1 parcial (Cas_05), 1 no implementado (Cas_06).
- Desviaciones relevantes para defender: numeración de casos, `ws://` vs WSS, Redis único, UPnP presente pero no cableado, sin STUN/UDP, restricciones en `file_reviews`, índice B-tree O(log n).
