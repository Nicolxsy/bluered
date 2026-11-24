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
