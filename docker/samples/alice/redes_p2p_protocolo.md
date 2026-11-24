# Protocolo Binario P2P de BlueRed

## Framing Determinista sobre TCP

Cada trama del plano de datos sigue el formato:

1. `header_len` (`uint32` big-endian, 4 bytes)
2. `payload_len` (`uint32` big-endian, 4 bytes)
3. `header_json` (`header_len` bytes UTF-8)
4. `payload` (`payload_len` bytes binarios crudos)

## Integridad y Reanudacion

- Cada bloque (`FILE_CHUNK`) incluye `chunk_index`, `offset` y `chunk_sha256`.
- Al finalizar la transferencia se valida el SHA-256 global del archivo.
- Ante cortes de socket TCP (`Cas_22`), el cliente reanuda desde el ultimo
  checkpoint confirmado en SQLite WAL.
