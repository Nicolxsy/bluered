# AGENTS.md — Reglas del Cliente Peer (Data Plane + Índice Local)

Activa obligatoriamente `clean-arch-solid-auditor`, `p2p-contract-sync` y `context7-docs-first` al modificar este subproyecto.

## Invariantes del Nodo Simétrico
1. **Simetría P2P:** El nodo ejecuta concurrentemente `TransferOrchestrator` (descargas salientes, `Cas_17`/`Cas_22`) y `PeerUploadListener` (subidas entrantes `Cas_21`, browse `Cas_16`, chat privado `Cas_23`).
2. **Swimlanes (Figura 57):** El nodo local verifica espacio en disco y colisiones de nombre; el Peer Remoto (emisor) arbitra `slots` de subida y encola en `FairShareQueuePolicy` (`Cas_18`).
3. **Máquina de Estados (Figura 56):** `TransferTask` valida transiciones entre `QUEUED`, `HANDSHAKING`, `TRANSFERRING`, `PAUSED`, `RESUMING`, `FAILED`, `COMPLETED`, `CANCELLED`. Cancelar borra archivos `.part`.
4. **Decorator Bloom:** `BloomFilterRepositoryDecorator` envuelve `SqliteFileRepository` implementando `IFileIndexRepository`, filtrando búsquedas negativas en $O(1)$ sin tocar SQLite.
5. **Estrategia NAT (OCP):** `INatTraversalStrategy` encapsula `DirectSocketNatStrategy`, `UpnpNatStrategy`, `TcpHolePunchingStrategy` y `RelayNatStrategy`.
