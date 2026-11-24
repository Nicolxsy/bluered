---
name: clean-arch-solid-auditor
description: Audita Clean Architecture, principios SOLID y el Principio de Dependencias Estables (SDP) calculando Ca, Ce e Inestabilidad I en shared/, server/ y client/. Activar al diseñar módulos, cerrar fases o realizar auditorías.
---

# Clean Architecture, SOLID & SDP Auditor

Ejecuta esta auditoría sistemática antes de aprobar cualquier fase:

## 1. Fronteras de Capas (DIP)
1. Ejecuta `uv run lint-imports` y `uv run mypy --strict shared server client`.
2. Confirma que:
   - `domain/` usa únicamente la librería estándar de Python.
   - `application/` solo recibe contratos de `contracts/` en `__init__`.
   - `presentation/` y `infrastructure/` no contienen reglas de negocio (cálculos de `avg_rating`, transiciones de `TransferTask`, políticas de cola ni validaciones de reseñas).

## 2. Patrones GoF y SOLID
- **SRP:** `DiscoveryAndReviewService` y `TransferOrchestrator` separados; en servidor, `PresenceAndSessionService`, `SearchCoordinatorService`, `FileReputationService`, `AuthService` y `CommunityAndChatService` con responsabilidades disjuntas.
- **OCP:** `INatTraversalStrategy`, `ISearchRoutingStrategy` e `ITransferQueuePolicy` extensibles sin `if/elif` sobre clases concretas en los orquestadores.
- **Decorator:** `BloomFilterRepositoryDecorator` implementa `IFileIndexRepository` y envuelve a `SqliteFileRepository`.
- **Observer:** `ITransferEventListener` desacopla `TransferOrchestrator` de la UI web local.
- **LSP:** `LocalPeer` y `RemotePeer` sustituibles como `PeerNode`; `FifoQueuePolicy` y `FairShareQueuePolicy` intercambiables.

## 3. Métrica Matemática SDP
Ejecuta `uv run python scripts/audit_sdp.py` para calcular $C_a$, $C_e$ e inestabilidad:
$$I = \frac{C_e}{C_a + C_e}$$

### Criterios de Aprobación
- `Domain Core` (cliente y servidor) DEBE tener $I = 0.00$.
- Toda dependencia interna $A \to B$ debe respetar $I(B) \le I(A)$.
- Si aparece cualquier violación, corrígela antes de avanzar.
