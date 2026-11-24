"""Unit tests for scripts/audit_sdp.py."""

from __future__ import annotations

from pathlib import Path

from scripts.audit_sdp import audit_workspace


def test_current_workspace_passes_sdp_audit() -> None:
    result = audit_workspace(Path.cwd())
    assert result.ok, f"Unexpected SDP violations: {result.violations}"
    assert result.metrics["bluered_client.domain"].instability == 0.0
    assert result.metrics["bluered_server.domain"].instability == 0.0


def test_audit_detects_domain_importing_outer_layer(tmp_path: Path) -> None:
    (tmp_path / "client" / "bluered_client" / "domain").mkdir(parents=True)
    (tmp_path / "client" / "bluered_client" / "infrastructure").mkdir(parents=True)
    (tmp_path / "client" / "bluered_client" / "domain" / "entity.py").write_text(
        "from bluered_client.infrastructure import repo\n",
        encoding="utf-8",
    )
    (tmp_path / "client" / "bluered_client" / "infrastructure" / "repo.py").write_text(
        "x = 1\n",
        encoding="utf-8",
    )

    result = audit_workspace(tmp_path)
    assert not result.ok
    assert any("Domain Core" in v for v in result.violations)


def test_audit_detects_sdp_instability_inversion(tmp_path: Path) -> None:
    base = tmp_path / "server" / "bluered_server"
    for layer in ("domain", "contracts", "application", "infrastructure"):
        (base / layer).mkdir(parents=True)

    (base / "domain" / "model.py").write_text("x = 1\n", encoding="utf-8")
    (base / "contracts" / "ports.py").write_text(
        "from bluered_server.domain import model\n",
        encoding="utf-8",
    )
    # Infrastructure depends on contracts and application -> I(infra) = 1.0
    (base / "infrastructure" / "db.py").write_text(
        "from bluered_server.contracts import ports\n"
        "from bluered_server.application import svc\n",
        encoding="utf-8",
    )
    # Application depends on infrastructure (Ca=1 from infra, Ce=1 to infra -> I=0.5 < I(infra)=1.0)
    (base / "application" / "svc.py").write_text(
        "from bluered_server.infrastructure import db\n",
        encoding="utf-8",
    )

    result = audit_workspace(tmp_path)
    assert not result.ok
    assert any("SDP violation" in v for v in result.violations)
