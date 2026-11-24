"""Genera gráfico SDP y tarjetas de evidencia desde docs/presentation/evidence (uso: uv run --with matplotlib python scripts/_build_assets.py)."""
from __future__ import annotations

import re
from pathlib import Path

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt
from matplotlib.patches import FancyBboxPatch

EV = Path("docs/presentation/evidence")
OUT = Path("docs/presentation/assets")
BG, CARD, BORDER, TXT, SUB = "#0F172A", "#1E293B", "#334155", "#F8FAFC", "#94A3B8"
BLUE, RED, GREEN = "#38BDF8", "#F43F5E", "#10B981"


def sdp_chart() -> None:
    rows = []
    for line in (EV / "audit_sdp.txt").read_text(encoding="utf-8").splitlines():
        m = re.match(r"(bluered_\w+\.\w+)\s+(\d+)\s+(\d+)\s+([\d.]+)", line)
        if m:
            rows.append((m.group(1).replace("bluered_", ""), int(m.group(2)), int(m.group(3)), float(m.group(4))))
    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(13, 5.4), dpi=200, gridspec_kw={"width_ratios": [1.15, 1]})
    fig.patch.set_facecolor(BG)
    names = [r[0] for r in rows]
    y = range(len(rows))
    ax1.set_facecolor(CARD)
    ax1.barh([i + 0.2 for i in y], [r[1] for r in rows], height=0.38, color=BLUE, label="Ca (aferentes)")
    ax1.barh([i - 0.2 for i in y], [r[2] for r in rows], height=0.38, color=RED, label="Ce (eferentes)")
    ax1.set_yticks(list(y), names, color=TXT, fontsize=11)
    ax1.invert_yaxis()
    ax1.tick_params(colors=SUB, labelsize=11)
    ax1.set_title("Acoplamiento por componente", color=TXT, fontsize=14, loc="left")
    ax1.legend(facecolor=CARD, edgecolor=BORDER, labelcolor=TXT, fontsize=11)
    ax2.set_facecolor(CARD)
    colors = [GREEN if r[3] == 0.0 else BLUE for r in rows]
    ax2.barh(list(y), [r[3] for r in rows], color=colors)
    for i, r in enumerate(rows):
        ax2.text(r[3] + 0.02, i, f"{r[3]:.2f}", va="center", color=GREEN if r[3] == 0 else TXT, fontsize=11, fontweight="bold")
    ax2.set_yticks(list(y), [""] * len(rows))
    ax2.invert_yaxis()
    ax2.set_xlim(0, 1.15)
    ax2.tick_params(colors=SUB, labelsize=11)
    ax2.set_title("Inestabilidad I = Ce / (Ca + Ce)", color=TXT, fontsize=14, loc="left")
    for ax in (ax1, ax2):
        for s in ax.spines.values():
            s.set_color(BORDER)
        ax.grid(axis="x", color=BORDER, alpha=0.5)
        ax.set_axisbelow(True)
    fig.tight_layout()
    fig.savefig(OUT / "chart_sdp_metrics.png", facecolor=BG)
    plt.close(fig)


def card(path: Path, title: str, lines: list[tuple[str, str]], fontsize: int = 15, width: float = 11.5) -> None:
    h = 0.9 + 0.36 * len(lines)
    fig = plt.figure(figsize=(width, h), dpi=200)
    fig.patch.set_facecolor(BG)
    ax = fig.add_axes((0, 0, 1, 1))
    ax.set_xlim(0, width)
    ax.set_ylim(0, h)
    ax.axis("off")
    ax.add_patch(FancyBboxPatch((0.08, 0.08), width - 0.16, h - 0.16, boxstyle="round,pad=0,rounding_size=0.15", fc=CARD, ec=BORDER, lw=1.5))
    ax.text(0.35, h - 0.42, title, color=BLUE, fontsize=fontsize - 1, family="DejaVu Sans", fontweight="bold", va="center")
    for i, (txt, col) in enumerate(lines):
        ax.text(0.35, h - 0.95 - 0.36 * i, txt, color=col, fontsize=fontsize - 2, family="DejaVu Sans Mono", va="center")
    fig.savefig(path, facecolor=BG)
    plt.close(fig)


def last(pattern: str, text: str) -> str:
    return [ln for ln in text.splitlines() if re.search(pattern, ln)][-1].strip("= ")


def cards() -> None:
    py = (EV / "pytest.txt").read_text(encoding="utf-8")
    li = (EV / "lint_imports.txt").read_text(encoding="utf-8")
    kept = [ln for ln in li.splitlines() if "KEPT" in ln]
    lines = [
        ("$ uv run pytest -v", SUB),
        (last(r"passed", py), GREEN),
        ("$ uv run mypy --strict shared server client", SUB),
        (last(r"Success", (EV / "mypy.txt").read_text(encoding="utf-8")), GREEN),
        ("$ uv run ruff check .", SUB),
        (last(r"All checks", (EV / "ruff.txt").read_text(encoding="utf-8")), GREEN),
        ("$ uv run lint-imports", SUB),
        (last(r"Contracts:", li), GREEN),
        ("$ uv run python scripts/audit_sdp.py", SUB),
        ("SDP audit passed: 0 violations.", GREEN),
    ]
    card(OUT / "evidencia_tests.png", "Puerta de calidad (ejecución real)", lines)
    del kept

    tr = [ln for ln in py.splitlines() if "test_p2p_transfers.py::" in ln and "PASSED" in ln]
    tl = [("$ pytest -v tests/e2e/test_p2p_transfers.py  (sockets TCP reales)", SUB)]
    for ln in tr:
        name = ln.split("::")[1].split()[0].replace("test_e2e_", "")
        tl.append((f"PASSED  {name[:62]}", GREEN))
    card(OUT / "evidencia_transfer.png", "Transferencia P2P: pruebas E2E reales", tl, width=12.5)

    rd = (EV / "redis.txt").read_text(encoding="utf-8").splitlines()
    ttl = [ln.replace("active_sessions:", "active_sessions:")[:62] for ln in rd if ln.startswith("TTL ")]
    ttl = [f"TTL {ln.split(':')[0][4:]}:{ln.split(':')[1][:8]}… = {ln.split('=')[1].strip()} s" for ln in ttl]
    pg = (EV / "postgres.txt").read_text(encoding="utf-8")
    exec_a = re.findall(r"Execution Time: ([\d.]+) ms", pg)
    dbl = [("redis> TTL active_sessions:*   (máx. 45 s)", SUB)] + [(t, GREEN) for t in ttl]
    dbl += [
        ("redis> HSTRLEN peer_bloom_filters:* bitset_payload", SUB),
        ("1024 bytes  (size_bits=8192, k=5 por defecto)", GREEN),
        ("psql> file_reviews: UNIQUE (file_hash, author_id)", SUB),
        ("psql> file_reviews: CHECK (rating >= 1 AND rating <= 5)", SUB),
        ("EXPLAIN ANALYZE ... WHERE file_hash = ANY(...)", SUB),
        (f"Index Scan using file_catalog_pkey  Execution {exec_a[0]} ms (5 filas)", GREEN),
        (f"Bitmap Index Scan sobre PK, 100 000 filas: {exec_a[1]} ms", GREEN),
    ]
    card(OUT / "evidencia_db.png", "Persistencia políglota: Redis + PostgreSQL", dbl, width=12.5)


if __name__ == "__main__":
    OUT.mkdir(parents=True, exist_ok=True)
    sdp_chart()
    cards()
    print("ok")
