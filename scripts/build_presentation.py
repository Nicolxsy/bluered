"""Genera docs/presentation/BlueRed_Sustentacion.pptx (16:9, compatible con Google Slides).

Uso: uv run --with python-pptx --with pillow python scripts/build_presentation.py
Todas las cifras provienen de docs/presentation/evidence/ (ejecución real sobre el repositorio).
"""
from __future__ import annotations

import re
from pathlib import Path

from PIL import Image
from pptx import Presentation
from pptx.dml.color import RGBColor
from pptx.enum.shapes import MSO_SHAPE
from pptx.enum.text import MSO_ANCHOR, PP_ALIGN
from pptx.util import Emu, Inches, Pt

ROOT = Path(__file__).resolve().parent.parent
EV = ROOT / "docs/presentation/evidence"
AS = ROOT / "docs/presentation/assets"
OUT = ROOT / "docs/presentation/BlueRed_Sustentacion.pptx"

BG, CARD, BORDER = "0F172A", "1E293B", "334155"
TXT, SUB = "F8FAFC", "94A3B8"
BLUE, BLUE2, RED, RED2, GREEN = "38BDF8", "2563EB", "F43F5E", "E11D48", "10B981"
SANS, MONO = "Arial", "Consolas"
W, H = 13.333, 7.5
TOTAL_S = 555  # 9:15

JOEL, NICO, SERGIO = "Joel Pérez", "Nicolás Sierra", "Sergio Osorio"


def rgb(h: str) -> RGBColor:
    return RGBColor.from_string(h)


# ---------------------------------------------------------------- métricas reales
def _last(pat: str, file: str) -> str:
    lines = [ln for ln in (EV / file).read_text(encoding="utf-8").splitlines() if re.search(pat, ln)]
    return lines[-1].strip("= ")


PY_LINE = _last(r"\d+ passed", "pytest.txt")
PY_SHORT = PY_LINE.split(" in ")[0]
N_TESTS = int(re.search(r"(\d+) passed", PY_LINE).group(1))  # type: ignore[union-attr]
MYPY_N = int(re.search(r"(\d+) source files", _last(r"Success", "mypy.txt")).group(1))  # type: ignore[union-attr]
KEPT = int(re.search(r"(\d+) kept", _last(r"Contracts:", "lint_imports.txt")).group(1))  # type: ignore[union-attr]
assert "0 violations" in (EV / "audit_sdp.txt").read_text(encoding="utf-8")
assert "All checks passed" in (EV / "ruff.txt").read_text(encoding="utf-8")
_pg = (EV / "postgres.txt").read_text(encoding="utf-8")
EXEC_5, EXEC_100K = re.findall(r"Execution Time: ([\d.]+) ms", _pg)
_rd = (EV / "redis.txt").read_text(encoding="utf-8")
TTLS = sorted(int(x) for x in re.findall(r"TTL active_sessions:\w+ = (\d+)", _rd))
TTL_RANGE = f"{TTLS[0]}–{TTLS[-1]} s"
SDP = {}
for ln in (EV / "audit_sdp.txt").read_text(encoding="utf-8").splitlines():
    m = re.match(r"(bluered_\w+\.\w+)\s+(\d+)\s+(\d+)\s+([\d.]+)", ln)
    if m:
        SDP[m.group(1)] = (int(m.group(2)), int(m.group(3)), float(m.group(4)))
CA_DOM_C, _, I_DOM_C = SDP["bluered_client.domain"]
CA_DOM_S, _, I_DOM_S = SDP["bluered_server.domain"]

# ---------------------------------------------------------------- helpers de dibujo
prs = Presentation()
prs.slide_width = Emu(12192000)  # 13.333 in exactos (16:9)
prs.slide_height = Emu(6858000)
BLANK = prs.slide_layouts[6]
OVERFLOW_WARN: list[str] = []
SLIDE_TEXT: list[list[str]] = []


def est_lines(text: str, w_in: float, size: float, mono: bool) -> int:
    cw = size * (0.60 if mono else 0.52) / 72  # ancho medio de carácter en pulgadas
    per = max(1, int((w_in - 0.2) / cw))
    return sum(max(1, -(-len(p) // per)) for p in text.split("\n"))


def rect(slide, x, y, w, h, fill=CARD, line=BORDER, shape=MSO_SHAPE.ROUNDED_RECTANGLE, lw=1.0):
    s = slide.shapes.add_shape(shape, Inches(x), Inches(y), Inches(w), Inches(h))
    if shape == MSO_SHAPE.ROUNDED_RECTANGLE:
        s.adjustments[0] = 0.06
    if fill:
        s.fill.solid()
        s.fill.fore_color.rgb = rgb(fill)
    else:
        s.fill.background()
    if line:
        s.line.color.rgb = rgb(line)
        s.line.width = Pt(lw)
    else:
        s.line.fill.background()
    s.shadow.inherit = False
    return s


def text(slide, x, y, w, h, paras, size=14, color=TXT, bold=False, mono=False,
         align=PP_ALIGN.LEFT, anchor=MSO_ANCHOR.TOP, spacing=1.0, track=True):
    """paras: str | list[str | (str, dict)] ; dict admite size,color,bold,mono."""
    tb = slide.shapes.add_textbox(Inches(x), Inches(y), Inches(w), Inches(h))
    tf = tb.text_frame
    tf.word_wrap = True
    tf.vertical_anchor = anchor
    tf.margin_left = tf.margin_right = Inches(0.08)
    tf.margin_top = tf.margin_bottom = Inches(0.04)
    items = [paras] if isinstance(paras, str) else paras
    total_h = 0.0
    plain = []
    for i, it in enumerate(items):
        t, o = (it, {}) if isinstance(it, str) else it
        sz, mo = o.get("size", size), o.get("mono", mono)
        p = tf.paragraphs[0] if i == 0 else tf.add_paragraph()
        p.alignment = align
        p.line_spacing = spacing
        r = p.add_run()
        r.text = t
        f = r.font
        f.name = MONO if mo else SANS
        f.size = Pt(sz)
        f.bold = o.get("bold", bold)
        f.color.rgb = rgb(o.get("color", color))
        total_h += est_lines(t, w, sz, mo) * sz * 1.22 * spacing / 72
        plain.append(t)
    if track:
        SLIDE_TEXT[-1].extend(plain)
        if total_h > h + 0.05:
            OVERFLOW_WARN.append(f"slide {len(SLIDE_TEXT)}: '{plain[0][:40]}' ~{total_h:.2f}in > {h:.2f}in")
    return tb


def pic(slide, path: Path, x, y, w, h, align="center"):
    """Inserta imagen conservando relación de aspecto dentro de la caja (x,y,w,h)."""
    iw, ih = Image.open(path).size
    sc = min(w / iw, h / ih)
    pw, ph = iw * sc, ih * sc
    px = x + (w - pw) / 2 if align == "center" else x
    py = y + (h - ph) / 2
    p = slide.shapes.add_picture(str(path), Inches(px), Inches(py), Inches(pw), Inches(ph))
    return p, (px, py, pw, ph)


def new_slide(n, title, speaker, window, end_s, subtitle=None):
    s = prs.slides.add_slide(BLANK)
    SLIDE_TEXT.append([])
    s.background.fill.solid()
    s.background.fill.fore_color.rgb = rgb(BG)
    rect(s, 0, 0, 0.12, H, fill=BLUE2, line=None, shape=MSO_SHAPE.RECTANGLE)
    text(s, 0.45, 0.28, 9.3, 0.7, title, size=26, bold=True, anchor=MSO_ANCHOR.MIDDLE)
    if subtitle:
        text(s, 0.45, 0.92, 9.6, 0.4, subtitle, size=14, color=SUB)
    chip = rect(s, 9.95, 0.34, 2.95, 0.62, fill=CARD, line=BLUE)
    del chip
    text(s, 9.98, 0.34, 2.9, 0.62, [(speaker, {"bold": True, "size": 14, "color": BLUE}),
                                    (window, {"size": 12, "color": SUB})],
         align=PP_ALIGN.CENTER, anchor=MSO_ANCHOR.MIDDLE)
    # barra de progreso + número
    rect(s, 0.45, 7.18, W - 0.9, 0.06, fill=BORDER, line=None, shape=MSO_SHAPE.RECTANGLE)
    rect(s, 0.45, 7.18, max(0.05, (W - 0.9) * end_s / TOTAL_S), 0.06, fill=BLUE, line=None,
         shape=MSO_SHAPE.RECTANGLE)
    text(s, W - 1.6, 7.2, 1.15, 0.26, f"{n} / 11", size=12, color=SUB, align=PP_ALIGN.RIGHT, track=False)
    text(s, 0.45, 7.2, 6, 0.26, "BlueRed · Universidad Distrital F.J.C.", size=12, color=SUB, track=False)
    return s


def card(s, x, y, w, h, title=None, accent=BLUE):
    rect(s, x, y, w, h)
    rect(s, x, y + 0.12, 0.06, h - 0.24, fill=accent, line=None, shape=MSO_SHAPE.RECTANGLE)
    if title:
        text(s, x + 0.15, y + 0.06, w - 0.3, 0.4, title, size=14, bold=True, color=accent)


def notes(slide, speaker, window, secs, body, handoff):
    tf = slide.notes_slide.notes_text_frame
    tf.text = (f"ORADOR: {speaker}  |  TIEMPO: {window} ({secs} s)\n\n{body}\n\n"
               f"TRANSICIÓN: {handoff}")


def code_lines(s, x, y, w, h, lines, size=12):
    text(s, x, y, w, h, [(ln, {"mono": True, "size": size}) for ln in lines], size=size, mono=True)


# ================================================================ SLIDE 1
s = new_slide(1, "BlueRed", JOEL, "0:00 – 0:25", 25,
              "Sistema P2P híbrido de intercambio de conocimiento · Fundamentos de Ingeniería de Software")
text(s, 0.45, 1.45, 8.5, 0.5, "Universidad Distrital Francisco José de Caldas", size=20, bold=True, color=BLUE)
text(s, 0.45, 1.95, 8.5, 0.4, "Plano de control centralizado + plano de datos P2P directo", size=16, color=SUB)
for i, (nm, code) in enumerate(((JOEL, "20242020017"), (NICO, "20222020197"), (SERGIO, "20241020073"))):
    y = 2.7 + i * 0.95
    card(s, 0.45, y, 5.2, 0.8, accent=BLUE if i != 1 else RED)
    text(s, 0.7, y + 0.04, 4.9, 0.4, nm, size=18, bold=True)
    text(s, 0.7, y + 0.42, 4.9, 0.35, f"Código {code}", size=14, color=SUB)
kpis = [(str(N_TESTS), "pruebas automáticas pasando", GREEN),
        (f"{KEPT}/{KEPT}", "contratos de importación cumplidos", GREEN),
        (f"{I_DOM_C:.2f}", "inestabilidad del Domain Core", GREEN),
        ("6", "contenedores Docker saludables", BLUE)]
for i, (num, lab, col) in enumerate(kpis):
    x = 6.2 + (i % 2) * 3.4
    y = 2.7 + (i // 2) * 1.75
    card(s, x, y, 3.2, 1.55, accent=col)
    text(s, x + 0.2, y + 0.12, 2.9, 0.75, num, size=36, bold=True, color=col)
    text(s, x + 0.2, y + 0.88, 2.9, 0.6, lab, size=14, color=TXT)
text(s, 0.45, 6.2, 12, 0.5, "24 casos de uso · Clean Architecture · SOLID · persistencia políglota Redis + PostgreSQL + SQLite",
     size=14, color=SUB)
notes(s, JOEL, "0:00–0:25", 25,
      (f"Buenas tardes. Somos Joel Pérez, Nicolás Sierra y Sergio Osorio. BlueRed es un sistema P2P híbrido: un servidor central "
      f"coordina presencia, búsqueda y reputación, y los archivos viajan directo entre pares. Medido hoy: {N_TESTS} pruebas pasando, "
      f"{KEPT} de {KEPT} contratos cumplidos y dominio con inestabilidad cero."),
      "Joel continúa con el proceso de ingeniería en la siguiente diapositiva.")

# ================================================================ SLIDE 2
s = new_slide(2, "Proceso de ingeniería: del diseño UML al código", JOEL, "0:25 – 1:20", 80,
              "Cinco fases, verificables en el historial de git y en la puerta de calidad")
phases = [
    ("1", "Especificación UML", "24 casos de uso y 60 figuras en el PDF de diseño (52 páginas)", BLUE),
    ("2", "Contratos congelados", "shared/: control_plane, peer_framing y bloom", BLUE),
    ("3", "Dominio puro + DIP", f"domain/ solo stdlib; Ca={CA_DOM_C} / {CA_DOM_S}, I={I_DOM_C:.2f}", GREEN),
    ("4", "Subsistemas desacoplados", f"server, client y shared; {KEPT} contratos import-linter", BLUE),
    ("5", "Verificación", f"{N_TESTS} pruebas, E2E con sockets TCP reales y Docker de 6 servicios", RED),
]
bx = 0.45
bw = 2.38
text(s, 0.45, 1.55, 12.4, 0.3, "", size=14, track=False)
rect(s, 0.9, 2.62, 11.6, 0.05, fill=BORDER, line=None, shape=MSO_SHAPE.RECTANGLE)
for i, (num, ttl, desc, col) in enumerate(phases):
    x = bx + i * (bw + 0.12)
    circ = rect(s, x + bw / 2 - 0.3, 2.35, 0.6, 0.6, fill=col, line=None, shape=MSO_SHAPE.OVAL)
    del circ
    text(s, x + bw / 2 - 0.3, 2.35, 0.6, 0.6, num, size=20, bold=True, color=BG,
         align=PP_ALIGN.CENTER, anchor=MSO_ANCHOR.MIDDLE, track=False)
    card(s, x, 3.2, bw, 2.45, accent=col)
    text(s, x + 0.12, 3.3, bw - 0.2, 0.7, ttl, size=16, bold=True)
    text(s, x + 0.12, 4.05, bw - 0.2, 1.55, desc, size=14, color=SUB)
text(s, 0.45, 6.05, 12.4, 0.8,
     "Historial git: fase 0 (monorepo y contratos) → fase 1 (dominio puro) → fases 2–3 (servidor y cliente) → "
     "fase 4 (plano de datos P2P) → fase 5 (UI web y Docker multi-peer)", size=14, color=SUB)
notes(s, JOEL, "0:25–1:20", 55,
      "Trabajamos en cinco fases. Primero, especificamos el diseño en UML: veinticuatro casos de uso y sesenta figuras "
      "en un documento de cincuenta y dos páginas. Segundo, congelamos los contratos compartidos: los mensajes del "
      "plano de control, la trama binaria P2P y el filtro de Bloom, de modo que cliente y servidor pudieran avanzar "
      "en paralelo. Tercero, construimos un dominio puro, solo con biblioteca estándar, y aplicamos inversión de "
      f"dependencias. Cuarto, desarrollamos servidor y cliente como subsistemas desacoplados, protegidos por {KEPT} "
      f"contratos de import-linter. Y quinto, verificamos: {N_TESTS} pruebas, extremo a extremo con sockets TCP reales y "
      "un entorno Docker de seis servicios. El historial de git refleja exactamente estas fases.",
      "Ahora comparo el diseño del PDF contra el código real: arquitectura, SOLID y patrones.")

# ================================================================ SLIDE 3
s = new_slide(3, "PDF vs. código: Clean Architecture, SOLID y GoF", JOEL, "1:20 – 2:30", 150,
              "Izquierda: Figs. 53 y 54 del diseño · Derecha: estructura real del repositorio")
rect(s, 0.45, 1.4, 6.3, 5.55)
text(s, 0.55, 1.42, 6.1, 0.3, "PDF Fig. 53: clases del cliente", size=12, color=SUB)
pic(s, AS / "pdf_fig53_cliente.png", 0.6, 1.72, 6.0, 1.5)
text(s, 0.55, 3.25, 6.1, 0.3, "PDF Fig. 54: clases del servidor (ISP)", size=12, color=SUB)
pic(s, AS / "pdf_fig54_servidor.png", 0.6, 3.55, 6.0, 1.15)
text(s, 0.55, 4.8, 6.15, 2.1, [
    ("✔ Nombres fieles al UML: PeerNode, LocalPeer, RemotePeer, SharedFile, FileReview, TransferTask", {"color": GREEN}),
    ("✔ ISP en servidor: ISessionStore separado de IFileReviewRepository", {"color": GREEN}),
    ("Δ El PDF nombra PresenceSessionManager; el código lo implementa como PresenceAndSessionService", {"color": BLUE}),
], size=14, spacing=1.05)
card(s, 6.95, 1.4, 5.95, 2.75, "Árbol real de paquetes", BLUE)
code_lines(s, 7.1, 1.85, 5.7, 2.3, [
    "client/bluered_client/",
    "  domain/ contracts/ application/",
    "  infrastructure/ presentation/ main.py",
    "server/bluered_server/  (misma estructura)",
    "shared/bluered_contracts/",
    f"domain I={I_DOM_C:.2f} · main.py = Composition Root",
], size=12)
card(s, 6.95, 4.3, 5.95, 2.65, "Patrones reales", RED)
rows = [("Strategy", "CompositeNatTraversalStrategy · BloomFilterRoutingStrategy"),
        ("Decorator", "BloomFilterRepositoryDecorator"),
        ("Observer", "WebSocketTransferEventListener (UI)"),
        ("LSP", "LocalPeer/RemotePeer; FifoQueuePolicy/FairShareQueuePolicy")]
for i, (k, v) in enumerate(rows):
    y = 4.78 + i * 0.52
    text(s, 7.1, y, 1.3, 0.5, k, size=14, bold=True, color=RED)
    text(s, 8.35, y, 4.5, 0.5, v, size=12)
notes(s, JOEL, "1:20–2:30", 70,
      "A la izquierda están las Figuras 53 y 54 del diseño; a la derecha, lo que existe en el repositorio. Los nombres "
      "del UML se conservaron: PeerNode con sus especializaciones LocalPeer y RemotePeer, SharedFile, FileReview y "
      "TransferTask. En el servidor aplicamos segregación de interfaces: el almacén de sesiones efímeras es una interfaz "
      "distinta del repositorio transaccional de reseñas. Una adaptación honesta: el PDF llama PresenceSessionManager al "
      "gestor de presencia; en código se llama PresenceAndSessionService. Los patrones son reales y verificables: "
      "Strategy en el recorrido de NAT y en el ruteo de búsquedas; Decorator, que añade el filtro de Bloom delante del "
      "repositorio SQLite sin modificarlo; Observer, para empujar el estado de las transferencias a la interfaz; y "
      "sustitución de Liskov, porque las dos políticas de cola y los dos tipos de peer son intercambiables. "
      "Main es el único punto de composición.",
      "Nicolás explica cómo se resolvió la persistencia políglota.")

# ================================================================ SLIDE 4
s = new_slide(4, "PDF vs. código: persistencia políglota", NICO, "2:30 – 3:35", 215,
              "Izquierda: Figura 55 · Derecha: evidencia medida en los contenedores")
rect(s, 0.45, 1.4, 6.0, 5.55)
text(s, 0.55, 1.42, 5.8, 0.3, "PDF Fig. 55: esquema dual Redis + PostgreSQL", size=12, color=SUB)
pic(s, AS / "pdf_fig55_persistencia.png", 0.55, 1.8, 5.8, 2.9)
text(s, 0.55, 4.85, 5.85, 2.1, [
    ("✔ Sesiones efímeras en Redis; reseñas y catálogo en PostgreSQL", {"color": GREEN}),
    ("Δ El PDF dice «Redis Cluster»: se usa una instancia redis:7", {"color": BLUE}),
    ("Δ Esquema simplificado: sin mime/malware en file_catalog ni nat_type en la sesión; tablas extra de comunidades", {"color": BLUE}),
], size=14, spacing=1.05)
card(s, 6.65, 1.4, 6.25, 1.75, "Redis (RAM, TTL)", RED)
code_lines(s, 6.8, 1.85, 6.0, 1.3, [
    f"active_sessions:<id>      TTL {TTL_RANGE} (máx 45 s)",
    "peer_bloom_filters:<id>   hash: bitset 1024 B",
    "                          size_bits=8192, k=5",
], size=12)
card(s, 6.65, 3.3, 6.25, 2.5, "PostgreSQL (ACID)", BLUE)
code_lines(s, 6.8, 3.75, 6.0, 2.05, [
    "file_reviews UNIQUE(file_hash, author_id)",
    "file_reviews CHECK(rating >= 1 AND <= 5)",
    f"EXPLAIN ANALYZE: Index Scan pkey  {EXEC_5} ms",
    f"100 000 filas (ROLLBACK): Bitmap Index {EXEC_100K} ms",
    "B-tree O(log n); O(1) = Bloom + avg_rating",
], size=12)
card(s, 6.65, 5.95, 6.25, 1.0, "SQLite WAL (cliente)", GREEN)
text(s, 6.8, 6.38, 6.0, 0.55, "Índice, checkpoints de descarga y cola de mensajes offline", size=14)
notes(s, NICO, "2:30–3:35", 65,
      (f"Soy Nicolás Sierra. La Figura 55 propone persistencia políglota. En Redis va lo volátil: cada sesión tiene TTL; hoy medimos "
      f"entre {TTLS[0]} y {TTLS[-1]} segundos, con tope de 45, y el filtro de Bloom de cada peer: 8192 bits. En PostgreSQL va lo "
      f"transaccional: la restricción UNIQUE de archivo y autor y el CHECK de uno a cinco están en file_reviews. El EXPLAIN ANALYZE usa "
      f"índice: {EXEC_5} milisegundos con cinco filas y {EXEC_100K} con cien mil sintéticas, en una transacción revertida. Es un B-tree, "
      f"O(log n); el O(1) está en el Bloom y el promedio desnormalizado. Adaptaciones: un solo Redis en vez de clúster y un esquema "
      f"más simple que la figura. En el cliente, SQLite en modo WAL."),
      "Sigo con la máquina de estados y los carriles de la descarga P2P.")

# ================================================================ SLIDE 5
s = new_slide(5, "PDF vs. código: estados y swimlanes P2P", NICO, "3:35 – 4:30", 270,
              "Izquierda: Figuras 56 y 57 · Derecha: TransferTask y reparto de responsabilidades")
rect(s, 0.45, 1.4, 6.3, 5.55)
text(s, 0.55, 1.42, 2.7, 0.3, "PDF Fig. 56", size=12, color=SUB)
pic(s, AS / "pdf_fig56_estados.png", 0.55, 1.75, 2.55, 5.1)
text(s, 3.3, 1.42, 3.4, 0.3, "PDF Fig. 57", size=12, color=SUB)
pic(s, AS / "pdf_fig57_actividades.png", 3.25, 1.9, 3.45, 3.3)
text(s, 3.25, 5.3, 3.5, 1.6, "Δ Estados: el código añade HANDSHAKING y renombra «Semilla» como QUEUED", size=14, color=BLUE)
card(s, 6.95, 1.4, 5.95, 3.1, "TransferTask · transiciones reales", RED)
code_lines(s, 7.1, 1.85, 5.7, 2.65, [
    "QUEUED       → HANDSHAKING | PAUSED",
    "HANDSHAKING  → TRANSFERRING | QUEUED",
    "TRANSFERRING → PAUSED | COMPLETED",
    "PAUSED       → RESUMING | QUEUED",
    "RESUMING     → TRANSFERRING | PAUSED",
    "FAILED       → RESUMING | QUEUED",
    "Todos → CANCELLED (salvo finales)",
    "FAILED desde estados activos; finales:",
    "COMPLETED y CANCELLED",
], size=12)
card(s, 6.95, 4.65, 5.95, 2.3, "Local vs. Peer remoto", BLUE)
text(s, 7.1, 5.08, 5.7, 1.85, [
    "Local: TransferOrchestrator verifica disco, SHA-256 y guarda checkpoints",
    "Remoto: PeerUploadListener arbitra slots; la cola es FairShareQueuePolicy",
    "NAT solo como contingencia si el TCP directo falla",
], size=14, spacing=1.05)
notes(s, NICO, "3:35–4:30", 55,
      ("La Figura 56 define el ciclo de vida de la descarga; TransferTask lo implementa con una tabla de transiciones válidas y "
      "rechaza cualquier otra. Adaptación: añadimos el estado HANDSHAKING, y la Semilla en cola del PDF es QUEUED. Pausado y Falla "
      "convergen en Reanudando, como en el diseño, y se reanuda desde el offset guardado en SQLite. La Figura 57 asigna slots y cola "
      "al peer remoto, porque el cliente local no puede arbitrar recursos ajenos: PeerUploadListener con FairShareQueuePolicy reparte "
      "por peer y evita inanición. El cliente verifica disco y SHA-256."),
      "Termino con componentes, nodos y el protocolo.")

# ================================================================ SLIDE 6
s = new_slide(6, "PDF vs. código: componentes, nodos y protocolo", NICO, "4:30 – 5:15", 315,
              "Izquierda: Figuras 59 y 60 · Derecha: dos planos y topología Docker real")
rect(s, 0.45, 1.4, 6.0, 5.55)
text(s, 0.55, 1.42, 5.8, 0.3, "PDF Fig. 59: componentes", size=12, color=SUB)
pic(s, AS / "pdf_fig59_componentes.png", 0.55, 1.72, 5.8, 2.7)
text(s, 0.55, 4.45, 5.8, 0.3, "PDF Fig. 60: nodos", size=12, color=SUB)
pic(s, AS / "pdf_fig60_nodos.png", 0.55, 4.75, 5.8, 2.15)
card(s, 6.65, 1.4, 6.25, 1.5, "IControlPlane · plano de control", BLUE)
text(s, 6.8, 1.85, 6.0, 1.05, "WebSocket con mensajes JSON, servidor en :8000. En Docker es ws:// (el PDF indica WSS)", size=14)
card(s, 6.65, 3.05, 6.25, 1.5, "IPeerTransport · plano de datos", RED)
text(s, 6.8, 3.5, 6.0, 1.05, "TCP binario con trama !II (cabecera + carga) e integridad SHA-256 de extremo a extremo", size=14)
card(s, 6.65, 4.7, 6.25, 2.25, "Topología Docker: 6 contenedores", GREEN)
code_lines(s, 6.8, 5.15, 6.0, 1.8, [
    "postgres :5433  redis :6379",
    "bluered-server :8000",
    "peer-alice  UI :8001  TCP :2234",
    "peer-bob    UI :8002  TCP :2235",
    "peer-carol  UI :8003  TCP :2236",
], size=12)
notes(s, NICO, "4:30–5:15", 45,
      ("Las Figuras 59 y 60 separan dos planos, y el código lo hace con dos interfaces. IControlPlane: WebSocket con mensajes JSON "
      "hacia el servidor, puerto 8000. IPeerTransport: sockets TCP directos con trama binaria y verificación SHA-256. Adaptaciones: "
      "en Docker usamos ws sin TLS, no WSS; y UPnP existe pero no está cableado en el arranque. La topología son seis contenedores: "
      "PostgreSQL, Redis, servidor y tres peers."),
      "Sergio toma la palabra y hace la demostración en vivo.")

# ================================================================ SLIDE 7
s = new_slide(7, "Demostración funcional en vivo", SERGIO, "5:15 – 8:15", 495,
              "Tres pestañas del navegador: Alice :8001 · Bob :8002 · Carol :8003")
steps = [
    ("1", "Presencia + Bloom", "0:50", BLUE, [
        "Carol → Búsqueda: «quantum_xyz»",
        "Sin coincidencias; Bloom descarta a Alice y Bob",
        "Carol → «arquitectura»: acierta solo Alice",
        "Alice → Sesión: Aciertos Bloom +1"]),
    ("2", "Descarga P2P + FairShare + reanudación", "1:10", RED, [
        "Carol → Descargar arquitectura_bluered_guia.txt",
        "Pestaña Transferencias: estado y slots",
        "Cola y Pausar/Reanudar con archivo ~20 MB",
        "o prueba E2E de corte y reanudación"]),
    ("3", "Reseña ACID + Chat P2P", "0:50", GREEN, [
        "Carol → Reseñas: 4 ★ y luego 5 ★",
        "Sigue 1 reseña (UNIQUE): promedio 5.0",
        "Alice → Chat privado a Bob (peer-bob:2235)",
        "Entrega directa TCP, sin pasar por el servidor"]),
]
cmd = ["GET /api/search?q=quantum_xyz", "POST /api/transfers/download", "POST /api/reviews/{file_hash}"]
for i, (num, ttl, tm, col, bl) in enumerate(steps):
    x = 0.45 + i * 4.2
    card(s, x, 1.5, 4.0, 5.35, accent=col)
    circ = rect(s, x + 0.2, 1.65, 0.6, 0.6, fill=col, line=None, shape=MSO_SHAPE.OVAL)
    del circ
    text(s, x + 0.2, 1.65, 0.6, 0.6, num, size=20, bold=True, color=BG, align=PP_ALIGN.CENTER,
         anchor=MSO_ANCHOR.MIDDLE, track=False)
    text(s, x + 0.9, 1.6, 3.0, 0.75, ttl, size=16, bold=True, anchor=MSO_ANCHOR.MIDDLE)
    text(s, x + 0.2, 2.45, 3.6, 0.35, f"Tiempo: {tm}", size=14, color=col, bold=True)
    text(s, x + 0.2, 2.9, 3.65, 3.1, ["• " + b for b in bl], size=16, spacing=1.08)
    text(s, x + 0.2, 6.05, 3.65, 0.7, cmd[i], size=12, mono=True, color=col)
notes(s, SERGIO, "5:15–8:15", 180,
      "Soy Sergio Osorio. Tenemos tres peers reales en Docker: Alice en el 8001, Bob en el 8002 y Carol en el 8003. "
      "Paso uno, presencia y Bloom: desde Carol busco un término que nadie tiene, quantum_xyz. El servidor consulta los "
      "filtros de Bloom y no molesta a Alice ni a Bob: sin coincidencias. Ahora busco arquitectura: el servidor enruta "
      "solo a Alice, y en su pestaña de sesión suben los aciertos del Bloom. Paso dos, descarga: desde Carol descargo "
      "arquitectura_bluered_guia.txt directamente de Alice por TCP; en Transferencias se ven estado, slots y progreso. "
      "Los archivos de muestra son de unos mil bytes y terminan al instante, así que para cola y reanudación apoyo con el "
      "archivo de veinte megas o con la prueba E2E de corte de socket, que reanuda desde el offset y verifica SHA-256. "
      "Paso tres: Carol reseña con cuatro estrellas y luego con cinco; por la restricción UNIQUE sigue habiendo una sola "
      "reseña, con promedio cinco. Y desde Alice envío un chat privado a Bob, directo por TCP a peer-bob 2235.",
      "Si algo falla, usamos la diapositiva 11 de respaldo. Joel presenta la evidencia cuantitativa.")

# ================================================================ SLIDE 8
s = new_slide(8, "Evidencia cuantitativa: SDP y calidad", JOEL, "8:15 – 8:35", 515,
              "Medido con scripts/audit_sdp.py y la puerta de calidad del repositorio")
card(s, 0.45, 1.4, 7.7, 4.1)
pic(s, AS / "chart_sdp_metrics.png", 0.55, 1.5, 7.5, 3.9)
text(s, 0.45, 5.6, 7.7, 1.3, [
    (f"Domain Core: I = {I_DOM_C:.2f} (cliente, Ca={CA_DOM_C}) y I = {I_DOM_S:.2f} (servidor, Ca={CA_DOM_S})", {"color": GREEN, "bold": True}),
    ("Las dependencias fluyen hacia menor inestabilidad: 0 violaciones", {"color": SUB}),
], size=14, spacing=1.05)
res = [(PY_LINE.split(" in ")[0], "pytest", GREEN),
       (f"{MYPY_N} archivos sin errores", "mypy --strict", GREEN),
       (f"{KEPT} contratos kept, 0 broken", "lint-imports", GREEN),
       ("All checks passed", "ruff", GREEN)]
for i, (val, lab, col) in enumerate(res):
    y = 1.4 + i * 1.4
    card(s, 8.35, y, 4.55, 1.25, accent=col)
    text(s, 8.55, y + 0.08, 4.25, 0.45, lab, size=14, color=SUB)
    text(s, 8.55, y + 0.5, 4.25, 0.7, val, size=18, bold=True, color=col)
notes(s, JOEL, "8:15–8:35", 20,
      (f"El dominio del cliente y el del servidor tienen inestabilidad cero y ninguna violación del principio de dependencias "
      f"estables. Puerta de calidad: {PY_SHORT}, mypy estricto limpio, {KEPT} contratos cumplidos y ruff limpio."),
      "Nicolás cierra con la trazabilidad y los ajustes técnicos.")

# ================================================================ SLIDE 9
s = new_slide(9, "Trazabilidad: 22/24 completos y ajustes técnicos", NICO, "8:35 – 8:55", 535,
              "Casos del PDF agrupados por sus tres diagramas · detalle en PDF_VS_CODE_AUDIT.md")
blocks = [("Cas_01–06 · comunidad", "4 completos · 1 parcial (Cas_05) · 1 no implementado (Cas_06)", RED),
          ("Cas_07–15 · cliente–servidor", "9 / 9 completos", GREEN),
          ("Cas_16–24 · cliente–cliente", "9 / 9 completos (Cas_19 handshake integrado en Cas_17/21)", GREEN)]
for i, (t, d, col) in enumerate(blocks):
    y = 1.45 + i * 1.75
    card(s, 0.45, y, 5.6, 1.6, accent=col)
    text(s, 0.65, y + 0.08, 5.3, 0.45, t, size=16, bold=True)
    text(s, 0.65, y + 0.6, 5.3, 0.95, d, size=14, color=SUB)
card(s, 6.25, 1.45, 6.65, 5.25, "Adaptaciones PDF → código", BLUE)
text(s, 6.4, 1.95, 6.4, 4.7, [
    "• Numeración: la matriz del repo difiere del PDF en Cas_02–06, 13 y 19",
    "• Plano de control: ws:// en Docker (PDF: WSS)",
    "• Redis: una instancia (PDF: Cluster)",
    "• NAT: UPnP implementado pero sin cablear; sin STUN ni UDP",
    f"• UNIQUE y CHECK en file_reviews; búsqueda por índice O(log n), {EXEC_5} ms",
], size=16, spacing=1.3)
notes(s, NICO, "8:35–8:55", 20,
      ("Casos agrupados como en el PDF: comunidad, cuatro completos, uno parcial y uno no implementado; cliente–servidor, nueve de "
      "nueve; cliente–cliente, nueve de nueve, con el handshake integrado en la descarga. Ajustes: numeración distinta, ws en Docker, "
      "un solo Redis y UPnP sin cablear."),
      "Sergio cierra con las conclusiones.")

# ================================================================ SLIDE 10
s = new_slide(10, "Conclusiones y preguntas", SERGIO, "8:55 – 9:15", 555)
finds = [("Arquitectura verificable", f"Dominio con I = {I_DOM_C:.2f} y {KEPT}/{KEPT} contratos: la regla de dependencias se mide, no se declara.", GREEN),
         ("Diseño y código, contrastados", "Figuras 53–60 reflejadas en el código; las desviaciones están documentadas con su justificación.", BLUE),
         ("Alcance honesto", "Pendiente: Cas_06, Cas_05 completo, TLS (WSS), UPnP en el arranque y STUN.", RED)]
for i, (t, d, col) in enumerate(finds):
    x = 0.45 + i * 4.2
    card(s, x, 1.5, 4.0, 3.6, accent=col)
    text(s, x + 0.2, 1.62, 3.7, 0.8, t, size=18, bold=True, color=col)
    text(s, x + 0.2, 2.5, 3.7, 2.5, d, size=14)
text(s, 0.45, 5.5, 12.4, 0.9, "¿Preguntas?", size=36, bold=True, color=BLUE, align=PP_ALIGN.CENTER)
text(s, 0.45, 6.4, 12.4, 0.5, f"{JOEL} · {NICO} · {SERGIO}", size=16, color=SUB, align=PP_ALIGN.CENTER)
notes(s, SERGIO, "8:55–9:15", 20,
      ("Tres conclusiones: la arquitectura se mide, con dominio en inestabilidad cero; contrastamos el diseño con el código y "
      "documentamos las desviaciones; y somos claros con lo pendiente: intereses comunes, TLS, UPnP y STUN. Gracias; quedamos "
      "atentos a sus preguntas."),
      "Abrir preguntas; diapositiva 11 solo si hay un fallo técnico.")

# ================================================================ SLIDE 11
s = new_slide(11, "[Respaldo] UIs y transferencia P2P", SERGIO, "Solo si falla la demo", 555,
              "Capturas reales de los contenedores Docker y pruebas E2E con sockets TCP")
for i, (fn, lab) in enumerate((("ui_alice.png", "Alice · :8001"), ("ui_carol.png", "Carol · :8003"))):
    x = 0.45 + i * 4.3
    rect(s, x, 1.4, 4.15, 2.85)
    text(s, x + 0.05, 1.42, 4.0, 0.3, lab, size=12, color=SUB)
    pic(s, AS / fn, x + 0.1, 1.72, 3.95, 2.45)
rect(s, 9.1, 1.4, 3.8, 2.85)
text(s, 9.2, 1.45, 3.6, 2.8, [
    ("Qué mostrar", {"bold": True, "color": BLUE}),
    "• Peer ID, endpoint TCP y slots",
    "• Bloom: m = 8192 bits, k = 5",
    "• Estado ONLINE de cada peer",
], size=14, spacing=1.05)
rect(s, 0.45, 4.4, 12.45, 2.55)
pic(s, AS / "evidencia_transfer.png", 0.55, 4.45, 12.25, 2.45)
notes(s, SERGIO, "Respaldo", 0,
      "Si la demostración falla: estas son capturas reales de Alice y Carol en Docker, y la ejecución de las cuatro pruebas "
      "E2E con sockets TCP reales: navegación y descarga de 1.5 MB, cola de slots con tres peers, corte de socket con "
      "reanudación desde offset, y chat privado con bloqueo y entrega offline.",
      "Volver a la diapositiva 10 para preguntas.")

prs.save(OUT)
print("OK", OUT, "slides:", len(prs.slides))
if OVERFLOW_WARN:
    print("AVISOS DE DESBORDE:")
    for w in OVERFLOW_WARN:
        print("  ", w)
