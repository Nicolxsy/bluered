"""Verifica el .pptx: tamaño, límites, proporción de imágenes, palabras prohibidas y ritmo de las notas."""
import re
import sys
from pathlib import Path

from PIL import Image
from pptx import Presentation

ROOT = Path(__file__).resolve().parent.parent
prs = Presentation(ROOT / "docs/presentation/BlueRed_Sustentacion.pptx")
FORBID = re.compile(r"antigravity|gemini|\bmcp\b|swarm|prompt|agente|\bllm\b|\bskills?\b|\bia\b|inteligencia artificial", re.I)
SECS = [25, 55, 70, 65, 55, 45, 180, 20, 20, 20, 0]
bad = 0
print("tamaño", prs.slide_width, prs.slide_height, "(esperado 12192000 x 6858000)")
assert (prs.slide_width, prs.slide_height) == (12192000, 6858000)
for i, sl in enumerate(prs.slides, 1):
    texts, body_min = [], 99
    for sh in sl.shapes:
        if sh.left < 0 or sh.top < 0 or sh.left + sh.width > prs.slide_width + 10 or sh.top + sh.height > prs.slide_height + 10:
            print(f"  [!] slide {i}: shape fuera de límites"); bad += 1
        if sh.shape_type == 13:  # picture
            iw, ih = Image.open(ROOT / "docs/presentation/assets" / Path(sh.name).name).size if False else (None, None)
        if sh.has_text_frame:
            for p in sh.text_frame.paragraphs:
                for r in p.runs:
                    texts.append(r.text)
                    if r.text.strip() and r.font.size:
                        body_min = min(body_min, r.font.size.pt)
    note = sl.notes_slide.notes_text_frame.text
    alltext = " ".join(texts) + " " + note
    hits = FORBID.findall(alltext)
    words = len(note.split()) - 12
    sec = SECS[i - 1]
    rate = f"{words / sec * 60:.0f} pal/min" if sec else "n/a"
    print(f"slide {i:2d}: fuente mín {body_min:.0f} pt | notas {words:3d} palabras / {sec:3d}s = {rate} | prohibidas: {hits or 'ninguna'}")
    bad += len(hits)
    assert note.startswith("ORADOR:")
# proporción de imágenes
for i, sl in enumerate(prs.slides, 1):
    for sh in sl.shapes:
        if sh.shape_type == 13:
            from io import BytesIO
            iw, ih = Image.open(BytesIO(sh.image.blob)).size
            r_img, r_box = iw / ih, sh.width / sh.height
            ok = abs(r_img - r_box) / r_img < 0.01
            if not ok:
                print(f"  [!] slide {i}: imagen deformada {r_img:.3f} vs {r_box:.3f}"); bad += 1
for f in ("PDF_VS_CODE_AUDIT.md", "GUION_SUSTENTACION_9MIN.md"):
    fp = ROOT / "docs/presentation" / f
    if fp.exists():
        h = FORBID.findall(fp.read_text(encoding="utf-8"))
        print(f, "prohibidas:", h or "ninguna"); bad += len(h)
print("RESULTADO:", "OK" if bad == 0 else f"{bad} problemas")
sys.exit(1 if bad else 0)
