"""Recorta las figuras del PDF de diseño a 300 DPI (uso: uv run --with pymupdf python scripts/_extract_pdf_figs.py)."""
from pathlib import Path

import pymupdf

PDF = Path("docs/ArquitecturaDiseñoBlueRed_DiseñoDinámico_Modular_Despliegue.pdf")
OUT = Path("docs/presentation/assets")
OUT.mkdir(parents=True, exist_ok=True)
doc = pymupdf.open(PDF)

# Figuras raster embebidas: se extraen a resolución nativa (xref estable del PDF).
for xref, name in ((148, "pdf_fig53_cliente"), (149, "pdf_fig54_servidor"),
                   (154, "pdf_fig55_persistencia"), (158, "pdf_fig56_estados")):
    pix = pymupdf.Pixmap(doc, xref)
    if pix.alpha or pix.n > 3:
        pix = pymupdf.Pixmap(pymupdf.csRGB, pix)
    pix.save(OUT / f"{name}.png")

# Figuras vectoriales: se renderizan a 300 DPI sobre un clip.
for page_no, clip, name in (
    (50, (112, 352, 484, 612), "pdf_fig57_actividades"),
    (52, (112, 60, 484, 282), "pdf_fig59_componentes"),
    (52, (112, 428, 484, 627), "pdf_fig60_nodos"),
):
    doc[page_no - 1].get_pixmap(dpi=300, clip=pymupdf.Rect(*clip)).save(OUT / f"{name}.png")
print("ok")
