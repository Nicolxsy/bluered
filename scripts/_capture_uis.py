"""Capturas de las UIs vivas (uso: uv run --with playwright python scripts/_capture_uis.py)."""
from pathlib import Path

from playwright.sync_api import sync_playwright

OUT = Path("docs/presentation/assets")
with sync_playwright() as p:
    try:
        br = p.chromium.launch(channel="msedge")
    except Exception:
        br = p.chromium.launch(channel="chrome")
    for name, port in (("ui_alice", 8001), ("ui_carol", 8003)):
        pg = br.new_page(viewport={"width": 1600, "height": 900}, device_scale_factor=2)
        pg.goto(f"http://localhost:{port}", wait_until="networkidle")
        pg.wait_for_timeout(1500)
        pg.get_by_role("button", name="ONLINE").first.click()
        pg.wait_for_timeout(1500)
        pg.screenshot(path=str(OUT / f"{name}.png"))
        print(name, pg.title())
    br.close()
