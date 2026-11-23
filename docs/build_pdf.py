"""
build_pdf.py — Convierte docs/Part2_Report.md en docs/Part2_Report.pdf.

Markdown -> HTML (con el diagrama Mermaid renderizado) -> PDF con Microsoft Edge headless.
    pip install markdown
    python docs/build_pdf.py
"""

import re
import subprocess
from pathlib import Path

import markdown

DOCS = Path(__file__).resolve().parent
SRC = DOCS / "Part2_Report.md"
HTML = DOCS / "Part2_Report.html"
PDF = DOCS / "Part2_Report.pdf"
REPO_BLOB = "https://github.com/Xixigv/ProyectoDM/blob/part2-streaming/"
EDGE = Path(r"C:\Program Files (x86)\Microsoft\Edge\Application\msedge.exe")

CSS = """
@page { size: Letter; margin: 18mm 16mm; }
body { font-family: 'Segoe UI', Arial, sans-serif; font-size: 10.5pt; line-height: 1.45; color: #1a1a1a; }
h1 { font-size: 20pt; margin: 0 0 4px; }
h2 { font-size: 15pt; border-bottom: 2px solid #2b6cb0; padding-bottom: 3px; margin-top: 22px; page-break-after: avoid; }
h3 { font-size: 12pt; margin-top: 16px; page-break-after: avoid; }
table { border-collapse: collapse; margin: 8px 0; width: 100%; page-break-inside: avoid; }
th, td { border: 1px solid #cbd5e0; padding: 4px 7px; text-align: left; vertical-align: top; }
th { background: #edf2f7; }
code { font-family: Consolas, monospace; font-size: 9pt; background: #f1f3f5; padding: 1px 3px; border-radius: 3px; }
pre { background: #f6f8fa; border: 1px solid #e2e8f0; padding: 8px 10px; border-radius: 4px;
      white-space: pre-wrap; word-break: break-word; page-break-inside: avoid; }
pre code { background: none; padding: 0; }
img { max-width: 100%; display: block; margin: 6px auto 14px; border: 1px solid #e2e8f0; page-break-inside: avoid; }
p em:only-child { color: #4a5568; font-size: 9.5pt; }
.mermaid { text-align: center; margin: 10px 0; page-break-inside: avoid; }
hr { border: none; border-top: 1px solid #e2e8f0; margin: 16px 0; }
a { color: #2b6cb0; text-decoration: none; }
"""


def main() -> None:
    md_text = SRC.read_text(encoding="utf-8")
    # Quitar la nota de "exportar el diagrama" (solo aplica al .md)
    md_text = re.sub(r"^> Export this diagram.*\n", "", md_text, flags=re.M)
    # Links relativos a archivos del repo -> links a GitHub
    md_text = md_text.replace("](../", f"]({REPO_BLOB}")

    body = markdown.markdown(md_text, extensions=["tables", "fenced_code"])
    body = re.sub(
        r'<pre><code class="language-mermaid">(.*?)</code></pre>',
        lambda m: f'<div class="mermaid">{m.group(1)}</div>',
        body,
        flags=re.S,
    )

    HTML.write_text(f"""<!doctype html><html><head><meta charset="utf-8">
<title>Final Project Part II</title><style>{CSS}</style>
<script src="https://cdn.jsdelivr.net/npm/mermaid@10.9.1/dist/mermaid.min.js"></script>
<script>mermaid.initialize({{ startOnLoad: true, theme: 'default', flowchart: {{ htmlLabels: true }} }});</script>
</head><body>{body}</body></html>""", encoding="utf-8")

    subprocess.run([
        str(EDGE), "--headless=new", "--disable-gpu", "--no-pdf-header-footer",
        "--virtual-time-budget=15000", "--run-all-compositor-stages-before-draw",
        f"--print-to-pdf={PDF}", HTML.as_uri(),
    ], check=True, timeout=120)
    print("PDF generado:", PDF)


if __name__ == "__main__":
    main()
