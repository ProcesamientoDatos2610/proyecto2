"""Tiny helper to build .ipynb files from a list of cells.

Usage from a spec script:
    from build_notebook import md, code, build
    cells = [
        md("# Title"),
        code("x = 1\\nx"),
    ]
    build(cells, "/path/to/notebook.ipynb")
"""
from __future__ import annotations
import os
import nbformat as nbf

KERNEL = {"name": "python3", "display_name": "Python 3"}

MEMBRETADO = """\
<table style="border:0; background:none; width:100%">
<tr>
<td style="vertical-align: middle; width: 60%;">
<b>Pontificia Universidad Javeriana</b><br/>
Facultad de Ingeniería · Departamento de Ingeniería de Sistemas<br/>
<b>Procesamiento de Datos a Gran Escala</b>
</td>
<td style="vertical-align: middle; text-align: right; width: 40%;">
<b>Proyecto de Investigación — Entrega 2</b><br/>
Brecha digital y resultados Saber 11 en Colombia<br/>
<b>Grupo: REST pAPIs</b>
</td>
</tr>
</table>

---
"""


def md(source: str):
    return nbf.v4.new_markdown_cell(source.strip("\n"))


def code(source: str):
    return nbf.v4.new_code_cell(source.strip("\n"))


def build(cells, path: str):
    """Build a notebook. Prepends the membretado as the first cell automatically."""
    # Avoid duplicating the header if a spec already includes it
    has_header = (cells and cells[0].get("cell_type") == "markdown"
                  and "Pontificia Universidad Javeriana" in cells[0].get("source", ""))
    if not has_header:
        cells = [md(MEMBRETADO)] + cells
    nb = nbf.v4.new_notebook(cells=cells)
    nb.metadata["kernelspec"] = KERNEL
    nb.metadata["language_info"] = {"name": "python", "version": "3.9"}
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        nbf.write(nb, f)
    print(f"  wrote {path}  ({len(cells)} cells)")
