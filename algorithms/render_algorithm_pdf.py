"""Render the algorithm reading edition. Requires reportlab; no trading imports.

Run: python algorithms/render_algorithm_pdf.py
An optional --output path selects a separate preview file.
"""

from __future__ import annotations

import argparse
import hashlib
import os
import re
import textwrap
from pathlib import Path
from xml.sax.saxutils import escape

from reportlab import rl_config
from reportlab.lib import colors
from reportlab.lib.pagesizes import A4
from reportlab.lib.styles import ParagraphStyle
from reportlab.pdfbase import pdfmetrics
from reportlab.pdfbase.ttfonts import TTFont
from reportlab.platypus import (
    BaseDocTemplate, Frame, KeepTogether, PageBreak, PageTemplate,
    Paragraph, Spacer, Table, TableStyle,
)
from reportlab.platypus.tableofcontents import TableOfContents

rl_config.invariant = 1
os.environ.setdefault("SOURCE_DATE_EPOCH", "1792108800")
HERE = Path(__file__).resolve().parent
SOURCE = HERE / "ATLAS_SYSTEM_DESIGN.md"
REPO_FOLDER = "https://github.com/KasunKarandagolla/Atlas/blob/docs/atlas-final-system-2026-10-16/algorithms/"
NAVY = colors.HexColor("#142F46")
TEAL = colors.HexColor("#087E89")
INK = colors.HexColor("#263947")
MUTED = colors.HexColor("#5D707F")
LIGHT = colors.HexColor("#F0F5F8")
LINE = colors.HexColor("#D5E0E7")
W, H = A4
LEFT = RIGHT = 47
CW = W - LEFT - RIGHT


def register_fonts() -> None:
    candidates = [
        Path("/usr/share/fonts/truetype/dejavu"),
        Path("C:/Windows/Fonts"),
        HERE / "fonts",
    ]
    for root in candidates:
        if (root / "DejaVuSans.ttf").exists():
            for name, filename in [
                ("AtlasSans", "DejaVuSans.ttf"),
                ("AtlasSans-Bold", "DejaVuSans-Bold.ttf"),
                ("AtlasMono", "DejaVuSansMono.ttf"),
            ]:
                pdfmetrics.registerFont(TTFont(name, str(root / filename)))
            pdfmetrics.registerFontFamily("AtlasSans", normal="AtlasSans", bold="AtlasSans-Bold")
            return
    pdfmetrics.registerFontFamily("AtlasSans", normal="Helvetica", bold="Helvetica-Bold")


register_fonts()
FONT = "AtlasSans" if "AtlasSans" in pdfmetrics.getRegisteredFontNames() else "Helvetica"
BOLD = "AtlasSans-Bold" if FONT == "AtlasSans" else "Helvetica-Bold"
MONO = "AtlasMono" if "AtlasMono" in pdfmetrics.getRegisteredFontNames() else "Courier"
STYLES = {
    "body": ParagraphStyle("Body", fontName=FONT, fontSize=9.05, leading=12.6,
                           textColor=INK, spaceAfter=7, splitLongWords=True,
                           allowWidows=0, allowOrphans=0),
    "small": ParagraphStyle("Small", fontName=FONT, fontSize=8.1, leading=11.4,
                            textColor=MUTED, spaceAfter=7),
    "title": ParagraphStyle("Title", fontName=BOLD, fontSize=35, leading=39,
                            textColor=NAVY, spaceAfter=14),
    "subtitle": ParagraphStyle("Subtitle", fontName=FONT, fontSize=15, leading=21,
                               textColor=MUTED, spaceAfter=20),
    "h2": ParagraphStyle("Section", fontName=BOLD, fontSize=17, leading=22,
                         textColor=NAVY, spaceBefore=13, spaceAfter=11,
                         keepWithNext=True),
    "h3": ParagraphStyle("Subsection", fontName=BOLD, fontSize=11.5, leading=15,
                         textColor=TEAL, spaceBefore=8, spaceAfter=7,
                         keepWithNext=True),
    "bullet": ParagraphStyle("Bullet", fontName=FONT, fontSize=9.05, leading=12.6,
                             textColor=INK, leftIndent=11, firstLineIndent=-9,
                             spaceAfter=5.5),
    "cell": ParagraphStyle("Cell", fontName=FONT, fontSize=8.0, leading=11.1,
                           textColor=INK, splitLongWords=True),
    "head": ParagraphStyle("Head", fontName=BOLD, fontSize=8.1, leading=11.2,
                           textColor=colors.white),
    "code": ParagraphStyle("Code", fontName=MONO, fontSize=7.45, leading=10.8,
                           textColor=INK, spaceAfter=1),
    "toc": ParagraphStyle("Contents", fontName=FONT, fontSize=10.0, leading=15.8,
                          textColor=INK, leftIndent=0, firstLineIndent=0,
                          rightIndent=25, spaceBefore=0),
}


def inline(text: str) -> str:
    text = escape(text)
    def link(match: re.Match[str]) -> str:
        label, target = match.groups()
        if not target.startswith(("https://", "http://")):
            target = REPO_FOLDER + target
        return '<link href="' + target.replace('"', '&quot;') + '" color="#087E89">' + label + '</link>'
    text = re.sub(r"\[([^\]]+)\]\(([^)]+)\)", link, text)
    text = re.sub(r"\*\*([^*]+)\*\*", r"<b>\1</b>", text)
    text = re.sub(r"`([^`]+)`", r'<font color="#34596C">\1</font>', text)
    return text


def para(text: str, style: str = "body") -> Paragraph:
    return Paragraph(inline(text), STYLES[style])


def make_table(rows: list[list[str]]) -> Table:
    count = len(rows[0])
    header = rows[0][0]
    if count == 2:
        widths = [CW * 0.35, CW * 0.65]
    elif header in {"Checkpoint", "Component"}:
        widths = [CW * 0.29, CW * 0.31, CW * 0.40]
    elif header == "Family":
        widths = [CW * 0.20, CW * 0.47, CW * 0.33]
    else:
        widths = [CW * 0.20, CW * 0.46, CW * 0.34]
    data = [[para(c, "head" if i == 0 else "cell") for c in row]
            for i, row in enumerate(rows)]
    table = Table(data, colWidths=widths, repeatRows=1, hAlign="LEFT", splitInRow=0)
    table.setStyle(TableStyle([
        ("BACKGROUND", (0, 0), (-1, 0), NAVY),
        ("ROWBACKGROUNDS", (0, 1), (-1, -1), [colors.white, LIGHT]),
        ("VALIGN", (0, 0), (-1, -1), "TOP"),
        ("LEFTPADDING", (0, 0), (-1, -1), 7),
        ("RIGHTPADDING", (0, 0), (-1, -1), 7),
        ("TOPPADDING", (0, 0), (-1, -1), 6),
        ("BOTTOMPADDING", (0, 0), (-1, -1), 6),
        ("LINEBELOW", (0, 0), (-1, 0), 0.5, NAVY),
        ("LINEBELOW", (0, 1), (-1, -1), 0.25, LINE),
    ]))
    return table


class AlgorithmDoc(BaseDocTemplate):
    def afterFlowable(self, flowable: object) -> None:
        if (isinstance(flowable, Paragraph) and flowable.style.name == "Section"
                and re.match(r"\d+\.", flowable.getPlainText())):
            title = flowable.getPlainText()
            key = "section-" + hashlib.sha256(title.encode()).hexdigest()[:12]
            self.canv.bookmarkPage(key)
            self.canv.addOutlineEntry(title, key, level=0, closed=False)
            self.notify("TOCEntry", (0, title, self.page, key))


def render(output: Path) -> None:
    source = SOURCE.read_text(encoding="utf-8")
    digest = hashlib.sha256(source.encode()).hexdigest()
    def furniture(canvas: object, doc: object) -> None:
        canvas.saveState()
        if doc.page > 1:
            canvas.setStrokeColor(LINE)
            canvas.line(LEFT, H - 40, W - RIGHT, H - 40)
            canvas.setFont(BOLD, 8.0)
            canvas.setFillColor(NAVY)
            canvas.drawString(LEFT, H - 28, "ATLAS  /  FINAL SYSTEM DESIGN")
            canvas.setFont(FONT, 7.3)
            canvas.setFillColor(MUTED)
            canvas.drawRightString(W - RIGHT, H - 28, "16 OCTOBER 2026")
        canvas.setStrokeColor(LINE)
        canvas.line(LEFT, 35, W - RIGHT, 35)
        canvas.setFont(FONT, 7.0)
        canvas.setFillColor(MUTED)
        canvas.drawString(LEFT, 22, "Final target. Operative/manual approval and qualification gates remain open.")
        canvas.drawRightString(W - RIGHT, 22, str(doc.page))
        canvas.restoreState()
    output.parent.mkdir(parents=True, exist_ok=True)
    doc = AlgorithmDoc(str(output), pagesize=A4, leftMargin=LEFT, rightMargin=RIGHT,
                       topMargin=58, bottomMargin=49, title="ATLAS - Final System Design",
                       author="ATLAS project design", subject="Algorithm, account, agent, models and qualification",
                       pageCompression=1)
    doc.addPageTemplates([PageTemplate(id="Algorithm", frames=[Frame(
        LEFT, 49, CW, H - 107, leftPadding=0, rightPadding=0,
        topPadding=0, bottomPadding=0)], onPage=furniture)])
    story = [Spacer(1, 32), para("ATLAS", "title"),
             para("Final system design", "subtitle"),
             para("One complete crypto futures trading and research system.", "h3"),
             para("Binance USDT perpetuals | Shared manual account | Broad altcoins | Concurrent positions | Compulsory API coordinator | 4 GB Windows/Linux", "body"),
             Spacer(1, 17)]
    card = Table([[para("**Objective.** Maximum validated programme value within explicit risk, reliability, permission and resource limits.")],
                  [para("**Scope.** Preserve existing strengths. Implement every core requirement. No weaker MVP or core-feature backlog.")],
                  [para("**Current authority.** Final target for planning. Operative amendment proposed; manual-policy owner approval pending. No capital permission.")]], colWidths=[CW])
    card.setStyle(TableStyle([("BACKGROUND", (0, 0), (-1, -1), LIGHT),
                              ("BOX", (0, 0), (-1, -1), 0.6, LINE),
                              ("LEFTPADDING", (0, 0), (-1, -1), 13),
                              ("RIGHTPADDING", (0, 0), (-1, -1), 13),
                              ("TOPPADDING", (0, 0), (-1, -1), 10),
                              ("BOTTOMPADDING", (0, 0), (-1, -1), 10)]))
    story.extend([card, Spacer(1, 22), para("Prepared 16 October 2026 | Asia/Colombo", "small"),
                  para("R9 code baseline: 96fc24a83bc90dd0b0e91eb69d55c23d85d4eefd", "small"),
                  para("Algorithm source SHA-256: " + digest, "small"),
                  para("Implementation sequence: P1 Sol, P2 Sol, P3 Luna, P4 Sol, P5 fresh Sol; later evidence review if needed. Complete scopes and acceptance register accompany this PDF.", "small"),
                  PageBreak(), para("Contents", "h2")])
    toc = TableOfContents()
    toc.levelStyles = [STYLES["toc"]]
    story.extend([toc, Spacer(1, 12),
                  para("Use the companion implementation plan, acceptance register and session prompts for engineering ownership. Tables define the complete intended scope; they are not qualification results. The source digest and MANIFEST.sha256 identify these exact documents.", "small")])
    lines = source.splitlines()
    first_section = next(i for i, line in enumerate(lines) if line.startswith("## 1."))
    i = first_section
    force_new = {1, 17, 19}
    while i < len(lines):
        line = lines[i]
        if not line.strip():
            i += 1
            continue
        if line.startswith("## "):
            match = re.match(r"## (\d+)\.", line)
            if match and int(match.group(1)) in force_new:
                story.append(PageBreak())
            story.append(para(line[3:], "h2"))
            i += 1
        elif line.startswith("### "):
            story.append(para(line[4:], "h3"))
            i += 1
        elif line.startswith("|"):
            rows = []
            while i < len(lines) and lines[i].startswith("|"):
                cells = [x.strip() for x in lines[i].strip().strip("|").split("|")]
                if not all(re.fullmatch(r"[-: ]+", x) for x in cells):
                    rows.append(cells)
                i += 1
            story.extend([make_table(rows), Spacer(1, 10)])
        elif line.startswith("```"):
            i += 1
            code = []
            while i < len(lines) and not lines[i].startswith("```"):
                for visual_line in textwrap.wrap(lines[i], width=100, replace_whitespace=False, drop_whitespace=False) or [" "]:
                    code.append(Paragraph(escape(visual_line).replace(" ", "&#160;"), STYLES["code"]))
                i += 1
            i += 1
            box = Table([[line] for line in code], colWidths=[CW], splitByRow=1)
            box.setStyle(TableStyle([("BACKGROUND", (0, 0), (-1, -1), LIGHT),
                                     ("BOX", (0, 0), (-1, -1), .5, LINE),
                                     ("LEFTPADDING", (0, 0), (-1, -1), 10),
                                     ("RIGHTPADDING", (0, 0), (-1, -1), 10),
                                     ("TOPPADDING", (0, 0), (-1, -1), 1),
                                     ("BOTTOMPADDING", (0, 0), (-1, -1), 1)]))
            story.extend([box, Spacer(1, 10)])
        elif line.startswith("- "):
            story.append(para("- " + line[2:], "bullet"))
            i += 1
        elif re.match(r"\d+\. ", line):
            story.append(para(line, "bullet"))
            i += 1
        else:
            paragraph = [line]
            i += 1
            while i < len(lines) and lines[i].strip() and not re.match(r"^(#|\||```|- |\d+\. )", lines[i]):
                paragraph.append(lines[i])
                i += 1
            story.append(para(" ".join(paragraph)))
    doc.multiBuild(story)
    print(str(output.resolve()))


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, default=HERE / "ATLAS_ALGORITHM.pdf")
    render(parser.parse_args().output)
