#!/usr/bin/env python3
import re
import sys
import zipfile
from pathlib import Path
from xml.sax.saxutils import escape

from PIL import Image


NS_W = "http://schemas.openxmlformats.org/wordprocessingml/2006/main"
NS_R = "http://schemas.openxmlformats.org/officeDocument/2006/relationships"
NS_WP = "http://schemas.openxmlformats.org/drawingml/2006/wordprocessingDrawing"
NS_A = "http://schemas.openxmlformats.org/drawingml/2006/main"
NS_PIC = "http://schemas.openxmlformats.org/drawingml/2006/picture"


def clean_inline(s: str) -> str:
    s = re.sub(r"`([^`]+)`", r"\1", s)
    s = s.replace("**", "")
    return s


def t_xml(text: str) -> str:
    attr = ' xml:space="preserve"' if text[:1].isspace() or text[-1:].isspace() else ""
    return f"<w:t{attr}>{escape(text)}</w:t>"


def run_xml(text: str, size=24, east="宋体", ascii_font="Times New Roman", bold=False, code=False):
    if not text:
        return ""
    font = "Courier New" if code else ascii_font
    east_font = "Courier New" if code else east
    b = "<w:b/>" if bold else ""
    return (
        "<w:r><w:rPr>"
        f"<w:rFonts w:ascii=\"{font}\" w:hAnsi=\"{font}\" w:eastAsia=\"{east_font}\"/>"
        f"{b}<w:sz w:val=\"{size}\"/><w:szCs w:val=\"{size}\"/>"
        "</w:rPr>"
        f"{t_xml(text)}"
        "</w:r>"
    )


def inline_runs(text: str, size=24, east="宋体", ascii_font="Times New Roman", bold=False):
    parts = []
    pattern = re.compile(r"(\*\*[^*]+\*\*|`[^`]+`)")
    pos = 0
    for m in pattern.finditer(text):
        if m.start() > pos:
            parts.append(run_xml(text[pos:m.start()], size=size, east=east, ascii_font=ascii_font, bold=bold))
        token = m.group(0)
        if token.startswith("**"):
            parts.append(run_xml(token[2:-2], size=size, east=east, ascii_font=ascii_font, bold=True))
        elif token.startswith("`"):
            parts.append(run_xml(token[1:-1], size=size, east=east, ascii_font=ascii_font, code=True))
        pos = m.end()
    if pos < len(text):
        parts.append(run_xml(text[pos:], size=size, east=east, ascii_font=ascii_font, bold=bold))
    return "".join(parts)


def ppr(style=None, align=None, first_line=False, left=0, hanging=0, spacing=True,
        page_break_before=False, keep_next=False, before=None, after=None):
    parts = ["<w:pPr>"]
    if style:
        parts.append(f"<w:pStyle w:val=\"{style}\"/>")
    if page_break_before:
        parts.append("<w:pageBreakBefore/>")
    if align:
        parts.append(f"<w:jc w:val=\"{align}\"/>")
    ind_attrs = []
    if first_line:
        ind_attrs.append('w:firstLine="480"')
    if left:
        ind_attrs.append(f'w:left="{left}"')
    if hanging:
        ind_attrs.append(f'w:hanging="{hanging}"')
    if ind_attrs:
        parts.append("<w:ind " + " ".join(ind_attrs) + "/>")
    if spacing:
        spacing_attrs = ['w:line="360"', 'w:lineRule="auto"']
        if before is not None:
            spacing_attrs.append(f'w:before="{before}"')
        if after is not None:
            spacing_attrs.append(f'w:after="{after}"')
        parts.append("<w:spacing " + " ".join(spacing_attrs) + "/>")
    parts.append("</w:pPr>")
    return "".join(parts)


def para(text="", style=None, align=None, size=24, east="宋体", ascii_font="Times New Roman",
         bold=False, first_line=False, left=0, hanging=0, spacing=True,
         page_break_before=False, keep_next=False, before=None, after=None):
    return (
        "<w:p>"
        + ppr(style, align, first_line, left, hanging, spacing, page_break_before, keep_next, before, after)
        + inline_runs(text, size=size, east=east, ascii_font=ascii_font, bold=bold)
        + "</w:p>"
    )


def page_break():
    return '<w:p><w:r><w:br w:type="page"/></w:r></w:p>'


def image_xml(rel_id: str, name: str, width_emu: int, height_emu: int, doc_pr_id: int):
    safe_name = escape(name)
    return f'''<w:p>
  <w:pPr><w:jc w:val="center"/><w:spacing w:line="360" w:lineRule="auto"/></w:pPr>
  <w:r>
    <w:drawing>
      <wp:inline distT="0" distB="0" distL="0" distR="0">
        <wp:extent cx="{width_emu}" cy="{height_emu}"/>
        <wp:effectExtent l="0" t="0" r="0" b="0"/>
        <wp:docPr id="{doc_pr_id}" name="{safe_name}"/>
        <wp:cNvGraphicFramePr><a:graphicFrameLocks noChangeAspect="1"/></wp:cNvGraphicFramePr>
        <a:graphic>
          <a:graphicData uri="{NS_PIC}">
            <pic:pic>
              <pic:nvPicPr>
                <pic:cNvPr id="0" name="{safe_name}"/>
                <pic:cNvPicPr/>
              </pic:nvPicPr>
              <pic:blipFill>
                <a:blip r:embed="{rel_id}"/>
                <a:stretch><a:fillRect/></a:stretch>
              </pic:blipFill>
              <pic:spPr>
                <a:xfrm><a:off x="0" y="0"/><a:ext cx="{width_emu}" cy="{height_emu}"/></a:xfrm>
                <a:prstGeom prst="rect"><a:avLst/></a:prstGeom>
              </pic:spPr>
            </pic:pic>
          </a:graphicData>
        </a:graphic>
      </wp:inline>
    </w:drawing>
  </w:r>
</w:p>'''


def toc_field():
    return (
        "<w:p>" + ppr(align="center", spacing=True) +
        run_xml("目 录", size=36, east="黑体", bold=True) + "</w:p>"
        "<w:p><w:r><w:fldChar w:fldCharType=\"begin\"/></w:r>"
        "<w:r><w:instrText xml:space=\"preserve\">TOC \\o \"1-3\" \\h \\z \\u</w:instrText></w:r>"
        "<w:r><w:fldChar w:fldCharType=\"separate\"/></w:r>"
        + run_xml("打开 Word 后右键更新域生成目录", size=24) +
        "<w:r><w:fldChar w:fldCharType=\"end\"/></w:r></w:p>"
    )


def sect_pr(header_footer=False, section_type=None):
    typ = f'<w:type w:val="{section_type}"/>' if section_type else ""
    refs = ""
    pgnum = ""
    if header_footer:
        refs = (
            '<w:headerReference w:type="default" r:id="rIdHeader1"/>'
            '<w:footerReference w:type="default" r:id="rIdFooter1"/>'
        )
        pgnum = '<w:pgNumType w:start="1"/>'
    return (
        "<w:sectPr>"
        + typ + refs +
        '<w:pgSz w:w="11906" w:h="16838"/>'
        '<w:pgMar w:top="1440" w:right="1440" w:bottom="1440" w:left="1440" '
        'w:header="850" w:footer="992" w:gutter="0"/>'
        + pgnum +
        "</w:sectPr>"
    )


def section_break():
    return "<w:p><w:pPr>" + sect_pr(header_footer=False, section_type="nextPage") + "</w:pPr></w:p>"


def table_xml(rows):
    col_count = max(len(r) for r in rows) if rows else 1
    width = max(1200, 9000 // col_count)
    out = [
        '<w:tbl><w:tblPr><w:tblW w:w="0" w:type="auto"/>'
        '<w:tblLayout w:type="autofit"/>'
        '<w:tblBorders>'
        '<w:top w:val="single" w:sz="8" w:space="0" w:color="000000"/>'
        '<w:left w:val="single" w:sz="8" w:space="0" w:color="000000"/>'
        '<w:bottom w:val="single" w:sz="8" w:space="0" w:color="000000"/>'
        '<w:right w:val="single" w:sz="8" w:space="0" w:color="000000"/>'
        '<w:insideH w:val="single" w:sz="4" w:space="0" w:color="000000"/>'
        '<w:insideV w:val="single" w:sz="4" w:space="0" w:color="000000"/>'
        '</w:tblBorders></w:tblPr>'
    ]
    for r_idx, row in enumerate(rows):
        out.append("<w:tr>")
        for cell in row:
            out.append(
                '<w:tc><w:tcPr>'
                f'<w:tcW w:w="{width}" w:type="dxa"/>'
                '</w:tcPr>'
                + para(clean_inline(cell), align="center" if r_idx == 0 else None,
                       size=18 if col_count >= 5 else 21, bold=(r_idx == 0),
                       first_line=False, spacing=True)
                + "</w:tc>"
            )
        out.append("</w:tr>")
    out.append("</w:tbl>")
    return "".join(out)


def parse_markdown(md: str):
    md = re.sub(r"<!--.*?-->", "", md, flags=re.S)
    lines = md.splitlines()
    blocks = []
    i = 0
    while i < len(lines):
        line = lines[i].rstrip()
        if not line.strip():
            i += 1
            continue
        if line.strip() == "---":
            blocks.append(("hr", ""))
            i += 1
            continue
        if line.startswith("```"):
            code = []
            i += 1
            while i < len(lines) and not lines[i].startswith("```"):
                code.append(lines[i])
                i += 1
            i += 1
            blocks.append(("code", "\n".join(code)))
            continue
        if line.startswith("|") and i + 1 < len(lines) and re.match(r"^\s*\|?\s*:?-{3,}", lines[i + 1]):
            rows = []
            while i < len(lines) and lines[i].startswith("|"):
                cells = [c.strip() for c in lines[i].strip().strip("|").split("|")]
                if not all(re.match(r"^:?-{3,}:?$", c.replace(" ", "")) for c in cells):
                    rows.append(cells)
                i += 1
            blocks.append(("table", rows))
            continue
        m = re.match(r"^!\[(.*?)\]\((.*?)\)$", line)
        if m:
            blocks.append(("image", {"alt": m.group(1).strip(), "path": m.group(2).strip()}))
            i += 1
            continue
        m = re.match(r"^(#{1,3})\s+(.+)$", line)
        if m:
            blocks.append((f"h{len(m.group(1))}", m.group(2).strip()))
            i += 1
            continue
        blocks.append(("p", line.strip()))
        i += 1
    return blocks


def render(blocks):
    out = []
    title_page = True
    in_toc = False
    body_started = False
    last_was_page_break = False

    for idx, (typ, val) in enumerate(blocks):
        if in_toc:
            if typ == "hr":
                out.append(section_break())
                in_toc = False
                last_was_page_break = True
            continue

        if typ == "hr":
            if not last_was_page_break:
                out.append(page_break())
                last_was_page_break = True
            title_page = False
            continue

        if typ == "h1":
            text = val
            if text == "目录":
                out.append(toc_field())
                in_toc = True
                last_was_page_break = False
                continue
            if text.startswith("第1章") and not body_started:
                body_started = True
            elif body_started:
                out.append(page_break())
            if text in {"摘  要", "摘 要"}:
                out.append(para("摘  要", style="Heading1", align="center", size=36, east="黑体",
                                bold=True, first_line=False, before=180, after=180))
            elif title_page:
                out.append(para(text, align="center", size=44, east="黑体", bold=True,
                                first_line=False, before=180, after=180))
            elif not body_started and re.search(r"[A-Za-z]", text) and not re.search(r"[\u4e00-\u9fff]", text):
                out.append(para(text, align="center", size=44, east="Times New Roman",
                                ascii_font="Times New Roman", first_line=False, before=180, after=180))
            elif text in {"原创性声明"}:
                out.append(para(text, align="center", size=36, east="黑体", bold=True, first_line=False))
            else:
                style = "Heading1" if text not in {"目录"} else None
                out.append(para(text, style=style, align="center", size=36, east="黑体", bold=True,
                                first_line=False))
            last_was_page_break = False
            continue

        if typ == "h2":
            out.append(para(val, style="Heading2", size=30, east="黑体", bold=True,
                            first_line=False))
            last_was_page_break = False
            continue
        if typ == "h3":
            out.append(para(val, style="Heading3", size=24, east="黑体", bold=True,
                            first_line=False))
            last_was_page_break = False
            continue
        if typ == "table":
            out.append(table_xml(val))
            last_was_page_break = False
            continue
        if typ == "image":
            out.append(image_xml(val["rel_id"], val["name"], val["width_emu"], val["height_emu"], val["doc_pr_id"]))
            last_was_page_break = False
            continue
        if typ == "code":
            for line in val.splitlines() or [""]:
                out.append(para(line, size=18, east="Courier New", ascii_font="Courier New",
                                first_line=False, left=420, spacing=True))
            last_was_page_break = False
            continue
        if typ == "p":
            text = val
            # Captions.
            if re.match(r"^图\d+-\d+", text) or re.match(r"^表\d+-\d+", text):
                out.append(para(text, align="center", size=21, east="黑体", bold=True, first_line=False))
            elif text.startswith("作者姓名："):
                out.append(para(text, align="center", size=30, east="宋体", first_line=False, before=180, after=180))
            elif text.startswith("关键词"):
                out.append(para(text, size=24, east="黑体", bold=True, first_line=False))
            elif text.startswith("Abstract") or text.startswith("Key words"):
                out.append(para(text, size=24, east="Times New Roman", ascii_font="Times New Roman",
                                first_line=False))
            elif re.match(r"^\d+\.\s+", text):
                out.append(para(text, size=24, first_line=False, left=420))
            elif text.startswith("学院：") or text.startswith("专业：") or text.startswith("班级：") or \
                 text.startswith("学生姓名：") or text.startswith("学号：") or text.startswith("指导教师：") or \
                 text.startswith("完成日期：") or text.startswith("题目：") or text.startswith("学生签名：") or \
                 text.startswith("日期："):
                out.append(para(text, align="center" if title_page else None, size=24, first_line=False))
            elif text == "英文文献" or text.endswith("文献"):
                out.append(para(text, size=21, east="黑体", bold=True, first_line=False))
            elif re.match(r"^[A-Z][A-Za-zÁ-ž ,\\.]+\\d{4}\\.", text):
                out.append(para(text, size=21, first_line=False, left=420, hanging=420))
            elif not body_started and not title_page and re.search(r"[A-Za-z]", text) and not re.search(r"[\u4e00-\u9fff]", text):
                out.append(para(text, size=24, east="Times New Roman", ascii_font="Times New Roman",
                                first_line=False))
            else:
                out.append(para(text, size=24, first_line=not title_page))
            last_was_page_break = False

    return "".join(out)


def styles_xml():
    return f'''<?xml version="1.0" encoding="UTF-8" standalone="yes"?>
<w:styles xmlns:w="{NS_W}">
  <w:style w:type="paragraph" w:default="1" w:styleId="Normal">
    <w:name w:val="Normal"/>
    <w:pPr><w:spacing w:line="360" w:lineRule="auto"/></w:pPr>
    <w:rPr><w:rFonts w:ascii="Times New Roman" w:hAnsi="Times New Roman" w:eastAsia="宋体"/><w:sz w:val="24"/></w:rPr>
  </w:style>
  <w:style w:type="paragraph" w:styleId="Heading1">
    <w:name w:val="heading 1"/><w:basedOn w:val="Normal"/><w:next w:val="Normal"/><w:qFormat/>
    <w:pPr><w:spacing w:before="120" w:after="120" w:line="360" w:lineRule="auto"/><w:outlineLvl w:val="0"/></w:pPr>
    <w:rPr><w:rFonts w:ascii="Times New Roman" w:hAnsi="Times New Roman" w:eastAsia="黑体"/><w:b/><w:sz w:val="36"/></w:rPr>
  </w:style>
  <w:style w:type="paragraph" w:styleId="Heading2">
    <w:name w:val="heading 2"/><w:basedOn w:val="Normal"/><w:next w:val="Normal"/><w:qFormat/>
    <w:pPr><w:spacing w:before="120" w:after="120" w:line="360" w:lineRule="auto"/><w:outlineLvl w:val="1"/></w:pPr>
    <w:rPr><w:rFonts w:ascii="Times New Roman" w:hAnsi="Times New Roman" w:eastAsia="黑体"/><w:b/><w:sz w:val="30"/></w:rPr>
  </w:style>
  <w:style w:type="paragraph" w:styleId="Heading3">
    <w:name w:val="heading 3"/><w:basedOn w:val="Normal"/><w:next w:val="Normal"/><w:qFormat/>
    <w:pPr><w:spacing w:before="120" w:after="120" w:line="360" w:lineRule="auto"/><w:outlineLvl w:val="2"/></w:pPr>
    <w:rPr><w:rFonts w:ascii="Times New Roman" w:hAnsi="Times New Roman" w:eastAsia="黑体"/><w:b/><w:sz w:val="24"/></w:rPr>
  </w:style>
</w:styles>'''


def document_xml(body_xml):
    return f'''<?xml version="1.0" encoding="UTF-8" standalone="yes"?>
<w:document xmlns:w="{NS_W}" xmlns:r="{NS_R}" xmlns:wp="{NS_WP}" xmlns:a="{NS_A}" xmlns:pic="{NS_PIC}">
  <w:body>
    {body_xml}
    {sect_pr(header_footer=True)}
  </w:body>
</w:document>'''


def header_xml():
    return f'''<?xml version="1.0" encoding="UTF-8" standalone="yes"?>
<w:hdr xmlns:w="{NS_W}" xmlns:r="{NS_R}">
  {para("成都理工大学****届学士学位论文（设计）", align="center", size=18, first_line=False, spacing=False)}
</w:hdr>'''


def footer_xml():
    return f'''<?xml version="1.0" encoding="UTF-8" standalone="yes"?>
<w:ftr xmlns:w="{NS_W}" xmlns:r="{NS_R}">
  <w:p><w:pPr><w:jc w:val="center"/></w:pPr>
    <w:r><w:rPr><w:rFonts w:ascii="Times New Roman" w:hAnsi="Times New Roman" w:eastAsia="宋体"/><w:sz w:val="18"/></w:rPr><w:fldChar w:fldCharType="begin"/></w:r>
    <w:r><w:rPr><w:rFonts w:ascii="Times New Roman" w:hAnsi="Times New Roman" w:eastAsia="宋体"/><w:sz w:val="18"/></w:rPr><w:instrText xml:space="preserve">PAGE</w:instrText></w:r>
    <w:r><w:rPr><w:rFonts w:ascii="Times New Roman" w:hAnsi="Times New Roman" w:eastAsia="宋体"/><w:sz w:val="18"/></w:rPr><w:fldChar w:fldCharType="end"/></w:r>
  </w:p>
</w:ftr>'''


def image_display_size_emu(path: Path):
    with Image.open(path) as im:
        width_px, height_px = im.size
    max_width = int(5.75 * 914400)
    max_height = int(4.65 * 914400)
    width_emu = max_width
    height_emu = int(width_emu * height_px / width_px)
    if height_emu > max_height:
        height_emu = max_height
        width_emu = int(height_emu * width_px / height_px)
    return width_emu, height_emu


def prepare_images(blocks, md_path: Path):
    image_rels = []
    image_files = {}
    image_idx = 1
    for typ, val in blocks:
        if typ != "image":
            continue
        raw_path = val["path"]
        src_path = Path(raw_path)
        if not src_path.is_absolute():
            src_path = (md_path.parent / src_path).resolve()
        if not src_path.exists():
            raise FileNotFoundError(f"image not found: {src_path}")
        ext = src_path.suffix.lower().lstrip(".")
        if ext == "jpg":
            ext = "jpeg"
        if ext not in {"png", "jpeg"}:
            raise ValueError(f"unsupported image type: {src_path}")
        rel_id = f"rIdImage{image_idx}"
        media_name = f"image{image_idx}.{ext}"
        width_emu, height_emu = image_display_size_emu(src_path)
        val.update(
            {
                "rel_id": rel_id,
                "name": val["alt"] or src_path.name,
                "doc_pr_id": image_idx,
                "width_emu": width_emu,
                "height_emu": height_emu,
            }
        )
        image_rels.append(
            f'<Relationship Id="{rel_id}" '
            'Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/image" '
            f'Target="media/{media_name}"/>'
        )
        image_files[f"word/media/{media_name}"] = src_path.read_bytes()
        image_idx += 1
    return image_rels, image_files


def write_docx(md_path: Path, out_path: Path):
    blocks = parse_markdown(md_path.read_text(encoding="utf-8"))
    image_rels, image_files = prepare_images(blocks, md_path)
    body = render(blocks)
    files = {
        "[Content_Types].xml": '''<?xml version="1.0" encoding="UTF-8" standalone="yes"?>
<Types xmlns="http://schemas.openxmlformats.org/package/2006/content-types">
  <Default Extension="rels" ContentType="application/vnd.openxmlformats-package.relationships+xml"/>
  <Default Extension="xml" ContentType="application/xml"/>
  <Default Extension="png" ContentType="image/png"/>
  <Default Extension="jpeg" ContentType="image/jpeg"/>
  <Override PartName="/word/document.xml" ContentType="application/vnd.openxmlformats-officedocument.wordprocessingml.document.main+xml"/>
  <Override PartName="/word/styles.xml" ContentType="application/vnd.openxmlformats-officedocument.wordprocessingml.styles+xml"/>
  <Override PartName="/word/settings.xml" ContentType="application/vnd.openxmlformats-officedocument.wordprocessingml.settings+xml"/>
  <Override PartName="/word/header1.xml" ContentType="application/vnd.openxmlformats-officedocument.wordprocessingml.header+xml"/>
  <Override PartName="/word/footer1.xml" ContentType="application/vnd.openxmlformats-officedocument.wordprocessingml.footer+xml"/>
  <Override PartName="/docProps/core.xml" ContentType="application/vnd.openxmlformats-package.core-properties+xml"/>
  <Override PartName="/docProps/app.xml" ContentType="application/vnd.openxmlformats-officedocument.extended-properties+xml"/>
</Types>''',
        "_rels/.rels": '''<?xml version="1.0" encoding="UTF-8" standalone="yes"?>
<Relationships xmlns="http://schemas.openxmlformats.org/package/2006/relationships">
  <Relationship Id="rId1" Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/officeDocument" Target="word/document.xml"/>
  <Relationship Id="rId2" Type="http://schemas.openxmlformats.org/package/2006/relationships/metadata/core-properties" Target="docProps/core.xml"/>
  <Relationship Id="rId3" Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/extended-properties" Target="docProps/app.xml"/>
</Relationships>''',
        "word/_rels/document.xml.rels": f'''<?xml version="1.0" encoding="UTF-8" standalone="yes"?>
<Relationships xmlns="http://schemas.openxmlformats.org/package/2006/relationships">
  <Relationship Id="rIdStyles" Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/styles" Target="styles.xml"/>
  <Relationship Id="rIdHeader1" Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/header" Target="header1.xml"/>
  <Relationship Id="rIdFooter1" Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/footer" Target="footer1.xml"/>
  <Relationship Id="rIdSettings" Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/settings" Target="settings.xml"/>
  {chr(10).join(image_rels)}
</Relationships>''',
        "word/document.xml": document_xml(body),
        "word/styles.xml": styles_xml(),
        "word/header1.xml": header_xml(),
        "word/footer1.xml": footer_xml(),
        "word/settings.xml": f'''<?xml version="1.0" encoding="UTF-8" standalone="yes"?>
<w:settings xmlns:w="{NS_W}"><w:updateFields w:val="true"/></w:settings>''',
        "docProps/core.xml": '''<?xml version="1.0" encoding="UTF-8" standalone="yes"?>
<cp:coreProperties xmlns:cp="http://schemas.openxmlformats.org/package/2006/metadata/core-properties" xmlns:dc="http://purl.org/dc/elements/1.1/" xmlns:dcterms="http://purl.org/dc/terms/" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"><dc:title>支持多写的OLTP数据库一体机页面亲和性关键技术研究</dc:title><dc:creator>Hybrid_Cloud_MP</dc:creator></cp:coreProperties>''',
        "docProps/app.xml": '''<?xml version="1.0" encoding="UTF-8" standalone="yes"?>
<Properties xmlns="http://schemas.openxmlformats.org/officeDocument/2006/extended-properties"><Application>Codex OOXML Generator</Application></Properties>''',
    }
    with zipfile.ZipFile(out_path, "w", zipfile.ZIP_DEFLATED) as zf:
        for name, data in files.items():
            zf.writestr(name, data)
        for name, data in image_files.items():
            zf.writestr(name, data)


def main():
    if len(sys.argv) != 3:
        print("usage: markdown_to_bachelor_docx.py input.md output.docx", file=sys.stderr)
        return 2
    write_docx(Path(sys.argv[1]), Path(sys.argv[2]))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
