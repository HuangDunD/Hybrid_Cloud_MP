#!/usr/bin/env python3
"""Replace thesis figures inside an existing docx while preserving document layout."""
import argparse
import io
import zipfile
from pathlib import Path
import xml.etree.ElementTree as ET

from PIL import Image


NS = {
    "w": "http://schemas.openxmlformats.org/wordprocessingml/2006/main",
    "r": "http://schemas.openxmlformats.org/officeDocument/2006/relationships",
    "wp": "http://schemas.openxmlformats.org/drawingml/2006/wordprocessingDrawing",
    "a": "http://schemas.openxmlformats.org/drawingml/2006/main",
    "rel": "http://schemas.openxmlformats.org/package/2006/relationships",
}

for prefix, uri in NS.items():
    ET.register_namespace(prefix, uri)


FIGURE_REPLACEMENTS = {
    "word/media/image2.png": "docs/photos/fig5_1_throughput_comparison.png",
    "word/media/image3.png": "docs/photos/fig5_2_access_ratio_comparison.png",
    "word/media/image4.png": "docs/photos/fig5_3_migration_status.png",
    "word/media/image5.png": "docs/photos/fig5_4_ownership_transfer_breakdown.png",
    "word/media/image6.png": "docs/photos/fig5_5_parmetis_edgecut_timeseries.png",
    "word/media/image7.png": "docs/photos/fig5_6_access_ratio_timeseries.png",
    "word/media/image8.png": "docs/photos/fig5_7_migration_timeseries.png",
}


def image_size_emu(image_bytes: bytes, width_emu: int) -> tuple[int, int]:
    with Image.open(io.BytesIO(image_bytes)) as im:
        width_px, height_px = im.size
    return width_emu, int(width_emu * height_px / width_px)


def relationship_targets(rels_xml: bytes) -> dict[str, str]:
    root = ET.fromstring(rels_xml)
    out = {}
    for rel in root:
        if rel.attrib.get("Type", "").endswith("/image"):
            target = rel.attrib["Target"]
            if not target.startswith("word/"):
                target = "word/" + target
            out[rel.attrib["Id"]] = target
    return out


def update_image_extents(document_xml: bytes, rel_targets: dict[str, str], replacement_bytes: dict[str, bytes]) -> bytes:
    root = ET.fromstring(document_xml)
    for drawing in root.findall(".//w:drawing", NS):
        blip = drawing.find(".//a:blip", NS)
        if blip is None:
            continue
        rel_id = blip.attrib.get(f"{{{NS['r']}}}embed")
        target = rel_targets.get(rel_id)
        if target not in replacement_bytes:
            continue

        extent = drawing.find(".//wp:extent", NS)
        if extent is None:
            continue
        current_width = int(extent.attrib["cx"])
        width_emu, height_emu = image_size_emu(replacement_bytes[target], current_width)
        extent.attrib["cx"] = str(width_emu)
        extent.attrib["cy"] = str(height_emu)

        for a_ext in drawing.findall(".//a:xfrm/a:ext", NS):
            a_ext.attrib["cx"] = str(width_emu)
            a_ext.attrib["cy"] = str(height_emu)

    return ET.tostring(root, encoding="utf-8", xml_declaration=True)


def optimize_docx(input_docx: Path, output_docx: Path, replacements: dict[str, str]):
    replacement_bytes = {}
    for media_name, source_name in replacements.items():
        source = Path(source_name)
        if not source.exists():
            raise FileNotFoundError(source)
        replacement_bytes[media_name] = source.read_bytes()

    with zipfile.ZipFile(input_docx, "r") as zin:
        rel_targets = relationship_targets(zin.read("word/_rels/document.xml.rels"))
        updated_document = update_image_extents(zin.read("word/document.xml"), rel_targets, replacement_bytes)

        output_docx.parent.mkdir(parents=True, exist_ok=True)
        with zipfile.ZipFile(output_docx, "w", zipfile.ZIP_DEFLATED) as zout:
            for item in zin.infolist():
                data = zin.read(item.filename)
                if item.filename == "word/document.xml":
                    data = updated_document
                elif item.filename in replacement_bytes:
                    data = replacement_bytes[item.filename]
                zout.writestr(item, data)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("input_docx", type=Path)
    parser.add_argument("output_docx", type=Path)
    args = parser.parse_args()
    optimize_docx(args.input_docx, args.output_docx, FIGURE_REPLACEMENTS)
    print(args.output_docx)


if __name__ == "__main__":
    main()
