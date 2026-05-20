# Defense PPT Redesign Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Rebuild the defense deck as a stronger 20-slide technical presentation with redrawn mechanisms, clearer claims, and actual LibreOffice render verification.

**Architecture:** Replace the card-heavy generator with a cleaner technical-deck generator in `ppt/generate_defense_ppt.py`. The generator uses native PowerPoint shapes for system diagrams, flows, ownership paths, tables, and charts, while using thesis figures only as optional references.

**Tech Stack:** Python 3, `python-pptx`, Pillow, LibreOffice `soffice`, Poppler `pdftoppm`.

---

### Task 1: Strengthen Verification

**Files:**
- Modify: `ppt/verify_defense_ppt.py`

- [ ] Check that the generated deck has 20 slides and the redesigned title sequence.
- [ ] Check that there are no placeholder strings, no out-of-bounds shapes, and no blank slides.
- [ ] Check that slides 7-12 contain core technical keywords: `亲和图`, `ParMETIS`, `AssignmentTable`, `线性化点`, `批量迁移`, and `不变量`.

### Task 2: Rebuild Generator

**Files:**
- Modify: `ppt/generate_defense_ppt.py`

- [ ] Replace large rounded template pages with a technical layout system: compact header, claim band, central diagram/chart area, and bottom conclusion.
- [ ] Add helper functions for native diagrams: node boxes, arrows, swimlanes, path comparison, mini charts, and result cards.
- [ ] Implement the 20-slide redesigned outline from `docs/superpowers/specs/2026-05-19-defense-ppt-redesign.md`.
- [ ] Use concise technical claims and defense-ready wording for each slide.

### Task 3: Generate And Verify

**Files:**
- Output: `ppt/支持多写OLTP数据库一体机页面亲和性答辩.pptx`
- Output: `/tmp/ppt-redesign-render/`

- [ ] Run `python3 ppt/generate_defense_ppt.py`.
- [ ] Run `python3 ppt/verify_defense_ppt.py ppt/支持多写OLTP数据库一体机页面亲和性答辩.pptx`.
- [ ] Convert the PPT to PDF with LibreOffice.
- [ ] Render PDF pages to PNG and inspect a contact sheet plus high-risk pages.
