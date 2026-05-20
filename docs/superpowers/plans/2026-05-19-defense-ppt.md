# Defense PPT Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Generate an editable 20-slide defense PowerPoint from the approved thesis outline and the existing blue CDUT template.

**Architecture:** A Python generation script reads the thesis `.docx`, extracts figure images and selected metadata, loads the existing `.pptx` template, rewrites the first 20 slides with defense content, and saves a new `.pptx` without modifying the template. Verification reloads the generated deck with `python-pptx` and checks slide count and key text.

**Tech Stack:** Python 3, `python-docx`, `python-pptx`, Pillow, zipfile validation.

---

### Task 1: Extract thesis figures and metadata

**Files:**
- Create: `ppt/generate_defense_ppt.py`
- Output: `ppt/generated_assets/`

- [ ] Read `docs/支持多写OLTP数据库一体机页面亲和性论文_修改版.docx` with `python-docx`.
- [ ] Extract each inline image to `ppt/generated_assets/figXX.<ext>`.
- [ ] Record adjacent figure captions where available so slides can use paper figures such as architecture, affinity pipeline, migration protocol, and experiment charts.

### Task 2: Generate the 20-slide deck

**Files:**
- Modify by script output only: `ppt/支持多写OLTP数据库一体机页面亲和性答辩.pptx`

- [ ] Load `ppt/一抹蓝.pptx`.
- [ ] Keep the first 20 slides and remove the template instruction slide.
- [ ] Clear placeholder text from template slides while preserving theme/background elements.
- [ ] Add title, subtitle, concise bullets, key metrics, and extracted figures according to the approved 20-slide outline.
- [ ] Use readable Chinese typography and a blue/white visual hierarchy consistent with the template.

### Task 3: Verify output

**Files:**
- Read: `ppt/支持多写OLTP数据库一体机页面亲和性答辩.pptx`

- [ ] Validate the `.pptx` ZIP package has no corrupt entries.
- [ ] Reload with `python-pptx` and confirm it has 20 slides.
- [ ] Print slide titles to confirm the generated deck follows the approved page sequence.
