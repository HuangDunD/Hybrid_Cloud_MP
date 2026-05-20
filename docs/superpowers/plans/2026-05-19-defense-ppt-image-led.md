# Defense PPT Image-Led Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Rebuild the defense PPT as a 20-slide thesis-image-led technical deck so paper figures and experiment charts dominate the slides, with concise supporting text tied to defensible claims.

**Architecture:** `ppt/generate_defense_ppt.py` extracts figures from the thesis docx, builds a 20-slide image-led PowerPoint, and saves over the current defense deck. `ppt/verify_defense_ppt.py` checks the image-led title sequence, slide count, placeholder cleanup, paper-image usage, and required technical evidence phrases.

**Tech Stack:** Python 3, `python-docx`, `python-pptx`, Pillow, LibreOffice, Poppler.

---

### Task 1: Update Verification

**Files:**
- Modify: `ppt/verify_defense_ppt.py`

- [x] Replace the expected title sequence with the image-led outline.
- [x] Require at least 15 embedded pictures across the deck.
- [x] Keep checks for 20 slides, no placeholders, required evidence phrases, and in-bounds shapes.

### Task 2: Rebuild Image-Led Generator

**Files:**
- Modify: `ppt/generate_defense_ppt.py`

- [x] Restore docx figure extraction into `ppt/generated_assets/`.
- [x] Use large figure-first layouts for mechanism slides.
- [x] Add a defense storyline slide and a ParMETIS objective slide to make the content chain more explicit.
- [x] Use native tables for paper tables that are not images.
- [x] Keep only short conclusion and speaking points on each slide.

### Task 3: Render Review

**Files:**
- Output: `ppt/支持多写OLTP数据库一体机页面亲和性答辩.pptx`
- Output: `/tmp/ppt-image-led-render/`

- [x] Run the generator.
- [x] Run the verifier.
- [x] Convert to PDF using LibreOffice.
- [x] Render PNG pages and inspect total contact sheet plus key slides.
