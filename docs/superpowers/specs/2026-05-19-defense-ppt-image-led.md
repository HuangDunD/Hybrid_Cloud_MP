# Defense PPT Image-Led Redesign

## Decision

Rebuild the defense deck as a thesis-image-led 20-slide technical presentation. Use figures and tables from `docs/支持多写OLTP数据库一体机页面亲和性论文_修改版.docx` as the visual center of the deck, while keeping each slide focused on one defensible claim.

## Design Rules

- Each technical slide should be led by a paper figure, table, or experiment chart.
- Page text is limited to a title, one core conclusion, and compact speaking points tied to paper evidence.
- Prefer these layouts:
  - large figure with a right-side takeaway panel;
  - full-width figure with a bottom conclusion;
  - two paper figures side by side for comparisons;
  - native table when the thesis table is the primary evidence.
- Keep the blue/white CDUT tone, but reduce decorative backgrounds and avoid large text-heavy cards.
- Do not redraw mechanisms unless needed to label or highlight the original paper figure.

## Slide Outline

1. Cover
2. Defense storyline: problem, mechanism, correctness, evidence
3. Research object and one-sentence contribution
4. Bottleneck: remote ownership transfer on the transaction critical path
5. Three classes of existing approaches and their limits
6. Concept decoupling: logical target, current owner, physical RID, and remote-access ratio
7. Transaction sampling and tuple affinity graph
8. ParMETIS partitioning objective and database semantics
9. Distributed graph partitioning and ParMETIS adaptive repartition
10. Snapshot AssignmentTable publication with epoch protection
11. Tuple migration protocol and BLink linearization point
12. Correctness invariants
13. Batched migration optimization
14. Experiment setup and four research questions
15. RQ1: throughput and tail-latency figures
16. RQ1: same-condition result table
17. RQ2: six-hour stability experiment
18. RQ3: migration completion versus locality conversion
19. RQ4: ablation experiment figures
20. Limitations, summary, and outlook

## Output

Overwrite `ppt/支持多写OLTP数据库一体机页面亲和性答辩.pptx` and verify with LibreOffice PDF/PNG rendering.
