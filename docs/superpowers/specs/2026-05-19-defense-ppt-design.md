# Defense PPT Design

## Source Materials

- Paper: `docs/支持多写OLTP数据库一体机页面亲和性论文_修改版.docx`
- Template: `ppt/一抹蓝.pptx`

## Approved Direction

Use a 20-slide technical deep-dive deck with a problem-driven closed-loop narrative.

The deck should explain why shared-storage multi-writer OLTP databases suffer from remote ownership transfer, then show how the proposed affinity pipeline turns transaction co-access into tuple placement and migration decisions. Experiments should prove the pipeline progressively improves locality and remains stable.

## Slide Outline

1. Cover
2. Contents
3. Research background
4. Core problem
5. Research goals and contributions
6. WookongDB-MP and Lazy Release background
7. Limitations of existing methods
8. Overall affinity pipeline
9. Tuple identifier and transaction sampling
10. Affinity graph construction and decay
11. ParMETIS repartitioning
12. AssignmentTable publication
13. Tuple migration protocol
14. Batched migration optimization
15. Correctness constraints
16. Experimental setup
17. RQ1 main performance results
18. RQ2/RQ3 stability and locality conversion
19. RQ4 ablation studies
20. Summary and outlook

## Visual Style

Reuse the blue CDUT template style, keep pages clean, and use large titles with concise bullets. Prefer paper figures for architecture, pipeline, protocol, and experiment slides. Use short oral-defense phrasing rather than copying long thesis paragraphs.

## Output

Create a new editable PowerPoint file under `ppt/`, leaving the original template unchanged.
