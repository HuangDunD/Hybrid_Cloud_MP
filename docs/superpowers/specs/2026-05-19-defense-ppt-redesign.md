# Defense PPT Redesign

## Decision

Rebuild the defense deck as a technical defense presentation. Keep only the blue/white CDUT visual tone from `ppt/一抹蓝.pptx`; do not reuse the template's large card-heavy page layouts.

## Problems In The Previous Deck

- Content read like a compressed thesis summary instead of a defense argument.
- Too many thesis figures were pasted directly, making mechanism pages hard to read.
- Layout relied on large rounded cards and repeated placeholder structure.
- Experiment slides did not consistently state problem, evidence, explanation, and conclusion.

## Redesign Direction

Use a problem-driven technical argument:

1. Shared-storage multi-writer OLTP improves write parallelism.
2. Related tuples distributed across pages/nodes create remote lock waits, page pushes, and ownership transfers.
3. Lazy Release and static/hash placement cannot actively gather runtime co-accessed tuples.
4. The proposed method converts committed transaction co-access into a tuple affinity graph.
5. ParMETIS creates target assignments; AssignmentTable publishes them without blocking the foreground path.
6. A rate-limited migration worker and batching protocol gradually turn logical assignment into physical locality.
7. Experiments prove performance, stability, locality conversion, and ablation-level mechanism contribution.

## Slide Outline

1. Cover
2. Defense route and one-sentence contribution
3. Research object: shared-storage multi-writer OLTP appliance
4. Bottleneck: remote ownership transfer enters the transaction critical path
5. Why existing approaches are insufficient
6. Core idea: turn runtime co-access into page locality
7. Transaction sampling and tuple affinity graph
8. Graph partitioning objective and ParMETIS role
9. AssignmentTable: publish target ownership preference
10. Migration protocol: make logical affinity physical
11. Batched migration: reduce repeated page-lock work
12. Correctness: invariants and linearization point
13. Implementation map in WookongDB-MP
14. Experiment design and evaluation questions
15. RQ1: throughput and tail latency
16. RQ2: six-hour stability
17. RQ3: locality conversion and migration execution
18. RQ4: ablation studies
19. Limitations and why they are acceptable
20. Contributions and outlook

## Visual Rules

- Rebuild diagrams with native PPT shapes wherever possible.
- Use original paper figures only when a detailed architecture/protocol reference is useful; otherwise redraw simplified versions.
- Every technical slide must have one central claim and one visual proof or mechanism.
- Keep text dense enough for a technical defense, but avoid paragraph-like bullet blocks.
- Use blue/white with restrained green/orange highlights for positive results and bottlenecks.

## Output

Overwrite `ppt/支持多写OLTP数据库一体机页面亲和性答辩.pptx` with the redesigned deck, keeping the old generation script recoverable in git status.
