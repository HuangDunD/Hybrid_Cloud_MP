# Defense PPT Story Logic Optimization: 10 Rounds

## Objective

Optimize the defense deck for story completeness and logical flow. The deck should read as a proof, not a collection of implementation slides.

## Story Spine

The presentation should answer five oral-defense questions in order:

1. **Is the problem real?** The system boundary and Lazy Release path show why page ownership transfer enters the transaction critical path.
2. **Why is a new mechanism needed?** Lazy Release, Schism, and static routing each miss the online-learning + low-intrusion-publication + background-physicalization loop.
3. **How does the mechanism close the loop?** Committed-transaction sampling builds the affinity graph; ParMETIS assigns logical targets; AssignmentTable publishes snapshots; migration turns targets into physical locality.
4. **Why is it safe?** BLink is the linearization point, epoch prevents stale plans, and five invariants cover visibility, locking, failure, and retry.
5. **How do experiments support the claim?** RQ1-RQ4 map to effectiveness, stability, locality conversion, and design-choice explanations.

## Ten Content Rounds

1. Rewrite cover bullets from mechanism listing into problem/idea/mechanism/evidence.
2. Replace the overview with five defense questions and evidence mapping.
3. Tighten research-object slide around the exact system boundary.
4. Turn the bottleneck slide into a causal chain: related tuples -> remote page path -> latency.
5. Make the baselines slide produce explicit design constraints.
6. Use the concept slide to prevent wrong interpretations of AssignmentTable and EdgeCut.
7. Make sampling/partitioning slides explain how workload observations become target nodes.
8. Make publication/migration/correctness slides explain low intrusion and safety.
9. Make experiment pages explicitly map RQs back to claims.
10. Make the summary page answer the five questions and separate proven claims from boundaries.

## Verification

- Generated deck remains 20 slides.
- The verifier requires story terms: 五个问题, 证据链, 设计约束, 低侵入, 故事回扣.
- PDF and PNG rendering must still produce 20 nonblank pages.
