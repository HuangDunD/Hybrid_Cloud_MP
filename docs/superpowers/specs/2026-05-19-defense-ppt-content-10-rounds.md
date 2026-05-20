# Defense PPT Content Optimization: 10 Rounds

## Objective

Continue optimizing the defense PPT for 10 content-focused rounds. The output remains `ppt/支持多写OLTP数据库一体机页面亲和性答辩.pptx`, generated from `ppt/generate_defense_ppt.py`.

## Round Checklist

1. **Main line:** sharpen the cover and storyline from generic mechanism wording into a proof chain: bottleneck -> online affinity -> observable benefit.
2. **Research object:** clarify the three-tier system boundary and make AssignmentTable part of the remote service/control plane.
3. **Bottleneck:** state that remote cost includes lock wait, page push, and WAL persistence constraints.
4. **Baselines:** make Schism fairness explicit with the 2009.3万-transaction training budget and 120 s apply window.
5. **Concepts:** separate EdgeCut from remote-access ratio and cite the 12 426 -> 44 092 cycle experiment counterexample.
6. **Modeling:** emphasize committed-transaction sampling, 50 ms aggregation, edge threshold/decay/TTL, and tuple hotness weights.
7. **Partitioning and publication:** add ubvec, maxChangedVerticesRatio, epoch barrier, UDS sidecar isolation, and stale-plan protection.
8. **Migration and batching:** strengthen BLink/Rid revalidation, nonblocking DataItem skip, page-pool reuse, and backlog/failure interpretation.
9. **Experiments:** make data sources and scope visible: summary.txt, MP-Router logs, affinityTimeseries.csv, SmallBank-Aff, WAL-off, single-run caveat.
10. **Summary and defense boundary:** turn limitations into explicit defense boundaries and tie future work to waitLockSuccess/page-push reduction.

## Verification Requirements

- PPT has exactly 20 slides.
- Required technical phrases include AssignmentTable, EdgeCut, maxChangedVerticesRatio, summary.txt, SmallBank-Aff, waitLockSuccess, WAL, and core experiment numbers.
- LibreOffice PDF export renders all 20 pages.
- PNG rendering produces 20 nonblank pages for visual review.
