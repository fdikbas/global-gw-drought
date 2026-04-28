# Run order for Paper 1

## 1) Upstream anomaly workflow
Run:

- `src/GlobalGWDrought.Anomaly.Calculation.HybridQC.Complete.No.Trimming.2025.12.03.v1.py`

This stage creates the anomaly contract used by Paper 1, including the QC-clean anomaly and decadal products.

## 2) Post-analysis workflow
Run:

- `src/GlobalGWDrought.Post.Analysis.2026.04.01.v1_p0_p100_PATCH_AQUIFER_PERSISTENCE.py`

This stage computes the Paper 1 analytical summaries used for screening and interpretation, including trend, persistence, asymmetry, hotspot, and regime outputs.

## 3) Archive only the Paper 1 outputs
When preparing a release, archive only the outputs actually used in the Paper 1 manuscript.

Do not move second-paper content into this repository release unless the manuscript scope changes.
