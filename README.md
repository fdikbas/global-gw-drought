# global-gw-drought

Observation-based screening of persistent groundwater depletion using annual groundwater anomalies.

## Scope

This repository contains the **minimum reproducible code package for Paper 1** associated with the manuscript:

**Identifying persistently depleting aquifers from global groundwater anomalies**

The repository is intentionally limited to the workflow required for **Paper 1 only**. Material reserved for the planned second paper is intentionally excluded.

## Included components

- anomaly construction from annual groundwater records
- HybridQC screening at the level-year stage
- station-level annual anomaly products
- station-level decadal summaries
- aquifer-linked decadal aggregation
- post-analysis for Paper 1 screening outputs, including:
  - aquifer trend summaries
  - persistence summaries
  - asymmetry summaries
  - hotspot diagnostics
  - station regime classification summaries

## Not included

This repository **does not include standalone figure-generation helper scripts** created later during manuscript polishing. The post-analysis workflow remains included because it produces the analytical summaries used in Paper 1.

## Suggested repository citation

Dikbaş, F. (2026). *global-gw-drought-paper1: Observation-based screening of persistent groundwater depletion from global groundwater anomalies* [Code repository]. GitHub. URL to be added after repository creation.

## Recommended repository description

Paper 1 code for global groundwater anomaly screening, aquifer depletion trends, persistence, asymmetry, and hotspot diagnostics.

## Recommended topics

`groundwater` `drought` `hydrology` `aquifer` `time-series` `water-resources` `quality-control` `trend-analysis`

## Repository structure

```text
src/        core Paper 1 scripts
docs/       submission-ready text blocks and repository notes
data/       placeholder notes for required inputs
results/    placeholder notes for generated outputs
```

## Run order

1. Run the anomaly calculation / HybridQC script.
2. Confirm that the required anomaly and decadal outputs have been written.
3. Run the post-analysis script.
4. Archive or version the exact Paper 1 outputs used in the manuscript.

See `docs/RUN_ORDER.md` and `docs/DATA_ACCESSIBILITY_STATEMENT_TEMPLATE.md`.

## Inputs expected by the scripts

The scripts expect local input files such as groundwater annual records and aquifer shapefiles. Because these source datasets may have licensing, size, or redistribution constraints, they are **not bundled in this repository**.

## Groundwater submission note

For submission to **Groundwater**, this repository supports a repository-linked data/code accessibility statement. A suggested version is provided in `docs/DATA_ACCESSIBILITY_STATEMENT_TEMPLATE.md`.
