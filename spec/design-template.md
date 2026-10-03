# Design: Pipewright

> Phase 2. Copy to `design.md`. Diagrams in Mermaid or ASCII are fine.

## 1. Architecture
Sources → ingestion → lake (bronze) → silver → gold → checks → publish → dashboard. One diagram, one paragraph per box.

## 2. Layers and their contracts
| Layer | Where | Grain | Who writes it | Who may read it | Immutable? |
|---|---|---|---|---|---|

## 3. Ingestion
Extraction windows, cursors, pagination, retries, idempotency (what happens when a day is re-run?), PII handling,
manifests.

## 4. Transformations
The silver and gold models, their keys, which rules from `contract/transform-spec.md` each implements.

## 5. Quality and the publish gate
Which checks run where (dbt tests, contract checks), severities, what happens when one fails.

## 6. Orchestration and schedule
Tasks, dependencies, retries, time limits, what happens when a file is late.

## 7. Tools and footprint
Every tool, its version, why it was chosen over the alternatives, and the RAM budget on an 8 GB laptop.

## 8. Risks
