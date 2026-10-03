# spec/

The decisions behind the pipeline, written **before** the code (Phases 1-2) and kept up to date after it.

| File | Phase | What it answers |
|---|---|---|
| `requirements-template.md` → `requirements.md` | 1 | Who needs which numbers, by when, how correct; user stories with acceptance criteria; SLAs; what is out of scope |
| `design-template.md` → `design.md` | 2 | The architecture (sources → lake → silver → gold → publish → dashboard), the layers and their contracts, idempotency, the run schedule, the tools chosen (and the RAM budget) |
| `adr/0000-template.md` → `adr/000N-*.md` | 2, 7 | One architecture decision per file: context, options, decision, consequences |

The reference versions are on the `solution` branch (`git show phase-2-end:spec/design.md`). Write yours first.
