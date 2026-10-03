# contract/

The single source of truth for "right". Both tracks, the shared package, the checks and the dashboard read it;
nothing in it is track-specific.

| Path | What it is |
|---|---|
| `data-contract.yaml` | **Version 1.0.0** (1.1.0 from Phase 7). Every source (endpoint or file pattern, partition key, cursor, primary key, required fields, PII to drop), the enums, the column names and types of every gold table (enforced: both tracks must produce exactly these, in this order), and the data-quality checks with their severities (`error` = do not publish). |
| `transform-spec.md` | Every rule in words, in the order it is applied: parsing (times, IST dates, paise), silver (one clean row per entity), gold dimensions and facts, the five reports. Every `TODO` hint points to a section here. |
| `seeds/city_map.csv` | The spelling variants of every city → canonical city + region (a dbt seed; the PySpark track reads the same file). |
| `fixture/source/` | A 3-day copy of the source (2026-06-01..03), small enough to check by hand. Every suite runs on it. |
| `fixture/incident-report.json` | A failed data-quality report, the input of the AI explainer's tests (Phase 6). |
| `golden/fixture/`, `golden/fixture-silver/` | The expected gold and silver tables of the fixture, as canonical CSV. |
| `golden/v1.0/` (+ `v1.1/`) | The expected reports of the full history, as canonical CSV + fingerprints of the big tables. |
| `source-manifest-1.0.json` (+ `1.1`) | The sha256 of every file the seeded generator writes: `make source` refuses to continue if your generated data differs. |
| `history/` (from 1.1) | Previous contract versions, used by the backward-compatibility check. |

Canonical CSV (`scripts/parity.py export`): rows sorted by the table key, dates `YYYY-MM-DD`, timestamps
`YYYY-MM-DD HH:MM:SS` UTC, booleans `true`/`false`, doubles with 12 significant digits, NULL as empty. Integers
and strings must match exactly; doubles to 1e-9 relative.

To change a rule: open an issue, change `transform-spec.md`, bump the contract version, regenerate the golden
files from **both** tracks and check they agree, and add a CHANGELOG entry. A changed number without a new rule
is a bug.
