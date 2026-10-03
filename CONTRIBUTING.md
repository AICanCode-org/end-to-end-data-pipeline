# Contributing

Thanks for helping! Pipewright is a teaching project, so clarity beats cleverness.

## Workflow
1. Open an issue first (templates: *Data bug*, *User story*). Quote the rule in `contract/transform-spec.md` or the
   check in `contract/data-contract.yaml` that your change is about.
2. Branch from `main`: `feat/...`, `fix/...`, `docs/...`, `test/...`, `ci/...`.
3. Commit in small steps with [Conventional Commits](https://www.conventionalcommits.org/): `feat(sql): ...`,
   `fix(pipewright): ...`, `test(pyspark): ...`, `docs: ...`, `ci: ...`.
4. Before you push: `scripts/check-all.sh` (lint + every suite) and, if any transformation changed,
   `scripts/check-all.sh --full` (both tracks on the full data, equal to the golden tables and to each other).
5. Open a pull request and fill in the template, including the **Numbers check** and **AI assistance** sections.

## Rules that keep the numbers right
- A gold number only changes together with its rule (`transform-spec.md`), a contract version bump, new golden
  files (`contract/golden/`) and a CHANGELOG entry. Both tracks change in the same PR.
- Money is integer paise; business dates are IST calendar days; never use the machine's time zone.
- Ingestion is idempotent: re-running a day replaces its partition, it never appends a second copy.
- PII (`email`, `phone`) is dropped before anything is written. It never reaches the lake, a log or a prompt.
- Tests are never edited to make them pass. If a test is wrong, open an issue quoting the spec.

## Style
- Python: ruff (`ruff.toml`), type hints on public functions, docstrings that say *why*.
- SQL: sqlfluff with the dbt templater (`tracks/sql/.sqlfluff`), one CTE per step, lower-case keywords.
- Shell: `set -euo pipefail`, shellcheck clean.
