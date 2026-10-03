## What & why
<!-- One or two sentences. Link the issue: "Closes #12" -->

## Track(s)
- [ ] SQL · dbt  - [ ] Python · PySpark  - [ ] Shared (ingestion / checks / DAG / dashboard / contract / docs / CI)

## How I tested it
<!-- Commands you ran and their result, e.g. `make test-sql` → 23 passed; `make parity` → PARITY RUN OK -->

## Numbers check
<!-- Did any gold table change? Run `python3 scripts/parity.py compare <warehouse> contract/golden/fixture`.
     If a number changed: which table, why, and is the rule in contract/transform-spec.md updated in this PR
     (with a contract version bump and new golden files)? A changed number without a reason is a bug.
     "No numbers changed" is a fine answer. -->

## AI assistance
<!-- Which tool, what it drafted, what you changed or rejected, how you verified it. "None" is fine.
     Add a matching entry to AI_LOG.md. -->

## Self-review checklist
- [ ] CI is green
- [ ] Tests cover the new behaviour (including at least one bad-data case: a duplicate, a late update, a bad value)
- [ ] Re-running the same date gives the same tables (idempotent), and nothing reads lake partitions after the run date
- [ ] Money stays in integer paise; times are converted with the spec's rules, not the machine's time zone
- [ ] No secrets, tokens, `.env` files or personal data (emails, phone numbers) in commits, logs or prompts
- [ ] Docs / CHANGELOG / runbook updated if an operator of the pipeline would notice the change
- [ ] I can explain every line of this diff without the AI's help
