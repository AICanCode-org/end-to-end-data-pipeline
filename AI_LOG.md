# AI log

One entry per meaningful use of an AI assistant: what you asked, what it produced, what you kept, changed or
rejected, and **how you verified it**. This log is part of the deliverable: a reviewer reads it to see that you
stayed in charge of every number.

Rules: no secrets, tokens or `.env` contents; no row-level personal data (names, emails, phones, addresses).
Schemas, your own code, error messages and aggregates are fine.

| Field | What to write |
|---|---|
| Date / phase | e.g. 2026-10-12 · Phase 3 M2 |
| Tool | the assistant and model you used |
| Prompt (short) | the gist, or a link to the prompt in docs/phase-N |
| Output | what it produced (summarise; paste only short snippets) |
| Decision | kept / changed / rejected, and why |
| Verified by | the command, test, check or document that proved it right (or wrong) |

---

## Entries

### 2026-08-03 · Phase 0 · Java for PySpark on WSL
* **Tool / model:** Gemini 2.5 Flash (free tier)
* **Prompt (summary or paste):**
  > Prompt 1 of Phase 0 with the output of `scripts/doctor.sh` (Python 3.12 found, Java missing).
* **What it produced:** `sudo apt install default-jdk` and an `export JAVA_HOME=...` line for `~/.bashrc`.
* **What I kept / changed / rejected, and why:** changed to `openjdk-21-jre-headless`: `default-jdk` pulls a full
  JDK and on some Ubuntu releases a Java version PySpark 4.2 does not support; a JRE is enough to run Spark. Rejected
  the hard-coded `JAVA_HOME` path (it did not exist on my machine; Spark finds `java` on the PATH).
* **How I verified it:** `java -version` (21), the one-line `SparkSession` check from Step 0.2, `scripts/doctor.sh` green.
* **Time saved / lost (honest guess):** saved ~15 minutes.
* **What I learned:** check a generic install answer against the project's pinned versions before running it.

### 2026-08-04 · Phase 1 · Ambiguities in "revenue by region"
* **Tool / model:** ChatGPT (free tier)
* **Prompt (summary or paste):**
  > Prompt 1 of Phase 1 with my first definition of R1 ("sum of order amounts per day and region").
* **What it produced:** 11 ambiguities, among them time zone, cancelled and returned orders, test orders, partial
  refunds, currency, and "region of the customer or of the shipping address".
* **What I kept / changed / rejected, and why:** kept 8 and wrote a rule for each (IST day of `created_at`, latest
  status in placed/shipped/delivered, test orders excluded, region from the shipping city). Rejected "convert
  currencies" (every amount is INR) and parked partial refunds as an open question for Finance.
* **How I verified it:** computed the 2026-06-01 revenue of one region by hand from `contract/fixture/source`, then
  compared my rules with `transform-spec.md` §5.1: same rules, except I had forgotten that a later cancellation must
  remove revenue from the original day.
* **Time saved / lost (honest guess):** saved ~30 minutes.
* **What I learned:** most metric disputes are definitions, not SQL.

### 2026-08-05 · Phase 2 · "Just use Airbyte and Delta"
* **Tool / model:** Claude (free tier)
* **Prompt (summary or paste):**
  > Prompt 2 of Phase 2: the strongest case for Airbyte against a small ingestion package.
* **What it produced:** a fair case for Airbyte (hundreds of connectors, a UI, state handling for free), and a claim
  that Airbyte "runs comfortably in 2 GB".
* **What I kept / changed / rejected, and why:** kept the connector-count argument in ADR-0002's consequences ("for
  dozens of sources a platform wins"). Rejected the 2 GB figure: Airbyte's own docs recommend far more memory for a
  local deployment, and it runs many containers.
* **How I verified it:** the official deployment requirements page; my laptop's free memory with Airflow already up
  (`free -h`).
* **Time saved / lost (honest guess):** saved ~20 minutes; the wrong number would have gone into the design.
* **What I learned:** ask for the other side of a decision, then check every number it brings.

### 2026-08-08 · Phase 3 M2 · An off-by-5:30 bug
* **Tool / model:** Copilot in VS Code
* **Prompt (summary or paste):**
  > Inline completion for the `utc_timestamp` macro / function.
* **What it produced:** `strptime(col, '%Y-%m-%dT%H:%M:%S%z')` / `F.to_timestamp(col)`.
* **What I kept / changed / rejected, and why:** rejected. Both return a value in the *session* time zone, so the
  same code gives different dates on my laptop (IST) and in CI (UTC). Wrote the spec's rule instead: the first 19
  characters as local time minus the parsed offset.
* **How I verified it:** the parsing tests with `Z` and `+05:30` inputs, then the whole suite with `TZ=UTC` and
  `TZ=Asia/Kolkata`: identical results only with my version.
* **Time saved / lost (honest guess):** lost ~25 minutes.
* **What I learned:** never let the machine's time zone into a business date.

### 2026-08-11 · Phase 3 M3 · A plausible CAC
* **Tool / model:** ChatGPT (free tier)
* **Prompt (summary or paste):**
  > Prompt 1 of Phase 3 with the `mart_cac_monthly` stub and §5.2.
* **What it produced:** a query joining spend to customers on month and channel, then `sum(spend) / count(*)`.
* **What I kept / changed / rejected, and why:** changed. The join multiplied every spend row by the number of new
  customers (a fan-out), and `/` gave a decimal. Aggregated each side first, then joined; integer division; NULL when
  there are no new customers; channels without spend kept with 0.
* **How I verified it:** the gold test against `golden/fixture/mart_cac_monthly.csv` (red before, green after) and
  `parity.py compare` on the full history (MATCH).
* **Time saved / lost (honest guess):** saved ~10 minutes of typing, lost 20 on the fan-out.
* **What I learned:** aggregate before you join; a reconciliation check would have caught it too.

### 2026-08-14 · Phase 3 M4 · Retrying a 401
* **Tool / model:** Claude (free tier)
* **Prompt (summary or paste):**
  > Prompt 3 of Phase 3: review `http.py`, `lake.py`, `ingest.py` for idempotency and safety.
* **What it produced:** three findings: retries on every `HTTPError` including 401; the manifest written before the
  partition swap; the bearer token printed in a debug line.
* **What I kept / changed / rejected, and why:** fixed all three (fail fast on 401/4xx, manifest last, debug line
  removed). Rejected a fourth suggestion to add a file lock library: `max_active_runs=1` already guarantees one writer.
* **How I verified it:** new tests: a 401 is attempted exactly once; a writer that raises halfway leaves the old
  partition; `caplog` never contains the token.
* **Time saved / lost (honest guess):** saved ~1 hour.
* **What I learned:** a review prompt with concrete questions finds real bugs; turn each into a test.
