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
