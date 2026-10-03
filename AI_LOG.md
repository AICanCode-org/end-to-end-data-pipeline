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
