---
phase: 8
slug: phase-8-retrospective
title: Retrospective and portfolio
duration: 1.5 h
start_tag: phase-7-end
end_tag: phase-8-end
---
# Phase 8 · Retrospective and portfolio

## 1. Why it matters
A project you cannot explain in two minutes does not exist for a hiring manager. And a project you do not reflect
on teaches you half of what it could. In this phase you look back at nine phases with numbers (what took longest,
what broke, what the tests caught, where AI helped and where it misled you), write a retrospective, turn the README
into a case study a reviewer can read in three minutes, and prepare the story of the September incident, the decision
you are proudest of and the one you would change.

## 2. Objectives
By the end of this phase you can:
- Run a structured retrospective (what went well, what did not, what to change) backed by evidence from your repo.
- Measure your project: tests, checks, parity, run times, image sizes, incidents, AI log entries.
- Write a README case study: problem, architecture, decisions, results, what you would do next.
- Tell the September incident as a two-minute story with a clear lesson.
- Present the project in a portfolio and an interview.

## 3. Step-by-step instructions

### Step 8.1 · Collect the evidence
```bash
git log --oneline | wc -l; git tag -l
scripts/check-all.sh --full 2>&1 | tail -15             # every suite + parity, as CI runs it
grep -c "^### " AI_LOG.md                               # entries (adjust to your log's format)
docker image ls 'pipewright-*'                          # image sizes
```
Note the numbers: tests per suite, coverage, dbt tests, contract checks, gold tables in parity, the full run time of
your track, the incidents you met (the drill, the backcompat check C, your own).

### Step 8.2 · Write the retrospective
Create `docs/retrospective.md`: what went well, what did not, what you would change, with evidence (a commit, a test,
a log line) for each point. Include a short section on AI: three times it saved you time, three times it was wrong
(and how you caught it), and the rule you will keep. The reference is on the solution branch
(`git show phase-8-end:docs/retrospective.md`); write yours first.

### Step 8.3 · Turn the README into a case study
Add a "Case study" section at the top of `README.md` (under the badges): the problem in two sentences, the
architecture diagram, the three decisions that mattered (with links to the ADRs), the results in numbers, and "what I
would do next". A reviewer should understand the project without cloning it.

<!-- track:sql -->
**SQL · dbt track:** show what dbt gave you: the lineage (`dbt docs generate` produces the DAG of models; a screenshot is
enough), the number of data tests and unit tests, enforced contracts, and the incremental strategy with its proof
(`make incremental`). Mention one SQL bug the tests caught.
<!-- /track -->

<!-- track:pyspark -->
**Python · PySpark track:** show what Spark asked of you: explicit schemas, column expressions instead of UDFs, window
functions, the incremental merge, and what changes at 100× the data (partitioning, a cluster, a table format). Mention
one bug parity with the SQL track caught.
<!-- /track -->

### Step 8.4 · Prepare the story and the portfolio
- Two-minute story: situation (the CEO's five numbers), the hard part (late and changing data), what you built (a
  gated, tested, scheduled pipeline in two implementations), the incident and what changed after it, the result.
- Portfolio card: title, one-line summary, three bullet results, link to the repo and to a screenshot of the
  dashboard. Pin the repository on your GitHub profile.
- Three interview questions you can now answer with your own example: "How do you make a pipeline idempotent?",
  "How do you test data?", "Tell me about an incident."

## 4. AI-assist prompts

### Prompt 1: Interview me about my project
```text
Act as a senior data engineer interviewing me about a project. Here is my README case study: <paste>. Ask me 8
questions, one at a time, that probe design decisions, failure handling, testing and trade-offs. After each answer,
tell me what was vague and what a strong answer would add. Do not answer for me.
```
**Verify the output by:** answering out loud first, then checking every technical claim you made against your repo
(a strong answer cites a test or a commit), and noting the questions you could not answer for your next project.

### Prompt 2: Tighten my case study
```text
Here is my README case study: <paste>. Make it readable in 3 minutes by a hiring manager: keep every number and
claim, remove jargon that does not carry information, make the structure problem → approach → results → next.
Return a diff-style list of suggested edits, not a rewrite.
```
**Verify the output by:** checking no number changed or appeared from nowhere, every claim still links to evidence in
the repo, and the voice is still yours.

### Prompt 3: Find patterns in my AI log
```text
Here is my AI log from a 9-phase project: <paste AI_LOG.md>. Group the entries by kind of task, and for each group:
how often the AI's first answer was accepted, changed or rejected, and what the verification step was. What rules
should I keep for my next project?
```
**Verify the output by:** spot-checking three groups against the raw log, and turning the rules into a short checklist
at the end of your retrospective.

## 5. Deliverables
- [ ] `docs/retrospective.md` with evidence for every point and an AI section.
- [ ] A "Case study" section at the top of `README.md` with architecture, decisions, results and next steps.
- [ ] Numbers: tests, checks, parity, run time, image sizes, incidents.
- [ ] A two-minute incident story (in the retrospective or your notes).
- [ ] The repository pinned on your GitHub profile; tag `phase-8-end`.

## 6. Self-check quiz

### Q1. What makes a retrospective point useful?
- [ ] It is positive
- [x] It is specific, backed by evidence (a commit, a test, a log line) and leads to an action
- [ ] It names who made the mistake
- [ ] It is short
> **Answer:** "Testing was hard" teaches nothing; "the IST-midnight bug cost 3 hours because the fixture had no order between 00:00 and 05:30 IST; next time I will add boundary rows first" does.

### Q2. A reviewer has three minutes. What should the top of your README show?
- [ ] The installation guide
- [ ] The full list of dependencies
- [x] The problem, the architecture, the key decisions and the results in numbers, with links to the evidence
- [ ] The commit history
> **Answer:** Lead with the outcome and the reasoning; setup instructions are for people who already decided to run it.

### Q3. Which result is the strongest evidence that your numbers are right?
- [ ] 95 % coverage
- [ ] The dashboard looks good
- [x] Two independent implementations produce the same gold tables, row by row, equal to golden files derived from a written spec, on three months of data, checked in CI
- [ ] The pipeline runs fast
> **Answer:** Coverage and screenshots are weak evidence of correctness. Agreement of independent implementations with a spec is strong evidence.

### Q4. How should you tell the September incident in an interview?
- [ ] Skip it: incidents look bad
- [x] Situation, impact, how it was detected (and how it should have been), root cause, what you changed (monitoring, contract versioning, backcompat checks), and what the gate prevented
- [ ] Blame the source team
- [ ] Only describe the fix
> **Answer:** Interviewers ask about incidents to hear how you think under pressure and what you learn. A contained incident with a clear improvement is a strong story.

### Q5. What is the most useful thing to write about AI in your retrospective?
- [ ] That you did not use it
- [ ] Which model is best
- [x] Where it helped, where it was wrong, how you caught it, and the verification rules you will keep
- [ ] The total number of prompts
> **Answer:** Employers want people who use AI to go faster without shipping its mistakes. Your AI log is the evidence.

## 7. What you learned
- A structured, evidence-based retrospective.
- Measuring a data project: tests, checks, parity, run times, images, incidents.
- A README case study and a two-minute incident story.
- Your own rules for working with AI, drawn from your log.

## 8. Catch-up git commands
```bash
git fetch upstream --tags
git switch -c my-retro phase-8-end
git show phase-8-end:docs/retrospective.md | less
```
