---
phase: 1
slug: phase-1-requirements
title: "Requirements: the five reports, SLAs and acceptance criteria"
duration: 2.5 h
start_tag: phase-0-end
end_tag: phase-1-end
---
# Phase 1 · Requirements: the five reports, SLAs and acceptance criteria

## 1. Why it matters
"Revenue by region" sounds like a requirement. It is not one until you know which orders count (placed? paid?
delivered? not cancelled? not test orders?), which day an order belongs to (the customer's day in India, or the
server's day in UTC?), what happens when an order is cancelled a week later, and by what time the number must be
right. Most dashboard disputes are not bugs in SQL; they are two people with two definitions. In this phase you
turn the CEO's five questions into **written, testable definitions**, service levels for freshness and
correctness, and user stories with acceptance criteria that include bad and late data. Everything you build in
Phases 3-7 is checked against this document.

## 2. Objectives
By the end of this phase you can:
- Name the stakeholders, the decision each one makes with the dashboard, and how often.
- Define each of the five reports exactly: grain, measures, filters, the day an event belongs to, and an example row.
- Write non-functional requirements with a measure and a test: freshness (by 06:30 IST), correctness (reconciliation), idempotency, privacy, footprint.
- Write user stories with Given / When / Then acceptance criteria, including a late update, a duplicate event and a missing file.
- Say what is out of scope for v1.0 and why.

## 3. Step-by-step instructions

### Step 1.1 · Stakeholders and their decisions
Copy `spec/requirements-template.md` to `spec/requirements.md`. Fill in §1: for the CEO, the Head of Marketing,
the Head of Category and Finance, write the decision they make with the dashboard ("move ad budget between
channels at the start of each month") and which report serves it. A report nobody decides with is a candidate
for "out of scope".

### Step 1.2 · Define the five reports precisely
For each report write: the question, the grain (one row per what?), every measure as a formula, the filters, the
date rule, and one example row you compute by hand from `contract/fixture/source`. Decide, and write down:
- Which order statuses count as revenue, and what happens to revenue when an order is cancelled or returned later?
- Which **calendar** decides the day: an order placed at 23:50 IST on 1 June is `2026-06-01T18:20:00Z`. Which day is it?
- Money: rupees with decimals, or integer paise? Why does it matter when you sum 18,000 order lines?
- CAC: spend divided by which customers? Test accounts? Customers whose channel is unknown? A month with no new customers?
- Prime MRR: how does an annual plan count? When does a cancelled subscription stop counting?
- Top 10: the last 30 days up to which date? Ties?
- Retention: "came back within 30 days" from which event? What about customers who signed up 10 days ago?

Compare your answers with `contract/transform-spec.md` §1 and §5 **after** you have written them. Where you
disagree, write down why: a good spec records the trade-off, not just the rule.

### Step 1.3 · Non-functional requirements (the SLAs)
Fill in §4 with a measure and a test for each:
- **Freshness:** yesterday's (IST) numbers are published by 06:30 IST; the dashboard always shows the business date
  it is showing.
- **Correctness:** every report reconciles to the order lines; a failed blocking check means the previous day stays
  published (a stale number is better than a wrong one).
- **Idempotency:** running the same day twice gives the same tables; re-running a past day replaces it.
- **Privacy:** no email or phone in the lake, the warehouse, the logs or any prompt.
- **Footprint:** runs on an 8 GB laptop and in free CI; no paid service.

### Step 1.4 · User stories with acceptance criteria
Write at least six stories in §3, each with Given / When / Then. At least three must be about **bad or late data**:
```text
Given an order placed on 2026-06-01 and cancelled on 2026-06-04
When the pipeline runs for 2026-06-04
Then the order no longer counts in the revenue of 2026-06-01, and the change is visible on the 2026-06-05 dashboard
```
Other good candidates: the payments file for a day is missing at 06:00; the same payment event arrives twice; a new
order status appears that the contract does not know; the API answers 429 for a minute.

### Step 1.5 · Sources, scope and open questions
Fill in §5 (what each source sends, when, and what can go wrong, from your Phase 0 notes), §6 (out of scope for
v1.0: e.g. real-time numbers, forecasting, per-customer views) and §7 (questions you would ask the CEO). Commit:
```bash
git switch -c docs/phase-1-requirements
git add spec/requirements.md AI_LOG.md
git commit -m "docs(spec): requirements for the five reports, SLAs and acceptance criteria"
```

<!-- track:sql -->
**SQL · dbt track:** add one line per report to your requirements: the gold model that will serve it
(`mart_revenue_daily`, ...). In dbt, the requirement becomes the model's `description` and its tests in Phase 4;
write the definitions so that you can paste them there.
<!-- /track -->

<!-- track:pyspark -->
**Python · PySpark track:** add one line per report: the function in `pipewright_spark/gold.py` that will build it.
Write each definition so that it can become the function's docstring, and note which rule needs a window
function (latest version per key, ranking) and which a join.
<!-- /track -->

## 4. AI-assist prompts

### Prompt 1: Find the ambiguities in a metric definition
```text
Here is my definition of a business metric for an e-commerce dashboard: <paste one report from your
requirements.md>. Act as a sceptical analytics engineer. List every ambiguity that would let two engineers
compute different numbers from the same data (time zones, statuses, late updates, duplicates, test data, ties,
empty periods, rounding). For each, propose a precise rule and say what it costs. Do not compute anything.
```
**Verify the output by:** turning each accepted rule into an example on the fixture (`contract/fixture/source`) and
computing the expected value by hand, and comparing with `contract/transform-spec.md`. Reject rules that need data
BazaarBox does not have.

### Prompt 2: Turn a requirement into acceptance criteria
```text
Requirement: <paste one user story>. Write 3-5 acceptance criteria in Given / When / Then form. At least two must
cover bad or late data (a duplicate, a late status change, a missing file, an unknown value). Each criterion must be
testable with a small fixture; say what the fixture contains.
```
**Verify the output by:** checking each criterion is observable from outside (a table value, an exit code, a file),
that none contradicts your report definitions, and that you could write the fixture in under 10 rows.

### Prompt 3: Challenge my SLAs
```text
My data pipeline must publish yesterday's numbers by 06:30 IST. Sources: an API (available all night) and three
files dropped by partners "around 05:00 IST". Here are my non-functional requirements: <paste §4>. What could make
me miss the SLA, what should happen when a file is late, and how would I measure freshness and correctness in
production? Keep it to the decisions I need to make now.
```
**Verify the output by:** making sure every proposal has a measure and an owner, that "what happens when a file is
late" is a decision (wait, publish partial, keep yesterday) and not a hope, and that nothing requires a paid tool.

## 5. Deliverables
- [ ] `spec/requirements.md` with all seven sections filled in.
- [ ] An exact definition of each of the five reports, with one hand-computed example row from the fixture.
- [ ] Non-functional requirements with a measure and a test each (freshness, correctness, idempotency, privacy, footprint).
- [ ] At least six user stories with acceptance criteria, three of them about bad or late data.
- [ ] An `AI_LOG.md` entry about a definition the AI helped you sharpen (and one suggestion you rejected).

## 6. Self-check quiz

### Q1. An order is placed at 23:50 IST on 1 June. Its timestamp is `2026-06-01T18:20:00Z`. On which business day does it count?
- [x] 1 June: business dates are IST calendar days
- [ ] 2 June, because UTC is the standard
- [ ] It depends on the server's time zone
- [ ] Neither: orders near midnight are excluded
> **Answer:** The contract fixes the calendar (`timezone: Asia/Kolkata`). Here the UTC date happens to be the same, but an order at 00:20 IST on 2 June is `2026-06-01T18:50:00Z`: grouping by the UTC date puts it on the wrong day. Never let the machine's time zone decide.

### Q2. Why does the contract store money as integer paise?
- [ ] Paise use less disk space
- [x] Floating-point sums of thousands of rupee amounts drift by fractions of a paisa, so two correct pipelines can disagree; integers sum exactly
- [ ] The dashboard cannot show decimals
- [ ] Indian law requires it
> **Answer:** `0.1 + 0.2 != 0.3` in floating point. With integers, the SQL and PySpark tracks produce the same totals to the paisa, and the reconciliation checks can demand exact equality.

### Q3. A blocking data-quality check fails at 06:05. What should the CEO see at 06:30?
- [ ] The new numbers with a warning banner
- [ ] An empty dashboard
- [x] Yesterday's last good numbers, clearly labelled with their business date, while the team investigates
- [ ] Whatever the pipeline produced; checks are only advisory
> **Answer:** A stale number that says it is stale is better than a wrong number that looks fresh. That is the publish gate you build in Phases 3-4.

### Q4. Which acceptance criterion is testable?
- [ ] "The dashboard is accurate"
- [ ] "Late data is handled gracefully"
- [x] "Given a payment event delivered twice, when the day is processed, then it is counted once in MRR"
- [ ] "The pipeline is fast"
> **Answer:** A testable criterion names the input, the action and an observable result. The other three are wishes until someone defines accurate, gracefully and fast.

### Q5. What belongs in "out of scope for v1.0"?
- [ ] Anything difficult
- [x] Things a stakeholder might expect but v1.0 deliberately does not do (e.g. intraday numbers), with the reason, so the expectation is managed early
- [ ] The tests
- [ ] The five reports
> **Answer:** Writing scope down prevents the worst kind of surprise: the one at the demo. It also gives v1.1 (Phase 7) a backlog.

### Q6. Your assistant suggests "count revenue when the payment is captured" instead of by order status. What do you do?
- [ ] Accept it: payments are more accurate
- [x] Treat it as a design question: check it against the brief and the sources (cash-on-delivery orders have no captured payment until delivery), and record the decision and its trade-off
- [ ] Reject it: AI suggestions are never right
- [ ] Implement both and let the CEO pick a number
> **Answer:** It may be a good idea, but it changes the meaning of the number and depends on data you have not checked. A definition change is a stakeholder decision, written down, not an implementation detail.

## 7. What you learned
- A metric is not defined until its grain, filters, calendar, money unit and late-data behaviour are.
- Service levels for a data product: freshness, correctness, idempotency, privacy, footprint, each with a measure.
- Acceptance criteria that include bad and late data, testable on a small fixture.
- Scope as a tool for managing expectations, and open questions as part of the deliverable.

## 8. Catch-up git commands
```bash
git fetch upstream --tags
git switch -c my-phase-2 phase-1-end
git show phase-1-end:spec/requirements.md | less      # the reference requirements (after writing yours)
```
