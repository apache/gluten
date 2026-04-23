---
name: ansi-analysis
description: Analyze Gluten ANSI-mode test results (run dev/verify-ansi-expressions.sh, parse JSON tracker output, produce root-cause analysis and fix recommendations). Trigger on user requests like "analyze ANSI tests", "run ANSI matrix", "why is this ANSI test failing".
---

# ANSI Test Analysis Skill

## Step 0 — MUST READ FIRST: shared analysis prompt

Before doing anything else, read the shared prompt that defines the analysis output format and reference source locations:

```
.github/skills/ansi-analysis/shared.md
```

This file is the single source of truth — the same content is consumed by the CI Python pipeline (`.github/skills/ansi-analysis/analyze-ansi.py --ai-analysis`). Your output structure, reference source locations, and self-investigation steps MUST follow it. If the file is missing, STOP and tell the user the repo is in a broken state.

## Step 1 — Decide entry point

Ask the user (or infer from request):
- Run new tests? → Step 2
- Re-analyze existing JSON in `target/ansi-offload/`? → Step 3
- Diagnose a single test failure? → Step 4

## Step 2 — Run the verification script

```bash
./dev/verify-ansi-expressions.sh <category> <spark41|spark40|all> [--clean]
```

Categories: `cast | arithmetic | collection | datetime | math | decimal | string | aggregate | errors | all`

Logs: `/tmp/ansi-matrix/latest/` (bash logs).
JSON: `target/ansi-offload/*.json` (written by `GlutenExpressionOffloadTracker.scala`, this is the structured input for analysis).

Notes from prior runs:
- Use `all` mode in single JVM (~28 min) when full coverage is needed
- After rebase / branch switch, run `./dev/builddep-veloxbe-inc.sh` first to refresh `libvelox.so` / `libgluten.so`

## Step 3 — Analyze JSON results

Two options:

### 3a. Local AI orchestration (this skill, recommended for interactive review)

1. Read `.github/skills/ansi-analysis/shared.md` (Step 0)
2. List `target/ansi-offload/*.json`
3. Read each JSON; extract: suite name, total/passed/failed/ignored counts, per-test `failCause`
4. Apply the analysis template from shared.md verbatim (sections, tables, constraints)
5. For each failure: extract Velox file:line from `failCause`, read those C++ files, verify root cause
6. **Always** grep `isAnsiSupported` in `ep/build-velox/build/velox_ep/velox/functions/sparksql/specialforms/SparkCastExpr.cpp` when the failure involves Cast — most NO_EXCEPTION/Cast failures stem from the small whitelist there
7. Output the markdown report

### 3b. Python script (CI / batch)

```bash
python3 .github/skills/ansi-analysis/analyze-ansi.py \
  --json-dir target/ansi-offload/ \
  --ai-analysis \
  --output ansi-report.md
```

The script loads the same shared prompt and calls the GitHub Models API.

## Step 4 — Single-failure diagnosis

When the user pastes one failing test:
1. Locate its JSON entry under `target/ansi-offload/`
2. Apply the self-investigation steps from shared.md (extract Velox file:line, check `isAnsiSupported`, cross-check `withAnsiEvalMode` in the shim)
3. Output: Symptom / Root Cause / Fix Point / Representative Tests / Estimated Impact

## Step 5 — Optional PR comment

If the user wants the report posted to a PR:

```bash
gh pr comment <pr-number> --body-file ansi-report.md
```

(or use the GitHub MCP server tool when available)

## Environment requirements

For Step 2 (running tests):
- `SPARK_ANSI_SQL_MODE=true`
- `SPARK_TESTING=true`
- `SPARK_SCALA_VERSION=2.13`
- JVM: `-Dspark.gluten.sql.ansiFallback.enabled=false`
- Maven profile: include `-Pdelta`

## What NOT to do

- Do NOT invent reference paths or line numbers — always grep / verify
- Do NOT skip Step 0 — drift between shared.md and your output is the failure mode this skill is designed to prevent
- Do NOT bypass the shared prompt by writing your own analysis structure
