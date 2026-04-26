#!/usr/bin/env python3
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
"""ANSI mode test analyzer for Gluten CI.

Data sources:
  --json-dir     JSON files from GlutenExpressionOffloadTracker (expression-level)
  --report-dir   Surefire XML reports (test-method-level, for backends-velox)

Output targets:
  stdout (default), --pr-comment, --job-summary, --output-file FILE
"""
import argparse
import glob
import json
import os
import pathlib
import re
import subprocess
import sys
import xml.etree.ElementTree as ET
from collections import defaultdict

# Shared analysis prompt — single source of truth consumed by both this script
# and the local agent SKILL (`.github/skills/ansi-analysis.md`).
SHARED_PROMPT_PATH = (
    pathlib.Path(__file__).resolve().parent / "shared.md"
)
PROMPT_PLACEHOLDER = "{json_data}"


def _load_prompt_template():
    """Load the shared prompt template. Fail-fast if missing — drift between
    this script and the SKILL is exactly the bug this layout prevents."""
    if not SHARED_PROMPT_PATH.is_file():
        sys.exit(
            f"FATAL: shared prompt not found at {SHARED_PROMPT_PATH}. "
            f"Repository layout is broken — refusing to fall back to a stale "
            f"in-script copy."
        )
    return SHARED_PROMPT_PATH.read_text(encoding="utf-8")

NO_EXCEPTION_RE = re.compile(
    r"Expected .+ to be thrown, but no exception was thrown")
WRONG_EXCEPTION_RE = re.compile(r"Expected (\S+) but got (\S+):")
MSG_MISMATCH_RE = re.compile(r"Expected error message containing")


def classify_fail_cause(message):
    if not message:
        return "OTHER"
    if NO_EXCEPTION_RE.search(message):
        return "NO_EXCEPTION"
    if WRONG_EXCEPTION_RE.search(message):
        return "WRONG_EXCEPTION"
    if MSG_MISMATCH_RE.search(message):
        return "MSG_MISMATCH"
    return "OTHER"


def _extract_short_message(message):
    if not message:
        return ""
    m = WRONG_EXCEPTION_RE.search(message)
    if m:
        return f"Expected {m.group(1)} but got {m.group(2)}"
    if NO_EXCEPTION_RE.search(message):
        m2 = re.search(
            r"Expected (.+?) to be thrown, but no exception was thrown",
            message)
        if m2:
            return f"Expected {m2.group(1)} but no exception was thrown"
    if message.startswith("Exception evaluating"):
        return message.split("\n")[0][:150]
    if message.startswith("Incorrect evaluation"):
        return message.split("\n")[0][:150]
    return message.split("\n")[0][:120]


# ===========================================================================
# DATA LAYER
# ===========================================================================

def load_json_data(json_dir):
    """Load all JSON files from Tracker output directory."""
    suites = []
    if not json_dir or not os.path.isdir(json_dir):
        return suites
    for path in sorted(glob.glob(os.path.join(json_dir, "**/*.json"), recursive=True)):
        with open(path) as f:
            try:
                data = json.load(f)
                suites.append(data)
            except json.JSONDecodeError:
                print(f"Warning: could not parse {path}", file=sys.stderr)
    return suites


def load_surefire_xml(report_dir):
    """Load surefire XML reports for backends-velox test results."""
    results = []
    if not report_dir or not os.path.isdir(report_dir):
        return results
    for xml_path in sorted(glob.glob(os.path.join(report_dir, "**/*.xml"),
                                     recursive=True)):
        try:
            tree = ET.parse(xml_path)
        except ET.ParseError:
            continue
        root = tree.getroot()
        suite_name = root.get("name", "")
        job = _infer_job_name(xml_path)
        for tc in root.iter("testcase"):
            test_name = tc.get("name", "")
            failure = tc.find("failure")
            error = tc.find("error")
            skipped = tc.find("skipped")
            if skipped is not None:
                status = "SKIPPED"
                msg = ""
            elif failure is not None:
                status = "FAILED"
                msg = failure.get("message", "")
            elif error is not None:
                status = "ERROR"
                msg = error.get("message", "")
            else:
                status = "PASSED"
                msg = ""
            results.append({
                "suite": suite_name,
                "test": test_name,
                "status": status,
                "message": msg,
                "job": job,
            })
    return results


def _infer_job_name(xml_path):
    parts = xml_path.replace("\\", "/").split("/")
    for p in parts:
        if "spark" in p and ("backend" in p or "ut" in p):
            return p
    return "unknown"


# ===========================================================================
# ANALYSIS LAYER
# ===========================================================================

def classify_record(offload_status, record_status):
    """Classify a single record (expression-level)."""
    is_pass = record_status in ("PASSED", "PASS")
    is_fallback = offload_status == "FALLBACK"
    if is_fallback:
        if is_pass:
            return "🔴", "Fallback"
        return "🔴", "Failed+Fallback"
    if is_pass:
        return "🟢", "Passed"
    return "🟡", "Failed"


def classify_test_for_xml(status):
    """Classify XML tests (no offload data)."""
    is_pass = status in ("PASSED", "PASS")
    is_skip = status in ("SKIPPED", "SKIP")
    if is_skip:
        return "⚪", "Skipped"
    if is_pass:
        return "⚪", "Passed (no data)"
    return "🟡", "Failed (no data)"


def analyze_json_tests(suites):
    """Analyze JSON data at record (expression) level. Returns flat record list."""
    records_out = []
    for suite_data in suites:
        suite_name = suite_data.get("suite", "")
        category = suite_data.get("category", "")
        for t in suite_data.get("tests", []):
            test_status = t.get("status", "PASSED")
            for rec in t.get("records", []):
                offload = rec.get("offload", "")
                rec_status = rec.get("status", "PASS")
                color, label = classify_record(offload, rec_status)
                records_out.append({
                    "suite": suite_name,
                    "test": t["name"],
                    "test_status": test_status,
                    "status": rec_status,
                    "color": color,
                    "label": label,
                    "category": category,
                    "offload": offload,
                    "expression": rec.get("expression", ""),
                    "failCause": rec.get("failCause", ""),
                    "meta": rec.get("meta", {}),
                })
    return records_out


def analyze_xml_tests(xml_results):
    """Analyze surefire XML at test method level."""
    tests = []
    for t in xml_results:
        color, label = classify_test_for_xml(t["status"])
        tests.append({
            "suite": t["suite"],
            "test": t["test"],
            "status": t["status"],
            "color": color,
            "label": label,
            "job": t.get("job", ""),
            "message": t.get("message", ""),
            "source": "xml",
        })
    return tests


def build_summary(json_records, xml_tests):
    """Build unified summary. json_records are at record (expression) level."""
    by_color = defaultdict(int)
    failures = []
    total = 0

    for r in json_records:
        total += 1
        by_color[r["label"]] += 1
        if r["status"] in ("FAILED", "ERROR", "FAIL"):
            fail_cause = r.get("failCause", "")
            failures.append({
                "suite": r["suite"],
                "test": r["test"],
                "color": r["color"],
                "label": r["label"],
                "message": fail_cause,
                "source": "json",
            })

    for t in xml_tests:
        total += 1
        by_color[t["label"]] += 1
        if t["status"] in ("FAILED", "ERROR"):
            failures.append({
                "suite": t["suite"],
                "test": t["test"],
                "color": t["color"],
                "label": t["label"],
                "message": t.get("message", ""),
                "job": t.get("job", ""),
                "source": "xml",
            })

    json_test_names = set()
    for r in json_records:
        json_test_names.add((r["suite"], r["test"]))

    return {
        "total": total,
        "by_color": dict(by_color),
        "failures": failures,
        "json_record_count": len(json_records),
        "json_test_count": len(json_test_names),
        "xml_test_count": len(xml_tests),
    }


# ===========================================================================
# OUTPUT LAYER
# ===========================================================================

def format_summary(summary, json_records, suites=None):
    """Format record-level summary as markdown."""
    lines = ["# ANSI Mode Test Analysis Report (Spark 4.1)\n"]
    lines.append("> [!NOTE]")
    lines.append("> Expression-level ANSI mode offload coverage analysis.")
    lines.append("> Test config: `spark.sql.ansi.enabled=true`,"
                 " `spark.gluten.sql.ansiFallback.enabled=false`.")
    lines.append("> - **Passed (🟢)**: Velox correctly handles ANSI semantics")
    lines.append("> - **Fallback (🔴)**: Expression falls back to Spark execution,"
                 " needs ANSI support in Velox")
    lines.append("> - **Failed (🟡)**: Velox executes but ANSI error behavior"
                 " differs from Spark, needs exception handling fix\n")
    json_test_count = summary["json_test_count"]
    json_record_count = summary["json_record_count"]
    xml_total = summary["xml_test_count"]
    lines.append(f"**ANSI Offload suites: {json_test_count} tests, "
                 f"{json_record_count} records** | "
                 f"**Other suites: {xml_total} tests**\n")

    lines.append("## ANSI Offload\n")

    lines.append("### Overview (ANSI Offload Expression Records)\n")
    lines.append("| Classification | Count | % |")
    lines.append("|---|---|---|")
    json_labels = ["Passed", "Failed", "Fallback"]
    color_map = {"Passed": "🟢", "Failed": "🟡",
                 "Fallback": "🔴"}
    for label in json_labels:
        count = summary["by_color"].get(label, 0)
        if count > 0:
            color = color_map.get(label, "")
            pct = count * 100 / json_record_count if json_record_count else 0
            lines.append(f"| {color} {label} | {count} | {pct:.1f}% |")
    lines.append("")

    if suites:
        lines.append("### Per-Suite Summary\n")
        lines.append("| Suite | 🟢 Passed | 🟡 Failed "
                     "| 🔴 Fallback |")
        lines.append("|---|---|---|---|")
        suite_rows = []
        for s in suites:
            name = s.get("suite", "").split(".")[-1]
            cat = s.get("category", "")
            counts = defaultdict(int)
            for t in s.get("tests", []):
                for rec in t.get("records", []):
                    offload = rec.get("offload", "")
                    rec_status = rec.get("status", "PASS")
                    _, label = classify_record(offload, rec_status)
                    counts[label] += 1
            total = sum(counts.values())
            po = counts.get("Passed", 0)
            pct = f"{po * 100 / total:.0f}%" if total else "0%"
            suite_rows.append((cat, name, po, pct,
                               counts.get("Failed", 0),
                               counts.get("Fallback", 0)))
        for cat, name, po, pct, fo, pfb in sorted(suite_rows):
            lines.append(f"| {name} | {po} ({pct}) | {fo} | {pfb} |")
        lines.append("")

    json_failures = [f for f in summary["failures"] if f.get("source") == "json"]
    xml_failures = [f for f in summary["failures"] if f.get("source") == "xml"]

    if json_failures:
        cause_counts = defaultdict(int)
        for f in json_failures:
            cause = classify_fail_cause(f.get("message", ""))
            cause_counts[cause] += 1

        lines.append(f"### Failure Cause Analysis "
                     f"({len(json_failures)} failures)\n")
        cause_desc = {
            "NO_EXCEPTION": "Velox did not throw expected ANSI exception",
            "WRONG_EXCEPTION": "Exception wrapped as SparkException",
            "MSG_MISMATCH": "Error message text mismatch",
            "OTHER": "Result mismatch or eval exception",
        }
        lines.append("| Cause | Count | Description |")
        lines.append("|---|---|---|")
        for cause in ["NO_EXCEPTION", "WRONG_EXCEPTION",
                      "MSG_MISMATCH", "OTHER"]:
            cnt = cause_counts.get(cause, 0)
            if cnt > 0:
                lines.append(f"| {cause} | {cnt} "
                             f"| {cause_desc.get(cause, '')} |")
        lines.append("")

    if xml_failures:
        json_suite_names = set()
        if suites:
            for s in suites:
                json_suite_names.add(s.get("suite", ""))
                json_suite_names.add(s.get("suite", "").split(".")[-1])
        xml_suite_counts = defaultdict(int)
        xml_suite_tests = defaultdict(list)
        for f in xml_failures:
            suite = f["suite"]
            short = suite.split(".")[-1]
            if suite not in json_suite_names and short not in json_suite_names:
                xml_suite_counts[short] += 1
                xml_suite_tests[short].append(f.get("test", ""))
        if xml_suite_counts:
            lines.append(f"## Other "
                         f"({sum(xml_suite_counts.values())} failures)\n")
            lines.append("| Suite | Failures |")
            lines.append("|---|---|")
            for suite, cnt in sorted(xml_suite_counts.items(),
                                     key=lambda x: -x[1]):
                if cnt <= 3:
                    tests = "<br/>".join(xml_suite_tests[suite])
                    lines.append(f"| {suite} | {tests} |")
                else:
                    lines.append(f"| {suite} | {cnt} |")
            lines.append("")

    return "\n".join(lines)


def format_report(summary, json_records, suites=None,
                  ai_content=None, ai_model=None):
    """Format full report: summary + optional AI analysis."""
    parts = [format_summary(summary, json_records, suites)]
    if ai_content:
        parts.append("")
        parts.append("<details>")
        parts.append("<summary>🤖 AI Deep Analysis</summary>\n")
        parts.append(ai_content)
        parts.append(f"\n---\n*Generated by {ai_model}. "
                     f"AI analysis may not be fully accurate — "
                     f"please verify before acting on recommendations.*")
        parts.append("</details>")
    return "\n".join(parts)


# ---------------------------------------------------------------------------
# AI analysis via GitHub Models API
# ---------------------------------------------------------------------------

GITHUB_MODELS_API = "https://models.inference.ai.azure.com/chat/completions"


def _build_ai_context(summary, suites):
    """Build a compact JSON context for AI analysis.

    Grouped by (cause, suite) to minimize tokens for the GitHub Models API
    (8000-token limit).
    """
    groups = defaultdict(lambda: {"count": 0, "tests": [], "sample_msg": ""})
    for f in summary["failures"][:100]:
        cause = classify_fail_cause(f.get("message", ""))
        suite = f["suite"].split(".")[-1]
        key = (cause, suite)
        groups[key]["count"] += 1
        if len(groups[key]["tests"]) < 10:
            groups[key]["tests"].append(f["test"])
        if not groups[key]["sample_msg"]:
            groups[key]["sample_msg"] = _extract_short_message(
                f.get("message", ""))[:120]

    grouped_failures = []
    for (cause, suite), g in sorted(groups.items(), key=lambda x: -x[1]["count"]):
        grouped_failures.append({
            "cause": cause, "suite": suite, "count": g["count"],
            "tests": g["tests"], "sample_msg": g["sample_msg"],
        })

    compact_cats = defaultdict(lambda: {"pass": 0, "fail": 0})
    for s in suites:
        cat = s.get("category", "unknown")
        for t in s.get("tests", []):
            if t.get("status") in ("PASS", "PASSED"):
                compact_cats[cat]["pass"] += 1
            elif t.get("status") in ("FAIL", "FAILED", "ERROR"):
                compact_cats[cat]["fail"] += 1

    json_colors = {k: v for k, v in summary["by_color"].items()
                    if k not in ("Passed (no data)", "Skipped")}

    output = {
        "records": summary["json_record_count"],
        "by_color": json_colors,
        "failures": grouped_failures,
        "categories": dict(compact_cats),
    }
    return json.dumps(output, separators=(',', ':'), ensure_ascii=False)


def call_ai_analysis(json_output, model=None):
    """Call GitHub Models API for deep analysis with fallback chain."""
    import requests

    token = os.environ.get("GITHUB_TOKEN") or os.environ.get("GH_TOKEN")
    if not token:
        print("Warning: no GITHUB_TOKEN/GH_TOKEN, skipping AI analysis",
              file=sys.stderr)
        return None, None

    models_to_try = []
    if model:
        models_to_try.append(model)
    models_to_try.extend(["gpt-4.1", "gpt-4o"])

    prompt = _load_prompt_template().replace(PROMPT_PLACEHOLDER, json_output)
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json",
    }

    for m in models_to_try:
        try:
            print(f"Calling GitHub Models API with model={m}...",
                  file=sys.stderr)
            resp = requests.post(
                GITHUB_MODELS_API,
                headers=headers,
                json={
                    "model": m,
                    "messages": [{"role": "user", "content": prompt}],
                },
                timeout=300,
            )
            if resp.status_code == 200:
                data = resp.json()
                content = data["choices"][0]["message"]["content"].strip()
                if content:
                    print(f"AI analysis completed with model={m}",
                          file=sys.stderr)
                    return content, m
            print(f"Warning: model {m} returned status {resp.status_code}: "
                  f"{resp.text[:300]}", file=sys.stderr)
        except Exception as e:
            print(f"Warning: model {m} failed: {e}", file=sys.stderr)

    print("Warning: all AI models failed, skipping analysis", file=sys.stderr)
    return None, None


# ---------------------------------------------------------------------------
# Output targets
# ---------------------------------------------------------------------------

def post_pr_comment(report):
    pr = os.environ.get("PR_NUMBER", "")
    repo = os.environ.get("GITHUB_REPOSITORY", "")
    token = os.environ.get("GH_TOKEN", "")
    if not all([pr, repo, token]):
        print("Warning: missing PR_NUMBER/GITHUB_REPOSITORY/GH_TOKEN, "
              "skipping PR comment", file=sys.stderr)
        return
    cmd = [
        "gh", "api",
        f"repos/{repo}/issues/{pr}/comments",
        "-f", f"body={report}",
    ]
    env = dict(os.environ, GH_TOKEN=token)
    subprocess.run(cmd, env=env, check=True)
    print(f"Posted PR comment to {repo}#{pr}")


def write_job_summary(report):
    summary_file = os.environ.get("GITHUB_STEP_SUMMARY")
    if summary_file:
        with open(summary_file, "a") as f:
            f.write(report + "\n")


# ===========================================================================
# MAIN
# ===========================================================================

def main():
    parser = argparse.ArgumentParser(description="ANSI mode test analyzer")
    parser.add_argument("--json-dir", help="JSON directory from Tracker")
    parser.add_argument("--report-dir", help="Surefire XML directory")
    parser.add_argument("--pr-comment", action="store_true")
    parser.add_argument("--job-summary", action="store_true")
    parser.add_argument("--output-file", help="Write output to file")
    parser.add_argument("--ai-analysis", action="store_true",
                        help="Call GitHub Models API for AI deep analysis")
    parser.add_argument("--ai-model", default="",
                        help="AI model (default: gpt-4.1)")
    args = parser.parse_args()

    suites = load_json_data(args.json_dir)
    xml_results = load_surefire_xml(args.report_dir)

    json_records = analyze_json_tests(suites)
    xml_tests = analyze_xml_tests(xml_results)
    summary = build_summary(json_records, xml_tests)

    ai_content, ai_model = None, None
    if args.ai_analysis:
        ai_context = _build_ai_context(summary, suites)
        model = args.ai_model or os.environ.get("AI_MODEL", "")
        ai_content, ai_model = call_ai_analysis(ai_context, model or None)

    report = format_report(summary, json_records, suites,
                           ai_content=ai_content, ai_model=ai_model)

    if args.output_file:
        with open(args.output_file, "w") as f:
            f.write(report)
        print(f"Report written to {args.output_file}")

    if args.pr_comment:
        post_pr_comment(report)

    if args.job_summary:
        write_job_summary(report)

    if not args.output_file and not args.pr_comment:
        print(report)

    test_count = summary["total"]
    fail_count = len(summary["failures"])
    print(f"Analysis complete: {test_count} tests, {fail_count} failures",
          file=sys.stderr)


if __name__ == "__main__":
    main()
