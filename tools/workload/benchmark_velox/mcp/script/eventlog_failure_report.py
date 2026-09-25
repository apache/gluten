"""
eventlog_failure_report.py
--------------------------
Analyse task failures, exceptions and driver errors in a Spark event log and
render the result as a GitHub-style markdown report.

The work is split in two steps so the rendering can be reused / tested
without a Spark session:

    collect_failure_data(spark, eventlog)  -> dict   (Spark queries)
    render_failure_report(data, ...)       -> str    (pure python)

``generate_failure_report`` chains both and is what the MCP server calls.
"""

import logging
import os
import re
from collections import defaultdict
from dataclasses import dataclass, field
from typing import List

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Exception parsing
# ---------------------------------------------------------------------------

@dataclass
class ExceptionInfo:
    """Fields extracted from a Gluten / Velox exception text block."""
    exception_type: str = ""
    error_source: str = ""
    error_code: str = ""
    reason: str = ""
    retriable: str = ""
    function: str = ""
    file: str = ""
    line: str = ""
    operator: str = ""
    task_info: str = ""
    root_cause: str = ""
    stack_trace: List[str] = field(default_factory=list)
    java_stack: List[str] = field(default_factory=list)


# (pattern, root-cause label) checked in order against the whole block.
_KNOWN_ROOT_CAUSES = [
    (re.compile(r"TProtocolException|Invalid data"), "TProtocolException: Invalid data"),
    (re.compile(r"Exceeded memory (pool )?cap|MEM_CAP_EXCEEDED"), "Velox memory cap exceeded"),
    (re.compile(r"java\.lang\.OutOfMemoryError"), "java.lang.OutOfMemoryError"),
    (re.compile(r"FileNotFoundException"), "FileNotFoundException"),
    (re.compile(r"SparkOutOfMemoryError"), "SparkOutOfMemoryError"),
]

_CAUSED_BY = re.compile(r"^Caused by:\s*(.+)$")
_ROOT_CAUSE_MAX_LEN = 150


def _detect_root_cause(block: str, info: ExceptionInfo) -> str:
    for pattern, label in _KNOWN_ROOT_CAUSES:
        if pattern.search(block):
            return label
    # Deepest "Caused by:" line is usually the most specific cause.
    caused_by = [m.group(1) for m in map(_CAUSED_BY.match, (l.strip() for l in block.splitlines())) if m]
    if caused_by:
        return caused_by[-1][:_ROOT_CAUSE_MAX_LEN]
    if info.reason:
        return info.reason[:_ROOT_CAUSE_MAX_LEN]
    first_line = block.strip().split("\n", 1)[0]
    return first_line[:_ROOT_CAUSE_MAX_LEN]


def parse_exception_block(block: str) -> ExceptionInfo:
    """Parse a single exception text block (task failure description or driver error)."""
    info = ExceptionInfo()
    in_stack_trace = False
    in_java_stack = False

    for line in block.split("\n"):
        line = line.strip()

        if not info.exception_type and "Exception:" in line:
            match = re.search(r"(org\.apache\.\S+Exception|Exception:\s+\w+)", line)
            if match:
                info.exception_type = match.group(1)

        # Keep the first occurrence: nested exceptions repeat these fields.
        if line.startswith("Error Source:") and not info.error_source:
            info.error_source = line.split(":", 1)[1].strip()
        elif line.startswith("Error Code:") and not info.error_code:
            info.error_code = line.split(":", 1)[1].strip()
        elif line.startswith("Reason:") and not info.reason:
            info.reason = line.split(":", 1)[1].strip()
        elif line.startswith("Retriable:") and not info.retriable:
            info.retriable = line.split(":", 1)[1].strip()
        elif line.startswith("Function:") and not info.function:
            info.function = line.split(":", 1)[1].strip()
        elif line.startswith("File:") and not info.file:
            info.file = line.split(":", 1)[1].strip()
        elif line.startswith("Line:") and not info.line:
            info.line = line.split(":", 1)[1].strip()
        elif line.startswith("Context:") and "Task" in line and not info.task_info:
            info.task_info = line.split(":", 1)[1].strip()

        if not info.operator and "getOutput failed" in line:
            match = re.search(r"operator:\s+(\w+)", line)
            if match:
                info.operator = match.group(1)

        if line.startswith("Stack trace:"):
            in_stack_trace, in_java_stack = True, False
            continue
        if line.startswith("at "):
            in_java_stack, in_stack_trace = True, False

        if in_stack_trace and line.startswith("#"):
            info.stack_trace.append(line)
        elif in_java_stack and line.startswith("at "):
            info.java_stack.append(line)

    info.root_cause = _detect_root_cause(block, info)
    return info


# ---------------------------------------------------------------------------
# Spark data collection
# ---------------------------------------------------------------------------

def _has_field(schema, *path: str) -> bool:
    """True if the nested struct field *path* exists in *schema*."""
    current = schema
    for name in path:
        fields = getattr(current, "fields", None)
        if fields is None:
            return False
        match = next((f for f in fields if f.name == name), None)
        if match is None:
            return False
        current = match.dataType
    return True


def collect_failure_data(spark, eventlog: str, desc_len: int = 10) -> dict:
    """
    Read the event log with *spark* and collect everything the report needs
    as plain python values.
    """
    import pyspark.sql.functions as F

    df = spark.read.json(eventlog)
    df = df.cache()
    try:
        df.count()
        return _collect(df, F, desc_len)
    finally:
        try:
            df.unpersist()
        except Exception:
            pass


def _collect(df, F, desc_len: int) -> dict:
    schema = df.schema
    data: dict = {}

    data["spark_version"] = "N/A"
    if _has_field(schema, "Spark Version"):
        rows = df.where("`Spark Version` is not null").select("Spark Version").limit(1).collect()
        if rows:
            data["spark_version"] = rows[0]["Spark Version"]

    data["gluten_commit"] = "N/A"
    if _has_field(schema, "info", "Gluten Revision"):
        rows = (df.where("info.`Gluten Revision` is not null")
                .select("info.`Gluten Revision`").limit(1).collect())
        if rows:
            data["gluten_commit"] = rows[0]["Gluten Revision"]

    data["app_id"] = None
    if _has_field(schema, "App ID"):
        rows = df.where("Event='SparkListenerApplicationStart'").select("App ID").limit(1).collect()
        if rows:
            data["app_id"] = rows[0]["App ID"]

    data["task_end_reasons"] = []
    if _has_field(schema, "Task End Reason", "Reason"):
        rows = (df.where("Event = 'SparkListenerTaskEnd'")
                .groupBy("`Task End Reason`.`Reason`").count()
                .orderBy("Reason").collect())
        data["task_end_reasons"] = [(r["Reason"], r["count"]) for r in rows]
    reason_counts = dict(data["task_end_reasons"])

    # stage id -> (execution id, description); left join keeps non-SQL stages.
    stage_desc = None
    if _has_field(schema, "Stage IDs") and _has_field(schema, "Properties", "spark.sql.execution.id"):
        stage_exec = (df.where("Event='SparkListenerJobStart'")
                      .select(F.col("Properties.`spark.sql.execution.id`").cast("long").alias("executionId"),
                              "Stage IDs")
                      .distinct()
                      .select("executionId", F.explode("Stage IDs").alias("Stage ID")))
        if _has_field(schema, "executionId") and _has_field(schema, "description"):
            exec_desc = (df.where("Event='org.apache.spark.sql.execution.ui.SparkListenerSQLExecutionStart'")
                         .select(F.col("executionId").cast("long").alias("executionId"),
                                 F.substring(F.col("description"), 1, desc_len).alias("description")))
            stage_desc = stage_exec.join(exec_desc, on="executionId", how="left")
        else:
            stage_desc = stage_exec.withColumn("description", F.lit(None).cast("string"))

    def _stages_for(reason: str) -> list:
        grouped = (df.where(f"`Task End Reason`.`Reason` = '{reason}'")
                   .groupBy("Stage ID", "Stage Attempt ID").count())
        if stage_desc is not None:
            grouped = grouped.join(stage_desc, on="Stage ID", how="left")
        rows = [r.asDict() for r in grouped.orderBy("Stage ID", "Stage Attempt ID").collect()]
        return [{
            "stage_id": r["Stage ID"],
            "attempt": r["Stage Attempt ID"],
            "count": r["count"],
            "execution_id": r.get("executionId"),
            "description": r.get("description"),
        } for r in rows]

    data["loss_reasons"] = []
    data["executor_lost_stages"] = []
    if reason_counts.get("ExecutorLostFailure"):
        if _has_field(schema, "Task End Reason", "Loss Reason"):
            rows = (df.where("`Task End Reason`.`Reason` = 'ExecutorLostFailure'")
                    .groupBy("`Task End Reason`.`Loss Reason`").count()
                    .orderBy(F.desc("count")).collect())
            data["loss_reasons"] = [(r["Loss Reason"], r["count"]) for r in rows]
        data["executor_lost_stages"] = _stages_for("ExecutorLostFailure")

    data["exception_descriptions"] = []
    data["exception_stages"] = []
    if reason_counts.get("ExceptionFailure"):
        if _has_field(schema, "Task End Reason", "Description"):
            rows = (df.where("`Task End Reason`.`Reason` = 'ExceptionFailure'")
                    .select("`Task End Reason`.`Description`").collect())
            data["exception_descriptions"] = [r["Description"] or "" for r in rows]
        data["exception_stages"] = _stages_for("ExceptionFailure")

    data["driver_errors"] = []
    if _has_field(schema, "errorMessage") and _has_field(schema, "executionId"):
        errs = (df.where("Event='org.apache.spark.sql.execution.ui.SparkListenerSQLExecutionEnd'"
                         " and errorMessage is not null and errorMessage != ''")
                .select(F.col("executionId").cast("long").alias("executionId"), "errorMessage"))
        if stage_desc is not None:
            errs = errs.join(stage_desc.select("executionId", "description").distinct(),
                             on="executionId", how="left")
        else:
            errs = errs.withColumn("description", F.lit(None).cast("string"))
        rows = errs.orderBy("executionId").collect()
        data["driver_errors"] = [{
            "execution_id": r["executionId"],
            "description": r["description"],
            "error": r["errorMessage"],
        } for r in rows]

    return data


# ---------------------------------------------------------------------------
# Markdown rendering
# ---------------------------------------------------------------------------

def _cell(value) -> str:
    """Escape a value for a markdown table cell."""
    if value is None or value == "":
        return "-"
    return str(value).replace("|", "\\|").replace("\n", " ")


def _stage_table(stages: list) -> list:
    lines = [
        "#### Affected stages",
        "",
        "| Stage ID | Attempt | Count | Execution ID | Description |",
        "|----------|--------:|------:|--------------|-------------|",
    ]
    for s in stages:
        lines.append(
            f"| {s['stage_id']} | {s['attempt']} | {s['count']} "
            f"| {_cell(s['execution_id'])} | {_cell(s['description'])} |"
        )
    lines.append("")
    return lines


def _header(appid: str, data: dict) -> list:
    return [
        "## Analysis Report",
        "",
        f"**Application ID:** `{appid}`  ",
        f"**Spark Version:** `{data.get('spark_version') or 'N/A'}`  ",
        f"**Gluten Commit:** `{data.get('gluten_commit') or 'N/A'}`  ",
        "",
    ]


def _group_exceptions(texts: list) -> list:
    """Group parsed exceptions by root cause and error signature; largest group first."""
    groups: dict = defaultdict(list)
    for raw in texts:
        info = parse_exception_block(raw)
        key = (info.root_cause or "Unknown", info.exception_type, info.error_code, info.operator)
        groups[key].append((info, raw))
    return sorted(groups.items(), key=lambda kv: -len(kv[1]))


def _exception_section(data: dict, max_traces_per_cause: int) -> list:
    descriptions = data.get("exception_descriptions") or []
    total = dict(data.get("task_end_reasons") or []).get("ExceptionFailure", 0)
    if not total:
        return []

    lines = [
        "### ExceptionFailure",
        "",
        f"Total tasks affected: **{total:,}**",
        "",
    ]
    if data.get("exception_stages"):
        lines += _stage_table(data["exception_stages"])
    if not descriptions:
        return lines

    groups = _group_exceptions(descriptions)
    lines += [
        "#### Exception classification summary",
        "",
        "| Root Cause | Exception Type | Error Code | Operator | Retriable | Count |",
        "|------------|----------------|------------|----------|----------:|------:|",
    ]
    for (cause, *_), items in groups:
        sample = items[0][0]
        lines.append(
            f"| {_cell(cause)} | {_cell(sample.exception_type)} | {_cell(sample.error_code)} "
            f"| {_cell(sample.operator)} | {_cell(sample.retriable)} | {len(items)} |"
        )
    lines.append("")

    lines += ["#### Exception stack traces", ""]
    idx = 0
    for (cause, *_), items in groups:
        lines += [f"**Root cause: {cause}** ({len(items)} occurrence(s))", ""]
        shown = items if max_traces_per_cause <= 0 else items[:max_traces_per_cause]
        for info, raw in shown:
            idx += 1
            summary = " ".join(p for p in (
                info.exception_type,
                f"[{info.error_code}]" if info.error_code else "",
            ) if p) or "Exception"
            reason = info.reason[:200] + "…" if len(info.reason) > 200 else info.reason
            lines += [
                "<details>",
                f"<summary>Exception {idx}: {summary}</summary>",
                "",
                f"- **Error source:** {info.error_source or '-'}",
                f"- **Error code:** {info.error_code or '-'}",
                f"- **Reason:** {reason or '-'}",
                f"- **Operator:** {info.operator or '-'}",
                f"- **Retriable:** {info.retriable or '-'}",
                f"- **Root cause:** {info.root_cause or '-'}",
                f"- **Task info:** {info.task_info or '-'}",
                "",
                "```",
                raw.strip(),
                "```",
                "",
                "</details>",
                "",
            ]
        if len(items) > len(shown):
            lines += [f"_{len(items) - len(shown)} more occurrence(s) with the same root cause omitted._", ""]
    return lines


def _executor_lost_section(data: dict) -> list:
    total = dict(data.get("task_end_reasons") or []).get("ExecutorLostFailure", 0)
    if not total:
        return []
    lines = [
        "### ExecutorLostFailure",
        "",
        f"Total tasks affected: **{total:,}**",
        "",
    ]
    if data.get("loss_reasons"):
        lines += [
            "#### Loss reason breakdown",
            "",
            "| Loss Reason | Count |",
            "|-------------|------:|",
        ]
        for reason, count in data["loss_reasons"]:
            lines.append(f"| {_cell(reason or '(none)')} | {count:,} |")
        lines.append("")
    if data.get("executor_lost_stages"):
        lines += _stage_table(data["executor_lost_stages"])
    return lines


def _driver_error_section(data: dict, max_error_len: int = 200) -> list:
    errors = data.get("driver_errors") or []
    if not errors:
        return []
    lines = [
        "### Driver Errors",
        "",
        f"Total failed SQL executions: **{len(errors):,}**",
        "",
        "| Execution ID | Description | Root Cause | Error |",
        "|-------------:|-------------|------------|-------|",
    ]
    for e in errors:
        msg = e["error"] or ""
        info = parse_exception_block(msg)
        err = msg.strip().split("\n", 1)[0]
        err = err[:max_error_len] + ("…" if len(err) > max_error_len else "")
        lines.append(
            f"| {_cell(e['execution_id'])} | {_cell(e['description'])} "
            f"| {_cell(info.root_cause)} | {_cell(err)} |"
        )
    lines.append("")
    return lines


def render_failure_report(data: dict, appid: str, max_traces_per_cause: int = 5) -> str:
    """Render the collected event log data as markdown."""
    lines = _header(appid, data)

    lines += [
        "### Task End Reason Summary",
        "",
        "| Reason | Count |",
        "|--------|------:|",
    ]
    for reason, count in data.get("task_end_reasons") or []:
        lines.append(f"| {_cell(reason)} | {count:,} |")
    lines.append("")

    lines += _executor_lost_section(data)
    lines += _exception_section(data, max_traces_per_cause)
    lines += _driver_error_section(data)

    lines += ["---", "", "_Generated by the eventlog analysis MCP server_", ""]
    return "\n".join(lines)


def generate_failure_report(spark, eventlog: str, max_traces_per_cause: int = 5) -> tuple:
    """
    Collect failure data from *eventlog* and return ``(markdown, appid)``.
    The appid comes from the SparkListenerApplicationStart event, falling
    back to the event log file name.
    """
    data = collect_failure_data(spark, eventlog)
    appid = data.get("app_id") or os.path.basename(eventlog.rstrip("/")) or "unknown"
    return render_failure_report(data, appid, max_traces_per_cause), appid
