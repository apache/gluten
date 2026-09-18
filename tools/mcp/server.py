import json
import logging
import os
import socket
import sys
from datetime import datetime, timedelta, timezone
from mcp.server.fastmcp import FastMCP

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "script"))

logger = logging.getLogger(__name__)

HOST = "0.0.0.0"
PORT = 5010

# Tool arguments may carry API tokens and passwords. Never write those to
# the log — anything logged here may land on disk in plaintext.
_SECRET_ARG_KEYS = frozenset({
    "jenkins_token",
    "jenkins_api_token",
    "ibm_github_token",
    "github_token",
    "token",
    "password",
})

_USAGE_DOC_PATH = os.path.join(os.path.dirname(__file__), "script", "usage.md")


def _redact(arguments: dict) -> dict:
    """Replace secret-bearing argument values with a placeholder."""
    return {
        key: ("***redacted***" if key.lower() in _SECRET_ARG_KEYS else value)
        for key, value in (arguments or {}).items()
    }


def _allowed_hosts(port: int) -> list[str]:
    """
    Host allowlist for DNS-rebinding protection.

    Defaults to loopback plus this machine's own hostname/FQDN (with and without
    the port) so a remote client connecting by name keeps working. Override with
    MCP_ALLOWED_HOSTS as a comma-separated list.
    """
    override = os.environ.get("MCP_ALLOWED_HOSTS", "").strip()
    if override:
        return [h.strip() for h in override.split(",") if h.strip()]

    names = {"localhost", "127.0.0.1", socket.gethostname(), socket.getfqdn()}
    hosts: list[str] = []
    for name in sorted(n for n in names if n):
        hosts += [name, f"{name}:{port}"]
    return hosts


# ---------------------------------------------------------------------------
# Usage documentation helper
# ---------------------------------------------------------------------------

def _usage_section(tool_name: str) -> str:
    """
    Extract the ``### `<tool_name>``` section from script/usage.md.

    Returns a short diagnostic string when the section or file cannot be found.
    """
    try:
        with open(_USAGE_DOC_PATH, "r", encoding="utf-8") as f:
            content = f.read()
    except OSError as exc:
        return f"Usage documentation unavailable: {exc}"

    marker = f"### `{tool_name}`"
    start = content.find(marker)
    if start == -1:
        return f"No usage section for `{tool_name}` found in {_USAGE_DOC_PATH}."
    # Next `---` separator marks the end of this section
    end = content.find("\n---", start + len(marker))
    return content[start:end].strip() if end != -1 else content[start:].strip()


_PLANTUML_JAR = "./plantuml-1.2026.5.jar"
_FILE_SERVER_ROOT = "./output"
_FILE_SERVER_BASE_URL = "http://127.0.0.1:6020"


def _get_appals(appid: str, spark=None, driver_ip: str = ""):
    """Create a Spark session (if not provided) and load App_Log_Analysis_Enhanced from database.

    If load_data_from_database fails or returns no data (query_num == 0), and the local
    event-log file output/<appid>/<appid> does not already exist, and driver_ip is provided,
    the log is fetched from the driver via:
        ssh centos@<driver_ip> docker ps --format '{{.Names}}' | head -1  # get container name
        ssh centos@<driver_ip> docker cp <container_name>:/opt/spark/events/<appid> /tmp/
        scp centos@<driver_ip>:/tmp/<appid> output/<appid>/<appid>
    The instance is then reloaded from the local file.
    """
    import subprocess as _sub
    from pyspark.sql import SparkSession
    from script.sparklog_extend import App_Log_Analysis_Enhanced
    if spark is None:
        spark = SparkSession.builder.remote("sc://127.0.0.1:15002/").getOrCreate()

    local_log = os.path.join(_FILE_SERVER_ROOT, appid, appid)

    # --- attempt database load -----------------------------------------------
    db_ok = False
    try:
        appals = App_Log_Analysis_Enhanced(None, None, spark=spark)
        appals.load_data_from_database(appid)
        if getattr(appals, "query_num", None) != 0:
            db_ok = True
    except Exception:
        appals = None

    if db_ok:
        return appals

    # --- fallback: fetch event log from driver via SSH/SCP -------------------
    if not os.path.exists(local_log):
        if driver_ip:
            os.makedirs(os.path.dirname(local_log), exist_ok=True)
            # get the Spark container name on the driver, then copy the event log
            get_container_cmd = f'ssh centos@{driver_ip} "docker ps --format \'{{{{.Names}}}}\' | head -1"'
            container_name = _sub.check_output(get_container_cmd, shell=True).decode().strip()
            ssh_cmd = (
                f'ssh centos@{driver_ip} '
                f'"docker cp {container_name}:/opt/spark/events/{appid} /tmp/"'
            )
            _sub.run(ssh_cmd, shell=True, check=True)
            # pull the file to local output/<appid>/<appid>
            scp_cmd = f"scp centos@{driver_ip}:/tmp/{appid} {local_log}"
            _sub.run(scp_cmd, shell=True, check=True)
        else:
            # no driver_ip — re-raise or return what we have (may be empty)
            if appals is None:
                appals = App_Log_Analysis_Enhanced(None, None, spark=spark)
            return appals

    # --- load from local file ------------------------------------------------
    appals = App_Log_Analysis_Enhanced(local_log, None, spark=spark)
    appals.load_data()
    return appals

mcp = FastMCP("eventlog analysis")

# --- Resource: readable usage doc ---
@mcp.resource("docs://usage/generate_operator_comparison")
def get_usage_doc() -> str:
    with open("script/usage.md", "r") as f:
        return f.read()

# ---------------------------------------------------------------------------
# compare_runs
# ---------------------------------------------------------------------------

@mcp.prompt()
def compare_runs_usage() -> str:
    return _usage_section("compare_runs")

@mcp.tool()
def compare_runs(run1_id: str, run2_id: str, run1_desc: str = "", run2_desc: str = "") -> str:
    """
    Compare two Spark application runs and generate an operator comparison report.

    If run1_desc or run2_desc are not provided, call get_comment_by_appid for the
    corresponding appid first, extract a short description (no spaces, use underscores)
    from the returned comment, and pass it here before proceeding.
    """
    # If either description is missing, ask the client to fetch it via get_comment_by_appid
    missing = []
    if not run1_desc:
        missing.append(f"run1_desc for appid '{run1_id}'")
    if not run2_desc:
        missing.append(f"run2_desc for appid '{run2_id}'")
    if missing:
        parts = []
        if not run1_desc:
            parts.append(
                f"call get_comment_by_appid(appid='{run1_id}') to get a comment for run1, "
                "then derive a short description with no spaces (replace spaces with underscores) "
                "and pass it as run1_desc"
            )
        if not run2_desc:
            parts.append(
                f"call get_comment_by_appid(appid='{run2_id}') to get a comment for run2, "
                "then derive a short description with no spaces (replace spaces with underscores) "
                "and pass it as run2_desc"
            )
        return (
            "Missing description(s). Please: "
            + "; ".join(parts)
            + "; then call compare_runs again with all four arguments."
        )

    try:
        from pyspark.sql import SparkSession
        from script.generate_operator_comparison import (
            extract_folder_name,
            export_sorted_comparison_to_markdown,
            generate_chart_html,
            compare_store_sales_scan_metrics,
            DEFAULT_OUTPUT_ROOT,
        )
        from compare_query_plans_html import compare_query_plans
        from compare_hottest_stages import compare_hottest_stages
        from generate_index import generate_html_main
        import os as _os

        appid1, appid2 = run1_id, run2_id
        appname1, appname2 = run1_desc, run2_desc

        comparison_folder = appname1 + "-" + appname2
        folder_name = _os.path.join(DEFAULT_OUTPUT_ROOT, comparison_folder)
        _os.makedirs(folder_name, exist_ok=True)

        spark = SparkSession.builder.remote("sc://127.0.0.1:15002/").getOrCreate()

        appals1 = _get_appals(appid1, spark=spark)
        appals2 = _get_appals(appid2, spark=spark)

        app1_cnt = appals1.getOperatorCount()
        app2_cnt = appals2.getOperatorCount()
        app1_runtime = appals1.get_query_time(plot=False)
        app2_runtime = appals2.get_query_time(plot=False)

        output_md = _os.path.join(folder_name, "operator_comparison.md")
        query_ids_with_diff = export_sorted_comparison_to_markdown(
            app1_cnt, app2_cnt,
            app1_runtime, app2_runtime,
            filename=output_md,
            folder_name=folder_name,
            appid1=appid1,
            appid2=appid2,
            appname1=appname1,
            appname2=appname2,
            appals1=appals1,
            appals2=appals2,
        )

        plan_comp_dir = _os.path.join(folder_name, "plan_comp")
        full_plan_dir = _os.path.join(folder_name, "full_plan")
        _os.makedirs(plan_comp_dir, exist_ok=True)
        _os.makedirs(full_plan_dir, exist_ok=True)

        if query_ids_with_diff:
            appid1_short = appid1.split("-")[1][-4:] if len(appid1.split("-")) > 1 else appid1
            appid2_short = appid2.split("-")[1][-4:] if len(appid2.split("-")) > 1 else appid2
            for query_id in query_ids_with_diff:
                output_html = _os.path.join(plan_comp_dir, f"{query_id}.html")
                try:
                    compare_query_plans(appid1, appid2, output_html, query_id, spark=spark,
                                        appals1=appals1, appals2=appals2)
                    chart_html = _os.path.join(plan_comp_dir, f"{query_id}_chart.html")
                    generate_chart_html(query_id, appid1_short, appid2_short, chart_html, appname1, appname2)
                except Exception:
                    pass

        puml_dir = _os.path.join(folder_name, "puml")
        if _os.path.exists(puml_dir) and _os.listdir(puml_dir):
            puml_pattern = _os.path.join(puml_dir, "*.puml")
            _os.system(f'java -Xmx2048m -DPLANTUML_LIMIT_SIZE=8192 -jar /home/ec2-user/mcp/bin/plantuml-1.2026.5.jar "{puml_pattern}"')

        try:
            compare_hottest_stages(appid1, appid2, appals1, appals2, folder_name, appname1, appname2)
        except Exception:
            pass

        try:
            store_sales_md = _os.path.join(folder_name, "store_sales_scan_metrics.md")
            compare_store_sales_scan_metrics(appals1, appals2, appname1, appname2, store_sales_md)
        except Exception:
            pass

        try:
            generate_html_main()
        except Exception:
            pass

        base_url = f"http://127.0.0.1:6020/{run1_desc}-{run2_desc}"
        comparison_url = f"{base_url}/comparison_{run1_id}_vs_{run2_id}.md#1-run-information"
        operator_url = f"{base_url}/operator_comparison.md#runtime-summary"
        return (
            f"Comparison report generated. Base URL: {base_url}\n"
            f"Please fetch the following two URLs using the fetch/read tool and then summarize the comparison for the customer:\n"
            f"1. {comparison_url}\n"
            f"2. {operator_url}"
        )

    except Exception as e:
        return f"Comparison failed: {e}"

# ---------------------------------------------------------------------------
# sparklog / sparklog_extend MCP tools
# ---------------------------------------------------------------------------

@mcp.prompt()
def get_basic_state_usage() -> str:
    return _usage_section("get_basic_state")

@mcp.tool()
def get_basic_state(appid: str, driver_ip: str = "") -> str:
    """
    Return basic runtime statistics for a Spark application: executor config,
    speculative task counts, runtime, spill, shuffle, I/O totals, etc.
    If the database load fails or returns no data and driver_ip is provided,
    the event log is fetched from the driver via SSH/SCP.
    """
    try:
        appals = _get_appals(appid, driver_ip=driver_ip)
        stats = appals.get_basic_state()
        return json.dumps(stats, default=str)
    except Exception as e:
        return f"Error: {e}"


@mcp.prompt()
def get_query_time_usage() -> str:
    return _usage_section("get_query_time")

@mcp.tool()
def get_query_time(appid: str, queryid: str = "", driver_ip: str = "") -> str:
    """
    Return per-query elapsed time, I/O, shuffle, spill, and task metrics as JSON.
    Leave queryid empty to get all queries.
    If the database load fails or returns no data and driver_ip is provided,
    the event log is fetched from the driver via SSH/SCP.
    """
    try:
        appals = _get_appals(appid, driver_ip=driver_ip)
        kwargs = {}
        if queryid:
            kwargs["queryid"] = queryid
        df = appals.get_query_time(plot=False, **kwargs)
        return df.to_json(orient="index")
    except Exception as e:
        return f"Error: {e}"


@mcp.prompt()
def get_spark_config_usage() -> str:
    return _usage_section("get_spark_config")

@mcp.tool()
def get_spark_config(appid: str, driver_ip: str = "") -> str:
    """
    Return the Spark configuration key/value pairs for the given application.
    If the database load fails or returns no data and driver_ip is provided,
    the event log is fetched from the driver via SSH/SCP.
    """
    try:
        appals = _get_appals(appid, driver_ip=driver_ip)
        df = appals.get_spark_config()
        return df.to_json()
    except Exception as e:
        return f"Error: {e}"


@mcp.prompt()
def get_operator_count_usage() -> str:
    return _usage_section("get_operator_count")

@mcp.tool()
def get_operator_count(appid: str, driver_ip: str = "") -> str:
    """
    Return the count of each physical plan operator per query for the application.
    If the database load fails or returns no data and driver_ip is provided,
    the event log is fetched from the driver via SSH/SCP.
    """
    try:
        appals = _get_appals(appid, driver_ip=driver_ip)
        df = appals.getOperatorCount()
        return df.to_json()
    except Exception as e:
        return f"Error: {e}"


@mcp.prompt()
def get_table_scan_metrics_usage() -> str:
    return _usage_section("get_table_scan_metrics")

@mcp.tool()
def get_table_scan_metrics(appid: str, driver_ip: str = "") -> str:
    """
    Return table scan metrics (bytes read, row groups, splits, Velox stats, etc.)
    aggregated by scan node for the application.
    If the database load fails or returns no data and driver_ip is provided,
    the event log is fetched from the driver via SSH/SCP.
    """
    try:
        appals = _get_appals(appid, driver_ip=driver_ip)
        appals.get_table_scan_metrics(plot=False)
        return "Table scan metrics retrieved (output suppressed in non-notebook mode)."
    except Exception as e:
        return f"Error: {e}"


@mcp.prompt()
def get_metric_output_rowcnt_usage() -> str:
    return _usage_section("get_metric_output_rowcnt")

@mcp.tool()
def get_metric_output_rowcnt(appid: str, queryid: str = "", driver_ip: str = "") -> str:
    """
    Return output row counts per operator and stage.
    Leave queryid empty to get all queries.
    If the database load fails or returns no data and driver_ip is provided,
    the event log is fetched from the driver via SSH/SCP.
    """
    try:
        appals = _get_appals(appid, driver_ip=driver_ip)
        kwargs = {}
        if queryid:
            kwargs["queryid"] = queryid
        result = appals.get_metric_output_rowcnt(plot=False, **kwargs)
        if hasattr(result, "toPandas"):
            return result.toPandas().to_json(orient="records")
        return result.to_json()
    except Exception as e:
        return f"Error: {e}"


@mcp.prompt()
def get_metric_input_rowcnt_usage() -> str:
    return _usage_section("get_metric_input_rowcnt")

@mcp.tool()
def get_metric_input_rowcnt(appid: str, queryid: str = "", driver_ip: str = "") -> str:
    """
    Return input row counts per operator and stage.
    Leave queryid empty to get all queries.
    If the database load fails or returns no data and driver_ip is provided,
    the event log is fetched from the driver via SSH/SCP.
    """
    try:
        appals = _get_appals(appid, driver_ip=driver_ip)
        kwargs = {}
        if queryid:
            kwargs["queryid"] = queryid
        result = appals.get_metric_input_rowcnt(plot=False, **kwargs)
        if hasattr(result, "toPandas"):
            return result.toPandas().to_json(orient="records")
        return result.to_json()
    except Exception as e:
        return f"Error: {e}"


@mcp.prompt()
def get_hottest_stages_usage() -> str:
    return _usage_section("get_hottest_stages")

@mcp.tool()
def get_hottest_stages(appid: str, queryid: str = "", top_n: int = 10, driver_ip: str = "") -> str:
    """
    Return the stages with the highest elapsed time, sorted descending.
    Optionally filter by queryid. top_n controls how many stages to return.
    If the database load fails or returns no data and driver_ip is provided,
    the event log is fetched from the driver via SSH/SCP.
    """
    try:
        appals = _get_appals(appid, driver_ip=driver_ip)
        kwargs = {"plot": False}
        if queryid:
            kwargs["queryid"] = queryid
        df = appals.get_hottest_stages(**kwargs)
        return df.head(top_n).to_json(orient="records")
    except Exception as e:
        return f"Error: {e}"


@mcp.prompt()
def get_critical_path_stages_usage() -> str:
    return _usage_section("get_critical_path_stages")

@mcp.tool()
def get_critical_path_stages(appid: str, driver_ip: str = "") -> str:
    """
    Return stages on the critical execution path with elapsed time, host,
    executor, bytes read, and shuffle read.
    If the database load fails or returns no data and driver_ip is provided,
    the event log is fetched from the driver via SSH/SCP.
    """
    try:
        appals = _get_appals(appid, driver_ip=driver_ip)
        df = appals.get_critical_path_stages()
        return df.to_json(orient="records")
    except Exception as e:
        return f"Error: {e}"


@mcp.prompt()
def get_stage_stat_usage() -> str:
    return _usage_section("get_stage_stat")

@mcp.tool()
def get_stage_stat(appid: str, queryid: str, driver_ip: str = "") -> str:
    """
    Return detailed per-stage statistics for a specific query: elapsed time,
    spill, shuffle, deserialize time, GC time, CPU time, etc.
    If the database load fails or returns no data and driver_ip is provided,
    the event log is fetched from the driver via SSH/SCP.
    """
    try:
        appals = _get_appals(appid, driver_ip=driver_ip)
        df = appals.get_stage_stat(queryid=queryid)
        return df.to_json(orient="records")
    except Exception as e:
        return f"Error: {e}"


@mcp.prompt()
def get_query_plan_usage() -> str:
    return _usage_section("get_query_plan")

@mcp.tool()
def get_query_plan(appid: str, queryid: str = "", stageid: int = 0, driver_ip: str = "") -> str:
    """
    Return the query execution plan data structure (nodes, stage times, metrics).
    Provide either queryid or stageid to filter; leave both empty for all queries.
    If the database load fails or returns no data and driver_ip is provided,
    the event log is fetched from the driver via SSH/SCP.
    """
    try:
        appals = _get_appals(appid, driver_ip=driver_ip)
        kwargs = {}
        if queryid:
            kwargs["queryid"] = queryid
        if stageid:
            kwargs["stageid"] = stageid
        data = appals.get_query_plan(**kwargs)
        return json.dumps(data, default=str)
    except Exception as e:
        return f"Error: {e}"


@mcp.prompt()
def print_query_plan_puml_usage() -> str:
    return _usage_section("print_query_plan_puml")

@mcp.tool()
def print_query_plan_puml(appid: str, queryid: str = "", stageid: int = 0, driver_ip: str = "") -> str:
    """
    Generate a PlantUML diagram source string for the query execution plan.
    Provide either queryid or stageid to filter.
    If the database load fails or returns no data and driver_ip is provided,
    the event log is fetched from the driver via SSH/SCP.
    """
    try:
        appals = _get_appals(appid, driver_ip=driver_ip)
        kwargs = {}
        if queryid:
            kwargs["queryid"] = queryid
        if stageid:
            kwargs["stageid"] = stageid
        return appals.print_query_plan_puml(**kwargs)
    except Exception as e:
        return f"Error: {e}"


@mcp.prompt()
def get_shuffle_stat_usage() -> str:
    return _usage_section("get_shuffle_stat")

@mcp.tool()
def get_shuffle_stat(appid: str, queryid: str = "", driver_ip: str = "") -> str:
    """
    Return shuffle statistics: split ratio, compress ratio, batch sizes,
    shuffle write time breakdown, and data type distribution.
    Leave queryid empty for all queries.
    If the database load fails or returns no data and driver_ip is provided,
    the event log is fetched from the driver via SSH/SCP.
    """
    try:
        appals = _get_appals(appid, driver_ip=driver_ip)
        kwargs = {}
        if queryid:
            kwargs["queryid"] = queryid
        result, _ = appals.get_shuffle_stat(**kwargs)
        if result is None:
            return "No shuffle data found."
        if hasattr(result, "toPandas"):
            return result.toPandas().to_json(orient="records")
        return result.to_json(orient="records")
    except Exception as e:
        return f"Error: {e}"


@mcp.prompt()
def get_stages_w_odd_partitions_usage() -> str:
    return _usage_section("get_stages_w_odd_partitions")

@mcp.tool()
def get_stages_w_odd_partitions(appid: str, driver_ip: str = "") -> str:
    """
    Return stages whose partition count does not divide evenly into the
    executor × cores configuration — a hint for partition tuning.
    If the database load fails or returns no data and driver_ip is provided,
    the event log is fetched from the driver via SSH/SCP.
    """
    try:
        appals = _get_appals(appid, driver_ip=driver_ip)
        df = appals.get_stages_w_odd_partitions()
        return df.to_json(orient="records")
    except Exception as e:
        return f"Error: {e}"


@mcp.prompt()
def get_metrics_by_node_usage() -> str:
    return _usage_section("get_metrics_by_node")

@mcp.tool()
def get_metrics_by_node(appid: str, node_name: str, driver_ip: str = "") -> str:
    """
    Return aggregated metrics for a specific query plan node type
    (e.g. 'ColumnarExchange', 'IcebergScanTransformer').
    If the database load fails or returns no data and driver_ip is provided,
    the event log is fetched from the driver via SSH/SCP.
    """
    try:
        appals = _get_appals(appid, driver_ip=driver_ip)
        result = appals.get_metrics_by_node(node_name)
        if hasattr(result, "toPandas"):
            return result.toPandas().to_json(orient="records")
        return result.to_json(orient="records")
    except Exception as e:
        return f"Error: {e}"

# ---------------------------------------------------------------------------
# compare_query_plan
# ---------------------------------------------------------------------------

@mcp.prompt()
def compare_query_plan_usage() -> str:
    return _usage_section("compare_query_plan")

@mcp.tool()
def compare_query_plan(appid1: str, appid2: str, queryid: str) -> str:
    """
    Compare the query execution plan for a specific query between two Spark applications.

    Workflow:
    1. Search every subdirectory of DEFAULT_OUTPUT_ROOT for a file named
       comparison_<appid1>_vs_<appid2>.md to locate the comparison base folder.
    2. If not found, fail with a message asking whether to run compare_runs first.
    3. Derive the 4-digit short IDs from appid1 / appid2, then check whether
       <base>/puml/<short1>-<queryid>.puml and <base>/puml/<short2>-<queryid>.puml
       both exist.
    4. If both exist, read them and return their contents as JSON together with
       the chart URL and PUML URLs, asking the client to summarize the differences.
    5. If the PUML files are missing, generate them via compare_query_plans
       (folder is already known from step 1).
    """
    import os as _os
    import json as _json

    try:
        from script.generate_operator_comparison import DEFAULT_OUTPUT_ROOT
    except Exception:
        DEFAULT_OUTPUT_ROOT = "/mnt/data1/mcp/output"

    # --- step 1: locate the comparison base folder via comparison_*.md --------
    target_md = f"comparison_{appid1}_vs_{appid2}.md"
    base_dir = None
    try:
        for entry in _os.scandir(DEFAULT_OUTPUT_ROOT):
            if not entry.is_dir():
                continue
            if _os.path.isfile(_os.path.join(entry.path, target_md)):
                base_dir = entry.path
                base_name = entry.name
                break
    except Exception as e:
        return f"Error scanning output directory: {e}"

    if base_dir is None:
        return (
            f"Didn't find a comparison of '{appid1}' and '{appid2}'. "
            f"Do you want to run compare_runs first?"
        )

    # --- step 2: derive short IDs and check for PUML files --------------------
    def _short(appid: str) -> str:
        parts = appid.split("-")
        return parts[1][-4:] if len(parts) > 1 else appid[-4:]

    short1 = _short(appid1)
    short2 = _short(appid2)
    puml_file1 = f"{short1}-{queryid}.puml"
    puml_file2 = f"{short2}-{queryid}.puml"
    puml_dir = _os.path.join(base_dir, "puml")
    p1 = _os.path.join(puml_dir, puml_file1)
    p2 = _os.path.join(puml_dir, puml_file2)

    # --- helper: build response once both PUML contents are known -------------
    def _build_response(puml1_content: str, puml2_content: str) -> str:
        base_url = f"http://127.0.0.1:6020/{base_name}"
        chart_url = f"{base_url}/plan_comp/{queryid}_chart.html"
        puml_url1 = f"{base_url}/puml/{puml_file1}"
        puml_url2 = f"{base_url}/puml/{puml_file2}"
        payload = _json.dumps({
            "chart_url": chart_url,
            "puml_url1": puml_url1,
            "puml_url2": puml_url2,
            puml_file1: puml1_content,
            puml_file2: puml2_content,
        }, ensure_ascii=False)
        return (
            f"{payload}\n\n"
            f"Chart URL: {chart_url}\n"
            f"Please also fetch the two PlantUML plan files from:\n"
            f"  {puml_url1}\n"
            f"  {puml_url2}\n"
            f"Then summarize the differences between the two query plans for the customer."
        )

    # --- step 3: return existing PUML files if present ------------------------
    if _os.path.isfile(p1) and _os.path.isfile(p2):
        with open(p1, "r", encoding="utf-8") as f:
            puml1_content = f.read()
        with open(p2, "r", encoding="utf-8") as f:
            puml2_content = f.read()
        return _build_response(puml1_content, puml2_content)

    # --- step 4: PUML files missing — generate them now ----------------------
    try:
        from pyspark.sql import SparkSession
        from compare_query_plans_html import compare_query_plans

        plan_comp_dir = _os.path.join(base_dir, "plan_comp")
        _os.makedirs(plan_comp_dir, exist_ok=True)
        _os.makedirs(puml_dir, exist_ok=True)

        spark = SparkSession.builder.remote("sc://127.0.0.1:15002/").getOrCreate()
        appals1 = _get_appals(appid1, spark=spark)
        appals2 = _get_appals(appid2, spark=spark)

        output_html = _os.path.join(plan_comp_dir, f"{queryid}.html")
        compare_query_plans(appid1, appid2, output_html, queryid,
                            spark=spark, appals1=appals1, appals2=appals2)

        # render .puml → .png
        if _os.path.exists(puml_dir) and _os.listdir(puml_dir):
            puml_pattern = _os.path.join(puml_dir, "*.puml")
            _os.system(
                f'java -Xmx2048m -DPLANTUML_LIMIT_SIZE=8192 '
                f'-jar /home/ec2-user/mcp/bin/plantuml-1.2026.5.jar "{puml_pattern}"'
            )

        if not _os.path.isfile(p1) or not _os.path.isfile(p2):
            return (
                f"compare_query_plans ran but expected PUML files were not created:\n"
                f"  {p1}\n  {p2}"
            )
        with open(p1, "r", encoding="utf-8") as f:
            puml1_content = f.read()
        with open(p2, "r", encoding="utf-8") as f:
            puml2_content = f.read()
        return _build_response(puml1_content, puml2_content)

    except Exception as e:
        return f"Failed to generate query plan comparison: {e}"


# ---------------------------------------------------------------------------
# generate_puml_from_eventlog
# ---------------------------------------------------------------------------


def _render_puml(puml_path: str) -> None:
    """Render a .puml file to .png using the local plantuml JAR."""
    os.system(
        f'java -Xmx1024m -DPLANTUML_LIMIT_SIZE=8192 '
        f'-jar {_PLANTUML_JAR} "{puml_path}"'
    )


def _file_server_url(abs_path: str) -> str:
    """Convert an absolute path under _FILE_SERVER_ROOT to a public URL."""
    rel = os.path.relpath(abs_path, _FILE_SERVER_ROOT)
    return f"{_FILE_SERVER_BASE_URL}/{rel}"


@mcp.prompt()
def generate_puml_from_eventlog_usage() -> str:
    return _usage_section("generate_puml_from_eventlog")


@mcp.tool()
def generate_puml_from_eventlog(
    appid: str,
    queryid: str,
    simple: bool = False,
    driver_ip: str = "",
) -> str:
    """
    Load a Spark application's query plan from the Iceberg database, generate
    a PlantUML diagram (.puml) via print_query_plan_puml, render it to a PNG,
    and write both files to /mnt/data1/mcp/output/<appid>/<queryid>.puml|png.

    Args:
        appid:     Spark application ID.
        queryid:   Query ID (e.g. "q7", "q23a").
        simple:    Not used by the database-backed generator; reserved for future use.
        driver_ip: IP of the Spark driver; used to fetch the event log via SSH/SCP
                   when the database load fails or returns no data.

    Returns a JSON object with puml_file, png_file, puml_url, and png_url.
    """
    import json as _json
    try:
        appals = _get_appals(appid, driver_ip=driver_ip)
        puml_content = appals.print_query_plan_puml(queryid=queryid)

        out_folder = os.path.join(_FILE_SERVER_ROOT, appid)
        os.makedirs(out_folder, exist_ok=True)
        puml_path = os.path.join(out_folder, f"{queryid}.puml")
        with open(puml_path, "w", encoding="utf-8") as f:
            f.write(puml_content)

        _render_puml(puml_path)
        png_path = os.path.join(out_folder, f"{queryid}.png")

        result: dict = {
            "puml_file": puml_path,
            "png_file":  png_path,
            "puml_url":  _file_server_url(puml_path),
            "png_url":   _file_server_url(png_path),
        }
        return _json.dumps(result)
    except Exception as e:
        return f"Error: {e}"


# ---------------------------------------------------------------------------
# generate_puml_from_rest_url
# ---------------------------------------------------------------------------

def _parse_spark_ui_url(url: str):
    """
    Parse any Spark UI or REST API URL and return (app_id, query_id, rest_url).

    Accepted input forms:
      • Spark History UI :
          http://host:18080/history/app-xxx/SQL/execution/?id=1
      • Spark UI proxy (PrestoDB / remote cluster) :
          https://host/proxy/app-xxx/SQL/execution/?id=1
      • Already a REST API URL :
          http://host/api/v1/applications/app-xxx/sql/1

    If the host resolves to 127.0.0.1 (loopback) the caller must supply
    driver_ip so the request can be tunnelled via SSH.

    Returns:
        app_id    (str)
        query_id  (int)
        rest_url  (str) – the canonical REST API URL,
                          always using the original host/port
    """
    import urllib.parse as _up
    import re as _re

    parsed = _up.urlparse(url)
    path_parts = [p for p in parsed.path.split('/') if p]

    # ---- Already a REST URL? (/api/v1/applications/…/sql/N) ---------------
    if 'api' in path_parts and 'applications' in path_parts:
        try:
            sql_idx = path_parts.index('sql')
            app_id  = path_parts[path_parts.index('applications') + 1]
            query_id = int(path_parts[sql_idx + 1])
            return app_id, query_id, url
        except (ValueError, IndexError):
            pass

    # ---- UI URL: find the segment before 'SQL' as app_id ------------------
    try:
        sql_idx = next(
            i for i, p in enumerate(path_parts)
            if p.upper() == 'SQL'
        )
    except StopIteration:
        raise ValueError(f"Cannot find 'SQL' segment in URL path: {url}")

    if sql_idx == 0:
        raise ValueError(f"No app_id segment before 'SQL' in: {url}")

    app_id = path_parts[sql_idx - 1]

    qs = _up.parse_qs(parsed.query)
    if 'id' not in qs:
        raise ValueError(f"No '?id=' query parameter in URL: {url}")
    query_id = int(qs['id'][0])

    # ---- Build REST URL ---------------------------------------------------
    # Detect PrestoDB-style proxy (non-loopback host) → tunnel via 127.0.0.1:4040
    host = parsed.hostname or ''
    is_loopback = host in ('127.0.0.1', 'localhost', '::1')

    # For proxy/remote URLs that are NOT loopback, the REST endpoint is on the
    # same host; for loopback (local Spark driver) it stays loopback.
    if is_loopback:
        base = f"http://127.0.0.1:{parsed.port or 18080}"
    else:
        # Check if this looks like a remote proxy (prestodb, YARN proxy, etc.)
        # Path pattern: /proxy/<app_id>/SQL/... → REST lives at 127.0.0.1:4040
        if 'proxy' in path_parts and path_parts.index('proxy') < sql_idx - 1:
            base = "http://127.0.0.1:4040"
        else:
            port = parsed.port or 18080
            base = f"http://{host}:{port}"

    rest_url = f"{base}/api/v1/applications/{app_id}/sql/{query_id}"
    return app_id, query_id, rest_url


@mcp.prompt()
def generate_puml_from_rest_url_usage() -> str:
    return _usage_section("generate_puml_from_rest_url")


@mcp.tool()
def generate_puml_from_rest_url(
    url: str,
    simple_chart: bool = False,
    from_explain_text: bool = False,
    driver_ip: str = "",
) -> str:
    """
    Fetch a Spark query plan from the REST API, generate a PlantUML diagram
    (.puml), and render it to a PNG.

    Accepts any of these URL forms and converts them to the REST API endpoint:
      • Spark History UI  : http://127.0.0.1:18080/history/app-xxx/SQL/execution/?id=1
      • PrestoDB proxy    : https://host/proxy/app-xxx/SQL/execution/?id=1
      • REST API directly : http://127.0.0.1:18080/api/v1/applications/app-xxx/sql/1

    When the REST URL resolves to 127.0.0.1 (the Spark driver), driver_ip must
    be supplied and the plan JSON is fetched via:
        ssh centos@<driver_ip> curl <url>

    Otherwise the URL is fetched directly with curl.

    The plan JSON is saved to output/<app_id>/<query_id>.plan before generating
    the diagram.

    Args:
        url:               Any Spark UI or REST API URL for a SQL execution.
        simple_chart:      Suppress Project / Filter / Exchange nodes.
        from_explain_text: Use PlanTextUMLGenerator on planDescription instead
                           of the nodes/edges REST graph.
        driver_ip:         IP of the Spark driver host; required when the REST
                           URL is on 127.0.0.1 / localhost.

    Returns a JSON object with puml_file, png_file, puml_url, png_url, plan_file.
    """
    import json as _json
    import subprocess as _sub
    import urllib.parse as _up

    try:
        # ---- 1. Parse / convert URL ---------------------------------------
        app_id, query_id, rest_url = _parse_spark_ui_url(url)

        parsed = _up.urlparse(rest_url)
        needs_ssh = parsed.hostname in ('127.0.0.1', 'localhost', '::1')

        if needs_ssh and not driver_ip:
            return (
                "Error: the REST URL resolves to 127.0.0.1 (the Spark driver). "
                "Please provide driver_ip so the plan can be fetched via SSH."
            )

        # ---- 2. Fetch plan JSON -------------------------------------------
        out_dir = os.path.join(_FILE_SERVER_ROOT, app_id)
        os.makedirs(out_dir, exist_ok=True)
        plan_file = os.path.join(out_dir, f"{query_id}.plan")

        if needs_ssh:
            cmd = f'ssh centos@{driver_ip} curl -s "{rest_url}"'
        else:
            cmd = f'curl -s "{rest_url}"'

        result_bytes = _sub.check_output(cmd, shell=True, stderr=_sub.PIPE)
        plan_json = _json.loads(result_bytes.decode('utf-8', errors='replace'))

        with open(plan_file, 'w', encoding='utf-8') as f:
            _json.dump(plan_json, f, indent=2)

        # ---- 3. Generate PlantUML ----------------------------------------
        from script.plan_rest_uml_generator import RestPlanUMLGenerator

        gen = RestPlanUMLGenerator()

        if from_explain_text:
            plan_description = plan_json.get('planDescription', '')
            if not plan_description:
                return "Error: 'planDescription' is empty in the fetched plan JSON."
            desc  = plan_json.get('description', '')
            short = desc[:60] + '...' if len(desc) > 60 else desc
            title = f"Query {query_id}: {short}" if short else f"Query {query_id}"
            puml_content = gen._puml_from_plan_description(
                plan_description, title=title, simple_chart=simple_chart
            )
        else:
            puml_content = gen.generate_plantuml(plan_json, simple_chart=simple_chart)

        puml_path = os.path.join(out_dir, f"{query_id}.puml")
        with open(puml_path, 'w', encoding='utf-8') as f:
            f.write(puml_content)

        # ---- 4. Render PNG -----------------------------------------------
        _render_puml(puml_path)
        png_path = os.path.splitext(puml_path)[0] + '.png'

        # ---- 5. Return result --------------------------------------------
        result: dict = {
            "plan_file": plan_file,
            "puml_file": puml_path,
            "png_file":  png_path,
        }
        if puml_path.startswith(_FILE_SERVER_ROOT):
            result["puml_url"] = _file_server_url(puml_path)
            result["png_url"]  = _file_server_url(png_path)

        return _json.dumps(result)

    except Exception as e:
        return f"Error: {e}"


if __name__ == "__main__":
    mcp.settings.host = HOST
    mcp.settings.port = PORT
    mcp.settings.log_level = os.environ.get("MCP_LOG_LEVEL", "INFO")
    mcp.settings.transport_security.enable_dns_rebinding_protection = False
    #mcp.settings.transport_security.allowed_hosts = _allowed_hosts(PORT)
    mcp.run(transport="sse")
