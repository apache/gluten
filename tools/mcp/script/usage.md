# Tool Usage Guide

This document describes the MCP tools exposed by this server for Spark query plan analysis and performance comparison.

---

## MCP Tools

### `compare_runs`

**Purpose:** Compare two Spark application runs and generate an operator comparison report (HTML + markdown) served at a local URL.

**Parameters:**

| Parameter | Required | Default | Description |
|---|---|---|---|
| `run1_id` | yes | — | Spark application ID for the first run |
| `run2_id` | yes | — | Spark application ID for the second run |
| `run1_desc` | no | `""` | Short label for run 1 (no spaces). If omitted, call `get_comment_by_appid(run1_id)` first, derive a short description (replace spaces with underscores), and pass it here |
| `run2_desc` | no | `""` | Short label for run 2 (no spaces). If omitted, call `get_comment_by_appid(run2_id)` first, derive a short description (replace spaces with underscores), and pass it here |

**Returns:** URL to the generated comparison report, e.g. `http://127.0.0.1:5010/<run1_desc>-<run2_desc>/`.

---

> All Spark Log Analysis tools below connect to a remote Spark session at `sc://127.0.0.1:15002/` and load application data from pre-processed Iceberg database tables using `App_Log_Analysis_Enhanced.load_data_from_database(appid)`. Every tool takes `appid` as a required first parameter and an optional `driver_ip` parameter. When the database load fails or returns no data (`query_num == 0`) and `driver_ip` is set, the event log is automatically fetched from the driver host via SSH/SCP (`ssh <driver_ip> docker cp …` then `scp <driver_ip>:/tmp/<appid> output/<appid>/<appid>`) and reloaded from the local file.

---

### `get_basic_state`

**Purpose:** Return a JSON summary of basic runtime statistics for a Spark application.

**Parameters:**

| Parameter | Required | Default | Description |
|---|---|---|---|
| `appid` | yes | — | Spark application ID |
| `driver_ip` | no | `""` | IP of the Spark driver; used to fetch the event log via SSH/SCP when the database load fails or returns no data |

**Returns:** JSON object with fields: `appid`, `executor.instances`, `executor.cores`, `shuffle.partitions`, `batch size`, `real executors`, `Failed Tasks`, `Speculative Tasks`, `Speculative Killed Tasks`, `Speculative Stage`, `runtime`, `disk spilled`, `memspilled`, `local_read`, `remote_read`, `shuffle_write`, `task run time`, `ser_time`, `f_wait_time`, `gc_time`, `input read`, `storage read`, `ram read`, `ssd read`, `acc_task_time`.

---

### `get_query_time`

**Purpose:** Return per-query elapsed time, I/O, shuffle, spill, peak memory, and task metrics.

**Parameters:**

| Parameter | Required | Default | Description |
|---|---|---|---|
| `appid` | yes | — | Spark application ID |
| `queryid` | no | `""` | Query ID to filter (e.g. `"q7"`); omit for all queries |
| `driver_ip` | no | `""` | IP of the Spark driver; used to fetch the event log via SSH/SCP when the database load fails or returns no data |

**Returns:** JSON keyed by `real_queryid` with columns: `runtime`, `input read`, `disk spilled`, `memspilled`, `local_read`, `remote_read`, `shuffle_write`, `run_time`, `ser_time`, `f_wait_time`, `gc_time`, `peak_mem`, `acc_task_time`, `output rows`, `storage read`, `executors`, `core/exec`, `parallelism`.

---

### `get_spark_config`

**Purpose:** Return all Spark configuration key/value pairs recorded in the event log for the application.

**Parameters:**

| Parameter | Required | Default | Description |
|---|---|---|---|
| `appid` | yes | — | Spark application ID |
| `driver_ip` | no | `""` | IP of the Spark driver; used to fetch the event log via SSH/SCP when the database load fails or returns no data |

**Returns:** JSON of configuration key → value.

---

### `get_operator_count`

**Purpose:** Return the count of each physical plan operator per query.

**Parameters:**

| Parameter | Required | Default | Description |
|---|---|---|---|
| `appid` | yes | — | Spark application ID |
| `driver_ip` | no | `""` | IP of the Spark driver; used to fetch the event log via SSH/SCP when the database load fails or returns no data |

**Returns:** JSON table with operators as rows and query IDs as columns.

---

### `get_table_scan_metrics`

**Purpose:** Return table scan metrics aggregated by scan node: bytes read, number of processed splits, row groups, Velox timing stats, etc.

**Parameters:**

| Parameter | Required | Default | Description |
|---|---|---|---|
| `appid` | yes | — | Spark application ID |
| `driver_ip` | no | `""` | IP of the Spark driver; used to fetch the event log via SSH/SCP when the database load fails or returns no data |

**Returns:** Confirmation string (metrics are large; use in a Jupyter session for full display).

---

### `get_metric_output_rowcnt`

**Purpose:** Return output row counts per operator and stage.

**Parameters:**

| Parameter | Required | Default | Description |
|---|---|---|---|
| `appid` | yes | — | Spark application ID |
| `queryid` | no | `""` | Query ID to filter; omit for all queries |
| `driver_ip` | no | `""` | IP of the Spark driver; used to fetch the event log via SSH/SCP when the database load fails or returns no data |

**Returns:** JSON records with `nodename`, `Stage ID`, `total_row` (millions), `stage time`.

---

### `get_metric_input_rowcnt`

**Purpose:** Return input row counts per operator and stage.

**Parameters:**

| Parameter | Required | Default | Description |
|---|---|---|---|
| `appid` | yes | — | Spark application ID |
| `queryid` | no | `""` | Query ID to filter; omit for all queries |
| `driver_ip` | no | `""` | IP of the Spark driver; used to fetch the event log via SSH/SCP when the database load fails or returns no data |

**Returns:** JSON records with `nodename`, `Stage ID`, `total_row` (millions), `stage time`.

---

### `get_hottest_stages`

**Purpose:** Return the stages with the highest elapsed time, sorted descending — useful for identifying bottlenecks.

**Parameters:**

| Parameter | Required | Default | Description |
|---|---|---|---|
| `appid` | yes | — | Spark application ID |
| `queryid` | no | `""` | Query ID to filter; omit for all queries |
| `top_n` | no | `10` | Number of top stages to return |
| `driver_ip` | no | `""` | IP of the Spark driver; used to fetch the event log via SSH/SCP when the database load fails or returns no data |

**Returns:** JSON records with `Stage ID`, `Job ID`, `real_queryid`, `total_time` (seconds), `stdev_time`, `partition#`, `acc_total`, `total`.

---

### `get_critical_path_stages`

**Purpose:** Return stages on the critical execution path — the chain of tasks that determined overall wall-clock time.

**Parameters:**

| Parameter | Required | Default | Description |
|---|---|---|---|
| `appid` | yes | — | Spark application ID |
| `driver_ip` | no | `""` | IP of the Spark driver; used to fetch the event log via SSH/SCP when the database load fails or returns no data |

**Returns:** JSON records with `real_queryid`, `elapsed` (seconds), `Host`, `executor ID`, `Stage ID`, `Task ID`, `file read` (MB), `shuffle read` (MB).

---

### `get_stage_stat`

**Purpose:** Return detailed per-stage statistics for a specific query: elapsed time, spill, shuffle, deserialize time, fetch wait, shuffle write time, GC time, CPU time, and input read (GB).

**Parameters:**

| Parameter | Required | Default | Description |
|---|---|---|---|
| `appid` | yes | — | Spark application ID |
| `queryid` | yes | — | Query ID to analyze (e.g. `"q7"`) |
| `driver_ip` | no | `""` | IP of the Spark driver; used to fetch the event log via SSH/SCP when the database load fails or returns no data |

**Returns:** JSON records per stage with columns: `Job ID`, `Stage ID`, `elapsed time`, `disk spilled`, `mem spilled`, `local read`, `remote read`, `shuffle write`, `deseri time`, `fetch wait time`, `shuffle write time`, `seri time`, `get result time`, `gc time`, `exe cpu time`, `input read`.

---

### `get_query_plan`

**Purpose:** Return the query execution plan as a structured data object: plan nodes with stage assignments, timing metrics, row counts, batch counts, and operator names.

**Parameters:**

| Parameter | Required | Default | Description |
|---|---|---|---|
| `appid` | yes | — | Spark application ID |
| `queryid` | no | `""` | Query ID to filter (e.g. `"q7"`) |
| `stageid` | no | `0` | Stage ID to filter |
| `driver_ip` | no | `""` | IP of the Spark driver; used to fetch the event log via SSH/SCP when the database load fails or returns no data |

**Returns:** JSON with `queries` (list of query plan trees), `plan_nodes` (flat list of node dicts with metrics), `apptotaltime`, executor config.

---

### `print_query_plan_puml`

**Purpose:** Generate a PlantUML diagram source (`.puml`) for the query execution plan, suitable for rendering with `plantuml`.

**Parameters:**

| Parameter | Required | Default | Description |
|---|---|---|---|
| `appid` | yes | — | Spark application ID |
| `queryid` | no | `""` | Query ID to filter |
| `stageid` | no | `0` | Stage ID to filter |
| `driver_ip` | no | `""` | IP of the Spark driver; used to fetch the event log via SSH/SCP when the database load fails or returns no data |

**Returns:** PlantUML source string starting with `@startuml`. Nodes are grouped by stage, coloured by type (scan=blue, join=green, agg=yellow, exchange=red, etc.), and stage boxes are coloured by execution time (green=fast → red=slow).

**Example:** Pipe the result into a `.puml` file and render with `java -jar plantuml.jar diagram.puml`.

---

### `get_shuffle_stat`

**Purpose:** Return shuffle statistics including split ratio, compression ratio, average batch sizes, shuffle write time breakdown, and data type distribution per ColumnarExchange operator.

**Parameters:**

| Parameter | Required | Default | Description |
|---|---|---|---|
| `appid` | yes | — | Spark application ID |
| `queryid` | no | `""` | Query ID to filter; omit for all queries |
| `driver_ip` | no | `""` | IP of the Spark driver; used to fetch the event log via SSH/SCP when the database load fails or returns no data |

**Returns:** JSON records per exchange node with fields: `map_stageid`, `reducer_stageid`, `shuffle bytes written` (GB), `data size` (GB), `time to split`, `time to compress`, `shuffle write time`, `time to spill`, `records read`, `local blocks read`, `remote bytes read`, and batch size metrics.

---

### `get_stages_w_odd_partitions`

**Purpose:** Return stages whose partition count does not divide evenly into `executor_instances × executor_cores / task_cpus` — a hint for partition-count tuning.

**Parameters:**

| Parameter | Required | Description |
|---|---|---|
| `appid` | yes | Spark application ID |

**Returns:** JSON records with `Stage ID`, `real_queryid`, `elapsed time` (seconds), `partitions`, sorted by elapsed time descending.

---

### `get_metrics_by_node`

**Purpose:** Return aggregated task metrics for a specific query plan node type across all stages and queries.

**Parameters:**

| Parameter | Required | Default | Description |
|---|---|---|---|
| `appid` | yes | — | Spark application ID |
| `node_name` | yes | — | Plan node name, e.g. `"ColumnarExchange"`, `"IcebergScanTransformer"`, `"HashAggregateTransformer"` |
| `driver_ip` | no | `""` | IP of the Spark driver; used to fetch the event log via SSH/SCP when the database load fails or returns no data |

**Returns:** JSON records with `real_queryid`, `nodeID`, `Stage ID`, plus all metric columns (sum, mean, stddev) and `ElapsedTime`, `partnum`.

---

### `compare_query_plan`

**Purpose:** Compare the query execution plan for a specific query between two Spark applications.
Locates the existing comparison folder, reads (or generates) the PUML plan files, and instructs
the client to fetch them and summarize the differences.

**Parameters:**

| Parameter | Required | Description |
|---|---|---|
| `appid1` | yes | First Spark application ID (e.g. `app-20260223232457-0000`) |
| `appid2` | yes | Second Spark application ID |
| `queryid` | yes | Query identifier to compare (e.g. `q04`, `q23a`) |

**Workflow:**
1. Scans `DEFAULT_OUTPUT_ROOT` subdirectories for `comparison_<appid1>_vs_<appid2>.md` to find the base folder.
2. If no comparison exists, returns: *"Didn't find a comparison of … Do you want to run compare_runs first?"*
3. Checks `<base>/puml/<last4-appid1>-<queryid>.puml` and `<base>/puml/<last4-appid2>-<queryid>.puml`.
4. If both PUML files exist, reads them and returns a JSON payload with their contents plus the chart URL and PUML URLs.
5. If PUML files are missing, calls `compare_query_plans` to generate them (Spark session required), renders PNGs, then returns the same payload.

**Returns:** JSON payload containing `chart_url`, `puml_url1`, `puml_url2`, and the raw content of both
PUML files, followed by instructions for the client to fetch the URLs and summarize the plan differences.

---

### `generate_puml_from_eventlog`

**Purpose:** Load a Spark application's query plan directly from the Iceberg database, generate a PlantUML diagram (`.puml`) via `print_query_plan_puml`, render it to a PNG, and write both files to `/mnt/data1/mcp/output/<appid>/<queryid>.puml|png`.

**Parameters:**

| Parameter | Required | Default | Description |
|---|---|---|---|
| `appid` | yes | — | Spark application ID |
| `queryid` | yes | — | Query ID to generate the plan for (e.g. `"q7"`, `"q23a"`) |
| `driver_ip` | no | `""` | IP of the Spark driver; used to fetch the event log via SSH/SCP when the database load fails or returns no data |

**Returns:** JSON object with:

| Field | Description |
|---|---|
| `puml_file` | Absolute path of the written `.puml` file |
| `png_file` | Absolute path of the rendered `.png` file |
| `puml_url` | Public URL of the `.puml` |
| `png_url` | Public URL of the `.png` |

**Example:**
```json
{
  "puml_file": "/mnt/data1/mcp/output/app-20260223232457-0000/q7.puml",
  "png_file":  "/mnt/data1/mcp/output/app-20260223232457-0000/q7.png",
  "puml_url":  "http://bwdaily1.fyre.ibm.com:6020/app-20260223232457-0000/q7.puml",
  "png_url":   "http://bwdaily1.fyre.ibm.com:6020/app-20260223232457-0000/q7.png"
}
```


---

### `generate_puml_from_rest_url`

**Purpose:** Fetch a Spark query plan from the REST API, auto-convert any UI or proxy URL to the REST endpoint, save the raw plan JSON, generate a PlantUML diagram (`.puml`), and render it to a PNG.

**URL conversion rules:**

| Input URL form | Converted REST URL |
|---|---|
| `http://host:18080/history/app-xxx/SQL/execution/?id=N` | `http://host:18080/api/v1/applications/app-xxx/sql/N` |
| `https://host/proxy/app-xxx/SQL/execution/?id=N` | `http://127.0.0.1:4040/api/v1/applications/app-xxx/sql/N` |
| `http://127.0.0.1:18080/api/v1/applications/app-xxx/sql/N` | (unchanged) |

When the REST URL points to `127.0.0.1`, `driver_ip` must be supplied and the JSON is fetched via `ssh centos@<driver_ip> curl <url>`. Otherwise `curl` is used directly.

**Parameters:**

| Parameter | Required | Default | Description |
|---|---|---|---|
| `url` | yes | — | Any Spark UI, proxy, or REST API URL for a SQL execution |
| `simple_chart` | no | `false` | Suppress `Project` / `Filter` / `Exchange` nodes and colour each scan with a unique per-table colour |
| `from_explain_text` | no | `false` | Use `PlanTextUMLGenerator` on `planDescription` instead of the nodes/edges REST graph |
| `driver_ip` | no | `""` | IP of the Spark driver; required when the REST URL resolves to `127.0.0.1` |

**Returns:** JSON object with:

| Field | Description |
|---|---|
| `plan_file` | Absolute path of the saved raw plan JSON (`.plan`) |
| `puml_file` | Absolute path of the written `.puml` file |
| `png_file` | Absolute path of the rendered `.png` file |
| `puml_url` | Public URL of the `.puml` |
| `png_url` | Public URL of the `.png` |

**Example:**
```json
{
  "plan_file": "/mnt/data1/mcp/output/app-1787794607022-0000/1.plan",
  "puml_file": "/mnt/data1/mcp/output/app-1787794607022-0000/1.puml",
  "png_file":  "/mnt/data1/mcp/output/app-1787794607022-0000/1.png",
  "puml_url":  "http://bwdaily1.fyre.ibm.com:6020/app-1787794607022-0000/1.puml",
  "png_url":   "http://bwdaily1.fyre.ibm.com:6020/app-1787794607022-0000/1.png"
}
```

---

## Typical Workflow

```
1. compare_runs(run1_id, run2_id, run1_desc, run2_desc)
       → generates comparison folder with operator_comparison.md,
         plan_comp/*.html, full_plan/*.html, puml/*.png

2. get_hottest_stages(appid) / get_stage_stat(appid, queryid)
       → identify bottleneck stages

3. get_query_plan(appid, queryid) / print_query_plan_puml(appid, queryid)
       → inspect the execution plan for a specific query

4. compare_query_plan(appid1, appid2, queryid)
       → side-by-side plan diff between two runs for a specific query

5. generate_puml_from_rest_url(url)
       → generate a standalone .puml + .png from a Spark UI, proxy, or REST URL

6. get_shuffle_stat / get_metrics_by_node / get_table_scan_metrics
       → deep-dive into specific operator or I/O metrics
```

