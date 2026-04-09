---
layout: page
title: QueryTrace
nav_order: 14
parent: Developer Overview
---

# Background
Currently, we have [MicroBenchmarks](https://github.com/apache/gluten/blob/main/docs/developers/MicroBenchmarks.md) to profile the Velox plan execution in stage level, now we have query trace replayer to specify plan node id to profile in operator level.

We cannot profile operator level directly because ValueStreamNode cannot serialize and deserialize, so we should generate benchmark first, and then enable query trace in benchmark which replaces the ValueStreamNode to ValuesNode.

Relevant link:
https://facebookincubator.github.io/velox/develop/debugging/tracing.html

https://facebookincubator.github.io/velox/configs.html#tracing

https://velox-lib.io/blog/velox-query-tracing

# Usage
Firstly, we should use MicroBenchmark to generate the input parquet data file and substrait json plan.

```shell
/tmp/saveDir
├── conf_35_0.ini
├── data_35_0_0.parquet
├── plan_35_0.json
└── split_35_0_0.json
```
Secondly, execute the benchmark to get the task id and query trace node id, for example, the task id is `Gluten_Stage_0_TID_0_VTID_0` and we will profile partial aggregation of node id `7`.

```shell
W0113 15:30:18.903225 2524110 GenericBenchmark.cc:251] Setting CPU for thread 0 to 0
I0113 15:30:19.093111 2524110 Task.cpp:2051] Terminating task Gluten_Stage_0_TID_0_VTID_0 with state Finished after running for 142ms
W0113 15:30:19.093384 2524110 GenericBenchmark.cc:485] {Task Gluten_Stage_0_TID_0_VTID_0 (Gluten_Stage_0_TID_0_VTID_0) Finished
Plan:
-- Project[8][expressions: (n8_3:INTEGER, hash_with_seed(42,"n6_5","n6_6")), (n8_4:DOUBLE, "n6_5"), (n8_5:DOUBLE, "n6_6"), (n8_6:BIGINT, "n7_2")] -> n8_3:INTEGER, n8_4:DOUBLE, n8_5:DOUBLE, n8_6:BIGINT
  -- Aggregation[7][PARTIAL [n6_5, n6_6] n7_2 := sum_partial("n6_7")] -> n6_5:DOUBLE, n6_6:DOUBLE, n7_2:BIGINT
    -- Project[6][expressions: (n6_5:DOUBLE, "n5_7"), (n6_6:DOUBLE, "n5_8"), (n6_7:BIGINT, unscaled_value("n5_9"))] -> n6_5:DOUBLE, n6_6:DOUBLE, n6_7:BIGINT
      -- Project[5][expressions: (n5_7:DOUBLE, "n1_4"), (n5_8:DOUBLE, "n1_5"), (n5_9:DECIMAL(7, 2), "n1_6"), (n5_10:DOUBLE, "n1_7"), (n5_11:DOUBLE, "n3_1")] -> n5_7:DOUBLE, n5_8:DOUBLE, n5_9:DECIMAL(7, 2), n5_10:DOUBLE, n5_11:DOUBLE
        -- HashJoin[4][INNER n1_8=n3_2] -> n1_4:DOUBLE, n1_5:DOUBLE, n1_6:DECIMAL(7, 2), n1_7:DOUBLE, n1_8:DOUBLE, n3_1:DOUBLE, n3_2:DOUBLE
          -- Project[1][expressions: (n1_4:DOUBLE, "n0_0"), (n1_5:DOUBLE, "n0_1"), (n1_6:DECIMAL(7, 2), "n0_2"), (n1_7:DOUBLE, "n0_3"), (n1_8:DOUBLE, "n0_3")] -> n1_4:DOUBLE, n1_5:DOUBLE, n1_6:DECIMAL(7, 2), n1_7:DOUBLE, n1_8:DOUBLE
            -- TableScan[0][table: hive_table, remaining filter: (isnotnull("sr_store_sk"))] -> n0_0:DOUBLE, n0_1:DOUBLE, n0_2:DECIMAL(7, 2), n0_3:DOUBLE
          -- Project[3][expressions: (n3_1:DOUBLE, "n2_0"), (n3_2:DOUBLE, "n2_0")] -> n3_1:DOUBLE, n3_2:DOUBLE
            -- ValueStream[2][] -> n2_0:DOUBLE
```

Thirdly, execute the benchmark and enable the query trace.

```shell
 ./generic_benchmark --conf /tmp/saveDir/conf_35_0.ini --plan /tmp/saveDir/plan_35_0.json \  --split /tmp/saveDir/split_35_0_0.json --data /tmp/saveDir/data_35_0_0.parquet  --query_trace_enabled=true --query_trace_dir=/tmp/query_trace --query_trace_node_ids=7 --query_trace_max_bytes=100000000000   --query_trace_task_reg_exp=Gluten_Stage_0_TID_0_VTID_0 --threads 1 --noprint-result
```

We can see the following message, which indicates the query trace runs successfully.
```shell
I0114 11:06:52.461263 2587302 Task.cpp:3072] Trace input for plan nodes 7 from task Gluten_Stage_0_TID_0_VTID_0
I0114 11:06:52.462595 2587302 Operator.cpp:135] Trace input for operator type: PartialAggregation, operator id: 5, pipeline: 0, driver: 0, task: Gluten_Stage_0_TID_0_VTID_0
```

Now we can see the data in query trace directory `/tmp/query_trace`.

```shell
/tmp/query_trace/
└── Gluten_Stage_0_TID_0_VTID_0
    └── Gluten_Stage_0_TID_0_VTID_0
        ├── 7
        │   └── 0
        │       └── 0
        │           ├── op_input_trace.data
        │           └── op_trace_summary.json
        └── task_trace_meta.json
```
Fourthly, replay the query. Show the query trace summary by following command.

```shell

/mnt/DP_disk1/code/velox/build/velox/tool/trace# ./velox_query_replayer  --root_dir /tmp/query_trace --task_id Gluten_Stage_0_TID_0_VTID_0 --query_id=Gluten_Stage_0_TID_0_VTID_0 --node_id=7 --summary
WARNING: Logging before InitGoogleLogging() is written to STDERR
I0115 20:27:25.821105 2684048 HiveConnector.cpp:56] Hive connector test-hive created with maximum of 20000 cached file handles.
I0115 20:27:25.823112 2684048 TraceReplayRunner.cpp:223]
++++++Query trace summary++++++
Number of tasks: 1

++++++Query configs++++++
        max_spill_bytes: 107374182400
        max_output_batch_rows: 4096
        max_partial_aggregation_memory: 80530636
        max_extended_partial_aggregation_memory: 120795955
        max_spill_level: 4
        spill_enabled: true
        spark.bloom_filter.max_num_bits: 4194304
        spillable_reservation_growth_pct: 25
        query_trace_dir: /tmp/query_trace
        spiller_num_partition_bits: 3
        preferred_output_batch_rows: 4096
        spill_write_buffer_size: 1048576
        spiller_start_partition_bit: 29
        spill_prefixsort_enabled: false
        abandon_partial_aggregation_min_rows: 100000
        adjust_timestamp_to_session_timezone: true
        query_trace_max_bytes: 100000000000
        query_trace_enabled: true
        spark.legacy_date_formatter: false
        join_spill_enabled: 1
        query_trace_task_reg_exp: Gluten_Stage_0_TID_0_VTID_0
        query_trace_node_ids: 7
        order_by_spill_enabled: 1
        aggregation_spill_enabled: 1
        abandon_partial_aggregation_min_pct: 90
        session_timezone: Etc/UTC
        driver_cpu_time_slice_limit_ms: 0
        max_spill_file_size: 1073741824
        max_spill_run_rows: 3145728
        spark.bloom_filter.expected_num_items: 1000000
        spill_read_buffer_size: 1048576
        spill_compression_codec: lz4
        spark.partition_id: 0
        max_split_preload_per_driver: 2
        spark.bloom_filter.num_bits: 8388608

++++++Connector configs++++++
test-hive
        file_column_names_read_as_lower_case: true
        partition_path_as_lower_case: false
        hive.reader.timestamp_unit: 6
        hive.parquet.writer.timestamp_unit: 6
        max_partitions_per_writers: 10000
        ignore_missing_files: 0

++++++Task query plan++++++
-- Project[8][expressions: (n8_3:INTEGER, hash_with_seed(42,"n6_5","n6_6")), (n8_4:DOUBLE, "n6_5"), (n8_5:DOUBLE, "n6_6"), (n8_6:BIGINT, "n7_2")] -> n8_3:INTEGER, n8_4:DOUBLE, n8_5:DOUBLE, n8_6:BIGINT
  -- Aggregation[7][PARTIAL [n6_5, n6_6] n7_2 := sum_partial("n6_7")] -> n6_5:DOUBLE, n6_6:DOUBLE, n7_2:BIGINT
    -- Project[6][expressions: (n6_5:DOUBLE, "n5_7"), (n6_6:DOUBLE, "n5_8"), (n6_7:BIGINT, unscaled_value("n5_9"))] -> n6_5:DOUBLE, n6_6:DOUBLE, n6_7:BIGINT
      -- Project[5][expressions: (n5_7:DOUBLE, "n1_4"), (n5_8:DOUBLE, "n1_5"), (n5_9:DECIMAL(7, 2), "n1_6"), (n5_10:DOUBLE, "n1_7"), (n5_11:DOUBLE, "n3_1")] -> n5_7:DOUBLE, n5_8:DOUBLE, n5_9:DECIMAL(7, 2), n5_10:DOUBLE, n5_11:DOUBLE
        -- HashJoin[4][INNER n1_8=n3_2] -> n1_4:DOUBLE, n1_5:DOUBLE, n1_6:DECIMAL(7, 2), n1_7:DOUBLE, n1_8:DOUBLE, n3_1:DOUBLE, n3_2:DOUBLE
          -- Project[1][expressions: (n1_4:DOUBLE, "n0_0"), (n1_5:DOUBLE, "n0_1"), (n1_6:DECIMAL(7, 2), "n0_2"), (n1_7:DOUBLE, "n0_3"), (n1_8:DOUBLE, "n0_3")] -> n1_4:DOUBLE, n1_5:DOUBLE, n1_6:DECIMAL(7, 2), n1_7:DOUBLE, n1_8:DOUBLE
            -- TableScan[0][table: hive_table, remaining filter: (isnotnull("sr_store_sk"))] -> n0_0:DOUBLE, n0_1:DOUBLE, n0_2:DECIMAL(7, 2), n0_3:DOUBLE
          -- Project[3][expressions: (n3_1:DOUBLE, "d_date_sk"), (n3_2:DOUBLE, "d_date_sk")] -> n3_1:DOUBLE, n3_2:DOUBLE
            -- Values[2][366 rows in 1 vectors] -> d_date_sk:DOUBLE

++++++Task Summaries++++++

++++++Task Gluten_Stage_0_TID_0_VTID_0++++++

++++++Pipeline 0++++++
driver 0: opType PartialAggregation, inputRows 293762,  inputBytes 5.69MB, rawInputRows 0, rawInputBytes 0B, peakMemory 5.39MB
```
Then you can use following command to re-execute the query plan.

```shell
/mnt/DP_disk1/code/velox/build/velox/tool/trace# ./velox_query_replayer  --root_dir /tmp/query_trace --task_id Gluten_Stage_0_TID_0_VTID_0 --query_id=Gluten_Stage_0_TID_0_VTID_0 --node_id=7
WARNING: Logging before InitGoogleLogging() is written to STDERR
I0115 20:30:17.665169 2685397 HiveConnector.cpp:56] Hive connector test-hive created with maximum of 20000 cached file handles.
I0115 20:30:17.676046 2685397 Cursor.cpp:192] Task spill directory[/tmp/velox_test_H163pi/test_cursor 1] created
I0115 20:30:17.941792 2685398 Task.cpp:2051] Terminating task test_cursor 1 with state Finished after running for 265ms
I0115 20:30:17.943285 2685397 OperatorReplayerBase.cpp:146] Stats of replaying operator PartialAggregation : Output: 292979 rows (9.27MB, 81 batches), Cpu time: 107.16ms, Wall time: 107.36ms, Blocked wall time: 0ns, Peak memory: 5.39MB, Memory allocations: 19, Threads: 1, CPU breakdown: B/I/O/F (252.36us/7.32ms/99.53ms/52.96us)
I0115 20:30:17.943373 2685397 OperatorReplayerBase.cpp:149] Memory usage: Aggregation_replayer usage 0B reserved 0B peak 6.00MB
    task.test_cursor 1 usage 0B reserved 0B peak 6.00MB
        node.N/A usage 0B reserved 0B peak 0B
            op.N/A.0.0.CallbackSink usage 0B reserved 0B peak 0B
        node.1 usage 0B reserved 0B peak 6.00MB
            op.1.0.0.PartialAggregation usage 0B reserved 0B peak 5.39MB
        node.0 usage 0B reserved 0B peak 0B
            op.0.0.0.OperatorTraceScan usage 0B reserved 0B peak 0B
I0115 20:30:17.943809 2685397 TempDirectoryPath.cpp:29] TempDirectoryPath:: removing all files from /tmp/velox_test_H163pi
```

Here is the full list of query trace flags in MicroBenchmark.
- query_trace_enabled: Whether to enable query trace.
- query_trace_dir: Base dir of a query to store tracing data.
- query_trace_node_ids: A comma-separated list of plan node ids whose input data will be traced. Empty string if only want to trace the query metadata.
- query_trace_max_bytes: The max trace bytes limit. Tracing is disabled if zero.
- query_trace_task_reg_exp: The regexp of traced task id. We only enable trace on a task if its id matches.
