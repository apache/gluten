---
layout: page
title: Velox GPU
nav_order: 9
parent: Getting-Started
---


# GPU Acceleration in Velox/Gluten
*Unified execution engine leveraging CUDF for hardware-accelerated Spark SQL queries*

---

## **1. Overview**
- **Purpose**: Accelerate Velox operators via CUDF APIs, replacing CPU execution when enabled.
- **Status**: Experimental (TPC-H SF1 validated). Integrates RAPIDS ecosystem with Apache Spark via Gluten .
- **Key Benefit**: Some queries achieved up to **8.1x speedup** on x86 vs. Spark Java engine .

---

## **2. Prerequisites**
- **CUDA Toolkit**: 12.8.0 ([download](https://developer.nvidia.com/cuda-downloads?target_os=Linux)).
- **NVIDIA Drivers**: Compatible with CUDA 12.8.
- **Container Toolkit**: Install `nvidia-container-toolkit` ([guide](https://docs.nvidia.com/datacenter/cloud-native/container-toolkit/latest/install-guide.html)).
- **System Reboot**: Required after driver installation.
- **Environment Setup**: Use [`start-cudf.sh`](https://github.com/apache/gluten/tree/main/dev/start-cudf.sh) for host configuration .

---

## **3. Implementation Mechanics**
- **Operator Conversion**:
    - Velox PlanNodes → **GPU operators** when `spark.gluten.sql.columnar.cudf=true`.
    - Falls back to CPU operators if GPU unsupported (triggers row/columnar data conversion) .
- **Debugging**: Enable `spark.gluten.debug.enabled.cudf=true` for operator replacement logs.
- **Memory**: Global [RMM](https://docs.rapids.ai/api/librmm/stable/) memory manager, cannot align with Spark memory system.

---

## **4. Docker Deployment**
```bash
docker pull apache/gluten:centos-9-jdk17-cuda13.1-cudf  # Pre-built GPU image
docker run --name gpu_gluten_container --gpus all -it apache/gluten:centos-9-jdk17-cuda13.1-cudf
```
- **Image Includes**: Native build cache, Gluten dependencies, Spark 3.4 environment.

---

## **5. Build & Deployment**
#### **Dependencies**
The OS, Spark version, Java version aligns with Gluten CPU.

### **Compilation Commands**
If building in the docker image, no need to set up script and build arrow.
```bash
./dev/buildbundle-veloxbe.sh --run_setup_script=OFF --build_arrow=OFF --enable_gpu=ON
```

---

## **6. GPU Operator Support Status**
| **Operator**    | **Status**      | **Notes**                |  
|-----------------|-----------------|--------------------------|
| **Scan**        |  ❌ Not supported| In Development           |  
| **Project**     | ⚠️ Partial      | Function TPCH-compatible |  
| **Filter**      | ✅ Implemented   | Core operator            |  
| **OrderBy**     | ✅ Implemented   |    |  
| **Aggregation** | ⚠️ Partial      | TPCH-compatible          |  
| **Join**        | ⚠️ Partial      | TPCH-compatible          |  
| **Spill**       | ❌ Not supported | In Planning              |  

---

## **7. Dynamic Execution**

Gluten uses Spark's Adaptive Query Execution (AQE) framework to evaluate each stage
independently at runtime and select the appropriate execution mode (CPU or GPU).

### **How It Works**

1. Gluten's `AdjustStageExecutionMode` optimizer rule runs for every AQE stage.
2. For each stage it checks whether the `WholeStageTransformer` is fully CUDF-tagged
   (i.e. all operators in the pipeline can run on GPU).
3. If yes, the stage's `ColumnarAQEShuffleReadExec` is switched to `GPUStageMode` and
   downstream `ColumnarShuffleExchangeExec` nodes are marked accordingly.

### **Only offload join stage**

By default, any fully CUDF-offloaded stage is routed to GPU. Setting
`spark.gluten.sql.columnar.gpu.onlyOffloadJoinStage = true` restricts GPU offload to
stages that contain a join operator. All other stages stay on CPU regardless of whether their operators support CUDF.

---

## **8. CPU/GPU Hybrid Execution**

With hybrid execution enabled, GPU stages identified in §7 are assigned a dedicated GPU
resource profile via `GlutenAutoAdjustStageResourceProfile`. Spark then schedules those
tasks only on executors that advertise a GPU resource. Scan stages and other non-GPU stages
continue to run on regular CPU executors.

### **Configuration**

Enable GPU acceleration and the hybrid scheduler together:

```properties
# Step 1 – enable CUDF operator replacement
spark.gluten.sql.columnar.cudf = true

# Step 2 – enable hybrid CPU/GPU execution
spark.gluten.auto.adjustStageResource.enabled = true
spark.gluten.sql.columnar.hybridExecution.enabled = true

# Step 3 – tell Spark about the GPU resource on each executor
spark.gluten.sql.columnar.hybridExecution.gpuResource.amountPerTask = 0.1 # fractional: 10 concurrent GPU tasks/executor
```

The full set of hybrid-execution knobs:

| Configuration Key | Recommended Value | Description |
|---|---|---|
| `spark.gluten.sql.columnar.cudf` | `true` | Enable CUDF GPU operator replacement. Must be `true` for any GPU execution. |
| `spark.gluten.auto.adjustStageResource.enabled` | `true` | Enable dynamic per-stage resource-profile adjustment. Required for hybrid execution to insert `ApplyResourceProfileExec` nodes. Must be combined with `spark.sql.adaptive.enabled=true`. |
| `spark.gluten.sql.columnar.hybridExecution.enabled` | `true` | Enable CPU/GPU hybrid execution. Stages are scheduled to CPU or GPU nodes based on their execution mode. Requires AQE (`spark.sql.adaptive.enabled=true`). |
| `spark.gluten.sql.columnar.hybridExecution.gpuResource.name` | `gpu` | The Spark custom-resource name for GPU. Must match `spark.executor.resource.<name>.*` and `spark.task.resource.<name>.*`. |
| `spark.gluten.sql.columnar.hybridExecution.cpuResource.name` | `cpu` | The Spark custom-resource name for CPU. Must match `spark.executor.resource.<name>.*` and `spark.task.resource.<name>.*` for CPU-stage scheduling to take effect. |
| `spark.gluten.sql.columnar.hybridExecution.gpuResource.amountPerTask` | `0.1` | Fractional GPU resource amount per task. Controls how many GPU tasks can run concurrently on a single executor (e.g. `0.1` → 10 tasks share 1 GPU). |
| `spark.gluten.sql.columnar.gpu.onlyOffloadJoinStage` | `true` | When `true`, only stages that contain a join operator are offloaded to GPU. All other stages execute on CPU. Useful for workloads where only join-heavy stages benefit from GPU acceleration. |

### **Cluster Setup for Hybrid Execution**

Hybrid execution requires a mixed cluster where CPU-only nodes and GPU-equipped nodes
coexist. Spark's custom resource API is used to label each worker type so the scheduler
can route CPU and GPU stages to the right nodes.

#### **Worker resource discovery scripts**

Each worker type needs a discovery script that reports its available resources to Spark.

**CPU workers** — `getCpuResources.sh`:
```bash
#!/usr/bin/env bash
echo {"name": "cpu", "addresses":["0"]}
```
> **Note**: This is a dummy script that doesn't report the actual number of cores on the worker. Its purpose is to register the `cpu` custom resource on CPU-only
nodes so that Spark's scheduler can identify them. CPU stages whose resource profile
requires the `cpu` resource will then be restricted to these nodes and will not be
scheduled on GPU workers (which do not register the `cpu` resource).

**GPU workers** — `getGpuResources.sh`:
```bash
#!/usr/bin/env bash
ADDRS=$(nvidia-smi --query-gpu=index --format=csv,noheader \
        | sed -e ':a' -e 'N' -e '$!ba' -e 's/\n/","/g')
echo {"name": "gpu", "addresses":["$ADDRS"]}
```

#### **Worker properties files**

Pass a properties file to each worker type at startup so Spark registers the correct
resource and discovery script. (via `--properties-file $PROPERTIES_FILE`)

**cpu-worker.conf** (placed on every CPU-only node):
```properties
spark.worker.resource.cpu.amount             = 1
spark.worker.resource.cpu.discoveryScript    /path/to/getCpuResources.sh
```

**gpu-worker.conf** (placed on every GPU node):
```properties
spark.worker.resource.gpu.amount             = 1
spark.worker.resource.gpu.discoveryScript    /path/to/getGpuResources.sh
```

> **Note**: With `amount = 1`, Spark allows only one executor per worker node for that
> resource type (since each executor consumes one unit and only one is available). To run
> multiple executors on the same node, increase `spark.worker.resource.<name>.amount` to
> match the desired executor count and update the discovery script to return the
> corresponding number of addresses.

#### **Recommended Spark application settings for hybrid clusters**

```properties
# Enable CPU/GPU hybrid execution
spark.gluten.sql.columnar.hybridExecution.enabled = true
spark.gluten.auto.adjustStageResource.enabled = true
spark.gluten.sql.columnar.cudf = true
spark.gluten.sql.columnar.gpu.onlyOffloadJoinStage = true

# Register CPU resource to Spark's default resource profile
spark.executor.resource.cpu.amount = 1
spark.worker.resource.cpu.discoveryScript = /path/to/getCpuResources.sh

# Register CPU/GPU resource name to Gluten
spark.gluten.sql.columnar.hybridExecution.cpuResource.name = cpu
spark.gluten.sql.columnar.hybridExecution.gpuResource.name = gpu
# Set GPU resource amount per task.
spark.gluten.sql.columnar.hybridExecution.gpuResource.amountPerTask = 0.1

# Set GPU concurrency per executor
spark.gluten.sql.columnar.backend.velox.cudf.concurrentGpuTasks = 3

# Enable async shuffle read(recommended for all GPU execution)
spark.gluten.sql.columnar.backend.velox.gpuAsyncShuffleReader.enabled = true
spark.gluten.sql.columnar.backend.velox.gpuAsyncShuffleReader.threadPoolSize = 8
gpuAsyncShuffleReader.maxPrefetchBytes = 2GB

```

---

## **9. Performance Tuning**

### **9.1 Concurrent GPU Tasks**

The `spark.gluten.sql.columnar.backend.velox.cudf.concurrentGpuTasks` setting controls how
many Velox GPU pipelines are allowed to execute simultaneously on a single executor.

```properties
spark.gluten.sql.columnar.backend.velox.cudf.concurrentGpuTasks = 2
```

**Guidance**: Two settings jointly determine GPU utilisation on a single executor:

- **Tasks per executor** (Spark level):
  `spark.gluten.sql.columnar.hybridExecution.gpuResource.amountPerTask` is used for setting the resource profile for GPU stages. It controls how
  many tasks Spark schedules on one executor:

  `A = min(spark.executor.cores / spark.task.cpus, floor(1 / amountPerTask))`

  It is a scheduling hint, not a hard GPU limit. Set it to a small value (e.g. `0.1`)
  so that CPU work within a GPU stage(such as shuffle read) is not throttled by the task slot limit.

- **GPU concurrency per executor** (Velox level):
  `spark.gluten.sql.columnar.backend.velox.cudf.concurrentGpuTasks` is the maximum
  number of cuDF operator pipelines that may run simultaneously on the device across all
  tasks on that executor:

  `total_gpu_concurrency = min(A, spark.gluten.sql.columnar.backend.velox.cudf.concurrentGpuTasks)`

  Because GPU capacity is primarily bounded by device memory, start with
  `spark.gluten.sql.columnar.backend.velox.cudf.concurrentGpuTasks = 2` and increase
  gradually while monitoring GPU memory usage. Setting it too high can trigger
  out-of-memory errors on the device.

---

### **9.2 Async Shuffle Read**

In a CPU/GPU hybrid workload the shuffle read phase is typically the bottleneck:
GPU tasks must wait for CPU-side decompression and deserialization of shuffle data
before they can start executing on device. The GPU async shuffle reader overlaps
CPU-side I/O with GPU execution, keeping the GPU busy while blocks are being fetched
and decoded in a background thread pool. It's recommended to enable Async shuffle read for GPU execution.

**Reference:** GPU async shuffle read design and PR [PR#12370](https://github.com/apache/gluten/pull/12370)

| Configuration Key | Recommended Value | Description |
|---|---|---|
| `spark.gluten.sql.columnar.backend.velox.gpuAsyncShuffleReader.enabled` | `true` | Enable the GPU async shuffle reader. When `true`, shuffle streams are read and deserialized by a background thread pool, overlapping I/O with GPU computation. When `false`, reads happen synchronously on the GPU task thread. |
| `spark.gluten.sql.columnar.backend.velox.gpuAsyncShuffleReader.threadPoolSize` | `2` | Number of background threads used for decompression and deserialization. |
| `spark.gluten.sql.columnar.backend.velox.gpuAsyncShuffleReader.maxPrefetchBytes` | `2GB` | Maximum CPU memory consumed by prefetched shuffle data while the GPU task thread is busy. Setting too low stalls prefetching; too high risks CPU OOM. |

---

## **10. Performance Validation**

GPU performs better on operator HashJoin and HashAggregation.
Single Operator like Hash Agg shows 5x speedup.

---

## **11. Relevant Resources**
1. [CUDF Docs](https://docs.rapids.ai/api/cudf/stable/libcudf_docs/) - GPU operator APIs.
2. [Gluten GPU Issue #9098](https://github.com/apache/gluten/issues/8851) - Development tracker.
