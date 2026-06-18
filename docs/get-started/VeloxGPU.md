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
docker pull apache/gluten:centos-9-jdk8-cudf  # Pre-built GPU image
docker run --name gpu_gluten_container --gpus all -it apache/gluten:centos-9-jdk8-cudf
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

## **7. Dynamic Execution

The first stage contains TableScan operator which is IO bound stage, schedule to CPU node.
The second stage that contains join which is computation intensive, schedule to GPU node.

---

## **8. Performance Validation**

GPU performs better on operator HashJoin and HashAggregation.
Single Operator like Hash Agg shows 5x speedup.

---

## **9. Relevant Resources**
1. [CUDF Docs](https://docs.rapids.ai/api/cudf/stable/libcudf_docs/) - GPU operator APIs.
2. [Gluten GPU Issue #9098](https://github.com/apache/gluten/issues/8851) - Development tracker.
