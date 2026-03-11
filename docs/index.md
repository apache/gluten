---
layout: default
title: Home
nav_order: 1
permalink: /
description: This site serves as a collection of documentation about the Gluten, a plugin to Double SparkSQL's Performance
---
# Overview
Gluten: Plugin to Double SparkSQL's Performance.

# 1 Introduction

## 1.1 Problem Statement

Apache Spark is a stable, mature project that has been developed for many years. It is one of the best frameworks to scale out for processing petabyte-scale datasets. However, the Spark community has had to address performance challenges that require various optimizations over time. As a key optimization in Spark 2.0, Whole Stage Code Generation is introduced to replace Volcano Model, which achieves 2x speedup. Henceforth, most optimizations are at query plan level. Single operator's performance almost stops growing.

<p align="center">
<img src="https://user-images.githubusercontent.com/47296334/199853029-b6d0ea19-f8e4-4f62-9562-2838f7f159a7.png" width="800">
</p>

On the other side, SQL engines have been researched for many years. There are a few libraries like Clickhouse, Arrow and Velox, etc. By using features like native implementation, columnar data format and vectorized data processing, these libraries can outperform Spark's JVM based SQL engine. However, these libraries only support single node execution.

## 1.2 Gluten's Solution

“Gluten” is Latin for glue. The main goal of Gluten project is to “glue" native libraries with SparkSQL. Thus, we can benefit from high scalability of Spark SQL framework and high performance of native libraries.

The basic rule of Gluten's design is that we would reuse spark's whole control flow and as many JVM code as possible but offload the compute-intensive data processing part to native code. Here is what Gluten does:
* Transform Spark’s whole stage physical plan to Substrait plan and send to native
* Offload performance-critical data processing to native library
* Define clear JNI interfaces for native libraries
* Switch available native backends easily
* Reuse Spark’s distributed control flow
* Manage data sharing between JVM and native
* Extensible to support more native accelerators

## 1.3 Target User

Gluten's target user is anyone who wants to accelerate SparkSQL fundamentally. As a plugin to Spark, Gluten doesn't require any change for dataframe API or SQL query, but only requires user to make correct configuration.
See Gluten configuration properties [here](https://github.com/apache/gluten/blob/main/docs/Configuration.md).

## 1.4 References

You can click below links for more related information.
- [Gluten Intro Video at Data AI Summit 2022](https://www.youtube.com/watch?v=0Q6gHT_N-1U)
- [Gluten Intro Article at Medium.com](https://medium.com/intel-analytics-software/accelerate-spark-sql-queries-with-gluten-9000b65d1b4e)
- [Gluten Intro Article at Kyligence.io(in Chinese)](https://cn.kyligence.io/blog/gluten-spark/)
- [Velox Intro from Meta](https://engineering.fb.com/2023/03/09/open-source/velox-open-source-execution-engine/)

# 2 Architecture

The overview chart is like below. Substrait provides a well-defined cross-language specification for data compute operations (see more details [here](https://substrait.io/)). Spark physical plan is transformed to Substrait plan. Then Substrait plan is passed to native through JNI call.
On native side, the native operator chain will be built out and offloaded to native engine. Gluten will return Columnar Batch to Spark and Spark Columnar API (since Spark-3.0) will be used at execution time. Gluten uses Apache Arrow data format as its basic data format, so the returned data to Spark JVM is ArrowColumnarBatch.
<p align="center">
<img src="https://user-images.githubusercontent.com/47296334/199617207-1140698a-4d53-462d-9bc7-303d14be060b.png" width="800">
</p>
Currently, Gluten only supports Clickhouse backend & Velox backend. Velox is a C++ database acceleration library which provides reusable, extensible and high-performance data processing components. More details can be found from https://github.com/facebookincubator/velox/. Gluten can also be extended to support more backends.

There are several key components in Gluten:
* **Query Plan Conversion**: converts Spark's physical plan to Substrait plan.
* **Unified Memory Management**: controls native memory allocation.
* **Columnar Shuffle**: shuffles Gluten columnar data. The shuffle service still reuses the one in Spark core. A kind of columnar exchange operator is implemented to support Gluten columnar data format.
* **Fallback Mechanism**: supports falling back to Vanilla spark for unsupported operators. Gluten ColumnarToRow (C2R) and RowToColumnar (R2C) will convert Gluten columnar data and Spark's internal row data if needed. Both C2R and R2C are implemented in native code as well
* **Metrics**: collected from Gluten native engine to help identify bugs, performance bottlenecks, etc. The metrics are displayed in Spark UI.
* **Shim Layer**: supports multiple Spark versions. We plan to only support Spark's latest 2 or 3 releases. Currently, Spark-3.2, Spark-3.3 & Spark-3.4 (experimental) are supported.
