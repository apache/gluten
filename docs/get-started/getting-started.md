---
layout: page
title: Getting Started
nav_order: 2
has_children: true
permalink: /getting-started/
---
# Getting Started with Apache Gluten

Apache Gluten is a plugin that accelerates Apache Spark SQL by offloading execution to native engines. It requires no changes to your existing Spark SQL queries or DataFrame API code and only needs configuration changes.

## Choosing a Backend

Gluten supports two native backends:

| Backend | Description | Guide |
|---------|-------------|-------|
| **Velox** | Meta's C++ execution library| [Velox Backend](Velox.md) |
| **ClickHouse** | Column-oriented DBMS ported as a native library| [ClickHouse Backend](ClickHouse.md) |

## Quick Start (Velox Backend)

### 1. Prerequisites

- **OS**: Ubuntu 20.04/22.04 or CentOS 7/8 (other Linux distros may work with static build but are not officially tested)
- **JDK**: OpenJDK 8 or 17 (Spark 4.0 requires JDK 17+)
- **Spark**: 3.3.1, 3.4.4, 3.5.5, 4.0.1, or 4.1.1
- **Scala**: 2.12 (Spark 4.0 requires Scala 2.13)

### 2. Build

```bash
## Set JAVA_HOME (JDK 8 or 17)
export JAVA_HOME=/path/to/your/jdk
export PATH=$JAVA_HOME/bin:$PATH

## Clone and build
git clone https://github.com/apache/gluten.git
cd gluten
./dev/buildbundle-veloxbe.sh
```

See the [Build Guide](build-guide.md) for build options and parameters.

### 3. Deploy

Add the Gluten jar and required configuration to your Spark session. Off-heap memory must be enabled, and the columnar shuffle manager is needed for native shuffle support:

```bash
spark-shell \
  --master yarn --deploy-mode client \
  --conf spark.plugins=org.apache.gluten.GlutenPlugin \
  --conf spark.memory.offHeap.enabled=true \
  --conf spark.memory.offHeap.size=20g \
  --conf spark.shuffle.manager=org.apache.spark.shuffle.sort.ColumnarShuffleManager \
  --jars /path/to/gluten-velox-bundle-*.jar
```

Adjust `offHeap.size` based on your environment.

Alternatively, you can enable **dynamic off-heap sizing** (experimental) to let Gluten manage the off-heap/on-heap split automatically based on `spark.executor.memory`:

```bash
spark-shell \
  --master yarn --deploy-mode client \
  --conf spark.plugins=org.apache.gluten.GlutenPlugin \
  --conf spark.gluten.memory.dynamic.offHeap.sizing.enabled=true \
  --conf spark.shuffle.manager=org.apache.spark.shuffle.sort.ColumnarShuffleManager \
  --jars /path/to/gluten-velox-bundle-*.jar
```

With dynamic sizing, `spark.memory.offHeap.enabled` and `spark.memory.offHeap.size` are not needed - Velox uses the on-heap size as its memory budget. See [Dynamic Off-Heap Sizing](../developers/VeloxDynamicSizingOffheap.md) for details.

See the [Velox Backend](Velox.md) guide for a complete example with executor and driver settings.

### 4. Verify

Run a simple query and check the Spark UI for nodes containing `Transformer` or `Velox` or `Columnar` in the query plan, which indicate native execution.

## Next Steps

- [Configuration](../Configuration.md) — Tune Gluten settings
- [Velox Configuration](../velox-configuration.md) — Velox-specific knobs
- [Gluten UI](GlutenUI.md) — Monitor native execution in Spark UI
- [Troubleshooting](../velox-backend-troubleshooting.md) — Common issues and solutions

