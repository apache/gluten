---
layout: page
title: Velox to Bolt Migration Guide
nav_order: 19
---

# Guide to Migrating to Bolt Backend from Velox in Gluten

This document provides a detailed guide for users already running Gluten with the Velox backend, facilitating a smooth migration to the Bolt backend while leveraging its unified lakehouse acceleration capabilities.

## Overview of Migration Steps

1.  **Verify Backend Build**: Confirm that the Gluten JAR was built with the `-Pbackends-bolt` Maven profile (e.g., via the `jar_spark3x` Makefile target). If necessary, decompress the JAR and verify the presence of `libbolt_backend.so`.

2.  **Environment Preparation and Build**:
    -   **Prepare Build Environment**: Ensure your build host meets the prerequisites specified in the *Environment and Build Instructions* → *Prerequisites* section (Linux OS, GCC/Clang toolchain, Python with Conan, compatible kernel version, etc.).
    -   **Build and Package Artifacts**: Execute `make bolt-recipe`, `make release`, `make arrow`, and `make jar_spark3x` in sequence to generate a Gluten JAR integrated with the Bolt backend.

3.  **Deploy JAR and Enable Gluten Plugin**: Deploy the newly built JAR to your cluster. In your Spark configuration, enable the plugin with `spark.plugins=org.apache.gluten.GlutenPlugin`, enable off-heap memory allocation, and adopt the Columnar Shuffle Manager.

4.  **Migrate Configuration Settings**: Switch your existing configurations from the `spark.gluten.sql.columnar.backend.velox.*` namespace to `spark.gluten.sql.columnar.backend.bolt.*`, with reference to the configuration mappings provided in [bolt-configuration](./bolt-configuration.md) and [bolt-spark-configuration](./bolt-spark-configuration.md).

5.  **Validate Migration and Prepare Fallback**: Use the Spark UI, service logs, and execution plans to confirm that the active backend is `Bolt`. If compatibility issues arise, follow the *Rollback and Downgrade Strategy* to revert to the Velox backend or fall back to Vanilla Spark.

---

## 1. Environment and Build Instructions

Prior to adjusting Spark and Gluten configurations, you must ensure the Bolt backend is correctly compiled and packaged in your environment. This section details the required build environment and corresponding packaging procedures.

### Prerequisites

- For detailed environment requirements, see: [Bolt Backend README](../README.md#bolt-backend)

### Build and Package

Migrating from the Velox to the Bolt backend primarily involves adjusting the Maven profile and Makefile targets during the build process, which lays the foundation for subsequent configuration and deployment steps.

#### Step 1: Build the Native C++ Backend

First, compile the Bolt backend for Gluten, which can be accomplished using the provided `Makefile`, following these sub-steps:

1.  **Install Bolt Recipe**:
    This command downloads the Bolt source code, installs its dependencies, and adds its C++ package recipe to your local Conan cache.
    ```bash
    # Use the main branch of Bolt
    make bolt-recipe BOLT_BUILD_VERSION=main
    ```
    You can specify `BOLT_BUILD_VERSION` to select a specific branch or tag of Bolt, `main` branch by default

2.  **Compile Gluten's C++ Components**:
    This command compiles Gluten's own C++ code and links it with the Bolt backend prepared in the previous sub-step.
    ```bash
    # Compile the Release version
    make release

    # or specific the version for Bolt, and the version for Gluten
    make release BOLT_BUILD_VERSION=main GLUTEN_BUILD_VERSION=main

    ```
    - This command generates the native library `libbolt_backend.so`, located in the `cpp/build/releases/` directory.
    - During the first build, Conan will automatically compile any missing third-party dependencies, which may take a significant amount of time.

#### Step 2: Package the Java JAR

Once the native components are compiled, package `libbolt_backend.so` together with all Java/Scala code into the final Gluten JAR.

1.  **Prepare Arrow Dependencies**:
    Gluten depends on a specific version of the Arrow library. Run the following command to build and install it.
    ```bash
    make arrow
    ```

2.  **Package the JAR**:
    Use Maven for packaging. The key is to switch the profile from `-Pbackends-velox` to `-Pbackends-bolt`. Select the appropriate `jar_sparkXX` target based on your Spark version.

    ```bash
    # Example: Package for Spark 3.5
    make jar_spark35
    ```
    This command is equivalent to running `mvn package -Pbackends-bolt -Pspark-3.5 ...`. Maven will automatically handle dependencies and package `libbolt_backend.so` into the final JAR file.

#### Build Artifacts

After the build is complete, you will have the following key artifacts:

-   **Gluten JAR**: Located in the `output/` directory, with a filename like `gluten-spark3.5_2.12-1.0.0-SNAPSHOT-jar-with-dependencies.jar`. This JAR contains all Java/Scala code and the `libbolt_backend.so` native library.


## 2. Configuration Switching

Switching a Spark job from the Velox backend to the Bolt backend primarily involves adjusting Spark configuration settings.

### 2.1. Core Spark Configurations

Below is the minimal set of configurations required to enable Gluten with Bolt backend. Ensure these settings are added to your `spark-defaults.conf` file or included in the `spark-submit` command.

```properties
# 1. Enable the Gluten plugin
spark.plugins=org.apache.gluten.GlutenPlugin

# 2. Enable off-heap memory (mandatory for Gluten)
spark.memory.offHeap.enabled=true
spark.memory.offHeap.size=20g # Adjust based on your hardware specifications and job requirements

# 3. Use Gluten's Columnar Shuffle Manager
spark.shuffle.manager=org.apache.spark.shuffle.sort.ColumnarShuffleManager

# 4. Specify the Gluten JAR path
spark.driver.extraClassPath=/path/to/gluten-spark3.5_...jar
spark.executor.extraClassPath=/path/to/gluten-spark3.5_...jar
```


### 2.2. Common Velox-to-Bolt Configuration Key Mappings

Most performance-tuning configurations are shared between Bolt and Velox; you only need to replace `velox` with `bolt` in the configuration key. The table below lists some common settings and their corresponding migration instructions.

| Velox Key (`spark.gluten.sql.columnar.backend.velox.*`) | Bolt Key (`spark.gluten.sql.columnar.backend.bolt.*`) | Migration Notes |
| :-------------------------------------------------------- | :------------------------------------------------------ | :--- |
| `IOThreads` | `IOThreads` | I/O thread pool size. Both backends have this; can remain unchanged. |
| `flushablePartialAggregation` | `flushablePartialAggregation` | Enables flushable partial aggregation. Bolt also supports this; keep it enabled (`true`). |
| `footerEstimatedSize` | `footerEstimatedSize` | Estimated size of Parquet file metadata. Can be migrated directly. |
| `loadQuantum` | `loadQuantum` | Read unit size for file scans. Bolt supports this; adjust based on storage. |
| `prefetchRowGroups` | `prefetchRowGroups` | Number of Parquet row groups to prefetch. Bolt also supports this optimization. |
| `memoryUseHugePages` | `memoryUseHugePages` | Whether to use huge pages. Also applicable in Bolt; requires OS support. |
| `bloomFilter.expectedNumItems`| `bloomFilter.expectedNumItems`| Bloom filter expected number of items. Path is the same. |
| `bloomFilter.numBits` | `bloomFilter.numBits` | Bloom filter bit array size. Path is the same. |
| `filePreloadThreshold` | `filePreloadThreshold` | File preload threshold. Path is the same. |
| `resizeBatches.shuffleInput` | `resizeBatches.shuffleInput` | Whether to resize batches before shuffle writes. Supported by Bolt. |
| `resizeBatches.shuffleOutput` | `resizeBatches.shuffleOutput` | Whether to resize batches after shuffle reads. Supported by Bolt. |
| `spillStrategy` | `spillStrategy` | Spill strategy (`auto` or `none`). Supported by Bolt, with similar behavior. |
| `ssdCachePath` | `ssdCachePath` | Path for local SSD cache. Bolt supports this with the same configuration. |
| `ssdCacheSize` | `ssdCacheSize` | Size of local SSD cache. Supported by Bolt. |
| `ssdCacheShards` | `ssdCacheShards` | Number of local SSD cache shards. Supported by Bolt. |

For a more detailed list of configurations, please refer to the official documentation:
- [Bolt Backend Configuration](./bolt-configuration.md)
- [Velox Backend Configuration](./velox-configuration.md)
- [Bolt Spark Configuration](./bolt-spark-configuration.md)
- [Velox Spark Configuration](./velox-spark-configuration.md)

## 3. Rollback and Downgrade Strategy

If compatibility or stability issues arise after migrating to the Bolt backend, you can revert to the Velox backend or fall back to Vanilla Spark using the following rollback and downgrade procedures.For persistent issues, we strongly recommend **submitting a detailed Issue on the [Bolt GitHub repository](https://github.com/bytedance/bolt)** to facilitate troubleshooting and resolution by the development team.

-   **Full Rollback to Velox**:
    1.  Repackage the Gluten JAR using the `-Pbackends-velox` Maven profile.
    2.  Update the `spark.driver.extraClassPath` and `spark.executor.extraClassPath` configurations in your Spark job to point to the newly built Velox backend JAR.
    3.  Revert all Spark configurations prefixed with `spark.gluten.sql.columnar.backend.bolt.*` back to `spark.gluten.sql.columnar.backend.velox.*`.

-   **Temporarily Disable Gluten (Fallback to Vanilla Spark)**:
    The fastest temporary workaround is to fully disable the Gluten plugin, allowing the job to run as a native Spark application.
    ```properties
    # Set the value of spark.plugins to empty or comment it out
    # spark.plugins=org.apache.gluten.GlutenPlugin
    ```
    Alternatively, disable Gluten at runtime by adding the configuration: `--conf spark.gluten.enabled=false`.

-   **Disable Specific Features or Operators**:
    If an issue is triggered by a specific operator or feature, you can mitigate it by disabling the problematic component via configuration. This will force the affected part of the query to fall back to Vanilla Spark execution, while the rest of the query continues to be accelerated by Bolt.
    For example, if window functions are suspected to be the root cause, disable them with the following setting:
    ```properties
    spark.gluten.sql.columnar.window=false
    ```
    This approach enables problem isolation while retaining partial acceleration benefits from Bolt.
