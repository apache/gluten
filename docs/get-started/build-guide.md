---
layout: page
title: Build Parameters for Velox Backend
nav_order: 4
parent: Getting-Started
---
## Build Parameters
### Native build parameters for buildbundle-veloxbe.sh or builddeps-veloxbe.sh
Please set them via `--`, e.g. `--build_type=Release`.

| Parameters             | Description                                                                                   | Default |
|------------------------|-----------------------------------------------------------------------------------------------|---------|
| build_type             | Build type for Velox & gluten cpp, CMAKE_BUILD_TYPE.                                          | Release |
| build_tests            | Build gluten cpp tests.                                                                       | OFF     |
| build_examples         | Build udf example.                                                                            | OFF     |
| build_benchmarks       | Build gluten cpp benchmarks.                                                                  | OFF     |
| enable_jemalloc_stats  | Print jemalloc stats for debugging.                                                           | OFF     |
| enable_qat             | Enable QAT for shuffle data de/compression.                                                   | OFF     |
| enable_s3              | Build with S3 support.                                                                        | OFF     |
| enable_gcs             | Build with GCS support.                                                                       | OFF     |
| enable_hdfs            | Build with HDFS support.                                                                      | OFF     |
| enable_abfs            | Build with ABFS support.                                                                      | OFF     |
| enable_vcpkg           | Enable vcpkg for static build.                                                                | OFF     |
| run_setup_script       | Run setup script to install Velox dependencies.                                               | ON      |
| velox_repo             | Specify your own Velox repo to build.                                                         | ""      |
| velox_branch           | Specify your own Velox branch to build.                                                       | ""      |
| velox_home             | Specify your own Velox source path to build.                                                  | ""      |
| build_velox_tests      | Build Velox tests.                                                                            | OFF     |
| build_velox_benchmarks | Build Velox benchmarks (velox_tests and connectors will be disabled if ON)                    | OFF     |
| build_arrow            | Build arrow java/cpp and install the libs in local. Can turn it OFF after first build.        | ON      |
| spark_version          | Build for specified version of Spark(3.3, 3.4, 3.5, ALL). `ALL` means build for all versions. | ALL     |

### Velox build parameters for build-velox.sh
Please set them via `--`, e.g., `--velox_home=/YOUR/PATH`.

| Parameters       | Description                                                   | Default                                  |
|------------------|---------------------------------------------------------------|------------------------------------------|
| velox_home       | Specify Velox source path to build.                           | GLUTEN_SRC/ep/build-velox/build/velox_ep |
| build_type       | Velox build type, i.e., CMAKE_BUILD_TYPE.                     | Release                                  |
| enable_s3        | Build Velox with S3 support.                                  | OFF                                      |
| enable_gcs       | Build Velox with GCS support.                                 | OFF                                      |
| enable_hdfs      | Build Velox with HDFS support.                                | OFF                                      |
| enable_abfs      | Build Velox with ABFS support.                                | OFF                                      |
| run_setup_script | Run setup script to install Velox dependencies before build.  | ON                                       |
| build_test_utils | Build Velox with cmake arg -DVELOX_BUILD_TEST_UTILS=ON if ON. | OFF                                      |
| build_tests      | Build Velox test.                                             | OFF                                      |
| build_benchmarks | Build Velox benchmarks.                                       | OFF                                      |

### Maven build parameters
The below parameters can be set via `-P` for mvn.

| Parameters          | Description                           | Default state |
|---------------------|---------------------------------------|---------------|
| backends-velox      | Build Gluten Velox backend.           | disabled      |
| backends-clickhouse | Build Gluten ClickHouse backend.      | disabled      |
| celeborn            | Build Gluten with Celeborn.           | disabled      |
| uniffle             | Build Gluten with Uniffle.            | disabled      |
| delta               | Build Gluten with Delta Lake support. | disabled      |
| iceberg             | Build Gluten with Iceberg support.    | disabled      |
| hudi                | Build Gluten with Hudi support.       | disabled      |
| spark-3.3           | Build Gluten for Spark 3.3.           | disabled       |
| spark-3.4           | Build Gluten for Spark 3.4.           | disabled      |
| spark-3.5           | Build Gluten for Spark 3.5.           | enabled      |

## Gluten Jar for Deployment
The gluten jar built out is under `GLUTEN_SRC/package/target/`.
It's name pattern is `gluten-<backend_type>-bundle-spark<spark.bundle.version>_<scala.binary.version>-<os.detected.release>_<os.detected.release.version>-<project.version>.jar`.

| Spark Version | spark.bundle.version | scala.binary.version |
|---------------|----------------------|----------------------|
| 3.3.1         | 3.3                  | 2.12                 |
| 3.4.4         | 3.4                  | 2.12                 |
| 3.5.5         | 3.5                  | 2.12                 |
