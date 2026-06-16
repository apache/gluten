---
layout: page
title: Velox UDF and UDAF
nav_order: 13
has_children: true
parent: /developer-overview/
---
# Velox User-Defined Functions (UDF) and User-Defined Aggregate Functions (UDAF)

## Introduction

Velox backend supports User-Defined Functions (UDF) and User-Defined Aggregate Functions (UDAF).
Users can implement custom functions using the UDF interface provided by Velox and compile them into libraries.
At runtime, these UDFs are registered alongside their Java implementations via `CREATE TEMPORARY FUNCTION`.
Once registered, Gluten can parse and offload these UDFs to Velox during execution, 
meanwhile ensuring proper fallback to Java UDFs when necessary.
Registered UDAFs can be used both as regular aggregate functions and as aggregate window functions.

## Create and Build UDF/UDAF library

The following steps demonstrate how to set up a UDF library project:

- **Include the UDF Interface Header:**
  First, include the UDF interface header file [Udf.h](../../cpp/velox/udf/Udf.h) in the project file.
  The header file defines the `UdfEntry` struct, along with the macros for declaring the necessary functions to integrate the UDF into Gluten and Velox.

- **Implement the UDF:**
  Implement UDF. These functions should be able to register to Velox.

- **Implement the Interface Functions:**
  Implement the following interface functions that integrate UDF into Project Gluten:

    - `getNumUdf()`:
      This function should return the number of UDF in the library.
      This is used to allocating udfEntries array as the argument for the next function `getUdfEntries`.

    - `getUdfEntries(gluten::UdfEntry* udfEntries)`:
      This function should populate the provided udfEntries array with the details of the UDF, including function names and signatures.

    - `registerUdf()`:
      This function is called to register the UDF to Velox function registry.
      This is where users should register functions by calling `facebook::velox::exec::registerVectorFunction` or other Velox APIs.

    - The interface functions are mapped to marcos in [Udf.h](../../cpp/velox/udf/Udf.h).
  
  Assuming there is an existing Hive UDF `org.apache.gluten.sql.hive.MyUDF`, its native UDF can be implemented as follows.

  ```
  #include <velox/expression/VectorFunction.h>
  #include <velox/udf/Udf.h>

  namespace {
  static const char* kInteger = "integer";
  static const char* kMyUdfFunctionName = "org.apache.gluten.sql.hive.MyUDF";
  }

  const int kNumMyUdf = 1;

  const char* myUdfArgs[] = {kInteger}:
  gluten::UdfEntry myUdfSig = {kMyUdfFunctionName, kInteger, 1, myUdfArgs};

  class MyUdf : public facebook::velox::exec::VectorFunction {
    ... // Omit concrete implementation
  }

  static std::vector<std::shared_ptr<exec::FunctionSignature>>
  myUdfSignatures() {
    return {facebook::velox::exec::FunctionSignatureBuilder()
                .returnType(myUdfSig.dataType)
                .argumentType(myUdfSig.argTypes[0])
                .build()};
  }

  DEFINE_GET_NUM_UDF { return kNumMyUdf; }

  DEFINE_GET_UDF_ENTRIES { udfEntries[0] = myUdfSig; }

  DEFINE_REGISTER_UDF {
    facebook::velox::exec::registerVectorFunction(
        myUdf[0].name, myUdfSignatures(), std::make_unique<MyUdf>());
  }

  ```

To build the UDF library, users need to compile the C++ code and link to `libvelox.so`.
It's recommended to create a CMakeLists.txt for the project. Here's an example:

```
project(myudf)

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

set(GLUTEN_HOME /path/to/gluten)

add_library(myudf SHARED "MyUDF.cpp")

find_library(VELOX_LIBRARY REQUIRED NAMES velox HINTS ${GLUTEN_HOME}/cpp/build/releases NO_DEFAULT_PATH)

target_include_directories(myudf PRIVATE ${GLUTEN_HOME}/cpp ${GLUTEN_HOME}/ep/build-velox/build/velox_ep)
target_link_libraries(myudf PRIVATE ${VELOX_LIBRARY})
```

The steps for creating and building a UDAF library are quite similar to those for a UDF library.
The major difference lies in including and defining specific functions within the UDAF header file [Udaf.h](../../cpp/velox/udf/Udaf.h)

- `getNumUdaf()`
- `getUdafEntries(gluten::UdafEntry* udafEntries)`
- `registerUdaf()`

`gluten::UdafEntry` requires an additional field `intermediateType`, to specify the output type from partial aggregation.
For detailed implementation, you can refer to the example code in [MyUDAF.cc](../../cpp/velox/udf/examples/MyUDAF.cc)

## Using UDF/UDAF in Gluten

Gluten loads the UDF libraries at runtime. You can upload UDF libraries via `--files` or `--archives`, and configure the library paths using the provided Spark configuration, which accepts comma separated list of library paths.

Note if running on Yarn client mode, the uploaded files are not reachable on driver side. Users should copy those files to somewhere reachable for driver and set `spark.gluten.sql.columnar.backend.velox.driver.udfLibraryPaths`.
This configuration is also useful when the `udfLibraryPaths` is different between driver side and executor side.

- Use the `--files` option to upload a library and configure its relative path

```shell
--files /path/to/gluten/cpp/build/velox/udf/examples/libmyudf.so
--conf spark.gluten.sql.columnar.backend.velox.udfLibraryPaths=libmyudf.so
# Needed for Yarn client mode
--conf spark.gluten.sql.columnar.backend.velox.driver.udfLibraryPaths=file:///path/to/gluten/cpp/build/velox/udf/examples/libmyudf.so
```

- Use the `--archives` option to upload an archive and configure its relative path

```shell
--archives /path/to/udf_archives.zip#udf_archives
--conf spark.gluten.sql.columnar.backend.velox.udfLibraryPaths=udf_archives
# Needed for Yarn client mode
--conf spark.gluten.sql.columnar.backend.velox.driver.udfLibraryPaths=file:///path/to/udf_archives.zip
```

- Configure URI

You can also specify the local or HDFS URIs to the UDF libraries or archives. Local URIs should exist on driver and every worker nodes.

```shell
--conf spark.gluten.sql.columnar.backend.velox.udfLibraryPaths=file:///path/to/library_or_archive
```

## Try the example

We provided Velox UDF examples in file [MyUDF.cc](../../cpp/velox/udf/examples/MyUDF.cc) and UDAF examples in file [MyUDAF.cc](../../cpp/velox/udf/examples/MyUDAF.cc).
You need to build the gluten project with `--build_example=ON` to get the example libraries.

```shell
./dev/buildbundle-veloxbe.sh --build_examples=ON
```

Then, you can find the example libraries at /path/to/gluten/cpp/build/velox/udf/examples/

Start spark-shell or spark-sql with below configuration

```shell
# Use the `--files` option to upload a library and configure its relative path
--files /path/to/gluten/cpp/build/velox/udf/examples/libmyudf.so
--conf spark.gluten.sql.columnar.backend.velox.udfLibraryPaths=libmyudf.so
```

or

```shell
# Only configure URI
--conf spark.gluten.sql.columnar.backend.velox.udfLibraryPaths=file:///path/to/gluten/cpp/build/velox/udf/examples/libmyudf.so
```

Start `spark-sql` and run query. You need to add jar "spark-hive_2.12-<spark.version>-tests.jar" to the classpath for hive udf `org.apache.spark.sql.hive.execution.UDFStringString`

```
spark-sql (default)> create table tbl as select * from values ('hello');
Time taken: 3.656 seconds
spark-sql (default)> CREATE TEMPORARY FUNCTION hive_string_string AS 'org.apache.spark.sql.hive.execution.UDFStringString';
Time taken: 0.047 seconds
spark-sql (default)> select hive_string_string(col1, 'world') from tbl;
hello world
Time taken: 1.217 seconds, Fetched 1 row(s)
```

You can verify the offload with "explain".
```
spark-sql (default)> explain select hive_string_string(col1, 'world') from tbl;
VeloxColumnarToRow
+- ^(2) ProjectExecTransformer [HiveSimpleUDF#org.apache.spark.sql.hive.execution.UDFStringString(col1#11,world) AS hive_string_string(col1, world)#12]
   +- ^(2) InputIteratorTransformer[col1#11]
      +- RowToVeloxColumnar
         +- Scan hive spark_catalog.default.tbl [col1#11], HiveTableRelation [`spark_catalog`.`default`.`tbl`, org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe, Data Cols: [col1#11], Partition Cols: []]
```

## Configurations

| Parameters                                                     | Description                                                                                                 |
|----------------------------------------------------------------|-------------------------------------------------------------------------------------------------------------|
| spark.gluten.sql.columnar.backend.velox.udfLibraryPaths        | Path to the udf/udaf libraries.                                                                             |
| spark.gluten.sql.columnar.backend.velox.driver.udfLibraryPaths | Path to the udf/udaf libraries on driver node. Only applicable on yarn-client mode.                         |
| spark.gluten.sql.columnar.backend.velox.udfAllowTypeConversion | Whether to inject possible `cast` to convert mismatched data types from input to one registered signatures. |

# Pandas UDFs (a.k.a. Vectorized UDFs)

## Introduction

Pandas UDFs are user defined functions that are executed by Spark using Arrow to transfer data and Pandas to work with the data, which allows vectorized operations. A Pandas UDF is defined using the pandas_udf() as a decorator or to wrap the function, and no additional configuration is required.
A Pandas UDF behaves as a regular PySpark function API in general. For more details, you can refer [doc](https://spark.apache.org/docs/latest/api/python/user_guide/sql/arrow_pandas.html).

## Using Pandas UDFs in Gluten with Velox Backend

Similar as in vanilla Spark, user needs to set up pyspark/arrow dependencies properly first. You may can refer following steps:

```
pip3 install pyspark==$SPARK_VERSION cython
pip3 install pandas pyarrow
```

Gluten provides a config to control enable `ColumnarArrowEvalPython` or not, with `true` as default.

```
spark.gluten.sql.columnar.arrowUdf
```

Then take following `PySpark` code for example:

```
from pyspark.sql.functions import pandas_udf, PandasUDFType
import pyspark.sql.functions as F
import os
@pandas_udf('long')
def pandas_plus_one(v):
    return (v + 1)
df = spark.read.orc("path_to_file").select("quantity").withColumn("processed_quantity", pandas_plus_one("quantity")).select("quantity")
```

The expected physical plan will be:

```
== Physical Plan ==
VeloxColumnarToRowExec
+- ^(2) ProjectExecTransformer [pythonUDF0#45L AS processed_quantity#41L]
   +- ^(2) InputIteratorTransformer[quantity#2L, pythonUDF0#45L]
      +- ^(2) InputAdapter
         +- ^(2) ColumnarArrowEvalPython [pandas_plus_one(quantity#2L)#40L], [pythonUDF0#45L], 200
            +- ^(1) NativeFileScan orc [quantity#2L] Batched: true, DataFilters: [], Format: ORC, Location: InMemoryFileIndex(1 paths)[file:/***], PartitionFilters: [], PushedFilters: [], ReadSchema: struct<quantity:bigint>
```
