---
layout: page
title: Delta Lake Support in Velox Backend
nav_order: 8
parent: Getting-Started
---

# Delta Lake Support in Velox Backend

This page tracks Delta Lake support in Apache Gluten with the Velox backend. The status is based on
merged code in Apache Gluten main. Open PRs, local branches, and in-progress work are not counted as
supported here.

## Supported Spark version

Delta support is available when Gluten is built with `-Pdelta` and the Velox backend profile.

| Spark profile | Spark version | Scala version | Delta artifact | Delta version | Read | Native write |
|---------------|---------------|---------------|----------------|---------------|------|--------------|
| `spark-3.3`  | Spark 3.3.1  | 2.12          | `delta-core`   | 2.3.0         | Offload | Fallback |
| `spark-3.4`  | Spark 3.4.4  | 2.12          | `delta-core`   | 2.4.0         | Offload | Fallback |
| `spark-3.5`  | Spark 3.5.5  | 2.12          | `delta-spark`  | 3.3.2         | Offload | ExperimentalOffload |
| `spark-4.0`  | Spark 4.0.1  | 2.13          | `delta-spark`  | 4.0.1         | Offload | ExperimentalOffload |
| `spark-4.1`  | Spark 4.1.1  | 2.13          | `delta-spark`  | 4.0.0         | Offload | ExperimentalOffload |

Spark 4.x profiles require JDK 17+ and Scala 2.13. Spark 4.1 uses Gluten's Delta 4.0 source set;
merged runtime command plan assertions are primarily Spark 3.5 and Spark 4.0, while Spark 4.1 is
covered by the Delta build profile on main. Native Delta write is experimental and disabled by
default.

## Support Status
Following value indicates the Delta support progress:

| Value                 | Description                                                              |
|-----------------------|--------------------------------------------------------------------------|
| Offload               | Offload to the Velox backend                                             |
| ExperimentalOffload   | Offload exists, but is experimental or disabled by default               |
| PartialOffload        | Some operators offload and some fallback                                 |
| Fallback              | Fallback to Spark or Delta Lake to execute                               |
| Exception             | Cannot fallback by some conditions, throw the exception                  |
| ResultMismatch        | Some hidden bug may cause result mismatch, especially for some corner case |
| NotTested             | No merged Gluten coverage is available, so support is not claimed        |

This page was audited against Apache Gluten main commit
`ea4d893fa382069de762244998fa189df03c72d6`. A Delta feature is marked Offload only when there is a
merged native code path and merged Gluten test coverage or validation coverage for that behavior. If
the audit found only Delta Lake behavior, or only implementation hooks without merged Gluten tests,
the feature is marked Fallback, PartialOffload, or NotTested instead of claiming native support.
In the configuration tables, Supported means merged Gluten tests cover that Spark/Delta behavior; it
does not mean native execution unless the status explicitly says Offload, ExperimentalOffload, or
PartialOffload.

| Area | Merged evidence checked |
|------|-------------------------|
| Spark and Delta versions | `pom.xml` Spark profile properties for Spark, Scala, and Delta versions |
| Delta scan offload | `VeloxDeltaComponent`, `OffloadDeltaScan`, `DeltaScanTransformer`, `VeloxDeltaSuite` |
| Column mapping reads | `DeltaPostTransformRules.columnMappingRule`, Delta column mapping tests in `gluten-delta` |
| Deletion vector reads | `DeltaScanTransformer.doValidateInternal` rejects Delta DV columns; `gluten-delta` DV test verifies fallback |
| TIMESTAMP_NTZ | Velox TimestampNTZ validation fallback and Delta TIMESTAMP_NTZ fallback tests |
| Native Delta write | `VeloxDelta33WriteComponent`, `VeloxDelta40WriteComponent`, `OffloadDeltaCommand`, `DeltaSQLCommandTest` enables native write in Delta 3.3/4.0 suites |
| Native Delta command plan checks | Spark 4.0 `DeltaNativeWriteSuite` asserts DELETE, UPDATE, CTAS, RTAS, DataFrameWriter, and OPTIMIZE compaction native commands; Spark 3.5 asserts OPTIMIZE compaction |
| Liquid/clustered OPTIMIZE | `OffloadDeltaCommand` excludes clustered-table OPTIMIZE; `ClusteredTableClusteringSuite` covers Delta fallback correctness |
| Delta table feature protocols | Configured Delta Lake 4.0.x dependency `TableFeature` classes |

## Adding catalogs
Fallback

Delta catalog configurations are transparent to Gluten.

````
spark.sql.extensions io.delta.sql.DeltaSparkSessionExtension
spark.sql.catalog.spark_catalog org.apache.spark.sql.delta.catalog.DeltaCatalog
````

## Creating a table
Fallback

````
CREATE TABLE delta_table (id BIGINT, data STRING) USING delta;
````

ExperimentalOffload

CTAS and RTAS can offload on Spark 3.5 and Spark 4.x when native Delta write is enabled.

````
CREATE TABLE delta_table USING delta AS
SELECT id, cast(id AS STRING) AS data FROM range(10);

REPLACE TABLE delta_table USING delta AS
SELECT id, concat('v', cast(id AS STRING)) AS data FROM range(10);
````

## Writing
Fallback

Delta write falls back by default.

````
INSERT INTO delta_table VALUES (1, 'a'), (2, 'b'), (3, 'c');
````

PartialOffload

For SQL writes that are not wrapped by the native Delta write command, the Delta command can
fallback while the source query may still offload.

````
INSERT INTO delta_table
SELECT id, data FROM source WHERE length(data) = 1;
````

ExperimentalOffload

Native Delta write is available on Spark 3.5 and Spark 4.x when
`spark.gluten.sql.columnar.backend.velox.delta.enableNativeWrite=true`.

| Operation | Status | Notes |
|-----------|--------|-------|
| DataFrameWriter append/overwrite | ExperimentalOffload | Spark 4.0 plan-tested, including partitioned writes |
| CTAS / RTAS | ExperimentalOffload | Spark 4.0 plan-tested for DeltaCatalog table creation and replacement writes |
| DELETE | ExperimentalOffload | Spark 4.0 plan-tested; shared command wrapper is compiled for Delta 3.3 and Delta 4.0 source sets |
| UPDATE | ExperimentalOffload | Spark 4.0 plan-tested; shared command wrapper is compiled for Delta 3.3 and Delta 4.0 source sets |
| OPTIMIZE compaction | ExperimentalOffload | Plain bin-packing compaction only; Spark 3.5 and Spark 4.0 have path, table, and partition-predicate plan tests |
| INSERT INTO / INSERT OVERWRITE | PartialOffload | Command execution is not fully native on main |
| MERGE INTO | PartialOffload | Delta command execution remains Spark/Delta; supported scans and expressions may offload |
| OPTIMIZE ZORDER, liquid OPTIMIZE, REORG, OPTIMIZE FULL | Fallback | Layout-specific OPTIMIZE variants are not native on main |
| VACUUM, RESTORE, CLONE | Fallback | No native Velox command support is claimed on main |

## Reading
### Read data
Offload/Fallback

| Table Type       | No Deletion Vector | Deletion Vector | TIMESTAMP_NTZ |
|------------------|--------------------|-----------------|---------------|
| unpartition      | Offload            | Fallback        | Fallback      |
| partition        | Offload            | Fallback        | Fallback      |
| column mapping   | Offload            | Fallback        | Fallback      |
| metadata/history | Fallback           | Fallback        | Fallback      |

Offload the simple query.

````
SELECT count(1) AS count, data
FROM delta_table
GROUP BY data;
````

SQL `VERSION AS OF` reads can offload when the resulting scan is valid for Gluten.

````
SELECT * FROM delta_table VERSION AS OF 1;
````

Column mapping `name` and `id` modes are supported for reads, including nested and complex types in
merged tests. Delta id column mapping requires Spark Parquet field ID read/write configurations.
Column mapping writes are not claimed as a native Gluten feature.

Deletion vector reads fall back to Spark on main. Native Delta deletion-vector reader support is
not merged.

DataFrame reads are supported and can reference tables by name using `spark.table`:

````
val df = spark.table("delta_table")
df.count()
````

### Read metadata
Fallback

````
DESCRIBE HISTORY delta_table;
DESCRIBE DETAIL delta_table;
SHOW TBLPROPERTIES delta_table;
````

## DataType
Primitive Parquet-backed Delta types are offloaded when the final physical plan passes Gluten
validation.

Struct, array, and map columns are supported in Delta reads, including column mapping reads.

TIMESTAMP_NTZ falls back to Spark and returns correct results.

Variant, variant shredding, collations, and type widening are not claimed as native Gluten support
on main.

## Format
Offload/Fallback

Delta Lake data files are Parquet. Gluten's Delta read and native write paths use Delta-aware
Parquet file formats.

No native support is claimed for non-Parquet Delta data files.

## SQL
PartialOffload

SELECT can offload when the resulting physical plan passes Gluten validation.

CREATE TABLE, ALTER TABLE, DESCRIBE, SHOW, VACUUM, and other metadata commands fall back to
Spark/Delta.

CTAS, RTAS, DataFrameWriter append/overwrite, DELETE, UPDATE, and plain OPTIMIZE compaction can use
ExperimentalOffload on Spark 3.5 and Spark 4.x when native Delta write is enabled.

INSERT INTO and MERGE INTO are PartialOffload on main. Liquid OPTIMIZE, OPTIMIZE ZORDER, REORG, and
OPTIMIZE FULL fall back to Delta's original command path.

Liquid clustering is a writer-only Delta table feature. Plain reads of clustered Delta tables are
not a separate native feature and may use normal Delta scan offload when the final plan validates,
but liquid clustering operations and clustered-table OPTIMIZE are not native on main.

## Schema evolution
PartialOffload

Delta schema evolution is handled by Delta Lake. Gluten offloads the resulting scans and write
sub-plans only when the final physical plan is valid for Velox.

Column mapping name and id modes are supported for reads, including renamed and nested fields in
merged tests. Schema changes that introduce unsupported data types or deletion-vector read paths
fall back to Spark.

## Delta table features

| Feature | Delta protocol | Gluten Support | Notes |
|---------|----------------|----------------|-------|
| Basic Delta table reads | Reader v1 | Offload | Delta Parquet scans are offloaded when valid for Gluten |
| Basic Delta writes | Writer v2 | ExperimentalOffload | Spark 3.5 and Spark 4.x only, disabled by default |
| CHECK constraints / NOT NULL constraints | CHECK Writer v3; NOT NULL/Invariants Writer v2 | Fallback | Delta invariant checker is used for correctness; no native Velox invariant-check offload is claimed |
| Change data feed | Writer v4 | PartialOffload | Gluten's write transaction path preserves CDC partitioning/change files; public CDF reads are NotTested and no native CDF scan is claimed |
| Generated columns | Writer v4 | NotTested | Implementation hooks exist through Delta constraints, but no merged native Gluten support is claimed |
| Column mapping | Reader v2, Writer v5 | PartialOffload | Name and id mapping are offloaded for reads; writes are Delta-handled and not claimed as native feature support |
| Identity columns | Writer v6 | NotTested | Native transaction code references identity tracking, but no merged dedicated Gluten test evidence was found |
| Row tracking | Writer v7; requires domainMetadata | NotTested | No merged native support is claimed |
| Deletion vectors | Reader v3, Writer v7 | Fallback | Native Delta DV reader support is not merged on main |
| TIMESTAMP_NTZ | Reader v3, Writer v7 | Fallback | Default validation fallback coverage exists |
| Liquid clustering | Writer v7; requires domainMetadata | PartialOffload | Normal reads may offload through the Delta scan path; clustered-table OPTIMIZE and liquid layout maintenance fall back |
| Column defaults | Writer v7 | Fallback | Delta/analyzer handles defaults; no native default-column feature offload is claimed |
| Iceberg readers / UniForm | Writer v7; requires column mapping | NotTested | No merged native feature support is claimed |
| Type widening | Reader v3, Writer v7 | NotTested | No merged native feature support is claimed |
| Variant | Reader v3, Writer v7 | NotTested | No merged native feature support is claimed |
| Variant shredding | Preview Reader v3, Writer v7 | NotTested | No merged native feature support is claimed |
| Collations | Not present in audited Delta 4.0.x artifacts | NotTested | No merged native feature support is claimed |
| V2 checkpoints | Reader v3, Writer v7 | NotTested | No merged native feature support is claimed |
| Protected checkpoints | Writer v7 | NotTested | No merged native feature support is claimed |

## Configuration
### Catalogs
All the catalog configurations are transparent to Gluten.

### SQL Extensions
Fallback

Supports the option `spark.sql.extensions`; Delta SQL command planning remains Spark/Delta unless a
specific physical plan is offloaded later.

### Runtime configuration
The "Gluten Support" column is now ready to be populated with:

Supported<br>
Not Supported<br>
Partial Support<br>
ExperimentalOffload<br>
Fallback<br>
NotTested<br>
In Progress<br>
Not applied or transparent to Gluten<br>

### Spark SQL Options
| Spark option | Default | Description | Gluten Support |
| --- | --- | --- | --- |
| spark.sql.extensions | Not set | Enables Delta SQL extension | Not applied or transparent to Gluten |
| spark.sql.catalog.spark_catalog | Spark catalog | Uses DeltaCatalog for Delta SQL | Not applied or transparent to Gluten |
| spark.gluten.sql.columnar.backend.velox.delta.enableNativeWrite | false | Enables native Delta write for Velox backend | ExperimentalOffload |
| spark.sql.parquet.fieldId.read.enabled | Spark default | Enables Parquet field ID reads; required for Delta id column mapping | Supported for column mapping reads |
| spark.sql.parquet.fieldId.write.enabled | Spark default | Enables Parquet field ID writes; required for Delta id column mapping | Supported for column mapping reads |
| spark.databricks.delta.stats.skipping | Delta default | Enables Delta stats skipping during planning | Not applied or transparent to Gluten |
| spark.databricks.delta.schema.autoMerge.enabled | Delta default | Enables Delta schema auto merge | Partial Support; Delta-handled |

#### Read options
| Spark option | Default | Description | Gluten Support |
| --- | --- | --- | --- |
| versionAsOf | not set (latest) | DataFrame read option for Delta table version | NotTested; SQL `VERSION AS OF` has coverage |
| timestampAsOf | not set (latest) | DataFrame read option for Delta table timestamp | NotTested |
| readChangeFeed | false | Reads Delta change data feed | NotTested for public read path; CDF write internals have correctness coverage |
| startingVersion | none | CDF or streaming starting version | NotTested |
| startingTimestamp | none | CDF or streaming starting timestamp | NotTested |

#### Write options

| Spark option | Default | Description | Gluten Support |
| --- | --- | --- | --- |
| mode=append | append | Appends data to an existing Delta table | ExperimentalOffload; Spark 4.0 plan-tested |
| mode=overwrite | error if exists | Overwrites Delta table data | ExperimentalOffload; Spark 4.0 plan-tested |
| partitionBy | none | Writes partitioned Delta data | ExperimentalOffload; Spark 4.0 plan-tested |
| replaceWhere | none | Predicate overwrite | Partial Support; Delta-handled in write suites |
| mergeSchema | false | Merge write schema with table schema | Partial Support; Delta-handled in write suites |
| overwriteSchema | false | Overwrite table schema | Partial Support; Delta-handled in write suites |

### Delta Table Properties

| Property | Default | Description | Gluten Support |
| --- | --- | --- | --- |
| delta.columnMapping.mode | none | Delta column mapping mode: none, name, or id | Supported for reads |
| delta.enableDeletionVectors | false | Enables Delta deletion vectors | Fallback |
| delta.enableChangeDataFeed | false | Enables Delta change data feed | Partial Support; write transaction path only |
| delta.feature.allowColumnDefaults | not enabled | Enables Delta column defaults | Fallback; no native default-column offload claimed |
| delta.universalFormat.enabledFormats | none | Enables UniForm generated metadata | NotTested |
| delta.enableRowTracking | false | Enables Delta row tracking | NotTested |
