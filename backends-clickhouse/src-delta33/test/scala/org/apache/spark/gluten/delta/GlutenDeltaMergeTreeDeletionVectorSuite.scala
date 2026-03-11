/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.spark.gluten.delta

import org.apache.gluten.backendsapi.clickhouse.CHConfig
import org.apache.gluten.config.GlutenConfig
import org.apache.gluten.execution.{CreateMergeTreeSuite, FileSourceScanExecTransformer}

import org.apache.spark.SparkConf
import org.apache.spark.sql.delta.files.TahoeFileIndex
import org.apache.spark.sql.delta.stats.PreparedDeltaFileIndex
import org.apache.spark.sql.execution.datasources.v2.clickhouse.metadata.AddMergeTreeParts

// Some sqls' line length exceeds 100
// scalastyle:off line.size.limit

class GlutenDeltaMergeTreeDeletionVectorSuite extends CreateMergeTreeSuite {

  /** Run Gluten + ClickHouse Backend with SortShuffleManager */
  override protected def sparkConf: SparkConf = {
    super.sparkConf
      .set("spark.shuffle.manager", "org.apache.spark.shuffle.sort.ColumnarShuffleManager")
      .set("spark.io.compression.codec", "LZ4")
      .set("spark.sql.shuffle.partitions", "5")
      .set("spark.sql.autoBroadcastJoinThreshold", "10MB")
      .set("spark.sql.adaptive.enabled", "true")
      .set("spark.sql.storeAssignmentPolicy", "legacy")
      .set("spark.databricks.delta.retentionDurationCheck.enabled", "false")
      .set(GlutenConfig.NATIVE_WRITER_ENABLED.key, "true")
      .set(CHConfig.ENABLE_ONEPIPELINE_MERGETREE_WRITE.key, spark35.toString)
  }

  test("Gluten-9334: column `_tmp_metadata_row_index` and `file_path` not found") {
    val tableName = "delta_metadata_column"
    withTable(tableName) {
      withTempDir {
        dirName =>
          val s = createTableBuilder(tableName, "clickhouse", s"$dirName/$tableName")
            .withProps(Map("delta.enableDeletionVectors" -> "'true'"))
            .withTableKey("lineitem")
            .build()
          spark.sql(s)

          spark.sql(s"insert into table $tableName select * from lineitem ")

          val df = sql(s"""
                          | select
                          |   _metadata.file_path,
                          |   _metadata.row_index
                          | from $tableName
                          | limit 1
                          |""".stripMargin)

          checkFallbackOperators(df, 0)
      }
    }
  }

  ignore("Gluten-9606: Support CH MergeTree + Delta DeletionVector reading") {
    val tableName = "mergetree_delta_dv"
    withTable(tableName) {
      withTempDir {
        dirName =>
          val s = createTableBuilder(tableName, "clickhouse", s"$dirName/$tableName")
            .withProps(Map("delta.enableDeletionVectors" -> "'true'"))
            .withTableKey("lineitem")
            .build()
          spark.sql(s)

          spark.sql(s"""
                       |insert into table $tableName
                       |select /*+ REPARTITION(6) */ * from lineitem
                       |""".stripMargin)

          spark.sql(s"""
                       | delete from $tableName
                       | where l_orderkey = 3
                       |""".stripMargin)

          val df = spark.sql(s"""
                                | select sum(l_linenumber) from $tableName
                                |""".stripMargin)
          val result = df.collect()
          assert(
            result.apply(0).get(0) === 1802425
          )
          checkFallbackOperators(df, 0)

          spark.sql(s"""
                       | delete from $tableName
                       | where mod(l_orderkey, 3) = 2
                       |""".stripMargin)

          val df1 = spark.sql(s"""
                                 | select sum(l_linenumber) from $tableName
                                 |""".stripMargin)
          assert(
            df1.collect().apply(0).get(0) === 1200650
          )
          checkFallbackOperators(df1, 0)

          spark.sql(s"optimize $tableName")

          val df2 = spark.sql(s"""
                                 | select sum(l_linenumber) from $tableName
                                 |""".stripMargin)
          val result2 = df2.collect()
          assert(result2.apply(0).get(0) === 1200650)

          val scanExec = collect(df2.queryExecution.executedPlan) {
            case f: FileSourceScanExecTransformer => f
          }
          assertResult(1)(scanExec.size)
          val mergetreeScan = scanExec.head
          assert(mergetreeScan.nodeName.startsWith("FileSourceScanExecTransformer mergetree"))
          val fileIndex = mergetreeScan.relation.location.asInstanceOf[TahoeFileIndex]
          val addFiles =
            fileIndex.matchingFiles(Nil, Nil).map(f => f.asInstanceOf[AddMergeTreeParts])
          assertResult(1)(addFiles.size)
      }
    }
  }

  ignore("Gluten-9606: Support CH MergeTree + Delta DeletionVector reading -- partition") {
    val tableName = "mergetree_delta_dv_partition"
    spark.sql(s"""
                 |DROP TABLE IF EXISTS $tableName;
                 |""".stripMargin)
    spark.sql(s"""
                 |CREATE TABLE IF NOT EXISTS $tableName
                 |(${table2columns.get("lineitem").get(true)})
                 |USING clickhouse
                 |PARTITIONED BY (l_returnflag)
                 |TBLPROPERTIES (delta.enableDeletionVectors='true')
                 |LOCATION '$dataHome/$tableName'
                 |""".stripMargin)

    spark.sql(s"""
                 | insert into table $tableName
                 | select /*+ REPARTITION(6) */ * from lineitem
                 |""".stripMargin)

    spark.sql(s"""
                 | delete from $tableName
                 | where mod(l_orderkey, 3) = 1
                 |""".stripMargin)

    var df = spark.sql(s"""
                          | select sum(l_linenumber) from $tableName
                          | where mod(l_orderkey, 3) = 1
                          |""".stripMargin)
    var result = df.collect()
    assert(
      result.apply(0).isNullAt(0)
    )
    checkFallbackOperators(df, 0)
    df = spark.sql(s"""
                      | select sum(l_linenumber) from $tableName
                      | where mod(l_orderkey, 3) = 2
                      |""".stripMargin)
    result = df.collect()
    assert(
      result.apply(0).get(0) === 601775
    )
    checkFallbackOperators(df, 0)
    df = spark.sql(s"""
                      | select sum(l_linenumber) from $tableName
                      |""".stripMargin)
    result = df.collect()
    assert(
      result.apply(0).get(0) === 1201486
    )
    checkFallbackOperators(df, 0)
  }

  // TODO fix reorg purge command in delta 3.3.1
  ignore("Gluten-9697: Add 'reorg' command ut for the mergetree + delta dv") {
    val tableName = "mergetree_delta_dv_reorg"
    withTable(tableName) {
      withTempDir {
        dirName =>
          val s = createTableBuilder(tableName, "clickhouse", s"$dirName/$tableName")
            .withProps(Map("delta.enableDeletionVectors" -> "'true'"))
            .withTableKey("lineitem")
            .build()
          spark.sql(s)

          spark.sql(s"""
                       |insert into table $tableName
                       |select /*+ REPARTITION(6) */ * from lineitem
                       |""".stripMargin)

          spark.sql(s"""
                       |delete from $tableName
                       |where mod(l_orderkey, 3) = 2
                       |""".stripMargin)

          var df = spark.sql(s"""
                                | select sum(l_linenumber) from $tableName
                                |""".stripMargin)
          var result = df.collect()
          assert(
            result(0).get(0) === 1200671
          )
          checkFallbackOperators(df, 0)

          spark.sql(s"""
                       | REORG TABLE $tableName APPLY (PURGE)
                       |""".stripMargin)
          df = spark.sql(s"""
                            | select sum(l_linenumber) from $tableName
                            |""".stripMargin)
          result = df.collect()
          assert(
            result(0).get(0) === 1200671
          )
          val scanExec = collect(df.queryExecution.executedPlan) {
            case f: FileSourceScanExecTransformer => f
          }
          val parquetScan = scanExec.head
          val fileIndex = parquetScan.relation.location.asInstanceOf[PreparedDeltaFileIndex]
          val addFiles = fileIndex.preparedScan.files
          assert(addFiles.size === 1)
          assert(addFiles(0).deletionVector === null)
      }
    }
  }

  // TODO fix reorg purge command in delta 3.3.1
  ignore("Gluten-9697: Add 'reorg' command ut for the mergetree + delta dv + partition") {
    val tableName = "mergetree_delta_dv_reorg_partition"
    spark.sql(s"""
                 |DROP TABLE IF EXISTS $tableName;
                 |""".stripMargin)
    spark.sql(s"""
                 |CREATE TABLE IF NOT EXISTS $tableName
                 |(${table2columns.get("lineitem").get(true)})
                 |USING clickhouse
                 |PARTITIONED BY (l_returnflag)
                 |TBLPROPERTIES (delta.enableDeletionVectors='true')
                 |LOCATION '$dataHome/$tableName'
                 |""".stripMargin)
    spark.sql(s"""
                 |insert into table $tableName
                 | select /*+ REPARTITION(6) */ * from lineitem
                 |""".stripMargin)
    spark.sql(s"""
                 |delete from $tableName
                 | where mod(l_orderkey, 3) = 1
                 |""".stripMargin)
    var df = spark.sql(s"""
                          |select sum(l_linenumber) from $tableName
                          |""".stripMargin)
    var result = df.collect()
    assert(
      result(0).get(0) === 1201486
    )
    checkFallbackOperators(df, 0)
    spark.sql(s"""
                 |REORG TABLE $tableName APPLY (PURGE)
                 |""".stripMargin)
    df = spark.sql(s"""
                      |select sum(l_linenumber) from $tableName
                      |""".stripMargin)
    result = df.collect()
    assert(
      result(0).get(0) === 1201486
    )
    val scanExec = collect(df.queryExecution.executedPlan) {
      case f: FileSourceScanExecTransformer => f
    }
    val parquetScan = scanExec.head
    val fileIndex = parquetScan.relation.location.asInstanceOf[PreparedDeltaFileIndex]
    val addFiles = fileIndex.preparedScan.files
    assert(addFiles.size === 3)
    assert(addFiles.forall(_.deletionVector === null))
  }
}
// scalastyle:off line.size.limit
