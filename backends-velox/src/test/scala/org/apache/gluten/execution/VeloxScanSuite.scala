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
package org.apache.gluten.execution

import org.apache.gluten.backendsapi.velox.VeloxBackendSettings
import org.apache.gluten.benchmarks.RandomParquetDataGenerator
import org.apache.gluten.config.{GlutenConfig, VeloxConfig}
import org.apache.gluten.utils.VeloxFileSystemValidationJniWrapper

import org.apache.spark.SparkConf
import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.expressions.GreaterThan
import org.apache.spark.sql.execution.ScalarSubquery
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types._

import scala.reflect.ClassTag

class VeloxScanSuite extends VeloxWholeStageTransformerSuite {
  protected val rootPath: String = getClass.getResource("/").getPath
  override protected val resourcePath: String = "/tpch-data-parquet"
  override protected val fileFormat: String = "parquet"

  protected val tpchQueries: String =
    rootPath + "../../../../tools/gluten-it/common/src/main/resources/tpch-queries"
  protected val queriesResults: String = rootPath + "queries-output"

  override protected def sparkConf: SparkConf = super.sparkConf
    .set("spark.sql.adaptive.enabled", "false")

  override def beforeAll(): Unit = {
    super.beforeAll()
  }

  def checkQuery[T <: GlutenPlan: ClassTag](query: String, expectedResults: Seq[Row]): Unit = {
    val df = sql(query)
    checkAnswer(df, expectedResults)
    checkGlutenPlan[T](df)
  }

  test("tpch q22 subquery filter pushdown - v1") {
    createTPCHNotNullTables()
    runTPCHQuery(22, tpchQueries, queriesResults, compareResult = false, noFallBack = false) {
      df =>
        val plan = df.queryExecution.executedPlan
        val exist = plan.collect { case scan: FileSourceScanExecTransformer => scan }.exists {
          scan =>
            scan.filterExprs().exists {
              case _ @GreaterThan(_, _: ScalarSubquery) => true
              case _ => false
            }
        }
        assert(exist)
    }
  }

  test("tpch q22 subquery filter pushdown - v2") {
    withSQLConf("spark.sql.sources.useV1SourceList" -> "") {
      // Tables must be created here, otherwise v2 scan will not be used.
      createTPCHNotNullTables()
      runTPCHQuery(22, tpchQueries, queriesResults, compareResult = false, noFallBack = false) {
        df =>
          val plan = df.queryExecution.executedPlan
          val exist = plan.collect { case scan: BatchScanExecTransformer => scan }.exists {
            scan =>
              scan.filterExprs().exists {
                case _ @GreaterThan(_, _: ScalarSubquery) => true
                case _ => false
              }
          }
          assert(exist)
      }
    }
  }

  test("coalesce v2 batch scan input partitions") {
    withTempDir {
      dir =>
        spark.range(8).repartition(4).write.mode("overwrite").parquet(dir.getCanonicalPath)

        withSQLConf(
          SQLConf.USE_V1_SOURCE_LIST.key -> "",
          SQLConf.FILES_MAX_PARTITION_BYTES.key -> "1",
          GlutenConfig.COLUMNAR_BATCHSCAN_MAX_INPUT_PARTITIONS.key -> "2") {
          val df = spark.read.parquet(dir.getCanonicalPath)
          checkAnswer(df, spark.range(8).toDF())

          val scans = getExecutedPlan(df).collect { case scan: BatchScanExecTransformer => scan }
          assert(scans.size == 1)
          val partitions = scans.head.getPartitions
          assert(partitions.size == 2)
          assert(
            partitions
              .map(_.asInstanceOf[SparkDataSourceRDDPartition].inputPartitions.size)
              .sum > 2)
        }
    }
  }

  test("Test file scheme validation") {
    withTempPath {
      path =>
        withSQLConf(GlutenConfig.NATIVE_WRITER_ENABLED.key -> "false") {
          spark
            .range(100)
            .selectExpr("cast(id % 9 as int) as c1")
            .write
            .format("parquet")
            .save(path.getCanonicalPath)
          runQueryAndCompare(s"SELECT count(*) FROM `parquet`.`${path.getCanonicalPath}`") {
            df =>
              val plan = df.queryExecution.executedPlan
              val fileScan = collect(plan) { case s: FileSourceScanExecTransformer => s }
              assert(fileScan.size == 1)
              val rootPaths = fileScan.head.getRootPathsInternal
              assert(rootPaths.length == 1)
              assert(rootPaths.head.startsWith("file:/"))
              assert(
                VeloxFileSystemValidationJniWrapper.allSupportedByRegisteredFileSystems(
                  rootPaths.toArray))
          }
        }
    }
    val filteredRootPath =
      VeloxBackendSettings.distinctRootPaths(
        Seq("file:/test_path/", "test://test/s", "test://test1/s"))
    assert(filteredRootPath.length == 1)
    assert(filteredRootPath.head.startsWith("test://"))
    assert(
      VeloxFileSystemValidationJniWrapper.allSupportedByRegisteredFileSystems(
        Array("file:/test_path/")))
    assert(
      !VeloxFileSystemValidationJniWrapper.allSupportedByRegisteredFileSystems(
        Array("unsupported://test_path")))
    assert(
      !VeloxFileSystemValidationJniWrapper.allSupportedByRegisteredFileSystems(
        Array("file:/test_path/", "unsupported://test_path")))
  }

  test("scan with filter on decimal/timestamp/binary field") {
    withTempView("t") {
      withTempDir {
        dir =>
          val path = dir.getAbsolutePath
          val schema = StructType(
            Array(
              StructField("short_decimal_field", DecimalType(5, 2), nullable = true),
              StructField("long_decimal_field", DecimalType(32, 8), nullable = true),
              StructField("binary_field", BinaryType, nullable = true),
              StructField("timestamp_field", TimestampType, nullable = true)
            ))
          RandomParquetDataGenerator(0).generateRandomData(spark, schema, 10, Some(path))
          spark.catalog.createTable("t", path, "parquet")

          runQueryAndCompare(
            """select * from t where long_decimal_field = 3.14""".stripMargin
          )(checkGlutenPlan[FileSourceScanExecTransformer])

          runQueryAndCompare(
            """select * from t where short_decimal_field = 3.14""".stripMargin
          )(checkGlutenPlan[FileSourceScanExecTransformer])

          runQueryAndCompare(
            """select * from t where binary_field = '3.14'""".stripMargin
          )(checkGlutenPlan[FileSourceScanExecTransformer])

          runQueryAndCompare(
            """select * from t where timestamp_field = current_timestamp()""".stripMargin
          )(checkGlutenPlan[FileSourceScanExecTransformer])
      }
    }
  }

  test("push partial filters to offload scan when filter need fallback - v1") {
    withSQLConf(GlutenConfig.EXPRESSION_BLACK_LIST.key -> "add") {
      createTPCHNotNullTables()
      val query = "select l_partkey from lineitem where l_partkey + 1 > 5 and l_partkey - 1 < 8"
      runQueryAndCompare(query) {
        df =>
          {
            val executedPlan = getExecutedPlan(df)
            val scans = executedPlan.collect { case p: FileSourceScanExecTransformer => p }
            assert(scans.size == 1)
            // isnotnull(l_partkey) and l_partkey - 1 < 8
            assert(scans.head.filterExprs().size == 2)
          }
      }
    }
  }

  test("push partial filters to offload scan when filter need fallback - v2") {
    withSQLConf(
      GlutenConfig.EXPRESSION_BLACK_LIST.key -> "add",
      SQLConf.USE_V1_SOURCE_LIST.key -> "") {
      createTPCHNotNullTables()
      val query = "select l_partkey from lineitem where l_partkey + 1 > 5 and l_partkey - 1 < 8"
      runQueryAndCompare(query) {
        df =>
          {
            val executedPlan = getExecutedPlan(df)
            val scans = executedPlan.collect { case p: BatchScanExecTransformer => p }
            assert(scans.size == 1)
            // isnotnull(l_partkey) and l_partkey - 1 < 8
            assert(scans.head.filterExprs().size == 2)
          }
      }
    }
  }

  test("test binary as string") {
    withTempDir {
      dir =>
        val path = dir.getCanonicalPath
        spark
          .range(2)
          .selectExpr("id as a", "cast(cast(id + 10 as string) as binary) as b")
          .write
          .mode("overwrite")
          .parquet(path)

        withTable("test") {
          sql("create table test (a long, b string) using parquet options (path '" + path + "')")
          checkQuery[FileSourceScanExecTransformer](
            "select b from test group by b order by b",
            Seq(Row("10"), Row("11")))
        }
    }
  }

  test("parquet index based schema evolution") {
    withSQLConf(
      VeloxConfig.PARQUET_USE_COLUMN_NAMES.key -> "false") {
      withTempDir {
        dir =>
          val path = dir.getCanonicalPath
          spark
            .range(2)
            .selectExpr("id as a", "cast(id + 10 as string) as b")
            .write
            .mode("overwrite")
            .parquet(path)

          withTable("test") {
            sql(s"""create table test (c long, d string, e float) using parquet options
                   |(path '$path')""".stripMargin)

            checkQuery[FileSourceScanExecTransformer](
              "select c, d from test",
              Seq(Row(0L, "10"), Row(1L, "11")))

            checkQuery[FileSourceScanExecTransformer](
              "select d from test",
              Seq(Row("10"), Row("11")))

            checkQuery[FileSourceScanExecTransformer]("select c from test", Seq(Row(0L), Row(1L)))

            checkQuery[FileSourceScanExecTransformer](
              "select d, c from test",
              Seq(Row("10", 0L), Row("11", 1L)))

            checkQuery[FileSourceScanExecTransformer](
              "select c, d, e from test",
              Seq(Row(0L, "10", null), Row(1L, "11", null)))

            checkQuery[FileSourceScanExecTransformer](
              "select e, d, c from test",
              Seq(Row(null, "10", 0L), Row(null, "11", 1L)))
          }
      }
    }
  }

  test("ORC index based schema evolution") {
    withSQLConf(
      VeloxConfig.ORC_USE_COLUMN_NAMES.key -> "false") {
      withTempDir {
        dir =>
          val path = dir.getCanonicalPath
          spark
            .range(2)
            .selectExpr("id as a", "cast(id + 10 as string) as b")
            .write
            .mode("overwrite")
            .orc(path)

          withTable("test") {
            sql(s"""create table test (c long, d string, e float) using orc options
                   |(path '$path')""".stripMargin)

            checkQuery[FileSourceScanExecTransformer](
              "select c, d from test",
              Seq(Row(0L, "10"), Row(1L, "11")))

            checkQuery[FileSourceScanExecTransformer](
              "select d from test",
              Seq(Row("10"), Row("11")))

            checkQuery[FileSourceScanExecTransformer]("select c from test", Seq(Row(0L), Row(1L)))

            checkQuery[FileSourceScanExecTransformer](
              "select d, c from test",
              Seq(Row("10", 0L), Row("11", 1L)))

            checkQuery[FileSourceScanExecTransformer](
              "select c, d, e from test",
              Seq(Row(0L, "10", null), Row(1L, "11", null)))

            checkQuery[FileSourceScanExecTransformer](
              "select e, d, c from test",
              Seq(Row(null, "10", 0L), Row(null, "11", 1L)))
          }
      }
    }
  }
}
