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

import org.apache.gluten.backendsapi.BackendsApiManager
import org.apache.gluten.test.FallbackUtil

import org.apache.spark.SparkConf
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.Row
import org.apache.spark.sql.functions.{col, concat, lit}
import org.apache.spark.sql.types.{DecimalType, DoubleType, FloatType, IntegerType, LongType, StructType}

import scala.reflect.ClassTag
import scala.util.Random

class BoltPaimonSuite extends WholeStageTransformerSuite {
  protected val rootPath: String = getClass.getResource("/").getPath
  override protected val resourcePath: String = "/tpch-data-parquet"
  override protected val fileFormat: String = "parquet"

  override protected def sparkConf: SparkConf = {
    super.sparkConf
      .set("spark.shuffle.manager", "org.apache.spark.shuffle.sort.ColumnarShuffleManager")
      .set("spark.sql.files.maxPartitionBytes", "1g")
      .set("spark.sql.shuffle.partitions", "1")
      .set("spark.memory.offHeap.size", "2g")
      .set("spark.unsafe.exceptionOnMemoryLeak", "true")
      .set("spark.sql.autoBroadcastJoinThreshold", "-1")
      .set("spark.sql.readSideCharPadding", "false")
      .set("spark.gluten.paimon.native.source.enabled", "true")
      .set("spark.gluten.paimon.native.mor.source.enabled", "true")
      .set("spark.gluten.paimon.native.mor.aggregate.engine.enabled", "true")
      .set("spark.gluten.paimon.native.mor.partial.update.engine.enabled", "true")
      .set(
        "spark.sql.extensions",
        "org.apache.paimon.spark.extensions.PaimonSparkSessionExtensions")
      .set("spark.sql.catalog.paimon", "org.apache.paimon.spark.SparkCatalog")
      .set("spark.sql.catalog.paimon.warehouse", s"file://$rootPath/data-paimon")
    // .set("spark.gluten.sql.debug", "true")
  }

  protected val dbName0: String = "test"
  protected val tableName0: String = "T"

  override def beforeAll(): Unit = {
    super.beforeAll()
    assume(
      BackendsApiManager.getBackendName == "bolt",
      "Skipping suite: backend is not Bolt"
    )
    spark.sql(s"USE paimon")
    spark.sql(s"CREATE DATABASE IF NOT EXISTS paimon.$dbName0")
  }

  override def afterAll(): Unit = {
    try {
      spark.sql(s"USE paimon")
      spark.sql(s"DROP TABLE IF EXISTS $dbName0.$tableName0")
      spark.sql("USE default")
      spark.sql(s"DROP DATABASE paimon.$dbName0 CASCADE")
    } finally {
      super.afterAll()
    }
  }

  /** Default is paimon catalog */
  override def beforeEach(): Unit = {
    super.beforeAll()
    spark.sql(s"USE paimon")
    spark.sql(s"USE paimon.$dbName0")
    spark.sql(s"DROP TABLE IF EXISTS $tableName0")
  }

  /**
   * Check whether the executed plan of a dataframe contains the expected plan.
   * @param df:
   *   the input dataframe.
   * @param tag:
   *   class of the expected plan.
   * @tparam T:
   *   type of the expected plan.
   */
  def checkOperatorMatch[T <: TransformSupport](df: DataFrame)(implicit tag: ClassTag[T]): Unit = {
    val executedPlan = getExecutedPlan(df)
    assert(
      executedPlan.exists(plan => tag.runtimeClass.isInstance(plan)),
      s"Expect ${tag.runtimeClass.getClass.getSimpleName} exists " +
        s"in executedPlan:\n $executedPlan"
    )
  }

  // test for append table
  test("paimon transformer exists: append table") {
    Seq("parquet").foreach {
      format =>
        {
          val tbl_name = s"paimon_$format"
          withTable(tbl_name) {
            spark.sql(s"""
                         |create table $tbl_name (id INT, name STRING) using paimon
                         |TBLPROPERTIES (
                         |'file.format' = '$format',
                         | 'bucket' = '3',
                         | 'bucket-key' = 'id')
                         |""".stripMargin)
            (1 to 10).foreach {
              id =>
                spark.sql(s"""
                             |insert into $tbl_name values($id, '$id')
                             |""".stripMargin)
            }

            runQueryAndCompare(s"""
                                  |select * from $tbl_name;
                                  |""".stripMargin) {
              checkOperatorMatch[PaimonScanTransformer]
            }

            runQueryAndCompare(s"""
                                  |select * from $tbl_name where id = 10;
                                  |""".stripMargin) {
              checkOperatorMatch[PaimonScanTransformer]
            }
          }
        }
    }
  }

  test("paimon transformer exists: append partition table") {
    val tbl_name = s"paimon_tb"
    withTable(tbl_name) {
      spark.sql(s"""
                   |create table $tbl_name (id INT, name STRING, dt STRING)
                   | using paimon
                   |TBLPROPERTIES (
                   | 'bucket' = '3',
                   | 'bucket-key' = 'id')
                   |PARTITIONED BY (dt)
                   |""".stripMargin)
      spark.sql(s"""
                   |insert into $tbl_name values(1, '1', '20250506')
                   |""".stripMargin)

      spark.sql(s"""
                   |insert into $tbl_name values(2, '2', '20250507')
                   |""".stripMargin)

      (1 to 10).foreach {
        id =>
          spark.sql(s"""
                       |insert into $tbl_name values($id, '$id', '20250507')
                       |""".stripMargin)
      }
      (1 to 10).foreach {
        id =>
          spark.sql(s"""
                       |insert into $tbl_name values($id, '$id', '20250506')
                       |""".stripMargin)
      }

      runQueryAndCompare(s"""
                            |select * from $tbl_name where dt = '20250506';
                            |""".stripMargin) {
        checkOperatorMatch[PaimonScanTransformer]
      }

      runQueryAndCompare(s"""
                            |select * from $tbl_name where dt = '20250506';
                            |""".stripMargin) {
        checkOperatorMatch[PaimonScanTransformer]
      }
    }
  }

  test("paimon transformer exists: primary key table(full compact)") {
    val tbl_name = s"paimon_tb"

    withTable("paimon_tb") {
      spark.sql(s"""
                   |create table $tbl_name (id INT, name STRING)
                   | using paimon
                   |TBLPROPERTIES (
                   | 'primary-key' = 'id',
                   | 'bucket' = '3',
                   | 'target-file-size' = '100b',
                   | 'write-only' = 'true',
                   | 'merge-engine' = 'partial-update'
                   |)
                   |""".stripMargin)

      import testImplicits._

      val randomData = (1 to 1000).map {
        _ =>
          val a = Random.nextInt(100) + 1
          val b = "b" + (Random.nextInt(90) + 10)

          (a, b)
      }

      randomData.toDF("a", "b").createOrReplaceTempView("source")
      spark.sql(s"""
                   |insert into $tbl_name select * from source
                   |""".stripMargin)
      spark.sql(s"""
                   |insert into $tbl_name select * from source
                   |""".stripMargin)

      runQueryAndCompare(s"""
                            |select * from $tbl_name;
                            |""".stripMargin) {
        checkOperatorMatch[PaimonScanTransformer]
      }

      // after full compact, it supports native scan
      spark.sql(s"CALL sys.compact(table => '$tbl_name')")
      runQueryAndCompare(s"""
                            |select * from $tbl_name;
                            |""".stripMargin) {
        checkOperatorMatch[PaimonScanTransformer]
      }
    }
  }

  test("paimon filter push down: value filter") {
    Seq(true, false).foreach {
      ignore =>
        val tbl_name = s"paimon_tb"

        withTable("paimon_tb") {
          spark.sql(s"""
                       |create table $tbl_name (id INT, seq INT, name STRING)
                       | using paimon
                       |TBLPROPERTIES (
                       | 'primary-key' = 'id',
                       | 'bucket' = '3',
                       | 'sequence.field' = 'seq',
                       | 'target-file-size' = '100b',
                       | 'write-only' = 'true',
                       | 'ignore-delete' = '$ignore'
                       |)
                       |""".stripMargin)

          spark.sql(s"""
                       |insert into $tbl_name values(1, 2, '1')
                       |""".stripMargin)
          spark.sql(s"""
                       |insert into $tbl_name values(1, 1, '2')
                       |""".stripMargin)

          runQueryAndCompare(s"""
                                |select * from $tbl_name where id = 1;
                                |""".stripMargin) {
            checkOperatorMatch[PaimonScanTransformer]
          }

          runQueryAndCompare(s"""
                                |select * from $tbl_name where name = '2';
                                |""".stripMargin) {
            checkOperatorMatch[PaimonScanTransformer]
          }
        }
    }
  }

  test("paimon transformer exists: primary key with sequence field") {
    val tbl_name = s"paimon_tb"
    withTable("paimon_tb") {
      spark.sql(s"""
                   |create table $tbl_name (id INT, seq INT, name STRING)
                   | using paimon
                   |TBLPROPERTIES (
                   | 'primary-key' = 'id',
                   | 'bucket' = '3',
                   | 'sequence.field' = 'seq',
                   | 'target-file-size' = '100b',
                   | 'write-only' = 'true'
                   |)
                   |""".stripMargin)
      val randomData1 = (1 to 1000).map {
        _ =>
          val a = Random.nextInt(100) + 1
          // inject some null sequence number
          val b = if (a % 3 == 0) {
            None
          } else {
            Some(Random.nextInt(10))
          }
          val c = "c" + (Random.nextInt(90) + 10)
          (a, b, c)
      }
      val randomData2 = (1 to 1000).map {
        _ =>
          val a = Random.nextInt(100) + 1
          // inject some other null in sequence field
          val b = if (a % 3 == 0 || a % 4 == 0) {
            None
          } else {
            Some(Random.nextInt(10))
          }
          val c = "c" + (Random.nextInt(90) + 10)
          (a, b, c)
      }
      import testImplicits._
      randomData1.toDF("a", "b", "c").createOrReplaceTempView("source1")
      randomData2.toDF("a", "b", "c").createOrReplaceTempView("source2")
      spark.sql(s"""
                   |insert into $tbl_name select * from source1
                   |""".stripMargin)
      spark.sql(s"""
                   |insert into $tbl_name select * from source2
                   |""".stripMargin)
      spark.sql(s"select * from $tbl_name")
      runQueryAndCompare(s"""
                            |select * from $tbl_name;
                            |""".stripMargin) {
        checkOperatorMatch[PaimonScanTransformer]
      }
    }
  }

  test("paimon transformer fallback: primary key table - custom single sequence key") {
    val tbl_name = s"paimon_tb"

    withTable("paimon_tb") {
      spark.sql(s"""
                   |create table $tbl_name (id INT, val INT, seq INT)
                   | using paimon
                   |TBLPROPERTIES (
                   | 'primary-key' = 'id',
                   | 'bucket' = '1',
                   | 'target-file-size' = '100b',
                   | 'write-only' = 'true',
                   | 'sequence.field' = 'seq'
                   |)
                   |""".stripMargin)

      import testImplicits._

      val firstBatch = (1 to 1000).map {
        x =>
          val a = x
          val b = x + 1000
          val c = x
          (a, b, c)
      }

      firstBatch.toDF("a", "b", "c").createOrReplaceTempView("firstBatch")
      spark.sql(s"""
                   |insert into $tbl_name select * from firstBatch
                   |""".stripMargin)

      val secondBatch = (2000 to 3000).map {
        x =>
          val a = x
          val b = x + 1000
          val c = x
          (a, b, c)
      }

      secondBatch.toDF("a", "b", "c").createOrReplaceTempView("secondBatch")
      spark.sql(s"""
                   |insert into $tbl_name select * from secondBatch
                   |""".stripMargin)

      runQueryAndCompare(s"select * from $tbl_name") { _ => }
    }
  }

  test("paimon transformer fallback: primary key table - custom multiple sequence key") {
    val tbl_name = s"paimon_tb"

    withTable("paimon_tb") {
      spark.sql(s"""
                   |create table $tbl_name (id INT, val INT, seq1 INT, seq2 INT)
                   | using paimon
                   |TBLPROPERTIES (
                   | 'primary-key' = 'id',
                   | 'bucket' = '1',
                   | 'target-file-size' = '100b',
                   | 'write-only' = 'true',
                   | 'sequence.field' = 'seq1,seq2'
                   |)
                   |""".stripMargin)

      import testImplicits._

      val firstBatch = (1 to 1000).map {
        x =>
          val a = x
          val b = x + 1000
          val c = x
          val d = 2 * x
          (a, b, c, d)
      }

      firstBatch.toDF("a", "b", "c", "d").createOrReplaceTempView("firstBatch")
      spark.sql(s"""
                   |insert into $tbl_name select * from firstBatch
                   |""".stripMargin)

      val secondBatch = (2000 to 3000).map {
        x =>
          val a = x
          val b = x + 1000
          val c = x
          val d = 3000 - x
          (a, b, c, d)
      }

      secondBatch.toDF("a", "b", "c", "d").createOrReplaceTempView("secondBatch")
      spark.sql(s"""
                   |insert into $tbl_name select * from secondBatch
                   |""".stripMargin)

      runQueryAndCompare(s"select * from $tbl_name") { _ => }
    }
  }

  ignore("paimon transformer fallback: primary key table with aggregate engine") {
    val tbl_name = s"paimon_tb"

    withTable("paimon_tb") {
      spark.sql(s"""
                   |create table $tbl_name (id INT, name STRING)
                   | using paimon
                   |TBLPROPERTIES (
                   | 'primary-key' = 'id',
                   | 'bucket' = '1',
                   | 'target-file-size' = '100b',
                   | 'write-only' = 'true',
                   | 'merge-engine' = 'aggregation',
                   | 'fields.name.aggregate-function' = 'first_value'
                   |)
                   |""".stripMargin)

      import testImplicits._

      val randomData = (1 to 1000).map {
        _ =>
          val a = Random.nextInt(100) + 1
          val b = "b" + (Random.nextInt(90) + 10)

          (a, b)
      }

      randomData.toDF("a", "b").createOrReplaceTempView("source")
      spark.sql(s"""
                   |insert into $tbl_name select * from source
                   |""".stripMargin)
      spark.sql(s"""
                   |insert into $tbl_name select * from source
                   |""".stripMargin)

      runQueryAndCompare(
        s"""
           |select * from $tbl_name;
           |""".stripMargin,
        noFallBack = true)(df => FallbackUtil.hasFallback(df.queryExecution.executedPlan))
    }
  }

  test("paimon transformer fallback: primary key table with partial-update engine") {
    val tbl_name = s"paimon_tb"

    withTable("paimon_tb") {
      spark.sql(s"""
                   |create table $tbl_name (id INT, name INT, g INT)
                   | using paimon
                   |TBLPROPERTIES (
                   | 'primary-key' = 'id',
                   | 'bucket' = '1',
                   | 'target-file-size' = '100b',
                   | 'write-only' = 'true',
                   | 'merge-engine' = 'partial-update',
                   | 'fields.g.sequence-group' = 'name'
                   |)
                   |""".stripMargin)

      import testImplicits._

      val randomData = (1 to 1000).map {
        _ =>
          val a = Random.nextInt(100) + 1
          val b = Random.nextInt(90) + 10
          val c = Random.nextInt(90) + 100
          (a, b, c)
      }

      randomData.toDF("a", "b", "c").createOrReplaceTempView("source")
      spark.sql(s"""
                   |insert into $tbl_name select * from source
                   |""".stripMargin)
      spark.sql(s"""
                   |insert into $tbl_name select * from source
                   |""".stripMargin)

      runQueryAndCompare(s"""
                            |select name from $tbl_name;
                            |""".stripMargin) {
        checkOperatorMatch[PaimonScanTransformer]
      }
    }
  }

  test("paimon transformer: primary key table - NULL + NOT NULL") {
    val tbl_name = s"paimon_tb"

    withTable("paimon_tb") {
      spark.sql(s"""
                   |create table $tbl_name (id INT, val INT)
                   | using paimon
                   |TBLPROPERTIES (
                   | 'primary-key' = 'id',
                   | 'bucket' = '1',
                   | 'target-file-size' = '100b',
                   | 'write-only' = 'true'
                   |)
                   |""".stripMargin)

      spark.sql(s"""
                   |insert into $tbl_name values (1, NULL)
                   |""".stripMargin)

      spark.sql(s"""
                   |insert into $tbl_name values (1, 2)
                   |""".stripMargin)

      runQueryAndCompare(s"select * from $tbl_name") { _ => }
    }
  }

  test("paimon transformer: primary key table - NOT NULL + NULL") {
    val tbl_name = s"paimon_tb"

    withTable("paimon_tb") {
      spark.sql(s"""
                   |create table $tbl_name (id INT, val INT)
                   | using paimon
                   |TBLPROPERTIES (
                   | 'primary-key' = 'id',
                   | 'bucket' = '1',
                   | 'target-file-size' = '100b',
                   | 'write-only' = 'true'
                   |)
                   |""".stripMargin)

      spark.sql(s"""
                   |insert into $tbl_name values (1, 3)
                   |""".stripMargin)

      spark.sql(s"""
                   |insert into $tbl_name values (1, NULL)
                   |""".stripMargin)

      runQueryAndCompare(s"select * from $tbl_name") { _ => }
    }
  }

  test("paimon transformer: primary key table - NOT NULL + NOT NULL") {
    val tbl_name = s"paimon_tb"

    withTable("paimon_tb") {
      spark.sql(s"""
                   |create table $tbl_name (id INT, val INT)
                   | using paimon
                   |TBLPROPERTIES (
                   | 'primary-key' = 'id',
                   | 'bucket' = '1',
                   | 'target-file-size' = '100b',
                   | 'write-only' = 'true'
                   |)
                   |""".stripMargin)

      spark.sql(s"""
                   |insert into $tbl_name values (1, NULL)
                   |""".stripMargin)

      spark.sql(s"""
                   |insert into $tbl_name values (1, NULL)
                   |""".stripMargin)

      runQueryAndCompare(s"select * from $tbl_name") { _ => }
    }
  }

  test("paimon transformer: primary key table - custom row kind") {
    val tbl_name = s"paimon_tb"

    withTable("paimon_tb") {
      spark.sql(s"""
                   |create table $tbl_name (id INT, val INT, kind String)
                   | using paimon
                   |TBLPROPERTIES (
                   | 'primary-key' = 'id',
                   | 'bucket' = '1',
                   | 'target-file-size' = '100b',
                   | 'write-only' = 'true',
                   | 'rowkind.field' = 'kind'
                   |)
                   |""".stripMargin)

      import testImplicits._

      val firstBatch = (1 to 1000).map {
        x =>
          val a = x
          val b = x + 1000
          val c = "+I"
          (a, b, c)
      }

      firstBatch.toDF("a", "b", "c").createOrReplaceTempView("firstBatch")
      spark.sql(s"""
                   |insert into $tbl_name select * from firstBatch
                   |""".stripMargin)

      val secondBatch = (2000 to 3000).map {
        x =>
          val a = x
          val b = x + 1000
          val c = "+I"
          (a, b, c)
      }

      secondBatch.toDF("a", "b", "c").createOrReplaceTempView("secondBatch")
      spark.sql(s"""
                   |insert into $tbl_name select * from secondBatch
                   |""".stripMargin)

      runQueryAndCompare(s"select * from $tbl_name") { _ => }
    }
  }

  test("paimon aggregate: sum") {
    val tbl_name = s"paimon_tb"
    withTable(tbl_name) {
      spark.sql(s"""
                   |create table $tbl_name (id INT, tint TINYINT, sint SMALLINT, iint INTEGER,
                   |                         bint BIGINT, f FLOAT, d DOUBLE,
                   |                        sdec DECIMAL(10, 2), ldec DECIMAL(38, 9))
                   |using paimon
                   |TBLPROPERTIES (
                   |'file.format' = 'parquet',
                   | 'primary-key' = 'id',
                   | 'bucket' = '3',
                   |'merge-engine' = 'aggregation',
                   |'fields.tint.aggregate-function' = 'sum',
                   |'fields.sint.aggregate-function' = 'sum',
                   |'fields.iint.aggregate-function' = 'sum',
                   |'fields.bint.aggregate-function' = 'sum',
                   |'fields.f.aggregate-function' = 'sum',
                   |'fields.d.aggregate-function' = 'sum',
                   |'fields.sdec.aggregate-function' = 'sum',
                   |'fields.ldec.aggregate-function' = 'sum')
                   |""".stripMargin)
      spark.sql(s"""
                   |insert into $tbl_name values(1,  3,  5,  7,  9, 3.5, 5.5, 7.5, 8.1),
                   |                            (2, 11, 13, 15, 17, 5.6, 7.6, 6.7, 9.1),
                   |                            (5, 19, 21, 23, 25, 9.3, 11.3, 7.8, 10.1)
                   |""".stripMargin)

      spark.sql(s"""
                   |insert into $tbl_name values(1,  2,  4,  6,  8, 2.5, 3.5, 1.2, 11.1),
                   |                            (2, 10, 12, 14, 16, 6.7, 8.9, 2.3, 12.1),
                   |                            (7, 18, 20, 22, 24, 1.3, 7.4, 3.4, 13.1)
                   |""".stripMargin)

      runQueryAndCompare(
        s"""
           |select * from $tbl_name;
           |""".stripMargin,
        noFallBack = true) {
        checkOperatorMatch[PaimonScanTransformer]
      }
    }
  }

  test("paimon aggregate empty: sum") {
    val tbl_name = s"paimon_tb"
    withTable(tbl_name) {
      spark.sql(s"""
                   |create table $tbl_name (id INT, tint TINYINT, sint SMALLINT, iint INTEGER,
                   |                         bint BIGINT, f FLOAT, d DOUBLE,
                   |                        sdec DECIMAL(10, 2), ldec DECIMAL(38, 9))
                   |using paimon
                   |TBLPROPERTIES (
                   |'file.format' = 'parquet',
                   | 'primary-key' = 'id',
                   | 'bucket' = '3',
                   |'merge-engine' = 'aggregation',
                   |'fields.tint.aggregate-function' = 'sum',
                   |'fields.sint.aggregate-function' = 'sum',
                   |'fields.iint.aggregate-function' = 'sum',
                   |'fields.bint.aggregate-function' = 'sum',
                   |'fields.f.aggregate-function' = 'sum',
                   |'fields.d.aggregate-function' = 'sum',
                   |'fields.sdec.aggregate-function' = 'sum',
                   |'fields.ldec.aggregate-function' = 'sum')
                   |""".stripMargin)

      runQueryAndCompare(
        s"""
           |select * from $tbl_name;
           |""".stripMargin,
        noFallBack = true) {
        checkOperatorMatch[PaimonScanTransformer]
      }
    }
  }

  test("paimon aggregate empty: product") {
    val tbl_name = s"paimon_tb"
    withTable(tbl_name) {
      spark.sql(s"""
                   |create table $tbl_name (id INT, tint TINYINT, sint SMALLINT, iint INTEGER,
                   |                        bint BIGINT, f FLOAT, d DOUBLE, ds DECIMAL(10,2),
                   |                        dl DECIMAL(38,12))
                   |using paimon
                   |TBLPROPERTIES (
                   |'file.format' = 'parquet',
                   | 'primary-key' = 'id',
                   | 'bucket' = '3',
                   |'merge-engine' = 'aggregation',
                   |'fields.tint.aggregate-function' = 'product',
                   |'fields.sint.aggregate-function' = 'product',
                   |'fields.iint.aggregate-function' = 'product',
                   |'fields.bint.aggregate-function' = 'product',
                   |'fields.f.aggregate-function' = 'product',
                   |'fields.ds.aggregate-function' = 'product',
                   |'fields.dl.aggregate-function' = 'product')
                   |""".stripMargin)

      runQueryAndCompare(
        s"""
           |select * from $tbl_name;
           |""".stripMargin,
        noFallBack = true) {
        checkOperatorMatch[PaimonScanTransformer]
      }
    }
  }

  test("paimon aggregate: product") {
    val tbl_name = s"paimon_tb"
    withTable(tbl_name) {
      spark.sql(s"""
                   |create table $tbl_name (id INT, tint TINYINT, sint SMALLINT, iint INTEGER,
                   |                        bint BIGINT, f FLOAT, d DOUBLE, ds DECIMAL(10,2),
                   |                        dl DECIMAL(38,12))
                   |using paimon
                   |TBLPROPERTIES (
                   |'file.format' = 'parquet',
                   | 'primary-key' = 'id',
                   | 'bucket' = '3',
                   |'merge-engine' = 'aggregation',
                   |'fields.tint.aggregate-function' = 'product',
                   |'fields.sint.aggregate-function' = 'product',
                   |'fields.iint.aggregate-function' = 'product',
                   |'fields.bint.aggregate-function' = 'product',
                   |'fields.f.aggregate-function' = 'product',
                   |'fields.ds.aggregate-function' = 'product',
                   |'fields.dl.aggregate-function' = 'product')
                   |""".stripMargin)
      spark.sql(s"""
                   |insert into $tbl_name values(1,  3,  5,  7,  9, 3.5, 5.5, 2.0, 3.1),
                   |                            (2, 11, 13, 15, 17, 5.6, 7.6, 4.5, 4.2),
                   |                            (5, 19, 21, 23, 25, 9.3, 11.3, 3.5, 5.3)
                   |""".stripMargin)

      spark.sql(s"""
                   |insert into $tbl_name values(1,  2,  4,  6,  8, 2.5, 3.5, 1.0, 6.4),
                   |                            (2, 10, 12, 14, 16, 6.7, 8.9, 3.4, 7.5),
                   |                            (7, 18, 20, 22, 24, 1.3, 7.4, 4.3, 8.6)
                   |""".stripMargin)

      runQueryAndCompare(
        s"""
           |select * from $tbl_name;
           |""".stripMargin,
        noFallBack = true) {
        checkOperatorMatch[PaimonScanTransformer]
      }
    }
  }

  // bug https://meego.larkoffice.com/dpus_queryengine_dataapplication/issue/detail/6762856624
  ignore("paimon aggregate: count") {
    val tbl_name = s"paimon_tb"
    withTable(tbl_name) {
      spark.sql(s"""
                   |create table $tbl_name (id INT, iint INTEGER, bint BIGINT)
                   |using paimon
                   |TBLPROPERTIES (
                   |'file.format' = 'parquet',
                   | 'primary-key' = 'id',
                   | 'bucket' = '3',
                   |'merge-engine' = 'aggregation',
                   |'fields.iint.aggregate-function' = 'count',
                   |'fields.bint.aggregate-function' = 'count')
                   |""".stripMargin)
      spark.sql(s"""
                   |insert into $tbl_name values(1,  3,  5),
                   |                            (2, 11, 13),
                   |                            (5, 19, 21)
                   |""".stripMargin)

      spark.sql(s"""
                   |insert into $tbl_name values(1,  2,  4),
                   |                            (2, 10, 12),
                   |                            (7, 18, 20)
                   |""".stripMargin)

      runQueryAndCompare(
        s"""
           |select * from $tbl_name;
           |""".stripMargin,
        noFallBack = true) {
        checkOperatorMatch[PaimonScanTransformer]
      }
    }
  }

  test("paimon aggregate: max") {
    val tbl_name = s"paimon_tb"
    withTable(tbl_name) {
      spark.sql(s"""
                   |create table $tbl_name (id INT, tint TINYINT,
                   |                        sint SMALLINT, iint INTEGER, bint BIGINT,
                   |                        f FLOAT, d DOUBLE, c CHAR(2), v VARCHAR(10),
                   |                        s STRING, da DATE)
                   |using paimon
                   |TBLPROPERTIES (
                   |'file.format' = 'parquet',
                   | 'primary-key' = 'id',
                   | 'bucket' = '3',
                   |'merge-engine' = 'aggregation',
                   |'fields.tint.aggregate-function' = 'max',
                   |'fields.sint.aggregate-function' = 'max',
                   |'fields.iint.aggregate-function' = 'max',
                   |'fields.bint.aggregate-function' = 'max',
                   |'fields.f.aggregate-function' = 'max',
                   |'fields.d.aggregate-function' = 'max',
                   |'fields.c.aggregate-function' = 'max',
                   |'fields.v.aggregate-function' = 'max',
                   |'fields.s.aggregate-function' = 'max',
                   |'fields.da.aggregate-function' = 'max')
                   |""".stripMargin)
      spark.sql(s"""
                   |insert into $tbl_name values
                   | (1,  3,  5,  7,  9, 3.5, 5.5, "AB", "Hello", "Hello", DATE '2025-09-15'),
                   | (2, 11, 13, 15, 17, 5.6, 7.6, "CD", "World", "World", DATE '2024-01-01'),
                   | (5, 19, 21, 23, 25, 9.3, 11.3, "EF", "String", "String", DATE '2023-07-04')
                   |""".stripMargin)

      spark.sql(s"""
                   |insert into $tbl_name values
                   | (1,  2,  4,  6,  8, 2.5, 3.5, 'ab', "New", "New", DATE '2022-12-31'),
                   | (2, 10, 12, 14, 16, 6.7, 8.9, 'cd', "olleH", "olleH", DATE '2000-02-29'),
                   | (7, 18, 20, 22, 24, 1.3, 7.4, 'ef', "dlroW", "dlroW", DATE '1970-01-01')
                   |""".stripMargin)

      runQueryAndCompare(
        s"""
           |select * from $tbl_name;
           |""".stripMargin,
        noFallBack = true) {
        checkOperatorMatch[PaimonScanTransformer]
      }
    }
  }

  test("paimon aggregate: min") {
    val tbl_name = s"paimon_tb"
    withTable(tbl_name) {
      spark.sql(s"""
                   |create table $tbl_name (id INT, tint TINYINT,
                   |                        sint SMALLINT, iint INTEGER, bint BIGINT,
                   |                        f FLOAT, d DOUBLE, c CHAR(2), v VARCHAR(10),
                   |                        s STRING, da DATE)
                   |using paimon
                   |TBLPROPERTIES (
                   |'file.format' = 'parquet',
                   | 'primary-key' = 'id',
                   | 'bucket' = '3',
                   |'merge-engine' = 'aggregation',
                   |'fields.tint.aggregate-function' = 'min',
                   |'fields.sint.aggregate-function' = 'min',
                   |'fields.iint.aggregate-function' = 'min',
                   |'fields.bint.aggregate-function' = 'min',
                   |'fields.f.aggregate-function' = 'min',
                   |'fields.d.aggregate-function' = 'min',
                   |'fields.c.aggregate-function' = 'min',
                   |'fields.v.aggregate-function' = 'min',
                   |'fields.s.aggregate-function' = 'min',
                   |'fields.da.aggregate-function' = 'min')
                   |""".stripMargin)
      spark.sql(s"""
                   |insert into $tbl_name values
                   | (1,  3,  5,  7,  9, 3.5, 5.5, "AB", "Hello", "Hello", DATE '2025-09-15'),
                   | (2, 11, 13, 15, 17, 5.6, 7.6, "CD", "World", "World", DATE '2024-01-01'),
                   | (5, 19, 21, 23, 25, 9.3, 11.3, "EF", "String", "String", DATE '2023-07-04')
                   |""".stripMargin)

      spark.sql(s"""
                   |insert into $tbl_name values
                   | (1,  2,  4,  6,  8, 2.5, 3.5, 'ab', "New", "New", DATE '2022-12-31'),
                   | (2, 10, 12, 14, 16, 6.7, 8.9, 'cd', "olleH", "olleH", DATE '2000-02-29'),
                   | (7, 18, 20, 22, 24, 1.3, 7.4, 'ef', "dlroW", "dlroW", DATE '1970-01-01')
                   |""".stripMargin)

      runQueryAndCompare(
        s"""
           |select * from $tbl_name;
           |""".stripMargin,
        noFallBack = true) {
        checkOperatorMatch[PaimonScanTransformer]
      }
    }
  }

  test("paimon aggregate: last_value") {
    val tbl_name = s"paimon_tb"
    withTable(tbl_name) {
      spark.sql(s"""
                   |create table $tbl_name (id INT, tint TINYINT,
                   |                        sint SMALLINT, iint INTEGER, bint BIGINT,
                   |                        f FLOAT, d DOUBLE, c CHAR(2), v VARCHAR(10),
                   |                        s STRING, da DATE)
                   |using paimon
                   |TBLPROPERTIES (
                   |'file.format' = 'parquet',
                   | 'primary-key' = 'id',
                   | 'bucket' = '3',
                   |'merge-engine' = 'aggregation',
                   |'fields.tint.aggregate-function' = 'last_value',
                   |'fields.sint.aggregate-function' = 'last_value',
                   |'fields.iint.aggregate-function' = 'last_value',
                   |'fields.bint.aggregate-function' = 'last_value',
                   |'fields.f.aggregate-function' = 'last_value',
                   |'fields.d.aggregate-function' = 'last_value',
                   |'fields.c.aggregate-function' = 'last_value',
                   |'fields.v.aggregate-function' = 'last_value',
                   |'fields.s.aggregate-function' = 'last_value',
                   |'fields.da.aggregate-function' = 'last_value')
                   |""".stripMargin)
      spark.sql(s"""
                   |insert into $tbl_name values
                   | (1,  3,  5,  7,  9, 3.5, 5.5, "AB", "Hello", "Hello", DATE '2025-09-15'),
                   | (2, 11, 13, 15, 17, 5.6, 7.6, "CD", "World", "World", DATE '2024-01-01'),
                   | (5, 19, 21, 23, 25, 9.3, 11.3, "EF", "String", "String", DATE '2023-07-04')
                   |""".stripMargin)

      spark.sql(s"""
                   |insert into $tbl_name values
                   | (1,  2,  4,  6,  8, 2.5, 3.5, 'ab', "New", "New", DATE '2022-12-31'),
                   | (2, 10, 12, 14, 16, 6.7, 8.9, 'cd', "olleH", "olleH", DATE '2000-02-29'),
                   | (7, 18, 20, 22, 24, 1.3, 7.4, 'ef', "dlroW", "dlroW", DATE '1970-01-01')
                   |""".stripMargin)

      runQueryAndCompare(
        s"""
           |select * from $tbl_name;
           |""".stripMargin,
        noFallBack = true) {
        checkOperatorMatch[PaimonScanTransformer]
      }
    }
  }

  test("paimon aggregate: last_non_null_value") {
    val tbl_name = s"paimon_tb"
    withTable(tbl_name) {
      spark.sql(s"""
                   |create table $tbl_name (id INT, tint TINYINT,
                   |                        sint SMALLINT, iint INTEGER, bint BIGINT,
                   |                        f FLOAT, d DOUBLE, c CHAR(2), v VARCHAR(10),
                   |                        s STRING, da DATE)
                   |using paimon
                   |TBLPROPERTIES (
                   |'file.format' = 'parquet',
                   | 'primary-key' = 'id',
                   | 'bucket' = '3',
                   |'merge-engine' = 'aggregation',
                   |'fields.tint.aggregate-function' = 'last_non_null_value',
                   |'fields.sint.aggregate-function' = 'last_non_null_value',
                   |'fields.iint.aggregate-function' = 'last_non_null_value',
                   |'fields.bint.aggregate-function' = 'last_non_null_value',
                   |'fields.f.aggregate-function' = 'last_non_null_value',
                   |'fields.d.aggregate-function' = 'last_non_null_value',
                   |'fields.c.aggregate-function' = 'last_non_null_value',
                   |'fields.v.aggregate-function' = 'last_non_null_value',
                   |'fields.s.aggregate-function' = 'last_non_null_value',
                   |'fields.da.aggregate-function' = 'last_non_null_value')
                   |""".stripMargin)
      spark.sql(s"""
                   |insert into $tbl_name values
                   | (1,  3,  5,  7,  9, 3.5, 5.5, "AB", "Hello", "Hello", DATE '2025-09-15'),
                   | (2, 11, 13, 15, 17, 5.6, 7.6, "CD", "World", "World", DATE '2024-01-01'),
                   | (5, 19, 21, 23, 25, 9.3, 11.3, "EF", "String", "String", DATE '2023-07-04')
                   |""".stripMargin)

      spark.sql(s"""
                   |insert into $tbl_name values
                   | (1,  2,  4,  6,  8, 2.5, 3.5, 'ab', "New", "New", DATE '2022-12-31'),
                   | (2, 10, 12, 14, 16, 6.7, 8.9, 'cd', "olleH", "olleH", DATE '2000-02-29'),
                   | (7, 18, 20, 22, 24, 1.3, 7.4, 'ef', "dlroW", "dlroW", DATE '1970-01-01')
                   |""".stripMargin)

      runQueryAndCompare(
        s"""
           |select * from $tbl_name;
           |""".stripMargin,
        noFallBack = true) {
        checkOperatorMatch[PaimonScanTransformer]
      }
    }
  }

  test("paimon aggregate: listagg") {
    val tbl_name = s"paimon_tb"
    withTable(tbl_name) {
      spark.sql(s"""
                   |create table $tbl_name (id INT, s STRING)
                   |using paimon
                   |TBLPROPERTIES (
                   |'file.format' = 'parquet',
                   | 'primary-key' = 'id',
                   | 'bucket' = '3',
                   |'merge-engine' = 'aggregation',
                   |'fields.s.aggregate-function' = 'listagg')
                   |""".stripMargin)
      spark.sql(s"""
                   |insert into $tbl_name values(1, "Hello "),
                   |                            (2, NULL),
                   |                            (3, "New"),
                   |                            (4, ""),
                   |                            (5, "Good"),
                   |                            (6, "")
                   |""".stripMargin)

      spark.sql(s"""
                   |insert into $tbl_name values(1, "World"),
                   |                            (2, " are "),
                   |                            (3, NULL),
                   |                            (6, NULL),
                   |                            (7, NULL)
                   |""".stripMargin)

      runQueryAndCompare(
        s"""
           |select * from $tbl_name;
           |""".stripMargin,
        noFallBack = true) {
        checkOperatorMatch[PaimonScanTransformer]
      }
    }
  }

  test("paimon aggregate: bool_and") {
    val tbl_name = s"paimon_tb"
    withTable(tbl_name) {
      spark.sql(s"""
                   |create table $tbl_name (id INT, b BOOLEAN)
                   |using paimon
                   |TBLPROPERTIES (
                   |'file.format' = 'parquet',
                   | 'primary-key' = 'id',
                   | 'bucket' = '3',
                   |'merge-engine' = 'aggregation',
                   |'fields.b.aggregate-function' = 'bool_and')
                   |""".stripMargin)
      spark.sql(s"""
                   |insert into $tbl_name values(1, TRUE),
                   |                            (2, TRUE),
                   |                            (3, TRUE),
                   |                            (4, TRUE),
                   |                            (5, FALSE),
                   |                            (6, FALSE),
                   |                            (7, FALSE),
                   |                            (8, FALSE),
                   |                            (9, NULL),
                   |                            (10,NULL),
                   |                            (11,NULL),
                   |                            (12,NULL)
                   |""".stripMargin)

      spark.sql(s"""
                   |insert into $tbl_name values(1, TRUE),
                   |                            (2, FALSE),
                   |                            (3, NULL),
                   |                            (5, TRUE),
                   |                            (6, FALSE),
                   |                            (7, NULL),
                   |                            (9, TRUE),
                   |                            (10,FALSE),
                   |                            (11,NULL),
                   |                            (13, TRUE),
                   |                            (14,FALSE),
                   |                            (15,NULL)
                   |""".stripMargin)

      runQueryAndCompare(
        s"""
           |select * from $tbl_name;
           |""".stripMargin,
        noFallBack = true) {
        checkOperatorMatch[PaimonScanTransformer]
      }
    }
  }

  test("paimon aggregate: bool_or") {
    val tbl_name = s"paimon_tb"
    withTable(tbl_name) {
      spark.sql(s"""
                   |create table $tbl_name (id INT, b BOOLEAN)
                   |using paimon
                   |TBLPROPERTIES (
                   |'file.format' = 'parquet',
                   | 'primary-key' = 'id',
                   | 'bucket' = '3',
                   |'merge-engine' = 'aggregation',
                   |'fields.b.aggregate-function' = 'bool_or')
                   |""".stripMargin)
      spark.sql(s"""
                   |insert into $tbl_name values(1, TRUE),
                   |                            (2, TRUE),
                   |                            (3, TRUE),
                   |                            (4, TRUE),
                   |                            (5, FALSE),
                   |                            (6, FALSE),
                   |                            (7, FALSE),
                   |                            (8, FALSE),
                   |                            (9, NULL),
                   |                            (10,NULL),
                   |                            (11,NULL),
                   |                            (12,NULL)
                   |""".stripMargin)

      spark.sql(s"""
                   |insert into $tbl_name values(1, TRUE),
                   |                            (2, FALSE),
                   |                            (3, NULL),
                   |                            (5, TRUE),
                   |                            (6, FALSE),
                   |                            (7, NULL),
                   |                            (9, TRUE),
                   |                            (10,FALSE),
                   |                            (11,NULL),
                   |                            (13, TRUE),
                   |                            (14,FALSE),
                   |                            (15,NULL)
                   |""".stripMargin)

      runQueryAndCompare(
        s"""
           |select * from $tbl_name;
           |""".stripMargin,
        noFallBack = true) {
        checkOperatorMatch[PaimonScanTransformer]
      }
    }
  }

  test("paimon aggregate: first_value") {
    val tbl_name = s"paimon_tb"
    withTable(tbl_name) {
      spark.sql(s"""
                   |create table $tbl_name (id INT, tint TINYINT,
                   |                        sint SMALLINT, iint INTEGER, bint BIGINT,
                   |                        f FLOAT, d DOUBLE, c CHAR(2), v VARCHAR(10),
                   |                        s STRING, da DATE)
                   |using paimon
                   |TBLPROPERTIES (
                   |'file.format' = 'parquet',
                   | 'primary-key' = 'id',
                   | 'bucket' = '3',
                   |'merge-engine' = 'aggregation',
                   |'fields.tint.aggregate-function' = 'first_value',
                   |'fields.sint.aggregate-function' = 'first_value',
                   |'fields.iint.aggregate-function' = 'first_value',
                   |'fields.bint.aggregate-function' = 'first_value',
                   |'fields.f.aggregate-function' = 'first_value',
                   |'fields.d.aggregate-function' = 'first_value',
                   |'fields.c.aggregate-function' = 'first_value',
                   |'fields.v.aggregate-function' = 'first_value',
                   |'fields.s.aggregate-function' = 'first_value',
                   |'fields.da.aggregate-function' = 'first_value')
                   |""".stripMargin)
      spark.sql(s"""
                   |insert into $tbl_name values
                   | (1,  3,  5,  7,  9, 3.5, 5.5, "AB", "Hello", "Hello", DATE '2025-09-15'),
                   | (2, 11, 13, 15, 17, 5.6, 7.6, "CD", "World", "World", DATE '2024-01-01'),
                   | (5, 19, 21, 23, 25, 9.3, 11.3, "EF", "String", "String", DATE '2023-07-04')
                   |""".stripMargin)

      spark.sql(s"""
                   |insert into $tbl_name values
                   | (1,  2,  4,  6,  8, 2.5, 3.5, 'ab', "New", "New", DATE '2022-12-31'),
                   | (2, 10, 12, 14, 16, 6.7, 8.9, 'cd', "olleH", "olleH", DATE '2000-02-29'),
                   | (7, 18, 20, 22, 24, 1.3, 7.4, 'ef', "dlroW", "dlroW", DATE '1970-01-01')
                   |""".stripMargin)

      runQueryAndCompare(
        s"""
           |select * from $tbl_name;
           |""".stripMargin,
        noFallBack = true) {
        checkOperatorMatch[PaimonScanTransformer]
      }
    }
  }

  test("paimon aggregate: first_non_null_value") {
    val tbl_name = s"paimon_tb"
    withTable(tbl_name) {
      spark.sql(s"""
                   |create table $tbl_name (id INT, tint TINYINT,
                   |                        sint SMALLINT, iint INTEGER, bint BIGINT,
                   |                        f FLOAT, d DOUBLE, c CHAR(2), v VARCHAR(10),
                   |                        s STRING, da DATE)
                   |using paimon
                   |TBLPROPERTIES (
                   |'file.format' = 'parquet',
                   | 'primary-key' = 'id',
                   | 'bucket' = '3',
                   |'merge-engine' = 'aggregation',
                   |'fields.tint.aggregate-function' = 'first_non_null_value',
                   |'fields.sint.aggregate-function' = 'first_non_null_value',
                   |'fields.iint.aggregate-function' = 'first_non_null_value',
                   |'fields.bint.aggregate-function' = 'first_non_null_value',
                   |'fields.f.aggregate-function' = 'first_non_null_value',
                   |'fields.d.aggregate-function' = 'first_non_null_value',
                   |'fields.c.aggregate-function' = 'first_non_null_value',
                   |'fields.v.aggregate-function' = 'first_non_null_value',
                   |'fields.s.aggregate-function' = 'first_non_null_value',
                   |'fields.da.aggregate-function' = 'first_non_null_value')
                   |""".stripMargin)
      spark.sql(s"""
                   |insert into $tbl_name values
                   | (1,  3,  5,  7,  9, 3.5, 5.5, "AB", "Hello", "Hello", DATE '2025-09-15'),
                   | (2, 11, 13, 15, 17, 5.6, 7.6, "CD", "World", "World", DATE '2024-01-01'),
                   | (5, 19, 21, 23, 25, 9.3, 11.3, "EF", "String", "String", DATE '2023-07-04')
                   |""".stripMargin)

      spark.sql(s"""
                   |insert into $tbl_name values
                   | (1,  2,  4,  6,  8, 2.5, 3.5, 'ab', "New", "New", DATE '2022-12-31'),
                   | (2, 10, 12, 14, 16, 6.7, 8.9, 'cd', "olleH", "olleH", DATE '2000-02-29'),
                   | (7, 18, 20, 22, 24, 1.3, 7.4, 'ef', "dlroW", "dlroW", DATE '1970-01-01')
                   |""".stripMargin)

      runQueryAndCompare(
        s"""
           |select * from $tbl_name;
           |""".stripMargin,
        noFallBack = true) {
        checkOperatorMatch[PaimonScanTransformer]
      }
    }
  }

  // Looks Java paimon has bug:
  // https://meego.larkoffice.com/dpus_queryengine_dataapplication/issue/detail/6781551769
  ignore("paimon partial-update: Single fields in group") {
    val tbl_name = s"paimon_tb"

    withTable("paimon_tb") {
      spark.sql(s"""
                   |create table $tbl_name (k   INT,
                   |    a   INT,
                   |    b   INT,
                   |    g_1 INT,
                   |    c   INT,
                   |    d   INT,
                   |    g_2 INT)
                   | using paimon
                   |TBLPROPERTIES (
                   | 'primary-key' = 'k',
                   | 'merge-engine' = 'partial-update',
                   | 'fields.g_1.sequence-group' = 'a,b',
                   | 'fields.g_2.sequence-group' = 'c,d'
                   |)
                   |""".stripMargin)

      spark.sql(s"""
                   |insert into $tbl_name
                   |values (1, 1, 1, 1, 1, 1, 1);
                   |""".stripMargin)
      spark.sql(s"""
                   |insert into $tbl_name
                   |values (1, 2, 2, 2, 2, 2, CAST(NULL AS INT));
                   |""".stripMargin)

      runQueryAndCompare(s"""
                            |select d from $tbl_name;
                            |""".stripMargin) {
        checkOperatorMatch[PaimonScanTransformer]
      }
    }
  }

  test(s"deduplicate engine- select less") {
    spark.sql(s"""
                 |CREATE TABLE T (id INT, name STRING, dt STRING, hh STRING)
                 |-- TBLPROPERTIES ('primary-key' = 'id,dt,hh', 'merge-engine' = 'deduplicate',
                 |-- 'bucket' = '4')
                 |TBLPROPERTIES ('bucket-key' = 'id', 'bucket' = '4')
                 |PARTITIONED BY (dt, hh)
                 |""".stripMargin)

    spark.sql("""
                |INSERT INTO T VALUES
                |(1, 'a', '2023-10-01', '12'),
                |(2, 'b', '2023-10-01', '12'),
                |(3, 'c', '2023-10-02', '12'),
                |(4, 'd', '2023-10-02', '13'),
                |(5, 'e', '2023-10-02', '14'),
                |(6, 'f', '2023-10-02', '15')
    """.stripMargin)

    runQueryAndCompare("SELECT name FROM T") {
      checkOperatorMatch[PaimonScanTransformer]
    }
  }

  test(s"test delete is drop partition") {
    spark.sql(s"""
                 |CREATE TABLE T (id INT, name STRING, dt STRING, hh STRING)
                 |-- TBLPROPERTIES ('primary-key' = 'id,dt,hh', 'merge-engine' = 'deduplicate',
                 |-- 'bucket' = '4')
                 |TBLPROPERTIES ('bucket-key' = 'id', 'bucket' = '4')
                 |PARTITIONED BY (dt, hh)
                 |""".stripMargin)

    spark.sql("""
                |INSERT INTO T VALUES
                |(1, 'a', '2023-10-01', '12'),
                |(2, 'b', '2023-10-01', '12'),
                |(3, 'c', '2023-10-02', '12'),
                |(4, 'd', '2023-10-02', '13'),
                |(5, 'e', '2023-10-02', '14'),
                |(6, 'f', '2023-10-02', '15')
    """.stripMargin)

    // delete isn't drop partition
    spark.sql("DELETE FROM T WHERE name = 'a' and hh = '12'")
    runQueryAndCompare("SELECT * FROM T ORDER BY id") {
      checkOperatorMatch[PaimonScanTransformer]
    }
  }

  test("paimon first row supports transform") {
    val tbl_name = s"paimon_tb"

    withTable("paimon_tb") {
      spark.sql(s"""
                   |create table $tbl_name (id INT, name STRING)
                   | using paimon
                   |TBLPROPERTIES (
                   | 'primary-key' = 'id',
                   | 'bucket' = '1',
                   | 'target-file-size' = '100b',
                   | 'write-only' = 'true',
                   | 'merge-engine' = 'first-row'
                   |)
                   |""".stripMargin)

      import testImplicits._

      val randomData = (1 to 1000).map {
        _ =>
          val a = Random.nextInt(100) + 1
          val b = "b" + (Random.nextInt(90) + 10)

          (a, b)
      }

      randomData.toDF("a", "b").createOrReplaceTempView("source")
      spark.sql(s"""
                   |insert into $tbl_name select * from source
                   |""".stripMargin)
      spark.sql(s"""
                   |insert into $tbl_name select * from source
                   |""".stripMargin)

      runQueryAndCompare(
        s"""
           |select * from $tbl_name;
           |""".stripMargin
      ) {
        checkOperatorMatch[PaimonScanTransformer]
      }
    }

  }

  test(s"insert overwrite date type partition table") {
    spark.sql(s"""
                 |CREATE TABLE T (
                 |  id STRING,
                 |  dt date)
                 | PARTITIONED BY (dt)
                 |TBLPROPERTIES (
                 |  'primary-key' = 'id,dt',
                 |  'bucket' = '3'
                 |);
                 |""".stripMargin)

    spark.sql("INSERT OVERWRITE T partition (dt='2024-04-18') values(1)")

    runQueryAndCompare(s"""
                          |SELECT * FROM T
                          |""".stripMargin) {
      checkOperatorMatch[PaimonScanTransformer]
    }
  }

  // todo fix null partition
  ignore(s"insert null as partition") {
    spark.sql(s"""
                 |CREATE TABLE T (
                 |  id STRING,
                 |  dt STRING)
                 | PARTITIONED BY (dt)
                 |TBLPROPERTIES (
                 |  'bucket' = '3',
                 |  'partition.default-name'  = 'null'
                 |);
                 |""".stripMargin)

    spark.sql("INSERT OVERWRITE T values('1', null)")

    runQueryAndCompare(s"""
                          |SELECT * FROM T
                          |""".stripMargin) {
      checkOperatorMatch[PaimonScanTransformer]
    }
  }

  // These cases that date/timestamp/bool is used as the partition field type are to be supported.
  test(s"insert overwrite table using different as the partition field type") {
    Seq(IntegerType, LongType, FloatType, DoubleType, DecimalType).foreach {
      dataType =>
        case class PartitionSQLAndValue(sql: Any, value: Any)

        val (ptField, sv1, sv2) = dataType match {
          case IntegerType =>
            ("INT", PartitionSQLAndValue(1, 1), PartitionSQLAndValue(2, 2))
          case LongType =>
            ("LONG", PartitionSQLAndValue(1L, 1L), PartitionSQLAndValue(2L, 2L))
          case FloatType =>
            ("FLOAT", PartitionSQLAndValue(12.3f, 12.3f), PartitionSQLAndValue(45.6f, 45.6f))
          case DoubleType =>
            ("DOUBLE", PartitionSQLAndValue(12.3d, 12.3), PartitionSQLAndValue(45.6d, 45.6))
          case DecimalType =>
            (
              "DECIMAL(5, 2)",
              PartitionSQLAndValue(11.222, 11.22),
              PartitionSQLAndValue(66.777, 66.78))
        }

        spark.sql(s"""
                     |CREATE TABLE T (a INT, b STRING, pt $ptField)
                     |PARTITIONED BY (pt)
                     |""".stripMargin)

        spark.sql(s"INSERT INTO T SELECT 1, 'a', ${sv1.sql} UNION ALL SELECT 2, 'b', ${sv2.sql}")
        checkAnswer(
          spark.sql("SELECT * FROM T ORDER BY a"),
          Row(1, "a", sv1.value) :: Row(2, "b", sv2.value) :: Nil)

        // overwrite the whole table
        spark.sql(
          s"INSERT OVERWRITE T SELECT 3, 'c', ${sv1.sql} UNION ALL SELECT 4, 'd', ${sv2.sql}")
        checkAnswer(
          spark.sql("SELECT * FROM T ORDER BY a"),
          Row(3, "c", sv1.value) :: Row(4, "d", sv2.value) :: Nil)

        // overwrite the a=1 partition
        spark.sql(s"INSERT OVERWRITE T PARTITION (pt = ${sv1.value}) VALUES (5, 'e'), (7, 'g')")
        checkAnswer(
          spark.sql("SELECT * FROM T ORDER BY a"),
          Row(4, "d", sv2.value) :: Row(5, "e", sv1.value) :: Row(7, "g", sv1.value) :: Nil)

        spark.sql("DROP TABLE T")
    }
  }

  private def validateMetadataColumnQuery(tbl_name: String, metadataColumn: String): Unit = {
    // Test different query formats
    Seq(
      s"""SELECT $metadataColumn FROM $tbl_name""",
      s"""SELECT $metadataColumn as metadataColumn FROM $tbl_name""",
      s"""SELECT *, $metadataColumn FROM $tbl_name""",
      s"""SELECT $metadataColumn, * FROM $tbl_name""",
      s"""SELECT $metadataColumn as metadataColumn, * FROM $tbl_name"""
    ).foreach {
      query =>
        runQueryAndCompare(query) {
          checkOperatorMatch[PaimonScanTransformer]
        }
    }
  }

  test("paimon metadata columns: __paimon_row_index - empty table") {
    val tbl_name = s"paimon_tb"
    val metadataColumn = "__paimon_row_index"

    withTable(tbl_name) {
      spark.sql(s"""
                   |create table $tbl_name (id INT, name STRING) using paimon
                   |TBLPROPERTIES (
                   |'file.format' = 'parquet'
                   |)""".stripMargin)

      validateMetadataColumnQuery(tbl_name, metadataColumn)
    }
  }

  test("paimon metadata columns: __paimon_row_index - tables with primary key definitions") {
    val tbl_name = s"paimon_tb"
    val metadataColumn = "__paimon_row_index"

    withTable(tbl_name) {
      spark.sql(s"""
                   |create table $tbl_name (id INT, name STRING) using paimon
                   |TBLPROPERTIES (
                   |'file.format' = 'parquet',
                   |'primary-key' = 'id'
                   |)""".stripMargin)

      spark.sql(s"INSERT INTO $tbl_name VALUES (4, '4'), (3, '3'), (2, '2'), (1, '1')")

      checkAnswer(
        spark.sql(s"SELECT id, $metadataColumn FROM $tbl_name ORDER BY id"),
        Seq(Row(1, 0), Row(2, 1), Row(3, 2), Row(4, 3)))
    }
  }

  test("paimon metadata columns: __paimon_row_index - deduplicate with single insert operation") {
    val tbl_name = s"paimon_tb"
    val metadataColumn = "__paimon_row_index"

    withTable(tbl_name) {
      spark.sql(s"""
                   |create table $tbl_name (id INT, name STRING) using paimon
                   |TBLPROPERTIES (
                   |'file.format' = 'parquet',
                   |'merge-engine' = 'deduplicate',
                   |'primary-key' = 'id'
                   |)""".stripMargin)

      spark.sql(s"INSERT INTO $tbl_name VALUES (1, '1'), (2, '2'), (3, '3')")

      val result = spark.sql(s"SELECT id, $metadataColumn FROM $tbl_name ORDER BY id").collect()
      assert(result.length == 3, "Expected 3 rows")
      // Verify indices are consistent and non-negative
      result.zipWithIndex.foreach {
        case (row, expectedIndex) =>
          assert(row.getLong(1) == expectedIndex, "Row index does not match")
      }
    }
  }

  test("paimon metadata columns: __paimon_row_index - MOR tables with multiple insert operations") {
    val tbl_name = s"paimon_tb"
    val metadataColumn = "__paimon_row_index"

    withTable(tbl_name) {
      spark.sql(s"""
                   |create table $tbl_name (id INT, name STRING) using paimon
                   |TBLPROPERTIES (
                   |'file.format' = 'parquet',
                   |'merge-engine' = 'deduplicate',
                   |'primary-key' = 'id'
                   |)""".stripMargin)

      spark.sql(s"INSERT INTO $tbl_name VALUES (1, '1'), (2, '2')")
      spark.sql(s"INSERT INTO $tbl_name VALUES (3, '3'), (4, '4')")

      checkAnswer(
        spark.sql(s"SELECT id, $metadataColumn FROM $tbl_name ORDER BY id"),
        Seq(Row(1, 0), Row(2, 1), Row(3, 0), Row(4, 1)))
    }
  }

  test("paimon metadata columns: __paimon_row_index - MOR table, overwrite records") {
    val tbl_name = s"paimon_tb"
    val metadataColumn = "__paimon_row_index"

    withTable(tbl_name) {
      spark.sql(s"""
                   |create table $tbl_name (id INT, name STRING) using paimon
                   |TBLPROPERTIES (
                   |'file.format' = 'parquet',
                   |'primary-key' = 'id',
                   |'merge-engine' = 'partial-update'
                   |)""".stripMargin)

      spark.sql(s"INSERT INTO $tbl_name VALUES (1, 'original1'), (2, 'original2')")
      // Overwrite some records
      spark.sql(s"INSERT INTO $tbl_name VALUES (1, 'updated1'), (3, 'new3')")

      // Verify final state - id=1 should have the updated value
      checkAnswer(
        spark.sql(s"SELECT id, name FROM $tbl_name ORDER BY id"),
        Row(1, "updated1") :: Row(2, "original2") :: Row(3, "new3") :: Nil)

      // Verify row indices for all records
      checkAnswer(
        spark.sql(s"SELECT id, $metadataColumn FROM $tbl_name ORDER BY id"),
        Row(1, 0) :: Row(2, 1) :: Row(3, 1) :: Nil)
    }
  }

  test("paimon metadata columns: __paimon_row_index - table with filter pushdown") {
    val tbl_name = s"paimon_tb"
    val metadataColumn = "__paimon_row_index"
    // set spark conf parquet row group size to 1KiB
    spark.conf.set("spark.sql.parquet.rowGroupSize", "1024")

    withTable(tbl_name) {
      // Create table with primary key
      spark.sql(s"""
                   |create table $tbl_name (id INT, name STRING) using paimon
                   |TBLPROPERTIES (
                   |'file.format' = 'parquet'
                   |)""".stripMargin)

      val dfName = s"30k_data"
      val maxValue = 30000
      val df =
        spark.range(1, maxValue + 1).toDF("id").withColumn("name", concat(lit("name_"), col("id")))
      df.createOrReplaceTempView(dfName)

      spark.sql(s"INSERT INTO $tbl_name SELECT * FROM $dfName")

      // query the table in chunks of 1000, verify the row index is equal to id-1
      val chunks = maxValue / 1000
      for (i <- 0 until chunks) {
        val minId = i * 1000 + 1
        val maxId = (i + 1) * 1000
        checkAnswer(
          spark.sql(s"""
                       |SELECT id, $metadataColumn
                       |FROM $tbl_name
                       |WHERE id >= $minId AND id <= $maxId
                       |ORDER BY id
                       |""".stripMargin),
          (minId until maxId + 1).map(id => Row(id, id - 1)).toSeq
        )
      }
    }
    spark.conf.unset("spark.sql.parquet.rowGroupSize")
  }

  test("paimon metadata columns: __paimon_file_path - empty table") {
    val tbl_name = s"paimon_tb"
    val metadataColumn = "__paimon_file_path"

    withTable(tbl_name) {
      spark.sql(s"""
                   |create table $tbl_name (id INT, name STRING) using paimon
                   |TBLPROPERTIES (
                   |'file.format' = 'parquet'
                   |)""".stripMargin)

      validateMetadataColumnQuery(tbl_name, metadataColumn)
    }
  }

  test("paimon metadata columns: __paimon_file_path - tables with primary key definitions") {
    val tbl_name = s"paimon_tb"
    val metadataColumn = "__paimon_file_path"

    withTable(tbl_name) {
      spark.sql(s"""
                   |create table $tbl_name (id INT, name STRING) using paimon
                   |TBLPROPERTIES (
                   |'file.format' = 'parquet',
                   |'primary-key' = 'id'
                   |)""".stripMargin)

      // Insert test data
      spark.sql(s"INSERT INTO $tbl_name VALUES (1, '1'), (2, '2'), (3, '3'), (4, '4')")

      checkAnswer(
        spark.sql(
          s"SELECT COUNT(*) FROM $tbl_name WHERE $metadataColumn IS NULL OR $metadataColumn = ''"),
        Row(0))

      // Verify all file paths are absolute paths and non empty and nonnull
      val results = spark.sql(s"SELECT $metadataColumn FROM $tbl_name").collect()
      results.foreach {
        row =>
          {
            val path = row.getString(0)
            assert(path.nonEmpty, "File path should be non-empty string")
            val uri = java.net.URI.create(path)
            assert(uri.isAbsolute, "uri must not be a relative path.")
          }
      }
    }
  }

  test("paimon metadata columns: __paimon_file_path - tables with single insert operation") {
    val tbl_name = s"paimon_tb"
    val metadataColumn = "__paimon_file_path"

    withTable(tbl_name) {
      spark.sql(s"""
                   |create table $tbl_name (id INT, name STRING) using paimon
                   |TBLPROPERTIES (
                   |'file.format' = 'parquet',
                   |'primary-key' = 'id'
                   |)""".stripMargin)

      spark.sql(s"INSERT INTO $tbl_name VALUES (1, '1'), (2, '2'), (3, '3')")
      checkAnswer(
        spark.sql(
          s"SELECT COUNT(*) FROM $tbl_name WHERE $metadataColumn IS NULL OR $metadataColumn = ''"),
        Row(0))

      val results = spark.sql(s"SELECT $metadataColumn FROM $tbl_name").collect()
      results.foreach {
        row =>
          {
            val path = row.getString(0)
            assert(path.nonEmpty, "File path should be non-empty string")
            val uri = java.net.URI.create(path)
            assert(uri.isAbsolute, "uri must not be a relative path.")
          }
      }
    }
  }

  test("paimon metadata columns: __paimon_file_path - tables with multiple insert operations") {
    val tbl_name = s"paimon_tb"
    val metadataColumn = "__paimon_file_path"

    withTable(tbl_name) {
      // Create Merge-On-Read table
      spark.sql(s"""
                   |create table $tbl_name (id INT, name STRING) using paimon
                   |TBLPROPERTIES (
                   |'file.format' = 'parquet',
                   |'primary-key' = 'id'
                   |)""".stripMargin)

      spark.sql(s"INSERT INTO $tbl_name VALUES (1, '1'), (2, '2')")
      spark.sql(s"INSERT INTO $tbl_name VALUES (3, '3'), (4, '4')")

      checkAnswer(
        spark.sql(
          s"SELECT COUNT(*) FROM $tbl_name WHERE $metadataColumn IS NULL OR $metadataColumn = ''"),
        Row(0))

      // check that the file paths are valid and that paths for 1/2 are different than 3/4
      val results = spark.sql(s"SELECT $metadataColumn FROM $tbl_name ORDER BY id").collect()
      results.foreach {
        row =>
          {
            val path = row.getString(0)
            assert(path.nonEmpty, "File path should be non-empty string")
            val uri = java.net.URI.create(path)
            assert(uri.isAbsolute, "uri must not be a relative path.")
          }
      }
      val firstTwoPaths = results.take(2).map(_.getString(0))
      val lastTwoPaths = results.drop(2).map(_.getString(0))
      assert(firstTwoPaths.toSet.size == 1, "First two file paths should be the same")
      assert(lastTwoPaths.toSet.size == 1, "Last two file paths should be the same")
      assert(firstTwoPaths.head != lastTwoPaths.head, "Paths from two different inserts")
    }
  }

  test("paimon metadata columns: __paimon_file_path - MOR table, overwrite records") {
    val tbl_name = s"paimon_tb"
    val metadataColumn = "__paimon_file_path"

    withTable(tbl_name) {
      spark.sql(s"""
                   |create table $tbl_name (id INT, name STRING) using paimon
                   |TBLPROPERTIES (
                   |'file.format' = 'parquet',
                   |'primary-key' = 'id',
                   |'merge-engine' = 'partial-update'
                   |)""".stripMargin)

      spark.sql(s"INSERT INTO $tbl_name VALUES (1, 'original1'), (2, 'original2')")
      val results = spark.sql(s"SELECT $metadataColumn FROM $tbl_name").collect()

      spark.sql(s"INSERT INTO $tbl_name VALUES (1, 'updated1'), (3, 'new3')")

      checkAnswer(
        spark.sql(
          s"SELECT COUNT(*) FROM $tbl_name WHERE $metadataColumn IS NULL OR $metadataColumn = ''"),
        Row(0))

      val allPaths =
        spark.sql(s"SELECT $metadataColumn FROM $tbl_name ORDER BY id").collect()
      allPaths.foreach {
        row => assert(row.getString(0).nonEmpty, "File path should be non-empty string")
      }
      // verify the overwritten data is different from the original
      val overwriteResults = spark.sql(s"SELECT $metadataColumn FROM $tbl_name WHERE id = 1")
      val overwriteResult = overwriteResults.head.getString(0)
      val originalResult = results.head.getString(0)
      assert(overwriteResult != originalResult, "Overwritten data should have a new file path")
    }
  }

  // Test for __paimon_bucket metadata column

  test("paimon metadata columns: __paimon_bucket, simple bucketed table") {
    val tbl_name = s"paimon_tb"
    val metadataColumn = "__paimon_bucket"

    withTable(tbl_name) {
      spark.sql(s"""
                   |create table $tbl_name (id BIGINT, part BIGINT) using paimon
                   |TBLPROPERTIES (
                   |'file.format' = 'parquet',
                   |'bucket' = '4',
                   |'bucket-key' = 'id'
                   |) PARTITIONED BY (part)""".stripMargin)
      spark.sql(s"INSERT INTO $tbl_name SELECT id, id % 2 AS part FROM range(100)")

      validateMetadataColumnQuery(tbl_name, metadataColumn)
      val schema = spark.sql(s"SELECT $metadataColumn FROM $tbl_name LIMIT 0").schema
      assert(schema.fields.length == 1)
      assert(schema.fields(0).dataType == IntegerType)
      assert(schema.fields(0).name == metadataColumn)
      checkAnswer(
        spark.sql(s"""SELECT $metadataColumn FROM $tbl_name
                     | GROUP BY $metadataColumn
                     | ORDER BY $metadataColumn""".stripMargin),
        Row(0) ::
          Row(1) ::
          Row(2) ::
          Row(3) :: Nil
      )
    }
  }

  test("paimon metadata columns: __paimon_bucket - cross-partition data consistency") {
    val tbl_name = s"paimon_tb"
    val metadataColumn = "__paimon_bucket"
    val numBuckets = 4

    withTable(tbl_name) {
      spark.sql(s"""
                   |create table $tbl_name (id BIGINT, part BIGINT) using paimon
                   |TBLPROPERTIES (
                   |'file.format' = 'parquet',
                   |'bucket' = '$numBuckets',
                   |'bucket-key' = 'id'
                   |) PARTITIONED BY (part)""".stripMargin)
      spark.sql(s"INSERT INTO $tbl_name SELECT id, id % 2 as part FROM range(100)")

      // For each partition, verify buckets are numbered from 0 to numBuckets-1
      val partitions = spark.sql(s"SELECT DISTINCT part FROM $tbl_name").collect()
      partitions.foreach {
        row =>
          val partition = row.getLong(0)
          val bucketIds = spark
            .sql(
              s"SELECT DISTINCT $metadataColumn FROM $tbl_name WHERE part = $partition"
            )
            .collect()
            .map(_.getInt(0))
            .sorted
          bucketIds.foreach {
            bucketId =>
              assert(
                bucketId >= 0 && bucketId < numBuckets,
                s"Invalid bucket ID: $bucketId in partition $partition")
          }
      }
    }
  }

  // Test for __paimon_partition metadata column

  test("paimon metadata columns: __paimon_partition - basic existence verification") {
    val tbl_name = s"paimon_tb"
    val metadataColumn = "__paimon_partition"

    withTable(tbl_name) {
      spark.sql(s"""
                   |create table $tbl_name (id BIGINT, part BIGINT) using paimon
                   |TBLPROPERTIES (
                   |'file.format' = 'parquet'
                   |) PARTITIONED BY (part)""".stripMargin)
      spark.sql(s"INSERT INTO $tbl_name SELECT id, id % 2 AS part FROM range(10)")

      validateMetadataColumnQuery(tbl_name, metadataColumn)
    }
  }

  test("paimon metadata columns: __paimon_partition - data type validation") {
    val tbl_name = s"paimon_tb"
    val metadataColumn = "__paimon_partition"

    withTable(tbl_name) {
      spark.sql(s"""
                   |create table $tbl_name (id BIGINT, part BIGINT) using paimon
                   |TBLPROPERTIES (
                   |'file.format' = 'parquet'
                   |) PARTITIONED BY (part)""".stripMargin)

      spark.sql(s"INSERT INTO $tbl_name VALUES (1, 0), (2, 0), (3, 1), (4, 1)")

      val schema = spark.sql(s"SELECT $metadataColumn FROM $tbl_name LIMIT 0").schema
      assert(schema.fields.length == 1)
      assert(
        schema.fields(0).dataType.isInstanceOf[StructType],
        s"Expected StructType for $metadataColumn, got ${schema.fields(0).dataType}")
      assert(schema.fields(0).name == metadataColumn)
      val structType = schema.fields(0).dataType.asInstanceOf[StructType]
      assert(structType.fields.length >= 1, "Partition struct should have at least one field")
      val hasPartitionField = structType.fields.exists(field => field.name == "part")
      assert(hasPartitionField, s"Partition struct does not contain field matching 'part'")
    }
  }

  test("paimon metadata columns: __paimon_partition - partition values correctness") {
    val tbl_name = s"paimon_tb"
    val metadataColumn = "__paimon_partition"

    withTable(tbl_name) {
      spark.sql(s"""
                   |create table $tbl_name (id BIGINT, part BIGINT) using paimon
                   |TBLPROPERTIES (
                   |'file.format' = 'parquet'
                   |) PARTITIONED BY (part)""".stripMargin)
      spark.sql(s"INSERT INTO $tbl_name VALUES (1, 0), (2, 0), (3, 1), (4, 1)")

      // Verify partition values match actual partition values
      // __paimon_partition is a ROW type that contains the partition key-value pairs
      val results = spark.sql(s"SELECT id, part, $metadataColumn FROM $tbl_name").collect()

      results.foreach {
        row =>
          val id = row.getLong(0)
          val expectedPartitionValue = row.getLong(1)
          val partitionStruct = row.get(2).asInstanceOf[Row]

          // Check that the struct is not null
          assert(partitionStruct != null, "Partition struct should not be null")

          // Get struct fields and check for matching partition value. The field
          // name in the struct is the same as the partition column name
          val partitionFieldValue = partitionStruct.getAs[Long]("part")
          assert(partitionFieldValue == expectedPartitionValue)

      }
    }
  }

  test("paimon metadata columns: __paimon_partition - multi-level partitions") {
    val tbl_name = s"paimon_tb"
    val metadataColumn = "__paimon_partition"

    withTable(tbl_name) {
      spark.sql(s"""
                   |create table $tbl_name (id BIGINT, part BIGINT, part2 BIGINT) using paimon
                   |TBLPROPERTIES (
                   |'file.format' = 'parquet'
                   |) PARTITIONED BY (part, part2)""".stripMargin)
      spark.sql(
        s"INSERT INTO $tbl_name SELECT id, id % 2 as part, id % 3 AS part2 FROM range(7)"
      )
      val results = spark.sql(s"SELECT id, part, part2, $metadataColumn FROM $tbl_name").collect()

      results.foreach {
        row =>
          val id = row.getLong(0)
          val expectedPartition1 = row.getLong(1)
          val expectedPartition2 = row.getLong(2)

          val partitionStruct = row.get(3).asInstanceOf[Row]

          assert(partitionStruct != null, "Partition struct should not be null")

          var foundPartition1 = false
          var foundPartition2 = false

          for (i <- 0 until partitionStruct.length) {
            val fieldValue = partitionStruct.get(i)
            if (fieldValue != null) {
              if (fieldValue == expectedPartition1) foundPartition1 = true
              if (fieldValue == expectedPartition2) foundPartition2 = true
            }
          }

          assert(
            foundPartition1,
            s"Partition struct doesn't contain expected value $expectedPartition1 for ID $id")
          assert(
            foundPartition2,
            s"Partition struct doesn't contain expected value $expectedPartition2 for ID $id")
      }
    }
  }

  test("paimon metadata columns: __paimon_partition - behavior under table modifications") {
    val tbl_name = s"paimon_tb"
    val metadataColumn = "__paimon_partition"

    withTable(tbl_name) {
      spark.sql(s"""
                   |create table $tbl_name (id BIGINT, part BIGINT) using paimon
                   |TBLPROPERTIES (
                   |'file.format' = 'parquet'
                   |) PARTITIONED BY (part)""".stripMargin)

      spark.sql(s"INSERT INTO $tbl_name VALUES (1, 0), (2, 0)")

      val initialResults = spark.sql(s"SELECT $metadataColumn FROM $tbl_name").collect()
      assert(initialResults.nonEmpty)

      initialResults.foreach {
        row =>
          val partitionStruct = row.get(0).asInstanceOf[Row]
          assert(partitionStruct != null, "Partition struct should not be null")
          assert(partitionStruct.length > 0, "Partition struct should have at least one field")
      }

      spark.sql(s"INSERT INTO $tbl_name VALUES (3, 1), (4, 1), (5, 0), (6, 1)")

      val updatedResults = spark.sql(s"SELECT $metadataColumn FROM $tbl_name").collect()
      assert(updatedResults.length >= initialResults.length)

      updatedResults.foreach {
        row =>
          val partitionStruct = row.get(0).asInstanceOf[Row]
          assert(partitionStruct != null, "Partition struct should not be null after update")
          assert(partitionStruct.length > 0, "Partition struct should have fields after update")
      }

      checkAnswer(
        spark.sql(s"SELECT COUNT(*) FROM $tbl_name WHERE $metadataColumn IS NULL"),
        Row(0))
    }
  }

  test("paimon metadata columns: combined usage of __paimon_bucket and __paimon_partition") {
    val tbl_name = s"paimon_tb"

    withTable(tbl_name) {
      spark.sql(s"""
                   |create table $tbl_name (id BIGINT, part BIGINT) using paimon
                   |TBLPROPERTIES (
                   |'file.format' = 'parquet',
                   |'bucket' = '4',
                   |'bucket-key' = 'id'
                   |) PARTITIONED BY (part)""".stripMargin)
      spark.sql(s"INSERT INTO $tbl_name VALUES (1, 0), (2, 0), (3, 1), (4, 1)")
      validateMetadataColumnQuery(tbl_name, "__paimon_bucket, __paimon_partition")

      val results =
        spark
          .sql(s"SELECT id, part, __paimon_bucket, __paimon_partition FROM $tbl_name")
          .collect()

      assert(results.length == 4, "Expected 4 rows in combined query")
      results.foreach {
        row =>
          val bucket = row.getInt(2)
          assert(bucket >= 0 && bucket < 4, s"Invalid bucket ID: $bucket")

          val partitionStruct = row.get(3).asInstanceOf[Row]
          assert(partitionStruct != null, "Partition metadata should not be null")
          assert(partitionStruct.length > 0, "Partition struct should have at least one field")

          // Verify the partition value matches the part column
          val id = row.getLong(0)
          val expectedPartitionValue = row.getLong(1) // Using actual part column value
          var foundMatchingValue = false

          for (i <- 0 until partitionStruct.length) {
            val fieldValue = partitionStruct.get(i)
            if (fieldValue != null && fieldValue == expectedPartitionValue) {
              foundMatchingValue = true
            }
          }

          assert(
            foundMatchingValue,
            s"Partition struct doesn't contain expected value $expectedPartitionValue for ID $id")
      }
    }
  }

  test("paimon metadata columns: __paimon_partition and __paimon_bucket no partition no bucket") {
    val tbl_name = s"tbl_no_partition_no_bucket"

    withTable(tbl_name) {
      spark.sql(s"""
                   |CREATE TABLE $tbl_name
                   |TBLPROPERTIES('file.format' = 'parquet')
                   |AS
                   |SELECT id, id % 4 as part
                   |FROM range(100)""".stripMargin)
      validateMetadataColumnQuery(tbl_name, "__paimon_partition, __paimon_bucket")

      // For non-partitioned table, __paimon_partition should still work
      // but with an empty struct value
      // val partitionResults =
      //   spark.sql(s"SELECT DISTINCT __paimon_partition FROM $tbl_name").collect()
      // assert(partitionResults.nonEmpty, "Should return results for __paimon_partition")
      // assert(partitionResults.length == 1, "Should return results for __paimon_partition")

      // For non-bucketed table, __paimon_bucket should still work but with default value of 0
      val bucketResults = spark.sql(s"SELECT DISTINCT __paimon_bucket FROM $tbl_name").collect()
      assert(bucketResults.nonEmpty, "Should return results for __paimon_bucket")
      assert(bucketResults.length == 1, "Should return results for __paimon_bucket")
      assert(bucketResults(0).get(0) == 0, "Should return results for __paimon_bucket")

      checkAnswer(spark.sql(s"SELECT COUNT(*) FROM $tbl_name"), Row(100))
    }
  }

  test("paimon metadata columns: __paimon_partition and __paimon_bucket no partition yes bucket") {
    val tbl_name = s"tbl_no_partition_yes_bucket"

    withTable(tbl_name) {
      spark.sql(s"""
                   |CREATE TABLE $tbl_name
                   |TBLPROPERTIES('file.format' = 'parquet', 'bucket' = '4', 'bucket-key' = 'id')
                   |AS
                   |SELECT id, id % 4 as part
                   |FROM range(100)""".stripMargin)
      validateMetadataColumnQuery(tbl_name, "__paimon_partition, __paimon_bucket")
      val bucketCounts =
        spark
          .sql(s"SELECT __paimon_bucket, COUNT(*) FROM $tbl_name GROUP BY __paimon_bucket")
          .collect()
      assert(bucketCounts.length == 4, "Should have 4 buckets as specified")

      bucketCounts.foreach {
        row =>
          val bucketId = row.getInt(0)
          assert(bucketId >= 0 && bucketId < 4, s"Invalid bucket ID: $bucketId")
      }
    }
  }

  test("paimon metadata columns: __paimon_partition and __paimon_bucket yes partition no bucket") {
    val tbl_name = s"tbl_yes_partition_no_bucket"

    withTable(tbl_name) {
      spark.sql(s"""
                   |CREATE TABLE $tbl_name
                   |PARTITIONED BY (part)
                   |TBLPROPERTIES('file.format' = 'parquet')
                   |AS
                   |SELECT id, id % 4 as part
                   |FROM range(100)""".stripMargin)
      validateMetadataColumnQuery(tbl_name, "__paimon_partition, __paimon_bucket")
      val partitionValues = spark.sql(s"SELECT DISTINCT part FROM $tbl_name").collect()
      assert(partitionValues.length == 4, "Should have 4 distinct partition values (0,1,2,3)")

      val results = spark.sql(s"SELECT part, __paimon_partition FROM $tbl_name LIMIT 10").collect()
      results.foreach {
        row =>
          val expectedPartition = row.getLong(0)
          val partitionStruct = row.get(1).asInstanceOf[Row]

          // Check partition struct contains the expected value
          var foundMatchingValue = false
          for (i <- 0 until partitionStruct.length) {
            val fieldValue = partitionStruct.get(i)
            if (fieldValue != null && fieldValue == expectedPartition) {
              foundMatchingValue = true
            }
          }

          assert(
            foundMatchingValue,
            s"Partition struct doesn't contain expected value $expectedPartition")
      }
    }
  }

  ignore("paimon metadata columns: metadata column filters") {
    val tbl_name = s"paimon_tb_meta_filters"

    withTable(tbl_name) {
      spark.sql(s"""
                   |CREATE TABLE $tbl_name
                   |PARTITIONED BY (part)
                   |TBLPROPERTIES('file.format' = 'parquet', 'bucket' = '4', 'bucket-key' = 'id')
                   |AS
                   |SELECT id, (id % 4) as part
                   |FROM range(100)""".stripMargin)
      validateMetadataColumnQuery(
        tbl_name,
        "__paimon_partition, __paimon_bucket, __paimon_row_index, __paimon_file_path")

      // verify that if we select a __paimon_row_index in the file we more than 0 records,
      // but less than 100
      val rowIndexDf = spark
        .sql(s"SELECT __paimon_row_index FROM $tbl_name WHERE __paimon_row_index = 7")
        .collect()
      assert(
        rowIndexDf.length > 0 && rowIndexDf.length < 100,
        "record count should be > 0 and < 100 for __paimon_row_index")

      // get all the file paths
      val filePaths = spark.sql(s"SELECT distinct __paimon_file_path FROM $tbl_name").collect()
      // take first file path, and run a query that filters on that path. Verify the results
      // are correct
      checkAnswer(
        spark.sql(s"""SELECT distinct __paimon_file_path FROM $tbl_name
                     |WHERE __paimon_file_path = '${filePaths(0).getString(0)}'
                     |""".stripMargin),
        Row(filePaths(0).getString(0)) :: Nil
      )

      // verify __paimon_bucket filters correctly
      checkAnswer(
        spark.sql(s"SELECT distinct __paimon_bucket FROM $tbl_name WHERE __paimon_bucket = 0"),
        Row(0) :: Nil
      )

      // verify __paimon_partition filters correctly
      checkAnswer(
        spark.sql(s"SELECT count(*) FROM $tbl_name WHERE __paimon_partition = 0"),
        Row(25) :: Nil
      )

    }
  }

  test("paimon metadata columns: __paimon_partition multiple partition keys") {
    val tbl_name = s"tbl_multi_partition"

    withTable(tbl_name) {
      spark.sql(s"""
                   |CREATE TABLE $tbl_name
                   |PARTITIONED BY (part, part2)
                   |TBLPROPERTIES('file.format' = 'parquet')
                   |AS
                   |SELECT id, id % 4 as part, id % 3 as part2
                   |FROM range(100)""".stripMargin)

      validateMetadataColumnQuery(tbl_name, "__paimon_partition")

      // Verify both partition keys are present in the __paimon_partition struct
      val results =
        spark.sql(s"SELECT part, part2, __paimon_partition FROM $tbl_name LIMIT 10").collect()
      results.foreach {
        row =>
          val expectedPartkey = row.getLong(0)
          val expectedPartkey2 = row.getLong(1)
          val partitionStruct = row.get(2).asInstanceOf[Row]

          // Check partition struct contains both expected values
          var foundPartkey = false
          var foundPartkey2 = false

          for (i <- 0 until partitionStruct.length) {
            val fieldValue = partitionStruct.get(i)
            if (fieldValue != null) {
              if (fieldValue == expectedPartkey) foundPartkey = true
              if (fieldValue == expectedPartkey2) foundPartkey2 = true
            }
          }

          assert(
            foundPartkey,
            s"Partition struct doesn't contain expected value $expectedPartkey for part")
          assert(
            foundPartkey2,
            s"Partition struct doesn't contain expected value $expectedPartkey2 for part2")
      }
    }
  }

  test("paimon metadata columns: _ROW_ID") {
    assume(
      !org.apache.spark.SPARK_VERSION_SHORT.startsWith("3.5"),
      "Spark 3.5 does not yet support _ROW_ID metadata column")
    val tbl_name = s"tbl_row_id"

    withTable(tbl_name) {
      spark.sql(s"""
                   |CREATE TABLE $tbl_name
                   |TBLPROPERTIES(
                   | 'file.format' = 'parquet',
                   | 'target-file-size' = '100b',
                   | 'row-tracking.enabled' = 'true',
                   | 'merge-engine' = 'partial-update'
                   |)
                   |AS
                   |SELECT id, id % 4 as data
                   |FROM range(100)""".stripMargin)
      validateMetadataColumnQuery(tbl_name, "_ROW_ID")
      spark.sql(s"INSERT INTO $tbl_name SELECT id, id % 4 FROM range(256, 512)")
      validateMetadataColumnQuery(tbl_name, "_ROW_ID")
    }
  }

  test("paimon metadata columns: _SEQUENCE_NUMBER") {
    assume(
      !org.apache.spark.SPARK_VERSION_SHORT.startsWith("3.5"),
      "Spark 3.5 does not yet support _SEQUENCE_NUMBER metadata column")
    val tbl_name = s"tbl_sequence_number"

    withTable(tbl_name) {
      spark.sql(s"""
                   |CREATE TABLE $tbl_name
                   |TBLPROPERTIES(
                   | 'file.format' = 'parquet',
                   | 'target-file-size' = '100b',
                   | 'row-tracking.enabled' = 'true',
                   | 'merge-engine' = 'partial-update'
                   |)
                   |AS
                   |SELECT id
                   |FROM range(100)""".stripMargin)

      validateMetadataColumnQuery(tbl_name, "_SEQUENCE_NUMBER")
      // insert into table, then read again
      spark.sql(s"INSERT INTO $tbl_name SELECT id FROM range(256, 512)")
      validateMetadataColumnQuery(tbl_name, "_SEQUENCE_NUMBER")
    }
  }
}

class DisableNativeSource extends WholeStageTransformerSuite {

  protected val rootPath: String = getClass.getResource("/").getPath
  override protected val resourcePath: String = "/tpch-data-parquet-velox"
  override protected val fileFormat: String = "parquet"

  override protected def sparkConf: SparkConf = {
    super.sparkConf
      .set("spark.shuffle.manager", "org.apache.spark.shuffle.sort.ColumnarShuffleManager")
      .set("spark.sql.files.maxPartitionBytes", "1g")
      .set("spark.sql.shuffle.partitions", "1")
      .set("spark.memory.offHeap.size", "2g")
      .set("spark.unsafe.exceptionOnMemoryLeak", "true")
      .set("spark.sql.autoBroadcastJoinThreshold", "-1")
      .set(
        "spark.sql.extensions",
        "org.apache.paimon.spark.extensions.PaimonSparkSessionExtensions")
      .set("spark.sql.catalog.spark_catalog", "org.apache.paimon.spark.SparkCatalog")
      .set("spark.sql.catalog.spark_catalog.warehouse", s"file://$rootPath/paimon-velox")
      .set("spark.gluten.paimon.native.source.enabled", "false")
  }

  test("paimon transformer exists: append table with fallback") {
    val tbl_name = s"paimon_tb"
    withTable(tbl_name) {
      spark.sql(s"""
                   |create table $tbl_name (id INT, name STRING) using paimon
                   |TBLPROPERTIES (
                   |'file.format' = 'parquet',
                   | 'bucket' = '3',
                   | 'bucket-key' = 'id')
                   |""".stripMargin)
      spark.sql(s"""
                   |insert into $tbl_name values(1, '1')
                   |""".stripMargin)

      runQueryAndCompare(
        s"""
           |select * from $tbl_name;
           |""".stripMargin,
        noFallBack = false)(df => FallbackUtil.hasFallback(df.queryExecution.executedPlan))
    }
  }
}
