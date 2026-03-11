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
package org.apache.spark.sql.execution

import org.apache.gluten.config.GlutenConfig

import org.apache.spark.SparkConf
import org.apache.spark.internal.config
import org.apache.spark.internal.config.UI.UI_ENABLED
import org.apache.spark.sql.{GlutenQueryTest, Row, SparkSession}
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.catalyst.expressions.CodegenObjectFactoryMode
import org.apache.spark.sql.catalyst.optimizer.ConvertToLocalRelation
import org.apache.spark.sql.classic.ClassicTypes._
import org.apache.spark.sql.hive.HiveUtils
import org.apache.spark.sql.internal.{SQLConf, StaticSQLConf}
import org.apache.spark.sql.test.SQLTestUtils
import org.apache.spark.util.Utils

import org.apache.hadoop.fs.Path
import org.apache.parquet.hadoop.ParquetFileReader
import org.apache.parquet.hadoop.util.HadoopInputFile

import java.io.File

class VeloxParquetWriteForHiveSuite extends GlutenQueryTest with SQLTestUtils with WriteUtils {
  private var _spark: SparkSession = _
  import testImplicits._

  override protected def beforeAll(): Unit = {
    super.beforeAll()

    if (_spark == null) {
      // By default, the classic SparkSession is constructed.
      _spark = SparkSession.builder().config(sparkConf).enableHiveSupport().getOrCreate()
    }

    _spark.sparkContext.setLogLevel("warn")
  }

  override protected def spark: ClassicSparkSession = _spark.asInstanceOf[ClassicSparkSession]

  protected def defaultSparkConf: SparkConf = {
    val conf = new SparkConf()
      .set("spark.master", "local[1]")
      .set("spark.sql.test", "")
      .set("spark.sql.testkey", "true")
      .set(SQLConf.CODEGEN_FALLBACK.key, "false")
      .set(SQLConf.CODEGEN_FACTORY_MODE.key, CodegenObjectFactoryMode.CODEGEN_ONLY.toString)
      .set(
        HiveUtils.HIVE_METASTORE_BARRIER_PREFIXES.key,
        "org.apache.spark.sql.hive.execution.PairSerDe")
      // SPARK-8910
      .set(UI_ENABLED, false)
      .set(config.UNSAFE_EXCEPTION_ON_MEMORY_LEAK, true)
      // Hive changed the default of hive.metastore.disallow.incompatible.col.type.changes
      // from false to true. For details, see the JIRA HIVE-12320 and HIVE-17764.
      .set("spark.hadoop.hive.metastore.disallow.incompatible.col.type.changes", "false")
      // Disable ConvertToLocalRelation for better test coverage. Test cases built on
      // LocalRelation will exercise the optimization rules better by disabling it as
      // this rule may potentially block testing of other optimization rules such as
      // ConstantPropagation etc.
      .set(SQLConf.OPTIMIZER_EXCLUDED_RULES.key, ConvertToLocalRelation.ruleName)

    conf.set(
      StaticSQLConf.WAREHOUSE_PATH,
      conf.get(StaticSQLConf.WAREHOUSE_PATH) + "/" + getClass.getCanonicalName)
  }

  protected def sparkConf: SparkConf = {
    defaultSparkConf
      .set("spark.plugins", "org.apache.gluten.GlutenPlugin")
      .set("spark.default.parallelism", "1")
      .set("spark.memory.offHeap.enabled", "true")
      .set("spark.memory.offHeap.size", "1024MB")
      .set(GlutenConfig.NATIVE_WRITER_ENABLED.key, "true")
  }

  test("test hive static partition write table") {
    withTable("t") {
      spark.sql(
        "CREATE TABLE t (c int, d long, e long)" +
          " STORED AS PARQUET partitioned by (c, d)")
      withSQLConf("spark.sql.hive.convertMetastoreParquet" -> "true") {
        checkNativeWrite(
          "INSERT OVERWRITE TABLE t partition(c=1, d=2)" +
            " SELECT 3 as e")
      }
      checkAnswer(spark.table("t"), Row(3, 1, 2))
      checkAnswer(spark.sql("SHOW PARTITIONS t"), Seq(Row("c=1/d=2")))
    }
  }

  test("test hive dynamic and static partition write table") {
    withTable("t") {
      spark.sql(
        "CREATE TABLE t (c int, d long, e long)" +
          " STORED AS PARQUET partitioned by (c, d)")
      withSQLConf("spark.sql.hive.convertMetastoreParquet" -> "true") {
        checkNativeWrite(
          "INSERT OVERWRITE TABLE t partition(c=1, d)" +
            " SELECT 3 as e, 2 as d")
      }
      checkAnswer(spark.table("t"), Row(3, 1, 2))
      checkAnswer(spark.sql("SHOW PARTITIONS t"), Seq(Row("c=1/d=2")))
    }
  }

  test("test hive dynamic and static partition write table, multiple partitions") {
    withTable("t") {
      spark.sql(
        "CREATE TABLE t (c int, d long, e long)" +
          " STORED AS PARQUET partitioned by (c, d)")
      withSQLConf("spark.sql.hive.convertMetastoreParquet" -> "true") {
        checkNativeWrite(
          "INSERT OVERWRITE TABLE t partition(c=1, d)" +
            " SELECT 3 as e, 2 as d" +
            " UNION ALL" +
            " SELECT 4 as e, 5 as d")
      }
      checkAnswer(spark.table("t"), Seq(Row(3, 1, 2), Row(4, 1, 5)))
      checkAnswer(spark.sql("SHOW PARTITIONS t"), Seq(Row("c=1/d=2"), Row("c=1/d=5")))
    }
  }

  test("test hive dynamic and static partition write table, multiple keys, multiple partitions") {
    withTable("t") {
      spark.sql(
        "CREATE TABLE t (c int, d long, e long, f int)" +
          " STORED AS PARQUET partitioned by (c, d, f)")
      withSQLConf("spark.sql.hive.convertMetastoreParquet" -> "true") {
        checkNativeWrite(
          "INSERT OVERWRITE TABLE t partition(c=1, d, f)" +
            " SELECT 3 as e, 2 as d, 7 as f" + // Partition 0.
            " UNION ALL" +
            " SELECT 4 as e, 5 as d, 9 as f" + // Partition 1.
            " UNION ALL" +
            " SELECT 6 as e, 2 as d, 7 as f" + // Partition 0.
            " UNION ALL" +
            " SELECT 8 as e, 5 as d, 7 as f" // Partition 2.
        )
      }
      checkAnswer(
        spark.table("t"),
        Seq(Row(3, 1, 2, 7), Row(4, 1, 5, 9), Row(6, 1, 2, 7), Row(8, 1, 5, 7)))
      checkAnswer(
        spark.sql("SHOW PARTITIONS t"),
        Seq(Row("c=1/d=2/f=7"), Row("c=1/d=5/f=7"), Row("c=1/d=5/f=9")))
    }
  }

  test(
    "test hive dynamic and static partition write table multiple keys, multiple partitions, " +
      "single batch to write") {
    withTable("t") {
      spark.sql(
        "CREATE TABLE t (c int, d long, e long, f int)" +
          " STORED AS PARQUET partitioned by (c, d, f)")
      withSQLConf("spark.sql.hive.convertMetastoreParquet" -> "true") {
        // Use of VALUES will result in a single input batch for DynamicPartitionDataSingleWriter
        // to test the sanity of #splitBlockByPartitionAndBucket.
        checkNativeWrite(
          "INSERT OVERWRITE TABLE t PARTITION (c=1, d, f) VALUES" +
            " (3, 2, 7)," +
            " (4, 5, 9)," +
            " (6, 2, 7)," +
            " (8, 5, 7)")
      }
      checkAnswer(
        spark.table("t"),
        Seq(Row(3, 1, 2, 7), Row(4, 1, 5, 9), Row(6, 1, 2, 7), Row(8, 1, 5, 7)))
      checkAnswer(
        spark.sql("SHOW PARTITIONS t"),
        Seq(Row("c=1/d=2/f=7"), Row("c=1/d=5/f=7"), Row("c=1/d=5/f=9")))
    }
  }

  test("test hive write table") {
    withTable("t") {
      spark.sql("CREATE TABLE t (c int) STORED AS PARQUET")
      withSQLConf("spark.sql.hive.convertMetastoreParquet" -> "false") {
        checkNativeWrite("INSERT OVERWRITE TABLE t SELECT 1 as c")
      }
      checkAnswer(spark.table("t"), Row(1))
    }
  }

  test("test hive write dir") {
    withTempPath {
      f =>
        // compatible with Spark3.3 and later
        withSQLConf("spark.sql.hive.convertMetastoreInsertDir" -> "false") {
          if (isSparkVersionGE("3.4")) {
            checkNativeWrite(
              s"""
                 |INSERT OVERWRITE DIRECTORY '${f.getCanonicalPath}' STORED AS PARQUET SELECT 1 as c
                 |""".stripMargin,
              expectNative = false
            )
          } else {
            checkNativeWrite(
              s"""
                 |INSERT OVERWRITE DIRECTORY '${f.getCanonicalPath}' STORED AS PARQUET SELECT 1 as c
                 |""".stripMargin)
          }
          checkAnswer(spark.read.parquet(f.getCanonicalPath), Row(1))
        }
    }
  }

  test("select plain hive table") {
    withTable("t") {
      sql("CREATE TABLE t AS SELECT 1 as c")
      checkAnswer(sql("SELECT * FROM t"), Row(1))
    }
  }

  test("native writer support CreateHiveTableAsSelectCommand") {
    withTable("t") {
      withSQLConf("spark.sql.hive.convertMetastoreParquet" -> "false") {
        checkNativeWrite("CREATE TABLE t STORED AS PARQUET AS SELECT 1 as c")
      }
      checkAnswer(spark.table("t"), Row(1))
    }
  }

  test("native writer should respect table properties") {
    Seq(true, false).foreach {
      enableNativeWrite =>
        withSQLConf(GlutenConfig.NATIVE_WRITER_ENABLED.key -> enableNativeWrite.toString) {
          withTable("t") {
            withSQLConf(
              "spark.sql.hive.convertMetastoreParquet" -> "false",
              "spark.sql.parquet.compression.codec" -> "gzip") {
              checkNativeWrite(
                "CREATE TABLE t STORED AS PARQUET TBLPROPERTIES ('parquet.compression'='zstd') " +
                  "AS SELECT 1 as c",
                expectNative = enableNativeWrite)
              val tableDir = new Path(s"${conf.getConf(StaticSQLConf.WAREHOUSE_PATH)}/t")
              val configuration = spark.sessionState.newHadoopConf()
              val files = tableDir
                .getFileSystem(configuration)
                .listStatus(tableDir)
                .filterNot(_.getPath.getName.startsWith("\\."))
              assert(files.nonEmpty)
              val in = HadoopInputFile.fromStatus(files.head, spark.sessionState.newHadoopConf())
              Utils.tryWithResource(ParquetFileReader.open(in)) {
                reader =>
                  val column = reader.getFooter.getBlocks.get(0).getColumns.get(0)
                  // native writer and vanilla spark hive writer should be consistent
                  "zstd".equalsIgnoreCase(column.getCodec.toString)
              }
            }
          }
        }
    }
  }

  test("Native writer support compatible hive bucket write with dynamic partition") {
    if (isSparkVersionGE("3.4")) {
      Seq("true", "false").foreach {
        enableConvertMetastore =>
          withSQLConf("spark.sql.hive.convertMetastoreParquet" -> enableConvertMetastore) {
            val source = "hive_source_table"
            val target = "hive_bucketed_table"
            withTable(source, target) {
              sql(s"""
                     |CREATE TABLE IF NOT EXISTS $target (i int, j string)
                     |PARTITIONED BY(k string)
                     |CLUSTERED BY (i, j) SORTED BY (i) INTO 8 BUCKETS
                     |STORED AS PARQUET
               """.stripMargin)

              val df =
                (0 until 50).map(i => (i % 13, i.toString, i % 5)).toDF("i", "j", "k")
              df.write.mode(SaveMode.Overwrite).saveAsTable(source)

              withSQLConf("hive.exec.dynamic.partition.mode" -> "nonstrict") {
                checkNativeWrite(s"INSERT INTO $target SELECT * FROM $source")
              }

              for (k <- 0 until 5) {
                testBucketing(
                  new File(tableDir(target), s"k=$k"),
                  "parquet",
                  8,
                  Seq("i", "j"),
                  Seq("i"),
                  df,
                  bucketIdExpression,
                  getBucketIdFromFileName)
              }
            }
          }
      }
    }
  }

  test("bucket writer with non-dynamic partition") {
    if (isSparkVersionGE("3.4")) {
      Seq("true", "false").foreach {
        enableConvertMetastore =>
          withSQLConf("spark.sql.hive.convertMetastoreParquet" -> enableConvertMetastore) {
            val source = "hive_source_table"
            val target = "hive_bucketed_table"
            withTable(source, target) {
              sql(s"""
                     |CREATE TABLE IF NOT EXISTS $target (i int, j string)
                     |PARTITIONED BY(k string)
                     |CLUSTERED BY (i, j) SORTED BY (i) INTO 8 BUCKETS
                     |STORED AS PARQUET
               """.stripMargin)

              val df =
                (0 until 50).map(i => (i % 13, i.toString, i % 5)).toDF("i", "j", "k")
              df.write.mode(SaveMode.Overwrite).saveAsTable(source)

              // hive relation convert always use dynamic, so it will offload to native.
              checkNativeWrite(s"INSERT INTO $target PARTITION(k='0') SELECT i, j FROM $source")
              val files = tableDir(target)
                .listFiles()
                .filterNot(f => f.getName.startsWith(".") || f.getName.startsWith("_"))
              assert(files.length == 1 && files.head.getName.contains("k=0"))
              checkAnswer(spark.table(target).select("i", "j"), df.select("i", "j"))
            }
          }
      }
    }
  }

  test("bucket writer with non-partition table") {
    if (isSparkVersionGE("3.4")) {
      Seq("true", "false").foreach {
        enableConvertMetastore =>
          withSQLConf("spark.sql.hive.convertMetastoreParquet" -> enableConvertMetastore) {
            val source = "hive_source_table"
            val target = "hive_bucketed_table"
            withTable(source, target) {
              sql(s"""
                     |CREATE TABLE IF NOT EXISTS $target (i int, j string)
                     |CLUSTERED BY (i, j) SORTED BY (i) INTO 8 BUCKETS
                     |STORED AS PARQUET
               """.stripMargin)

              val df =
                (0 until 50).map(i => (i % 13, i.toString)).toDF("i", "j")
              df.write.mode(SaveMode.Overwrite).saveAsTable(source)

              checkNativeWrite(s"INSERT INTO $target SELECT i, j FROM $source")

              checkAnswer(spark.table(target), df)
            }
          }
      }
    }
  }

  testWithMaxSparkVersion(
    "Native writer should keep the same compression codec if `hive.exec.compress.output` is true",
    "3.3") {
    Seq(false, true).foreach {
      enableNativeWrite =>
        withSQLConf(GlutenConfig.NATIVE_WRITER_ENABLED.key -> enableNativeWrite.toString) {
          withTable("t") {
            withSQLConf(
              "spark.sql.hive.convertMetastoreParquet" -> "false",
              "spark.sql.parquet.compression.codec" -> "gzip") {
              spark.sql("SET hive.exec.compress.output=true")
              spark.sql("SET parquet.compression=gzip")
              spark.sql(
                "SET mapred.output.compression.codec=org.apache.hadoop.io.compress.SnappyCodec")
              checkNativeWrite(
                "CREATE TABLE t STORED AS PARQUET TBLPROPERTIES ('parquet.compression'='zstd') " +
                  "AS SELECT 1 as c",
                expectNative = enableNativeWrite)
              val tableDir = new Path(s"${conf.getConf(StaticSQLConf.WAREHOUSE_PATH)}/t")
              val configuration = spark.sessionState.newHadoopConf()
              val files = tableDir
                .getFileSystem(configuration)
                .listStatus(tableDir)
                .filterNot(_.getPath.getName.startsWith("\\."))
              assert(files.nonEmpty)
              val in = HadoopInputFile.fromStatus(files.head, spark.sessionState.newHadoopConf())
              Utils.tryWithResource(ParquetFileReader.open(in)) {
                reader =>
                  val compression =
                    reader.getFooter.getBlocks.get(0).getColumns.get(0).getCodec.toString
                  // native writer and vanilla spark hive writer should be consistent
                  assert("zstd".equalsIgnoreCase(compression))
              }
            }
          }
        }
    }
  }
}
