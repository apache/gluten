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

import org.apache.spark.SparkConf
import org.apache.spark.internal.config.IGNORE_CORRUPT_FILES
import org.apache.spark.internal.config.UI.UI_ENABLED
import org.apache.spark.sql.{DataFrame, GlutenQueryTest, Row, SparkSession}
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.classic.ClassicTypes._
import org.apache.spark.sql.hive.{HiveTableScanExecTransformer, HiveUtils}
import org.apache.spark.sql.internal.{SQLConf, StaticSQLConf}
import org.apache.spark.sql.test.SQLTestUtils
import org.apache.spark.util.SparkVersionUtil

import org.apache.hadoop.fs.Path

class VeloxHiveScanSuite extends GlutenQueryTest with SQLTestUtils {
  private var _spark: SparkSession = _

  override protected def beforeAll(): Unit = {
    super.beforeAll()

    if (_spark == null) {
      _spark = SparkSession.builder().config(sparkConf).enableHiveSupport().getOrCreate()
    }

    _spark.sparkContext.setLogLevel("warn")
  }

  override protected def spark: ClassicSparkSession = _spark.asInstanceOf[ClassicSparkSession]

  protected def sparkConf: SparkConf = {
    val conf = new SparkConf()
      .set("spark.master", "local[1]")
      .set("spark.sql.test", "")
      .set(UI_ENABLED, false)
      .set(SQLConf.ADAPTIVE_EXECUTION_ENABLED.key, "false")
      .set("spark.plugins", "org.apache.gluten.GlutenPlugin")
      .set("spark.default.parallelism", "1")
      .set("spark.memory.offHeap.enabled", "true")
      .set("spark.memory.offHeap.size", "1024MB")
    conf.set(
      StaticSQLConf.WAREHOUSE_PATH,
      conf.get(StaticSQLConf.WAREHOUSE_PATH) + "/" + getClass.getCanonicalName)
  }

  // Spark reads a Hive table with a HadoopRDD, which skips corrupt files by
  // spark.sql.files.ignoreCorruptFiles since Spark 4.0 (SPARK-45316), and by the core conf
  // spark.files.ignoreCorruptFiles before.
  private def withIgnoreCorruptFiles(ignore: Boolean)(f: => Unit): Unit = {
    if (SparkVersionUtil.gteSpark40) {
      withSQLConf(SQLConf.IGNORE_CORRUPT_FILES.key -> ignore.toString)(f)
    } else {
      // Spark reads the core conf when it builds the HadoopRDD
      val conf = spark.sparkContext.conf
      val previous = conf.get(IGNORE_CORRUPT_FILES)
      conf.set(IGNORE_CORRUPT_FILES, ignore)
      try f
      finally conf.set(IGNORE_CORRUPT_FILES, previous)
    }
  }

  test("fall back the Hive table scan when Spark would skip corrupt files") {
    withTable("t") {
      spark.sql("CREATE TABLE t (a BIGINT) STORED AS ORC")
      spark.sql("INSERT INTO t VALUES (1)")
      val location =
        new Path(spark.sessionState.catalog.getTableMetadata(TableIdentifier("t")).location)
      val fs = location.getFileSystem(spark.sessionState.newHadoopConf())
      val out = fs.create(new Path(location, "corrupt"), true)
      try out.write("not an orc file".getBytes)
      finally out.close()

      def isOffloaded(df: DataFrame): Boolean =
        df.queryExecution.executedPlan.collect {
          case s: HiveTableScanExecTransformer => s
        }.nonEmpty

      // The BI split strategy doesn't read the footers to split the files, so the corrupt file
      // fails when it is read, which is where Spark's HadoopRDD skips it.
      withSQLConf(
        HiveUtils.CONVERT_METASTORE_ORC.key -> "false",
        "hive.exec.orc.split.strategy" -> "BI") {
        Seq(true, false).foreach {
          ignore =>
            withIgnoreCorruptFiles(ignore) {
              val df = spark.table("t")
              if (ignore) {
                checkAnswer(df, Row(1))
              } else {
                intercept[Exception](df.collect())
              }
              // The native scan fails on a corrupt file, so it must fall back when Spark would
              // skip the file, and only then.
              assert(isOffloaded(df) == !ignore)
            }
        }
      }
    }
  }
}
