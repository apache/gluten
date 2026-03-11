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
package org.apache.spark.sql.execution.datasources.orc

import org.apache.spark.sql.{GlutenSQLTestsBaseTrait, GlutenTestConstants, Row}
import org.apache.spark.sql.execution.FileSourceScanExec
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.{DayTimeIntervalType, IntegerType, StructField, StructType, YearMonthIntervalType}

import java.sql.Date
import java.time.{Duration, Period}

class GlutenOrcSourceSuite extends OrcSourceSuite with GlutenSQLTestsBaseTrait {
  import testImplicits._

  override def withAllNativeOrcReaders(code: => Unit): Unit = {
    // test the row-based reader
    withSQLConf(SQLConf.ORC_VECTORIZED_READER_ENABLED.key -> "false")(code)
  }

  testGluten("SPARK-31238: compatibility with Spark 2.4 in reading dates") {
    Seq(false).foreach {
      vectorized =>
        withSQLConf(SQLConf.ORC_VECTORIZED_READER_ENABLED.key -> vectorized.toString) {
          checkAnswer(
            readResourceOrcFile("test-data/before_1582_date_v2_4.snappy.orc"),
            Row(java.sql.Date.valueOf("1200-01-01")))
        }
    }
  }

  testGluten("SPARK-31238, SPARK-31423: rebasing dates in write") {
    withTempPath {
      dir =>
        val path = dir.getAbsolutePath
        Seq("1001-01-01", "1582-10-10")
          .toDF("dateS")
          .select($"dateS".cast("date").as("date"))
          .write
          .orc(path)

        Seq(false).foreach {
          vectorized =>
            withSQLConf(SQLConf.ORC_VECTORIZED_READER_ENABLED.key -> vectorized.toString) {
              checkAnswer(
                spark.read.orc(path),
                Seq(Row(Date.valueOf("1001-01-01")), Row(Date.valueOf("1582-10-15"))))
            }
        }
    }
  }

  testGluten("SPARK-31284: compatibility with Spark 2.4 in reading timestamps") {
    Seq(false).foreach {
      vectorized =>
        withSQLConf(SQLConf.ORC_VECTORIZED_READER_ENABLED.key -> vectorized.toString) {
          checkAnswer(
            readResourceOrcFile("test-data/before_1582_ts_v2_4.snappy.orc"),
            Row(java.sql.Timestamp.valueOf("1001-01-01 01:02:03.123456")))
        }
    }
  }

  testGluten("SPARK-31284, SPARK-31423: rebasing timestamps in write") {
    withTempPath {
      dir =>
        val path = dir.getAbsolutePath
        Seq("1001-01-01 01:02:03.123456", "1582-10-10 11:12:13.654321")
          .toDF("tsS")
          .select($"tsS".cast("timestamp").as("ts"))
          .write
          .orc(path)

        Seq(false).foreach {
          vectorized =>
            withSQLConf(SQLConf.ORC_VECTORIZED_READER_ENABLED.key -> vectorized.toString) {
              checkAnswer(
                spark.read.orc(path),
                Seq(
                  Row(java.sql.Timestamp.valueOf("1001-01-01 01:02:03.123456")),
                  Row(java.sql.Timestamp.valueOf("1582-10-15 11:12:13.654321"))))
            }
        }
    }
  }

  testGluten("SPARK-34862: Support ORC vectorized reader for nested column") {
    withTempPath {
      dir =>
        val path = dir.getCanonicalPath
        val df = spark
          .range(10)
          .map {
            x =>
              val stringColumn = s"$x" * 10
              val structColumn = (x, s"$x" * 100)
              val arrayColumn = (0 until 5).map(i => (x + i, s"$x" * 5))
              val mapColumn = Map(
                s"$x" -> (x * 0.1, (x, s"$x" * 100)),
                (s"$x" * 2) -> (x * 0.2, (x, s"$x" * 200)),
                (s"$x" * 3) -> (x * 0.3, (x, s"$x" * 300)))
              (x, stringColumn, structColumn, arrayColumn, mapColumn)
          }
          .toDF("int_col", "string_col", "struct_col", "array_col", "map_col")
        df.write.format("orc").save(path)

        // Rewrite because Gluten does not support Spark's vectorized reading.
        withSQLConf(SQLConf.ORC_VECTORIZED_READER_NESTED_COLUMN_ENABLED.key -> "false") {
          val readDf = spark.read.orc(path)
          val vectorizationEnabled = readDf.queryExecution.executedPlan.find {
            case scan: FileSourceScanExec => scan.supportsColumnar
            case _ => false
          }.isDefined
          assert(!vectorizationEnabled)
          checkAnswer(readDf, df)
        }
    }
  }
  withAllNativeOrcReaders {
    Seq(false).foreach {
      vecReaderNestedColEnabled =>
        val vecReaderEnabled = SQLConf.get.orcVectorizedReaderEnabled
        test(
          GlutenTestConstants.GLUTEN_TEST +
            "SPARK-36931: Support reading and writing ANSI intervals (" +
            s"${SQLConf.ORC_VECTORIZED_READER_ENABLED.key}=$vecReaderEnabled, " +
            s"${SQLConf.ORC_VECTORIZED_READER_NESTED_COLUMN_ENABLED.key}" +
            s"=$vecReaderNestedColEnabled)") {

          withSQLConf(
            SQLConf.ORC_VECTORIZED_READER_ENABLED.key ->
              vecReaderEnabled.toString,
            SQLConf.ORC_VECTORIZED_READER_NESTED_COLUMN_ENABLED.key ->
              vecReaderNestedColEnabled.toString
          ) {
            Seq(
              YearMonthIntervalType() -> ((i: Int) => Period.of(i, i, 0)),
              DayTimeIntervalType() -> ((i: Int) => Duration.ofDays(i).plusSeconds(i))
            ).foreach {
              case (it, f) =>
                val data = (1 to 10).map(i => Row(i, f(i)))
                val schema = StructType(
                  Array(StructField("d", IntegerType, false), StructField("i", it, false)))
                withTempPath {
                  file =>
                    val df = spark.createDataFrame(sparkContext.parallelize(data), schema)
                    df.write.orc(file.getCanonicalPath)
                    val df2 = spark.read.orc(file.getCanonicalPath)
                    checkAnswer(df2, df.collect().toSeq)
                }
            }

            // Tests for ANSI intervals in complex types.
            withTempPath {
              file =>
                val df = spark.sql("""SELECT
                                     |  named_struct('interval', interval '1-2' year to month) a,
                                     |  array(interval '1 2:3' day to minute) b,
                                     |  map('key', interval '10' year) c,
                                     |  map(interval '20' second, 'value') d""".stripMargin)
                df.write.orc(file.getCanonicalPath)
                val df2 = spark.read.orc(file.getCanonicalPath)
                checkAnswer(df2, df.collect().toSeq)
            }
          }
        }
    }
  }
}
