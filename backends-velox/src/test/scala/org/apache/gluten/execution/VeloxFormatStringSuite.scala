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

import org.apache.gluten.test.FallbackUtil

import org.apache.spark.sql.{DataFrame, Row}

class VeloxFormatStringSuite extends VeloxWholeStageTransformerSuite {

  override protected val resourcePath: String = "/tpch-data-parquet"
  override protected val fileFormat: String = "parquet"

  private def withFormatValues(f: => Unit): Unit = {
    withTempView("format_values") {
      withTempPath {
        path =>
          spark.range(0, 3).write.parquet(path.getAbsolutePath)
          spark.read.parquet(path.getAbsolutePath).createOrReplaceTempView("format_values")
          f
      }
    }
  }

  private def assertFallback(sql: String): Unit = {
    val dataFrame = spark.sql(sql)
    dataFrame.collect()
    assert(
      FallbackUtil.hasFallback(dataFrame.queryExecution.executedPlan),
      s"Expected format_string fallback: ${dataFrame.queryExecution.executedPlan}")
  }

  private def assertFallbackWithoutExecution(sql: String): Unit = {
    val executedPlan = spark.sql(sql).queryExecution.executedPlan
    assert(
      FallbackUtil.hasFallback(executedPlan),
      s"Expected format_string fallback: $executedPlan")
  }

  private def assertNative(dataFrame: DataFrame): Unit = {
    val executedPlan = dataFrame.queryExecution.executedPlan
    assert(
      !FallbackUtil.hasFallback(executedPlan),
      s"Expected native format_string execution: $executedPlan")
    assert(
      executedPlan.find(_.isInstanceOf[ProjectExecTransformer]).isDefined,
      s"Expected a native project for format_string: $executedPlan")
  }

  test("format_string and printf share native integer formatting") {
    withFormatValues {
      runQueryAndCompare("""
                           |SELECT
                           |  format_string('id=%04d', CAST(id AS INT)),
                           |  printf('id=%04d', CAST(id AS INT))
                           |FROM format_values
                           |""".stripMargin)(assertNative)
    }
  }

  test("format_string offloads verified integral conversions") {
    withFormatValues {
      runQueryAndCompare("""
                           |SELECT format_string(
                           |  '%d|%05d|%o|%x|%X',
                           |  CAST(id AS INT),
                           |  CAST(id AS INT),
                           |  CAST(id AS INT),
                           |  CAST(id AS INT),
                           |  CAST(id AS INT))
                           |FROM format_values
                           |""".stripMargin)(assertNative)
    }
  }

  test("format_string offloads strings, booleans, integrals, and nulls with percent-s") {
    withFormatValues {
      runQueryAndCompare("""
                           |SELECT format_string(
                           |  '%s|%s|%s|%s',
                           |  CAST(id AS STRING),
                           |  id % 2 = 0,
                           |  id,
                           |  CASE WHEN id = 1 THEN NULL ELSE CAST(id AS STRING) END)
                           |FROM format_values
                           |""".stripMargin)(assertNative)
    }
  }

  test("format_string offloads padded nullable BIGINT values") {
    withFormatValues {
      runQueryAndCompare("""
                           |SELECT format_string(
                           |  '%+05d|%05X|%-5d',
                           |  CASE WHEN id = 1 THEN CAST(NULL AS BIGINT) ELSE id END,
                           |  CASE WHEN id = 1 THEN CAST(NULL AS BIGINT) ELSE id END,
                           |  CASE WHEN id = 1 THEN CAST(NULL AS BIGINT) ELSE id END)
                           |FROM format_values
                           |""".stripMargin)(assertNative)
    }
  }

  test("format_string falls back outside the verified compatibility subset") {
    withFormatValues {
      Seq(
        "SELECT format_string(CASE WHEN id = 0 THEN '%d' ELSE '%x' END, id) " +
          "FROM format_values",
        "SELECT format_string('%b', id = 0) FROM format_values",
        "SELECT format_string('%c', CAST(id + 65 AS INT)) FROM format_values",
        "SELECT format_string('%g', CAST(id AS DOUBLE) + 0.25D) FROM format_values",
        "SELECT format_string('%.2f', CAST(id AS DOUBLE) + 0.25D) FROM format_values",
        "SELECT format_string('%.3e', CAST(id AS DOUBLE) + 0.25D) FROM format_values",
        "SELECT format_string('%.3E', CAST(id AS DOUBLE) + 0.25D) FROM format_values",
        "SELECT format_string('%1$s', CAST(id AS STRING)) FROM format_values",
        "SELECT format_string('%10s', CAST(id AS STRING)) FROM format_values",
        "SELECT format_string('%s', CAST(id AS DOUBLE)) FROM format_values",
        "SELECT format_string('%f', CAST(id AS DOUBLE) + 0.25D) FROM format_values"
      ).foreach(assertFallback)
    }
  }

  test("format_string handles null arguments and uppercase integral conversions") {
    withFormatValues {
      val dataFrame =
        runQueryAndCompare("""
                             |SELECT format_string(
                             |  '%d|%X',
                             |  CASE WHEN id = 1 THEN NULL ELSE CAST(id AS INT) END,
                             |  CASE WHEN id = 1 THEN NULL ELSE CAST(id AS INT) END)
                             |FROM format_values
                             |""".stripMargin)(assertNative)
      checkAnswer(dataFrame, Seq(Row("0|0"), Row("null|NULL"), Row("2|2")))
    }
  }

  test("format_string accepts the maximum native width") {
    withFormatValues {
      val dataFrame =
        runQueryAndCompare("""
                             |SELECT length(format_string('%1048576d', CAST(id AS INT)))
                             |FROM format_values
                             |WHERE id = 0
                             |""".stripMargin)(assertNative)
      checkAnswer(dataFrame, Row(1048576))
    }
  }

  test("format_string rejects malformed and unsafe literals before native execution") {
    withFormatValues {
      Seq(
        "SELECT format_string('%--5d', CAST(id AS INT)) FROM format_values",
        "SELECT format_string('%-d', CAST(id AS INT)) FROM format_values",
        "SELECT format_string('%0d', CAST(id AS INT)) FROM format_values",
        "SELECT format_string('%-05d', CAST(id AS INT)) FROM format_values",
        "SELECT format_string('%+ d', CAST(id AS INT)) FROM format_values",
        "SELECT format_string('%.d', CAST(id AS INT)) FROM format_values",
        "SELECT format_string('%1048577d', CAST(id AS INT)) FROM format_values",
        "SELECT format_string('%', CAST(id AS INT)) FROM format_values",
        "SELECT format_string('%d %d', CAST(id AS INT)) FROM format_values"
      ).foreach(assertFallbackWithoutExecution)
    }
  }

  test("format_string replaces malformed UTF-8 in string arguments") {
    withFormatValues {
      val dataFrame =
        runQueryAndCompare("""
                             |SELECT id, hex(format_string(
                             |  '%s',
                             |  CAST(unhex(
                             |    CASE id
                             |      WHEN 0 THEN 'FF'
                             |      WHEN 1 THEN 'E282'
                             |      ELSE 'E08080'
                             |    END) AS STRING)))
                             |FROM format_values
                             |""".stripMargin)(assertNative)
      checkAnswer(
        dataFrame,
        Seq(Row(0L, "EFBFBD"), Row(1L, "EFBFBD"), Row(2L, "EFBFBDEFBFBDEFBFBD")))
    }
  }

  test("format_string replaces malformed UTF-8 in a constant pattern") {
    withFormatValues {
      val dataFrame =
        runQueryAndCompare("""
                             |SELECT id, hex(format_string(
                             |  CAST(unhex('FF2573') AS STRING),
                             |  concat('v', id)))
                             |FROM format_values
                             |""".stripMargin)(assertNative)
      checkAnswer(
        dataFrame,
        Seq(Row(0L, "EFBFBD7630"), Row(1L, "EFBFBD7631"), Row(2L, "EFBFBD7632")))
    }
  }
}
