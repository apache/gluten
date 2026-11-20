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

import org.apache.gluten.config.GlutenConfig
import org.apache.gluten.sql.shims.SparkShimLoader

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.catalyst.expressions.{Encode, Expression}
import org.apache.spark.sql.catalyst.expressions.objects.StaticInvoke
import org.apache.spark.sql.execution.ProjectExec

class VeloxEncodeSuite extends VeloxWholeStageTransformerSuite {
  override protected val resourcePath: String = "/tpch-data-parquet"
  override protected val fileFormat: String = "parquet"

  private val payload =
    "CASE id WHEN 0 THEN CAST(NULL AS STRING) WHEN 1 THEN '' WHEN 2 THEN 'ASCII' " +
      "WHEN 3 THEN '\u007f\u0080\u00ff\u0100' ELSE '\u20ac\u4e2d\uD83D\uDE00' END"

  private def withEncodeValues(f: => Unit): Unit = {
    withSQLConf(
      GlutenConfig.SPARK_LEGACY_CODING_ERROR_ACTION -> "true",
      "spark.sql.legacy.javaCharsets" -> "false",
      "spark.sql.adaptive.enabled" -> "false") {
      withTempView("encode_values") {
        withTempPath {
          path =>
            spark.range(0, 5).write.parquet(path.getAbsolutePath)
            spark.read.parquet(path.getAbsolutePath).createOrReplaceTempView("encode_values")
            f
        }
      }
    }
  }

  private def containsEncode(expr: Expression): Boolean = expr.exists {
    case _: Encode => true
    case i: StaticInvoke =>
      i.functionName == "encode" &&
      i.objectName.stripSuffix("$") == "org.apache.spark.sql.catalyst.expressions.Encode"
    case _ => false
  }

  private def assertNative(df: DataFrame): Unit = {
    assert(
      collect(df.queryExecution.executedPlan) {
        case p: ProjectExecTransformer if p.projectList.exists(containsEncode) => p
      }.nonEmpty,
      df.queryExecution.executedPlan.toString
    )
  }

  private def assertFallback(df: DataFrame): Unit = {
    assert(
      collect(df.queryExecution.executedPlan) {
        case p: ProjectExec if p.projectList.exists(containsEncode) => p
      }.nonEmpty,
      df.queryExecution.executedPlan.toString
    )
  }

  test(
    "encode matches Spark bytes for all canonical charsets, boundaries, nulls and empty strings") {
    withEncodeValues {
      for (
        charset <- Seq(
          "US-ASCII",
          "ISO-8859-1",
          "uTf-8",
          "UTF-16",
          "UTF-16BE",
          "UTF-16LE",
          "UTF-32")
      ) {
        runQueryAndCompare(
          s"SELECT encode($payload, '$charset') FROM encode_values")(assertNative)
      }
    }
  }

  test("encode canonicalizes legacy aliases for non-ASCII input") {
    withEncodeValues {
      withSQLConf("spark.sql.legacy.javaCharsets" -> "true") {
        for (charset <- Seq("UTF8", "latin1", "Unicode")) {
          runQueryAndCompare(
            s"SELECT encode($payload, '$charset') FROM encode_values")(assertNative)
        }
      }
    }
  }

  test("encode matches Spark replacement for malformed UTF-8") {
    withEncodeValues {
      val malformed =
        "CAST(unhex(CASE id WHEN 0 THEN 'C080' WHEN 1 THEN 'EDA080' " +
          "WHEN 2 THEN 'E282' WHEN 3 THEN 'F4908080' ELSE 'FF41' END) AS STRING)"
      for (charset <- Seq("UTF-8", "US-ASCII", "ISO-8859-1", "UTF-16", "UTF-32")) {
        runQueryAndCompare(
          s"SELECT encode($malformed, '$charset') FROM encode_values")(assertNative)
      }
    }
  }

  test("encode falls back when disabled or the charset is dynamic") {
    withEncodeValues {
      withSQLConf(GlutenConfig.NATIVE_ENCODE_ENABLED.key -> "false") {
        runQueryAndCompare(
          s"SELECT encode($payload, 'UTF-8') FROM encode_values",
          noFallBack = false)(assertFallback)
      }
      runQueryAndCompare(
        s"SELECT encode($payload, CASE WHEN id = 0 THEN 'UTF-8' ELSE 'UTF-16' END) " +
          "FROM encode_values",
        noFallBack = false)(assertFallback)
    }
  }

  test("encode keeps strict coding errors on Spark") {
    assume(!SparkShimLoader.getSparkShims.legacyCodingErrorActionDefault)
    withEncodeValues {
      withSQLConf(GlutenConfig.SPARK_LEGACY_CODING_ERROR_ACTION -> "false") {
        runQueryAndCompare(
          "SELECT encode(CAST(id AS STRING), 'US-ASCII') FROM encode_values",
          noFallBack = false)(assertFallback)
        val df = spark.sql(s"SELECT encode($payload, 'US-ASCII') FROM encode_values")
        assertFallback(df)
        val error = intercept[Exception] {
          df.collect()
        }
        assert(
          Iterator
            .iterate[Throwable](error)(_.getCause)
            .takeWhile(_ != null)
            .exists(e => Option(e.getMessage).exists(_.contains("MALFORMED_CHARACTER_CODING"))))
      }
    }
  }

  test("encode keeps unsupported charsets and restricted aliases on Spark") {
    withEncodeValues {
      withSQLConf("spark.sql.legacy.javaCharsets" -> "true") {
        runQueryAndCompare(
          s"SELECT encode($payload, 'windows-1252') FROM encode_values",
          noFallBack = false)(assertFallback)
      }
      if (!SparkShimLoader.getSparkShims.legacyCodingErrorActionDefault) {
        val df = spark.sql(s"SELECT encode($payload, 'UTF8') FROM encode_values")
        assertFallback(df)
      }
    }
  }
}
