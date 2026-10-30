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
package org.apache.gluten.functions

import org.apache.gluten.execution.{FileSourceScanExecTransformer, ProjectExecTransformer, RowToVeloxColumnarExec, VeloxColumnarToRowExec}

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.execution.{ColumnarShuffleExchangeExec, FileSourceScanExec}

/** Variant type and functions, offloaded to Velox's VARIANT. */
class VariantFunctionsValidateSuite extends FunctionsValidateSuite {

  disableFallbackCheck
  import testImplicits._

  private val minSparkVersion = "4.1"

  private val jsonStrings = Seq(
    """{"a": 1, "b": "x", "c": [1, 2, {"d": true}], "e": {"f": 1.5}}""",
    """{"a": -2147483649, "b": null, "c": [], "e": {"f": 1e-3}}""",
    """[1, "two", 3.25, null, {"k": "v"}]""",
    """"just a string"""",
    """12345678901234567890""",
    """3.14159265358979323846""",
    """true""",
    """null""",
    null
  )

  // Velox reads unshredded Variant only. Spark 4.1 shreds on write by default.
  private def writeUnshredded(df: DataFrame, path: String): Unit = {
    withSQLConf("spark.sql.variant.writeShredding.enabled" -> "false") {
      df.write.parquet(path)
    }
  }

  private def withJsonTable(f: => Unit): Unit = {
    withTempPath {
      path =>
        jsonStrings.zipWithIndex
          .toDF("s", "id")
          .write
          .parquet(path.getCanonicalPath)
        spark.read.parquet(path.getCanonicalPath).createOrReplaceTempView("json_tbl")
        f
    }
  }

  testWithMinSparkVersion("parse_json and to_json", minSparkVersion) {
    withJsonTable {
      runQueryAndCompare("SELECT id, to_json(parse_json(s)) FROM json_tbl") {
        checkGlutenPlan[ProjectExecTransformer]
      }
      runQueryAndCompare(
        "SELECT id, to_json(try_parse_json(s)), try_parse_json('{\"a\": ') IS NULL " +
          "FROM json_tbl") {
        checkGlutenPlan[ProjectExecTransformer]
      }
    }
  }

  testWithMinSparkVersion("variant output goes through columnar to row", minSparkVersion) {
    withJsonTable {
      runQueryAndCompare("SELECT id, parse_json(s) AS v FROM json_tbl") {
        df =>
          checkGlutenPlan[ProjectExecTransformer](df)
          checkGlutenPlan[VeloxColumnarToRowExec](df)
      }
      runQueryAndCompare(
        "SELECT id, named_struct('v', parse_json(s), 'i', id) AS st, " +
          "array(parse_json(s), parse_json('1')) AS arr, " +
          "map('k', parse_json(s)) AS m FROM json_tbl") {
        checkGlutenPlan[ProjectExecTransformer]
      }
    }
  }

  testWithMinSparkVersion("variant_get and try_variant_get", minSparkVersion) {
    withJsonTable {
      runQueryAndCompare(
        """SELECT id,
          |  variant_get(parse_json(s), '$.a', 'bigint'),
          |  variant_get(parse_json(s), '$.b', 'string'),
          |  variant_get(parse_json(s), '$.c[2].d', 'boolean'),
          |  variant_get(parse_json(s), '$.e.f', 'double'),
          |  variant_get(parse_json(s), '$[1]', 'string'),
          |  to_json(variant_get(parse_json(s), '$.c')),
          |  try_variant_get(parse_json(s), '$.a', 'int'),
          |  try_variant_get(parse_json(s), '$', 'decimal(38, 2)')
          |FROM json_tbl""".stripMargin) {
        checkGlutenPlan[ProjectExecTransformer]
      }
    }
  }

  testWithMinSparkVersion("is_variant_null and schema_of_variant", minSparkVersion) {
    withJsonTable {
      runQueryAndCompare(
        "SELECT id, is_variant_null(parse_json(s)), schema_of_variant(parse_json(s)) " +
          "FROM json_tbl") {
        checkGlutenPlan[ProjectExecTransformer]
      }
    }
  }

  testWithMinSparkVersion("cast from variant", minSparkVersion) {
    withJsonTable {
      runQueryAndCompare(
        "SELECT id, try_cast(parse_json(s) AS int), try_cast(parse_json(s) AS string), " +
          "cast(variant_get(parse_json(s), '$.a') AS bigint) FROM json_tbl") {
        checkGlutenPlan[ProjectExecTransformer]
      }
    }
  }

  testWithMinSparkVersion("to_variant_object", minSparkVersion) {
    withJsonTable {
      runQueryAndCompare(
        "SELECT id, to_json(to_variant_object(named_struct('id', id, 's', s))), " +
          "to_json(to_variant_object(map('k', id))) FROM json_tbl") {
        checkGlutenPlan[ProjectExecTransformer]
      }
    }
  }

  testWithMinSparkVersion("variant through columnar shuffle", minSparkVersion) {
    withJsonTable {
      runQueryAndCompare(
        "SELECT id, v FROM (SELECT id, parse_json(s) AS v FROM json_tbl) DISTRIBUTE BY id") {
        df => checkGlutenPlan[ColumnarShuffleExchangeExec](df)
      }
    }
  }

  testWithMinSparkVersion("variant through row to columnar", minSparkVersion) {
    withTempPath {
      path =>
        writeUnshredded(
          spark.sql(
            "SELECT id, parse_json(s) AS v FROM VALUES (1, '{\"a\": 1}'), (2, null) AS t(id, s)"),
          path.getCanonicalPath)
        // A row-based vanilla scan feeds the native project through RowToVeloxColumnarExec.
        withSQLConf(
          "spark.gluten.sql.columnar.filescan" -> "false",
          "spark.sql.parquet.enableVectorizedReader" -> "false") {
          spark.read.parquet(path.getCanonicalPath).createOrReplaceTempView("variant_rows")
          runQueryAndCompare("SELECT id, variant_get(v, '$.a', 'int'), v FROM variant_rows") {
            df => checkGlutenPlan[RowToVeloxColumnarExec](df)
          }
        }
    }
  }

  testWithMinSparkVersion("read unshredded variant from parquet", minSparkVersion) {
    withJsonTable {
      withTempPath {
        path =>
          // The native writer does not support Variant, so this write falls back.
          writeUnshredded(
            spark.sql("SELECT id, parse_json(s) AS v FROM json_tbl"),
            path.getCanonicalPath)
          spark.read.parquet(path.getCanonicalPath).createOrReplaceTempView("variant_tbl")
          runQueryAndCompare("SELECT id, v, to_json(v) FROM variant_tbl") {
            checkGlutenPlan[FileSourceScanExecTransformer]
          }
          runQueryAndCompare(
            "SELECT id, variant_get(v, '$.a', 'bigint') FROM variant_tbl " +
              "WHERE is_variant_null(v) OR id < 3") {
            checkGlutenPlan[FileSourceScanExecTransformer]
          }
      }
    }
  }

  testWithMinSparkVersion("shredded variant in parquet falls back", minSparkVersion) {
    withTempPath {
      path =>
        withSQLConf(
          "spark.sql.variant.writeShredding.enabled" -> "true",
          "spark.sql.variant.forceShreddingSchemaForTest" -> "a int") {
          spark
            .sql("SELECT id, parse_json(concat('{\"a\": ', id, '}')) AS v FROM range(10)")
            .write
            .parquet(path.getCanonicalPath)
        }
        spark.read.parquet(path.getCanonicalPath).createOrReplaceTempView("shredded_tbl")
        runQueryAndCompare("SELECT id, to_json(v) FROM shredded_tbl", noFallBack = false) {
          df =>
            val plan = getExecutedPlan(df)
            assert(plan.exists(_.isInstanceOf[FileSourceScanExec]))
            assert(!plan.exists(_.isInstanceOf[FileSourceScanExecTransformer]))
        }
    }
  }
}
