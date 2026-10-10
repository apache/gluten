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

import org.apache.gluten.execution.ProjectExecTransformer

import org.apache.spark.sql.execution.ProjectExec
import org.apache.spark.sql.internal.SQLConf

/**
 * Verifies the fallback behavior of `from_json` in the Velox backend.
 *
 * Velox's `from_json` has the following known limitations -- each one forces the expression to fall
 * back to the vanilla Spark [[ProjectExec]]:
 *
 *   1. `spark.sql.json.enablePartialResults = false` (or unset, which is the default on Spark <
 *      3.4) -- Velox only supports the partial-results parsing mode.
 *   2. Non-empty options map passed to `from_json`.
 *   3. `spark.sql.caseSensitive = true`.
 *   4. Duplicate column names in the target schema (either exact duplicates or names that differ
 *      only in case when case-insensitive mode is active).
 *   5. A `_corrupt_record` (or the configured equivalent) column present in the schema.
 *
 * When none of the above restrictions apply the expression is offloaded to Velox and the plan
 * should contain a [[ProjectExecTransformer]] instead.
 */
class FromJsonFallbackSuite extends FunctionsValidateSuite {

  disableFallbackCheck
  import testImplicits._

  // ---------------------------------------------------------------------------
  // Helpers
  // ---------------------------------------------------------------------------

  private def withJsonTable(rows: String*)(body: => Unit): Unit = {
    withTempPath {
      path =>
        rows.toDF("txt").write.parquet(path.getCanonicalPath)
        spark.read.parquet(path.getCanonicalPath).createOrReplaceTempView("json_tbl")
        body
    }
  }

  // ---------------------------------------------------------------------------
  // Limitation 1 -- spark.sql.json.enablePartialResults = false
  //
  // When this setting is false (the default before Spark 3.4) Velox cannot honour
  // the semantics, so the expression falls back to vanilla Spark.
  // ---------------------------------------------------------------------------

  test("from_json falls back when enablePartialResults is false") {
    withJsonTable("""{"id":1}""", """{"id":2}""") {
      withSQLConf("spark.sql.json.enablePartialResults" -> "false") {
        runQueryAndCompare("SELECT from_json(txt, 'id INT') FROM json_tbl") {
          checkSparkPlan[ProjectExec]
        }
      }
    }
  }

  // ---------------------------------------------------------------------------
  // Limitation 2 -- from_json called with a non-empty options map
  //
  // Any option (e.g. dateFormat, timestampFormat, allowSingleQuotes ...) triggers
  // a fallback because Velox does not implement the options plumbing.
  // ---------------------------------------------------------------------------

  test("from_json falls back when options map is non-empty (dateFormat)") {
    withJsonTable("""{"d":"2024-01-15"}""", """{"d":"2024-06-30"}""") {
      withSQLConf("spark.sql.json.enablePartialResults" -> "true") {
        runQueryAndCompare(
          "SELECT from_json(txt, 'id INT', map('dateFormat', 'yyyy-MM-dd')) FROM json_tbl") {
          checkSparkPlan[ProjectExec]
        }
      }
    }
  }

  test("from_json falls back when options map is non-empty (allowSingleQuotes)") {
    withJsonTable("""{'id':1}""", """{'id':2}""") {
      withSQLConf("spark.sql.json.enablePartialResults" -> "true") {
        runQueryAndCompare(
          "SELECT from_json(txt, 'id INT', map('allowSingleQuotes', 'true')) FROM json_tbl") {
          checkSparkPlan[ProjectExec]
        }
      }
    }
  }

  // ---------------------------------------------------------------------------
  // Limitation 3 -- spark.sql.caseSensitive = true
  //
  // When case-sensitive analysis is enabled the JSON field lookup semantics
  // change, which Velox does not support.
  // ---------------------------------------------------------------------------

  test("from_json falls back when caseSensitive is true") {
    withJsonTable("""{"Id":1}""", """{"id":2}""") {
      withSQLConf(
        "spark.sql.json.enablePartialResults" -> "true",
        SQLConf.CASE_SENSITIVE.key -> "true") {
        runQueryAndCompare("SELECT from_json(txt, 'id INT') FROM json_tbl") {
          checkSparkPlan[ProjectExec]
        }
      }
    }
  }

  // ---------------------------------------------------------------------------
  // Limitation 4 -- duplicate keys in the target schema
  //
  // Two sub-cases:
  //   a) exact duplicate field names ("id INT, id INT")
  //   b) field names that differ only in case ("id INT, Id INT") -- these are
  //      duplicates in the default case-insensitive mode
  // ---------------------------------------------------------------------------

  test("from_json falls back with exact duplicate field names in schema") {
    withJsonTable("""{"id":1}""", """{"id":2}""") {
      withSQLConf("spark.sql.json.enablePartialResults" -> "true") {
        runQueryAndCompare("SELECT from_json(txt, 'id INT, id INT') FROM json_tbl") {
          checkSparkPlan[ProjectExec]
        }
      }
    }
  }

  test("from_json falls back with case-only duplicate field names in schema") {
    withJsonTable("""{"id":1,"Id":2}""", """{"id":3,"Id":4}""") {
      withSQLConf("spark.sql.json.enablePartialResults" -> "true") {
        runQueryAndCompare("SELECT from_json(txt, 'id INT, Id INT') FROM json_tbl") {
          checkSparkPlan[ProjectExec]
        }
      }
    }
  }

  // ---------------------------------------------------------------------------
  // Limitation 5 -- _corrupt_record column in the schema
  //
  // Velox does not implement the corrupt-record tracking column that Spark uses
  // to capture malformed JSON rows.
  // ---------------------------------------------------------------------------

  test("from_json falls back when schema includes _corrupt_record column") {
    withJsonTable("""{"id":1}""", """{"id":not-a-number}""") {
      withSQLConf("spark.sql.json.enablePartialResults" -> "true") {
        runQueryAndCompare(
          "SELECT from_json(txt, 'id INT, _corrupt_record STRING') FROM json_tbl") {
          checkSparkPlan[ProjectExec]
        }
      }
    }
  }

  // ---------------------------------------------------------------------------
  // Positive cases -- expression IS offloaded to Velox
  //
  // When enablePartialResults = true and none of the restrictions apply, the
  // query should use ProjectExecTransformer, not the vanilla ProjectExec.
  // ---------------------------------------------------------------------------

  test("from_json offloads to Velox for struct schema") {
    withJsonTable("""{"id":1,"name":"alice"}""", """{"id":2,"name":"bob"}""") {
      withSQLConf("spark.sql.json.enablePartialResults" -> "true") {
        runQueryAndCompare("SELECT from_json(txt, 'id INT, name STRING') FROM json_tbl") {
          checkGlutenPlan[ProjectExecTransformer]
        }
      }
    }
  }

  test("from_json offloads to Velox for array schema") {
    withJsonTable("""[1,2,3]""", """[4,5,6]""") {
      withSQLConf("spark.sql.json.enablePartialResults" -> "true") {
        runQueryAndCompare("SELECT from_json(txt, 'array<int>') FROM json_tbl") {
          checkGlutenPlan[ProjectExecTransformer]
        }
      }
    }
  }

  test("from_json offloads to Velox for map schema") {
    withJsonTable("""{"a":1,"b":2}""", """{"c":3}""") {
      withSQLConf("spark.sql.json.enablePartialResults" -> "true") {
        runQueryAndCompare("SELECT from_json(txt, 'map<string,int>') FROM json_tbl") {
          checkGlutenPlan[ProjectExecTransformer]
        }
      }
    }
  }

  test("from_json offloads to Velox when schema fields are unique (case-insensitive)") {
    // The schema has a single unique field "id" -- no duplicate, so it must offload.
    // Note: Velox matches JSON keys case-insensitively, so the data must use the same
    // case as the schema field name to avoid a result mismatch with Spark.
    withJsonTable("""{"id":1}""", """{"id":2}""") {
      withSQLConf("spark.sql.json.enablePartialResults" -> "true") {
        runQueryAndCompare("SELECT from_json(txt, 'id INT') FROM json_tbl") {
          checkGlutenPlan[ProjectExecTransformer]
        }
      }
    }
  }
}
