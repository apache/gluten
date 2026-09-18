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

import org.apache.gluten.execution.WindowExecTransformer

class WindowFunctionsValidateSuite extends FunctionsValidateSuite {

  private def createNtzWindowTable(): Unit = {
    spark
      .sql("""
             |select * from values
             |  (1, TIMESTAMP_NTZ '2024-01-01 00:00:00', 10),
             |  (1, TIMESTAMP_NTZ '2024-01-02 00:00:00', 20),
             |  (2, TIMESTAMP_NTZ '2024-01-01 00:00:00', 30)
             |as t(k, ts, v)
          """.stripMargin)
      .write
      .saveAsTable("ntz_window_data")
  }

  test("lag/lead window function with negative input offset") {
    runQueryAndCompare(
      "select lag(l_orderkey, -2) over" +
        " (partition by l_suppkey order by l_orderkey) from lineitem") {
      checkGlutenPlan[WindowExecTransformer]
    }

    runQueryAndCompare(
      "select lead(l_orderkey, -2) over" +
        " (partition by l_suppkey order by l_orderkey) from lineitem") {
      checkGlutenPlan[WindowExecTransformer]
    }
  }

  test("lag/lead, nth_value window function with constant input") {
    runQueryAndCompare(
      "select lag(10, 2) over" +
        " (partition by l_suppkey order by l_orderkey) from lineitem") {
      checkGlutenPlan[WindowExecTransformer]
    }

    runQueryAndCompare(
      "select lead(10, 2) over" +
        " (partition by l_suppkey order by l_orderkey) from lineitem") {
      checkGlutenPlan[WindowExecTransformer]
    }

    runQueryAndCompare(
      "select nth_value(10, 2) over" +
        " (partition by l_suppkey order by l_orderkey) from lineitem") {
      checkGlutenPlan[WindowExecTransformer]
    }
  }

  test("count window function with multiple arguments is rewritten and offloaded") {
    // Velox only supports count() / count(T) for window functions. Spark's
    // count(c1, c2, ...) variant must be rewritten into count(if(or(isnull(c1),
    // isnull(c2), ...), null, 1)) so the WindowExec can still be offloaded.
    runQueryAndCompare(
      "select l_orderkey, " +
        "count(l_partkey, l_suppkey, l_linenumber) " +
        "over (partition by l_orderkey) as cnt " +
        "from lineitem") {
      checkGlutenPlan[WindowExecTransformer]
    }
  }

  test("count window function with multiple arguments returns vanilla Spark result") {
    // Validate semantic equivalence with vanilla Spark: counts rows where ALL
    // arguments are non-null in the window partition.
    withTable("nullable_window_data") {
      spark
        .sql("""
               |select * from values
               |  (1, 'a',  10),
               |  (1, 'a',  null),
               |  (1, null, 20),
               |  (2, 'b',  30),
               |  (2, 'b',  40),
               |  (2, 'c',  null)
               |as t(k, s, v)
            """.stripMargin)
        .write
        .saveAsTable("nullable_window_data")

      runQueryAndCompare(
        "select k, count(s, v) over (partition by k) as cnt " +
          "from nullable_window_data") {
        checkGlutenPlan[WindowExecTransformer]
      }
    }
  }

  test("window partition/order by TIMESTAMP_NTZ goes native") {
    withTable("ntz_window_data") {
      createNtzWindowTable()

      runQueryAndCompare(
        "select k, ts, rank() over (partition by k order by ts) as rnk " +
          "from ntz_window_data") {
        checkGlutenPlan[WindowExecTransformer]
      }
    }
  }

  test("lag(TIMESTAMP_NTZ) window function goes native") {
    withTable("ntz_window_data") {
      createNtzWindowTable()
      runQueryAndCompare(
        "select k, lag(ts, 1) over (partition by k order by v) " +
          "from ntz_window_data") {
        checkGlutenPlan[WindowExecTransformer]
      }
    }
  }

  test("lead(TIMESTAMP_NTZ) window function goes native") {
    withTable("ntz_window_data") {
      createNtzWindowTable()
      runQueryAndCompare(
        "select k, lead(ts, 1) over (partition by k order by v) " +
          "from ntz_window_data") {
        checkGlutenPlan[WindowExecTransformer]
      }
    }
  }

  test("nth_value(TIMESTAMP_NTZ) window function goes native") {
    withTable("ntz_window_data") {
      createNtzWindowTable()
      runQueryAndCompare(
        "select k, nth_value(ts, 1) over (partition by k order by v) " +
          "from ntz_window_data") {
        checkGlutenPlan[WindowExecTransformer]
      }
    }
  }

  test("first_value(TIMESTAMP_NTZ) window function goes native") {
    withTable("ntz_window_data") {
      createNtzWindowTable()
      runQueryAndCompare(
        "select k, first_value(ts) over (partition by k order by v) " +
          "from ntz_window_data") {
        checkGlutenPlan[WindowExecTransformer]
      }
    }
  }

  test("last_value(TIMESTAMP_NTZ) window function goes native") {
    withTable("ntz_window_data") {
      createNtzWindowTable()
      runQueryAndCompare(
        "select k, last_value(ts) over (partition by k order by v) " +
          "from ntz_window_data") {
        checkGlutenPlan[WindowExecTransformer]
      }
    }
  }

  test("count(TIMESTAMP_NTZ) window function goes native") {
    withTable("ntz_window_data") {
      createNtzWindowTable()
      runQueryAndCompare(
        "select k, count(ts) over (partition by k) " +
          "from ntz_window_data") {
        checkGlutenPlan[WindowExecTransformer]
      }
    }
  }

  test("min(TIMESTAMP_NTZ) window function goes native") {
    withTable("ntz_window_data") {
      createNtzWindowTable()
      runQueryAndCompare(
        "select k, min(ts) over (partition by k) " +
          "from ntz_window_data") {
        checkGlutenPlan[WindowExecTransformer]
      }
    }
  }

  test("max(TIMESTAMP_NTZ) window function goes native") {
    withTable("ntz_window_data") {
      createNtzWindowTable()
      runQueryAndCompare(
        "select k, max(ts) over (partition by k) " +
          "from ntz_window_data") {
        checkGlutenPlan[WindowExecTransformer]
      }
    }
  }

  test("unaudited window function argument with TIMESTAMP_NTZ falls back") {
    withTable("ntz_window_data") {
      createNtzWindowTable()

      val df = spark.sql(
        "select k, collect_list(ts) over (partition by k order by v) " +
          "from ntz_window_data")
      // TODO: 1 is a structural estimate, verify against a real build before
      // this PR is marked ready for review. collect_list is deliberately not
      // whitelisted -- it hasn't been audited for NTZ support (see
      // CODING_INSTRUCTIONS.md) -- so this must still fall back.
      checkFallbackOperators(df, 1)
    }
  }
}
