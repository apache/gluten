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
}
