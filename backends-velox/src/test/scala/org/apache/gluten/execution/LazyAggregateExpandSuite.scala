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

import org.apache.gluten.config.VeloxConfig

import org.apache.spark.SparkConf
import org.apache.spark.sql.DataFrame

class LazyAggregateExpandSuite extends VeloxWholeStageTransformerSuite {
  override protected val resourcePath: String = "/tpch-data-parquet"
  override protected val fileFormat: String = "parquet"

  override def beforeAll(): Unit = {
    super.beforeAll()
    createTPCHNotNullTables()
  }

  override protected def sparkConf: SparkConf = {
    super.sparkConf
      .set("spark.shuffle.manager", "org.apache.spark.shuffle.sort.ColumnarShuffleManager")
      .set("spark.sql.shuffle.partitions", "2")
      .set("spark.memory.offHeap.size", "2g")
      .set("spark.unsafe.exceptionOnMemoryLeak", "true")
      .set(VeloxConfig.VELOX_LAZY_AGGREGATE_EXPAND_ENABLED.key, "true")
  }

  // The rewrite leaves an ExpandExecTransformer whose child is the FLUSHABLE finest-grain
  // aggregate, and FlushableHashAggregateRule must have converted the merge stage above it.
  // A regular stage on either side would reintroduce the high-cardinality regression that hit
  // the ClickHouse backend (GLUTEN-7986), so both are asserted.
  private def checkLazyExpand(df: DataFrame, fired: Boolean = true): Unit = {
    val plans = getExecutedPlan(df)
    if (fired) {
      val lazyExpands = plans.collect {
        case e: ExpandExecTransformer
            if e.child.isInstanceOf[FlushableHashAggregateExecTransformer] =>
          e
      }
      assert(
        lazyExpands.nonEmpty,
        s"expected lazy expand to fire but it did not:\n${df.queryExecution.executedPlan}")
      val flushableMergeExists = plans.exists {
        case a: FlushableHashAggregateExecTransformer =>
          lazyExpands.exists(e => a.child.find(_ eq e).isDefined)
        case _ => false
      }
      assert(
        flushableMergeExists,
        s"expected a flushable merge stage above the expand:\n${df.queryExecution.executedPlan}")
    } else {
      val lazyExpands = plans.collect {
        case e: ExpandExecTransformer if e.child.isInstanceOf[HashAggregateExecBaseTransformer] =>
          e
      }
      assert(
        lazyExpands.isEmpty,
        s"expected lazy expand to not fire but it did:\n${df.queryExecution.executedPlan}")
    }
  }

  test("rollup with sum/count/min/max") {
    runQueryAndCompare(
      "select l_orderkey, l_partkey, sum(l_suppkey), count(l_suppkey), " +
        "min(l_suppkey), max(l_suppkey) from lineitem " +
        "group by rollup(l_orderkey, l_partkey) " +
        "order by l_orderkey, l_partkey") {
      df => checkLazyExpand(df)
    }
  }

  test("cube with grouping and grouping_id") {
    runQueryAndCompare(
      "select l_orderkey, l_partkey, grouping(l_orderkey), grouping(l_partkey), " +
        "grouping_id(l_orderkey, l_partkey), sum(l_suppkey) from lineitem " +
        "group by cube(l_orderkey, l_partkey) " +
        "order by l_orderkey, l_partkey, 3, 4, 5") {
      df => checkLazyExpand(df)
    }
  }

  test("avg buffers are merged, not averaged") {
    // Groups with very different sizes: any average-of-averages shortcut diverges from vanilla.
    withTable("t_lazy_avg") {
      sql("create table t_lazy_avg (k1 int, k2 int, v int) using parquet")
      sql(
        "insert into t_lazy_avg values " +
          "(1, 1, 1000), (1, 2, 0), (1, 2, 0), (1, 2, 0), (1, 2, 0), (1, 2, 0), " +
          "(2, 1, 10), (2, 1, 20), (2, 2, 500)")
      runQueryAndCompare(
        "select k1, k2, avg(v), count(*) from t_lazy_avg " +
          "group by rollup(k1, k2) order by k1, k2") {
        df => checkLazyExpand(df)
      }
    }
  }

  test("genuine null key vs rolled-up null") {
    withTable("t_lazy_nulls") {
      sql("create table t_lazy_nulls (k1 string, k2 string, v int) using parquet")
      sql(
        "insert into t_lazy_nulls values " +
          "('a', null, 1), ('a', 'x', 2), (null, 'x', 3), (null, null, 4), ('b', 'x', 5)")
      runQueryAndCompare(
        "select k1, k2, grouping(k1), grouping(k2), grouping_id(k1, k2), sum(v), count(*) " +
          "from t_lazy_nulls group by rollup(k1, k2) " +
          "order by k1, k2, 3, 4, 5") {
        df => checkLazyExpand(df)
      }
    }
  }

  test("decimal sum buffer (sum, isEmpty) binds through the expand") {
    runQueryAndCompare(
      "select l_orderkey, l_partkey, sum(cast(l_extendedprice as decimal(12, 2))) " +
        "from lineitem group by rollup(l_orderkey, l_partkey) " +
        "order by l_orderkey, l_partkey") {
      df => checkLazyExpand(df)
    }
  }

  test("pre-projected aggregate input (TPC-DS q67 shape)") {
    runQueryAndCompare(
      "select l_orderkey, l_partkey, sum(l_suppkey * l_linenumber + 1) from lineitem " +
        "group by rollup(l_orderkey, l_partkey) " +
        "order by l_orderkey, l_partkey") {
      df => checkLazyExpand(df)
    }
  }

  test("aggregate over a grouping key") {
    runQueryAndCompare(
      "select l_orderkey, l_partkey, sum(l_orderkey) from lineitem " +
        "group by rollup(l_orderkey, l_partkey) " +
        "order by l_orderkey, l_partkey") {
      df => checkLazyExpand(df)
    }
  }

  test("empty input returns no rows") {
    // AQE's empty-relation propagation collapses the whole plan on empty input, which would
    // make the plan assertion vacuous; disable it so the rewritten plan is observable.
    withSQLConf("spark.sql.adaptive.enabled" -> "false") {
      withTable("t_lazy_empty") {
        sql("create table t_lazy_empty (k1 int, k2 int, v int) using parquet")
        runQueryAndCompare(
          "select k1, k2, sum(v), count(*) from t_lazy_empty " +
            "group by grouping sets ((k1, k2), ())") {
          df => checkLazyExpand(df)
        }
      }
    }
  }

  test("degenerate grouping sets without attribute keys are not rewritten") {
    // A keyless finest-grain aggregate would emit a spurious row on empty input. AQE disabled
    // for the same reason as the empty-input test.
    withSQLConf("spark.sql.adaptive.enabled" -> "false") {
      withTable("t_lazy_degenerate") {
        sql("create table t_lazy_degenerate (v int) using parquet")
        runQueryAndCompare(
          "select sum(v), count(*) from t_lazy_degenerate group by grouping sets ((), ())") {
          df => checkLazyExpand(df, fired = false)
        }
      }
    }
  }

  test("duplicate grouping sets keep duplicate result rows") {
    runQueryAndCompare(
      "select l_orderkey, sum(l_suppkey) from lineitem " +
        "where l_orderkey < 100 " +
        "group by grouping sets ((l_orderkey), (l_orderkey)) " +
        "order by l_orderkey") {
      df => checkLazyExpand(df)
    }
  }

  test("single count distinct rewrites the dedup aggregate") {
    runQueryAndCompare(
      "select l_orderkey, l_partkey, count(distinct l_suppkey), sum(l_linenumber) " +
        "from lineitem group by rollup(l_orderkey, l_partkey) " +
        "order by l_orderkey, l_partkey") {
      df => checkLazyExpand(df)
    }
  }

  test("multiple count distinct stays correct") {
    // RewriteDistinctAggregates produces a second, look-alike Expand that must not be rewritten;
    // correctness is what matters here, the fire state depends on the surviving plan shape.
    runQueryAndCompare(
      "select l_orderkey, count(distinct l_suppkey), count(distinct l_partkey) " +
        "from lineitem group by rollup(l_orderkey) " +
        "order by l_orderkey") { _ => }
  }

  test("aggregate filter clause is not rewritten") {
    runQueryAndCompare(
      "select l_orderkey, l_partkey, sum(l_suppkey) filter (where l_linenumber > 2) " +
        "from lineitem group by rollup(l_orderkey, l_partkey) " +
        "order by l_orderkey, l_partkey") {
      df => checkLazyExpand(df, fired = false)
    }
  }

  test("floating point sum is not rewritten in strict mode") {
    withSQLConf(VeloxConfig.FLOATING_POINT_MODE.key -> "strict") {
      runQueryAndCompare(
        "select l_orderkey, l_partkey, sum(cast(l_quantity as double)) from lineitem " +
          "group by rollup(l_orderkey, l_partkey) " +
          "order by l_orderkey, l_partkey") {
        df => checkLazyExpand(df, fired = false)
      }
    }
  }

  test("filter on grouping keys between aggregate and expand") {
    // The predicate references a nulled key copy, so it cannot be pushed below the Expand and
    // sits between the partial aggregate and the Expand, the rule's preFilter branch.
    runQueryAndCompare(
      "select * from (" +
        "select l_orderkey, l_partkey, sum(l_suppkey) as s from lineitem " +
        "group by cube(l_orderkey, l_partkey)) " +
        "where l_orderkey is not null order by l_orderkey, l_partkey") {
      df => checkLazyExpand(df)
    }
  }

  test("high cardinality keys with early abandon") {
    withSQLConf(
      "spark.gluten.sql.columnar.backend.velox.abandonPartialAggregationMinRows" -> "10",
      "spark.gluten.sql.columnar.backend.velox.abandonPartialAggregationMinPct" -> "1"
    ) {
      runQueryAndCompare(
        "select l_orderkey, l_partkey, l_suppkey, sum(l_linenumber) from lineitem " +
          "group by rollup(l_orderkey, l_partkey, l_suppkey) " +
          "order by l_orderkey, l_partkey, l_suppkey") {
        df => checkLazyExpand(df)
      }
    }
  }

  test("adaptive execution disabled") {
    withSQLConf("spark.sql.adaptive.enabled" -> "false") {
      runQueryAndCompare(
        "select l_orderkey, l_partkey, sum(l_suppkey) from lineitem " +
          "group by rollup(l_orderkey, l_partkey) " +
          "order by l_orderkey, l_partkey") {
        df => checkLazyExpand(df)
      }
    }
  }

  test("disabled by default when flushable aggregation is off") {
    withSQLConf(VeloxConfig.VELOX_FLUSHABLE_PARTIAL_AGGREGATION_ENABLED.key -> "false") {
      runQueryAndCompare(
        "select l_orderkey, l_partkey, sum(l_suppkey) from lineitem " +
          "group by rollup(l_orderkey, l_partkey) " +
          "order by l_orderkey, l_partkey") {
        df => checkLazyExpand(df, fired = false)
      }
    }
  }

  test("non-whitelisted aggregate function is not rewritten") {
    runQueryAndCompare(
      "select l_orderkey, l_partkey, stddev_samp(l_suppkey) from lineitem " +
        "group by rollup(l_orderkey, l_partkey) " +
        "order by l_orderkey, l_partkey") {
      df => checkLazyExpand(df, fired = false)
    }
  }

  test("floating point sum is rewritten in loose mode") {
    // floatingPointMode defaults to loose, the same policy that already allows flushing float
    // aggregates. Small integers cast to double keep the sums exact regardless of merge order.
    runQueryAndCompare(
      "select l_orderkey, l_partkey, sum(cast(l_linenumber as double)) from lineitem " +
        "group by rollup(l_orderkey, l_partkey) " +
        "order by l_orderkey, l_partkey") {
      df => checkLazyExpand(df)
    }
  }

  test("non-atomic grouping key is not rewritten") {
    runQueryAndCompare(
      "select array(l_orderkey), l_partkey, sum(l_suppkey) from lineitem " +
        "group by rollup(array(l_orderkey), l_partkey) " +
        "order by 2, 1") {
      df => checkLazyExpand(df, fired = false)
    }
  }
}
