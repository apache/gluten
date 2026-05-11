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

import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, GlutenTestUtils, Row}
import org.apache.spark.sql.execution.InputIteratorTransformer
import org.apache.spark.sql.execution.aggregate.SortAggregateExec

import org.scalatest.DoNotDiscover

import scala.collection.mutable

@DoNotDiscover
class GlutenClickHouseTPCHBucketSuite
  extends GlutenClickHouseTPCHAbstractSuite
  with TPCHBucketTableSource
  with TPCHMergeTreeResult {

  // On Spark 3.2, the bucket table does not support creating a bucket column
  // with sort columns for DS V2
  lazy val hasSortByCol: Boolean = !spark32
  lazy val tableFormat: String = "clickhouse"

  override protected def sparkConf: SparkConf = {
    import org.apache.gluten.backendsapi.clickhouse.CHConfig._
    super.sparkConf
      .set("spark.shuffle.manager", "org.apache.spark.shuffle.sort.ColumnarShuffleManager")
      .set("spark.io.compression.codec", "LZ4")
      .set("spark.sql.shuffle.partitions", "5")
      .set("spark.sql.autoBroadcastJoinThreshold", "-1") // for test bucket join
      .set("spark.sql.adaptive.enabled", "true")
      .set("spark.gluten.sql.columnar.backend.ch.shuffle.hash.algorithm", "sparkMurmurHash3_32")
      .setCHConfig("enable_pre_projection_for_join_conditions", "false")
      .setCHConfig("enable_grace_aggregate_spill_test", "true")
  }

  test("TPCH Q1") {
    customCheck(1)(
      df => {
        val plans = collect(df.queryExecution.executedPlan) {
          case scanExec: BasicScanExecTransformer => scanExec
        }
        assert(!plans.head.asInstanceOf[FileSourceScanExecTransformer].bucketedScan)
        assert(plans.head.metrics("numFiles").value === 2)
        assert(plans.head.metrics("pruningTime").value === pruningTimeValueSpark)
        assert(plans.head.metrics("numOutputRows").value === 591673)
      })
  }

  test("TPCH Q2") {
    customCheck(2)(
      df => {
        val plans = collect(df.queryExecution.executedPlan) {
          case scanExec: BasicScanExecTransformer => scanExec
          case joinExec: HashJoinLikeExecTransformer => joinExec
        }
        assert(
          plans(3)
            .asInstanceOf[HashJoinLikeExecTransformer]
            .left
            .isInstanceOf[InputIteratorTransformer])
        assert(
          plans(3)
            .asInstanceOf[HashJoinLikeExecTransformer]
            .right
            .isInstanceOf[InputIteratorTransformer])

        // Check the bucket join
        assert(
          plans(4)
            .asInstanceOf[HashJoinLikeExecTransformer]
            .left
            .isInstanceOf[ProjectExecTransformer])
        assert(
          plans(4)
            .asInstanceOf[HashJoinLikeExecTransformer]
            .right
            .isInstanceOf[FilterExecTransformerBase])

        assert(
          plans(9)
            .asInstanceOf[HashJoinLikeExecTransformer]
            .left
            .isInstanceOf[InputIteratorTransformer])
        if (spark32) {
          assert(
            plans(9)
              .asInstanceOf[HashJoinLikeExecTransformer]
              .right
              .isInstanceOf[InputIteratorTransformer])
        } else {
          assert(
            plans(9)
              .asInstanceOf[HashJoinLikeExecTransformer]
              .right
              .isInstanceOf[FilterExecTransformerBase])
        }

        if (spark32) {
          assert(!plans(11).asInstanceOf[FileSourceScanExecTransformer].bucketedScan)
        } else {
          assert(plans(11).asInstanceOf[FileSourceScanExecTransformer].bucketedScan)
        }
        assert(plans(11).metrics("numFiles").value === 1)
        assert(plans(11).metrics("numOutputRows").value === 1000)
      })
  }

  test("TPCH Q3") {
    customCheck(3)(
      df => {
        val plans = collect(df.queryExecution.executedPlan) {
          case scanExec: BasicScanExecTransformer => scanExec
          case joinExec: HashJoinLikeExecTransformer => joinExec
        }
        if (spark32) {
          assert(
            plans(1)
              .asInstanceOf[HashJoinLikeExecTransformer]
              .left
              .isInstanceOf[InputIteratorTransformer])
        } else {
          assert(
            plans(1)
              .asInstanceOf[HashJoinLikeExecTransformer]
              .left
              .isInstanceOf[ProjectExecTransformer])
        }
        assert(
          plans(1)
            .asInstanceOf[HashJoinLikeExecTransformer]
            .right
            .isInstanceOf[InputIteratorTransformer])

        if (spark32) {
          assert(!plans(2).asInstanceOf[FileSourceScanExecTransformer].bucketedScan)
        } else {
          assert(plans(2).asInstanceOf[FileSourceScanExecTransformer].bucketedScan)
        }
        assert(plans(2).metrics("numFiles").value === 2)
        assert(plans(2).metrics("numOutputRows").value === 3111)

        assert(!plans(3).asInstanceOf[FileSourceScanExecTransformer].bucketedScan)
        assert(plans(3).metrics("numFiles").value === 2)
        assert(plans(3).metrics("numOutputRows").value === 72678)
      })

    withSQLConf(
      ("spark.sql.optimizer.runtime.bloomFilter.applicationSideScanSizeThreshold", "1KB"),
      ("spark.sql.optimizer.runtime.bloomFilter.enabled", "true")) {
      customCheck(3)(
        df => {
          if (spark33) {
            val plans = collectWithSubqueries(df.queryExecution.executedPlan) {
              case aggExec: HashAggregateExecBaseTransformer
                  if aggExec.aggregateExpressions.exists(
                    _.aggregateFunction.getClass.getSimpleName.equals("BloomFilterAggregate")) =>
                aggExec
            }
            assert(plans.size == 8)
          }
        })
    }
  }

  test("TPCH Q4") {
    customCheck(4)(
      df => {
        val plans = collect(df.queryExecution.executedPlan) {
          case scanExec: BasicScanExecTransformer => scanExec
          case joinExec: HashJoinLikeExecTransformer => joinExec
        }
        // bucket join
        assert(
          plans.head
            .asInstanceOf[HashJoinLikeExecTransformer]
            .left
            .isInstanceOf[ProjectExecTransformer])
        assert(
          plans.head
            .asInstanceOf[HashJoinLikeExecTransformer]
            .right
            .isInstanceOf[ProjectExecTransformer])

        assert(plans(1).asInstanceOf[FileSourceScanExecTransformer].bucketedScan)
        assert(plans(1).metrics("numFiles").value === 2)
        assert(plans(1).metrics("numOutputRows").value === 5552)

        assert(plans(2).asInstanceOf[FileSourceScanExecTransformer].bucketedScan)
        assert(plans(2).metrics("numFiles").value === 2)
        assert(plans(2).metrics("numOutputRows").value === 379809)
      })

    withSQLConf(
      ("spark.sql.optimizer.runtime.bloomFilter.applicationSideScanSizeThreshold", "1KB"),
      ("spark.sql.optimizer.runtime.bloomFilter.enabled", "true")) {
      customCheck(4)(
        df => {
          if (spark33) {
            val plans = collectWithSubqueries(df.queryExecution.executedPlan) {
              case aggExec: HashAggregateExecBaseTransformer
                  if aggExec.aggregateExpressions.exists(
                    _.aggregateFunction.getClass.getSimpleName.equals("BloomFilterAggregate")) =>
                aggExec
            }
            assert(plans.size == 4)
          }
        })
    }
  }

  test("TPCH Q6") {
    customCheck(6)(
      df => {
        val plans = collect(df.queryExecution.executedPlan) {
          case scanExec: BasicScanExecTransformer => scanExec
        }
        assert(!plans.head.asInstanceOf[FileSourceScanExecTransformer].bucketedScan)
        assert(plans.head.metrics("numFiles").value === 2)
        assert(plans.head.metrics("pruningTime").value === pruningTimeValueSpark)
        assert(plans.head.metrics("numOutputRows").value === 11618)
      })
  }

  test("TPCH Q12") {
    customCheck(12)(
      df => {
        val plans = collect(df.queryExecution.executedPlan) {
          case scanExec: BasicScanExecTransformer => scanExec
          case joinExec: HashJoinLikeExecTransformer => joinExec
        }
        // bucket join
        assert(
          plans.head
            .asInstanceOf[HashJoinLikeExecTransformer]
            .left
            .isInstanceOf[FilterExecTransformerBase])
        assert(
          plans.head
            .asInstanceOf[HashJoinLikeExecTransformer]
            .right
            .isInstanceOf[ProjectExecTransformer])

        assert(plans(1).asInstanceOf[FileSourceScanExecTransformer].bucketedScan)
        assert(plans(1).metrics("numFiles").value === 2)
        assert(plans(1).metrics("numOutputRows").value === 150000)

        assert(plans(2).asInstanceOf[FileSourceScanExecTransformer].bucketedScan)
        assert(plans(2).metrics("numFiles").value === 2)
        assert(plans(2).metrics("numOutputRows").value === 3155)
      })

    withSQLConf(
      ("spark.sql.optimizer.runtime.bloomFilter.applicationSideScanSizeThreshold", "1KB"),
      ("spark.sql.optimizer.runtime.bloomFilter.enabled", "true")) {
      customCheck(12)(
        df => {
          if (spark33) {
            val plans = collectWithSubqueries(df.queryExecution.executedPlan) {
              case aggExec: HashAggregateExecBaseTransformer
                  if aggExec.aggregateExpressions.exists(
                    _.aggregateFunction.getClass.getSimpleName.equals("BloomFilterAggregate")) =>
                aggExec
            }
            assert(plans.size == 4)
          }
        })
    }
  }

  test("TPCH Q18") {
    customCheck(18)(
      df => {
        val plans = collect(df.queryExecution.executedPlan) {
          case joinExec: HashJoinLikeExecTransformer => joinExec
        }
        // bucket join
        assert(
          plans(2)
            .asInstanceOf[HashJoinLikeExecTransformer]
            .left
            .isInstanceOf[FilterExecTransformerBase])
        assert(
          plans(2)
            .asInstanceOf[HashJoinLikeExecTransformer]
            .right
            .isInstanceOf[ProjectExecTransformer])
        // bucket join
        assert(
          plans(3)
            .asInstanceOf[HashJoinLikeExecTransformer]
            .left
            .isInstanceOf[FilterExecTransformerBase])
        assert(
          plans(3)
            .asInstanceOf[HashJoinLikeExecTransformer]
            .right
            .isInstanceOf[ProjectExecTransformer])
      })
  }

  test("TPCH Q20") {
    customCheck(20)(
      df => {
        val plans = collect(df.queryExecution.executedPlan) {
          case joinExec: HashJoinLikeExecTransformer => joinExec
        }
        if (spark32) {
          assert(
            plans(1)
              .asInstanceOf[HashJoinLikeExecTransformer]
              .left
              .isInstanceOf[InputIteratorTransformer])
        } else {
          assert(
            plans(1)
              .asInstanceOf[HashJoinLikeExecTransformer]
              .left
              .isInstanceOf[FilterExecTransformerBase])
        }
        assert(
          plans(1)
            .asInstanceOf[HashJoinLikeExecTransformer]
            .right
            .isInstanceOf[InputIteratorTransformer])

        assert(
          plans(2)
            .asInstanceOf[HashJoinLikeExecTransformer]
            .left
            .isInstanceOf[InputIteratorTransformer])
        assert(
          plans(2)
            .asInstanceOf[HashJoinLikeExecTransformer]
            .right
            .isInstanceOf[InputIteratorTransformer])

        assert(
          plans(3)
            .asInstanceOf[HashJoinLikeExecTransformer]
            .left
            .isInstanceOf[FilterExecTransformerBase])
        assert(
          plans(3)
            .asInstanceOf[HashJoinLikeExecTransformer]
            .right
            .isInstanceOf[ProjectExecTransformer])
      })

    withSQLConf(
      ("spark.sql.optimizer.runtime.bloomFilter.applicationSideScanSizeThreshold", "1KB"),
      ("spark.sql.optimizer.runtime.bloomFilter.enabled", "true")) {
      customCheck(20)(
        df => {
          if (spark33) {
            val plans = collectWithSubqueries(df.queryExecution.executedPlan) {
              case aggExec: HashAggregateExecBaseTransformer
                  if aggExec.aggregateExpressions.exists(
                    _.aggregateFunction.getClass.getSimpleName.equals("BloomFilterAggregate")) =>
                aggExec
            }
            assert(plans.size == 6)
          }
        })
    }
  }

  test("check bucket pruning on filter") {
    // TODO use comparewithvanilla
    val df = spark.sql("select count(*) from lineitem where l_orderkey = 12647")
    val result = df.collect()
    val scanExec = collect(df.queryExecution.executedPlan) {
      case f: FileSourceScanExecTransformer => f
    }
    val touchedParts = scanExec.head.getPartitions
      .flatMap(partition => partition.asInstanceOf[GlutenMergeTreePartition].partList)
      .map(_.name)
      .distinct
    assert(touchedParts.size == 1)
    assert(result.apply(0).apply(0) == 1)
  }

  test("GLUTEN-4668: Merge two phase hash-based aggregate into one aggregate") {
    def checkHashAggregateCount(df: DataFrame, expectedCount: Int): Unit = {
      val plans = collect(df.queryExecution.executedPlan) {
        case agg: HashAggregateExecBaseTransformer => agg
      }
      assert(plans.size == expectedCount)
    }

    def checkResult(df: DataFrame, exceptedResult: Seq[Row]): Unit = {
      // check the result
      val result = df.collect()
      assert(result.length == exceptedResult.size)
      val sortedRes = result.map {
        s =>
          Row.fromSeq(s.toSeq.map {
            case a: mutable.WrappedArray[_] => a.sortBy(_.toString.toInt)
            case o => o
          })
      }
      GlutenTestUtils.compareAnswers(sortedRes, exceptedResult)
    }

    val SQL =
      """
        |select l_orderkey, l_returnflag, collect_list(l_linenumber) as t
        |from lineitem group by l_orderkey, l_returnflag
        |order by l_orderkey, l_returnflag, t limit 5
        |""".stripMargin
    runSql(SQL)(
      df => {
        checkResult(
          df,
          Array(
            Row(1, "N", mutable.WrappedArray.make(Array(1, 2, 3, 4, 5, 6))),
            Row(2, "N", mutable.WrappedArray.make(Array(1))),
            Row(3, "A", mutable.WrappedArray.make(Array(3, 4, 6))),
            Row(3, "R", mutable.WrappedArray.make(Array(1, 2, 5))),
            Row(4, "N", mutable.WrappedArray.make(Array(1)))
          )
        )
        checkHashAggregateCount(df, 1)
      })

    val SQL1 =
      """
        |select l_orderkey, l_returnflag,
        |sum(l_linenumber) as t,
        |count(l_linenumber) as t1,
        |min(l_linenumber) as t2
        |from lineitem group by l_orderkey, l_returnflag
        |order by l_orderkey, l_returnflag, t, t1, t2 limit 5
        |""".stripMargin
    runSql(SQL1)(
      df => {
        checkResult(
          df,
          Array(
            Row(1, "N", 21, 6, 1),
            Row(2, "N", 1, 1, 1),
            Row(3, "A", 13, 3, 3),
            Row(3, "R", 8, 3, 1),
            Row(4, "N", 1, 1, 1)
          )
        )
        checkHashAggregateCount(df, 1)
      })

    val SQL2 =
      """
        |select l_returnflag, l_orderkey, collect_list(l_linenumber) as t
        |from lineitem group by l_orderkey, l_returnflag
        |order by l_returnflag, l_orderkey, t limit 5
        |""".stripMargin
    runSql(SQL2)(
      df => {
        checkResult(
          df,
          Array(
            Row("A", 3, mutable.WrappedArray.make(Array(3, 4, 6))),
            Row("A", 5, mutable.WrappedArray.make(Array(3))),
            Row("A", 6, mutable.WrappedArray.make(Array(1))),
            Row("A", 33, mutable.WrappedArray.make(Array(1, 2, 3))),
            Row("A", 37, mutable.WrappedArray.make(Array(1, 2, 3)))
          )
        )
        checkHashAggregateCount(df, 1)
      })

    // will merge four aggregates into two one.
    val SQL3 =
      """
        |select l_returnflag, l_orderkey,
        |count(distinct l_linenumber) as t
        |from lineitem group by l_orderkey, l_returnflag
        |order by l_returnflag, l_orderkey, t limit 5
        |""".stripMargin
    runSql(SQL3)(
      df => {
        checkResult(
          df,
          Array(
            Row("A", 3, 3),
            Row("A", 5, 1),
            Row("A", 6, 1),
            Row("A", 33, 3),
            Row("A", 37, 3)
          )
        )
        checkHashAggregateCount(df, 2)
      })

    // not support when there are more than one count distinct
    val SQL4 =
      """
        |select l_returnflag, l_orderkey,
        |count(distinct l_linenumber) as t,
        |count(distinct l_discount) as t1
        |from lineitem group by l_orderkey, l_returnflag
        |order by l_returnflag, l_orderkey, t, t1 limit 5
        |""".stripMargin
    runSql(SQL4)(
      df => {
        checkResult(
          df,
          Array(
            Row("A", 3, 3, 3),
            Row("A", 5, 1, 1),
            Row("A", 6, 1, 1),
            Row("A", 33, 3, 3),
            Row("A", 37, 3, 2)
          )
        )
        checkHashAggregateCount(df, 4)
      })

    val SQL5 =
      """
        |select l_returnflag, l_orderkey,
        |count(distinct l_linenumber) as t,
        |sum(l_linenumber) as t1
        |from lineitem group by l_orderkey, l_returnflag
        |order by l_returnflag, l_orderkey, t, t1 limit 5
        |""".stripMargin
    runSql(SQL5)(
      df => {
        checkResult(
          df,
          Array(
            Row("A", 3, 3, 13),
            Row("A", 5, 1, 3),
            Row("A", 6, 1, 1),
            Row("A", 33, 3, 6),
            Row("A", 37, 3, 6)
          )
        )
        checkHashAggregateCount(df, 4)
      })

    val SQL6 =
      """
        |select count(1) from lineitem
        |""".stripMargin
    runSql(SQL6)(
      df => {
        checkResult(df, Array(Row(600572)))
        if (spark32) {
          // there is a shuffle between two phase hash aggregate.
          checkHashAggregateCount(df, 2)
        } else {
          // the delta will use the delta log meta to response this sql
          checkHashAggregateCount(df, 0)
        }
      })

    // test sort aggregates
    val SQL7 =
      """
        |select l_orderkey, l_returnflag, max(l_shipinstruct) as t
        |from lineitem
        |group by l_orderkey, l_returnflag
        |order by l_orderkey, l_returnflag, t
        |limit 10
        |""".stripMargin
    runSql(SQL7)(
      df => {
        checkResult(
          df,
          Array(
            Row(1, "N", "TAKE BACK RETURN"),
            Row(2, "N", "TAKE BACK RETURN"),
            Row(3, "A", "TAKE BACK RETURN"),
            Row(3, "R", "TAKE BACK RETURN"),
            Row(4, "N", "DELIVER IN PERSON"),
            Row(5, "A", "DELIVER IN PERSON"),
            Row(5, "R", "NONE"),
            Row(6, "A", "TAKE BACK RETURN"),
            Row(7, "N", "TAKE BACK RETURN"),
            Row(32, "N", "TAKE BACK RETURN")
          )
        )
        checkHashAggregateCount(df, 1)
      })

    withSQLConf(("spark.gluten.sql.columnar.force.hashagg", "false")) {
      val SQL =
        """
          |select l_orderkey, l_returnflag, max(l_shipinstruct) as t
          |from lineitem
          |group by l_orderkey, l_returnflag
          |order by l_orderkey, l_returnflag, t
          |limit 10
          |""".stripMargin
      runSql(SQL7, noFallBack = false)(
        df => {
          checkResult(
            df,
            Array(
              Row(1, "N", "TAKE BACK RETURN"),
              Row(2, "N", "TAKE BACK RETURN"),
              Row(3, "A", "TAKE BACK RETURN"),
              Row(3, "R", "TAKE BACK RETURN"),
              Row(4, "N", "DELIVER IN PERSON"),
              Row(5, "A", "DELIVER IN PERSON"),
              Row(5, "R", "NONE"),
              Row(6, "A", "TAKE BACK RETURN"),
              Row(7, "N", "TAKE BACK RETURN"),
              Row(32, "N", "TAKE BACK RETURN")
            )
          )
          checkHashAggregateCount(df, 0)
          val plans = collect(df.queryExecution.executedPlan) { case a: SortAggregateExec => a }
          assert(plans.size == 2)
        })
    }
  }
}
