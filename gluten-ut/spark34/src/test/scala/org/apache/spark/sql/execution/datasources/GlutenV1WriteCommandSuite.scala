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
package org.apache.spark.sql.execution.datasources

import org.apache.gluten.execution.SortExecTransformer

import org.apache.spark.sql.GlutenSQLTestsBaseTrait
import org.apache.spark.sql.catalyst.expressions.{Ascending, AttributeReference, NullsFirst, SortOrder}
import org.apache.spark.sql.catalyst.plans.logical.{LogicalPlan, Sort}
import org.apache.spark.sql.execution.{ColumnarWriteFilesExec, QueryExecution, SortExec}
import org.apache.spark.sql.execution.adaptive.AdaptiveSparkPlanExec
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.{IntegerType, StringType}
import org.apache.spark.sql.util.QueryExecutionListener

trait GlutenV1WriteCommandSuiteBase extends V1WriteCommandSuiteBase {

  override def beforeAll(): Unit = {
    super.beforeAll()
  }

  override def afterAll(): Unit = {
    super.afterAll()
  }

  override def executeAndCheckOrdering(
      hasLogicalSort: Boolean,
      orderingMatched: Boolean,
      hasEmpty2Null: Boolean = false)(query: => Unit): Unit = {
    var optimizedPlan: LogicalPlan = null

    val listener = new QueryExecutionListener {
      override def onSuccess(funcName: String, qe: QueryExecution, durationNs: Long): Unit = {
        qe.optimizedPlan match {
          case w: V1WriteCommand =>
            if (hasLogicalSort && conf.getConf(SQLConf.PLANNED_WRITE_ENABLED)) {
              assert(w.query.isInstanceOf[WriteFiles])
              assert(w.partitionColumns == w.query.asInstanceOf[WriteFiles].partitionColumns)
              optimizedPlan = w.query.asInstanceOf[WriteFiles].child
            } else {
              optimizedPlan = w.query
            }
          case _ =>
        }
      }
      override def onFailure(funcName: String, qe: QueryExecution, exception: Exception): Unit = {}
    }
    spark.listenerManager.register(listener)

    query

    // RemoveNativeWriteFilesSortAndProject remove SortExec or SortExecTransformer,
    // thus, FileFormatWriter.outputOrderingMatched is false.
    if (!conf.getConf(SQLConf.PLANNED_WRITE_ENABLED)) {
      // Check whether the output ordering is matched before FileFormatWriter executes rdd.
      assert(
        FileFormatWriter.outputOrderingMatched == orderingMatched,
        s"Expect: $orderingMatched, Actual: ${FileFormatWriter.outputOrderingMatched}")
    }

    sparkContext.listenerBus.waitUntilEmpty()

    assert(optimizedPlan != null)
    // Check whether exists a logical sort node of the write query.
    // If user specified sort matches required ordering, the sort node may not at the top of query.
    assert(
      optimizedPlan.exists(_.isInstanceOf[Sort]) == hasLogicalSort,
      s"Expect hasLogicalSort: $hasLogicalSort," +
        s"Actual: ${optimizedPlan.exists(_.isInstanceOf[Sort])}"
    )

    // Check empty2null conversion.
    val empty2nullExpr = optimizedPlan.exists(p => V1WritesUtils.hasEmptyToNull(p.expressions))
    assert(
      empty2nullExpr == hasEmpty2Null,
      s"Expect hasEmpty2Null: $hasEmpty2Null, Actual: $empty2nullExpr. Plan:\n$optimizedPlan")

    spark.listenerManager.unregister(listener)
  }
}

class GlutenV1WriteCommandSuite
  extends V1WriteCommandSuite
  with GlutenV1WriteCommandSuiteBase
  with GlutenSQLTestsBaseTrait {

  testGluten("GLUTEN-12474: preserve ordering for dynamic partition writes") {
    withSQLConf(
      "spark.sql.maxConcurrentOutputFileWriters" -> "0",
      "spark.sql.sources.partitionOverwriteMode" -> "DYNAMIC") {
      withTable("gluten_12474_src", "gluten_12474_tgt") {
        sql(
          """
            |CREATE TABLE gluten_12474_src USING ORC AS
            |SELECT concat('k', id) AS k, format_string('v%02d', id) AS v,
            |  if(id % 2 = 1, '2026-06-01', '2026-06-02') AS day
            |FROM range(0, 10)
            |""".stripMargin)

        sql(
          """
            |CREATE TABLE gluten_12474_tgt (k STRING, m STRING)
            |USING ORC
            |PARTITIONED BY (day STRING)
            |""".stripMargin)

        sql(
          """
            |INSERT OVERWRITE TABLE gluten_12474_tgt PARTITION (day)
            |SELECT k, max(v) AS m, day
            |FROM gluten_12474_src
            |GROUP BY day, k
            |""".stripMargin)

        checkAnswer(
          sql("SELECT k, m, day FROM gluten_12474_tgt"),
          sql("SELECT k, v, day FROM gluten_12474_src"))
      }
    }
  }

  testGluten(
    "SPARK-41914: v1 write with AQE and in-partition sorted - non-string partition column") {
    withSQLConf(SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "true") {
      withPlannedWrite {
        enabled =>
          withTable("t") {
            sql("""
                  |CREATE TABLE t(b INT, value STRING) USING PARQUET
                  |PARTITIONED BY (key INT)
                  |""".stripMargin)
            executeAndCheckOrdering(hasLogicalSort = true, orderingMatched = true) {
              sql("""
                    |INSERT INTO t
                    |SELECT b, value, key
                    |FROM testData JOIN testData2 ON key = a
                    |SORT BY key, value
                    |""".stripMargin)
            }

            // inspect the actually executed plan (that is different to executeAndCheckOrdering)
            assert(FileFormatWriter.executedPlan.isDefined)
            val executedPlan = FileFormatWriter.executedPlan.get

            val plan = if (enabled) {
              assert(executedPlan.isInstanceOf[ColumnarWriteFilesExec])
              executedPlan.asInstanceOf[ColumnarWriteFilesExec].child
            } else {
              executedPlan.transformDown { case a: AdaptiveSparkPlanExec => a.executedPlan }
            }

            // assert the outer most sort in the executed plan
            assert(
              plan
                .collectFirst {
                  case s: SortExec => s
                  case ns: SortExecTransformer => ns
                }
                .exists {
                  case SortExec(
                        Seq(
                          SortOrder(
                            AttributeReference("key", IntegerType, _, _),
                            Ascending,
                            NullsFirst,
                            _),
                          SortOrder(
                            AttributeReference("value", StringType, _, _),
                            Ascending,
                            NullsFirst,
                            _)
                        ),
                        false,
                        _,
                        _
                      ) =>
                    true
                  case SortExecTransformer(
                        Seq(
                          SortOrder(
                            AttributeReference("key", IntegerType, _, _),
                            Ascending,
                            NullsFirst,
                            _),
                          SortOrder(
                            AttributeReference("value", StringType, _, _),
                            Ascending,
                            NullsFirst,
                            _)
                        ),
                        false,
                        _,
                        _
                      ) =>
                    true
                  case _ => false
                },
              plan
            )
          }
      }
    }
  }

  testGluten("SPARK-41914: v1 write with AQE and in-partition sorted - string partition column") {
    withPlannedWrite {
      enabled =>
        withTable("t") {
          sql("""
                |CREATE TABLE t(key INT, b INT) USING PARQUET
                |PARTITIONED BY (value STRING)
                |""".stripMargin)
          executeAndCheckOrdering(
            hasLogicalSort = true,
            orderingMatched = true,
            hasEmpty2Null = enabled) {
            sql("""
                  |INSERT INTO t
                  |SELECT key, b, value
                  |FROM testData JOIN testData2 ON key = a
                  |SORT BY value, key
                  |""".stripMargin)
          }

          // inspect the actually executed plan (that is different to executeAndCheckOrdering)
          assert(FileFormatWriter.executedPlan.isDefined)
          val executedPlan = FileFormatWriter.executedPlan.get

          val plan = if (enabled) {
            assert(executedPlan.isInstanceOf[ColumnarWriteFilesExec])
            executedPlan.asInstanceOf[ColumnarWriteFilesExec].child
          } else {
            executedPlan.transformDown { case a: AdaptiveSparkPlanExec => a.executedPlan }
          }

          // assert the outer most sort in the executed plan
          assert(
            plan
              .collectFirst {
                case s: SortExec => s
                case ns: SortExecTransformer => ns
              }
              .exists {
                case SortExec(
                      Seq(
                        SortOrder(
                          AttributeReference("value", StringType, _, _),
                          Ascending,
                          NullsFirst,
                          _),
                        SortOrder(
                          AttributeReference("key", IntegerType, _, _),
                          Ascending,
                          NullsFirst,
                          _)
                      ),
                      false,
                      _,
                      _
                    ) =>
                  true
                case SortExecTransformer(
                      Seq(
                        SortOrder(
                          AttributeReference("value", StringType, _, _),
                          Ascending,
                          NullsFirst,
                          _),
                        SortOrder(
                          AttributeReference("key", IntegerType, _, _),
                          Ascending,
                          NullsFirst,
                          _)
                      ),
                      false,
                      _,
                      _
                    ) =>
                  true
                case _ => false
              },
            plan
          )
        }
    }
  }
}
