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
import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.execution.QueryExecution
import org.apache.spark.sql.util.QueryExecutionListener

import java.util.concurrent.{CountDownLatch, TimeUnit}

class GlutenSQLCollectTailExecSuite extends WholeStageTransformerSuite {

  override protected val resourcePath: String = "N/A"
  override protected val fileFormat: String = "N/A"

  override protected def sparkConf: SparkConf = {
    super.sparkConf
      .set("spark.shuffle.manager", "org.apache.spark.shuffle.sort.ColumnarShuffleManager")
  }

  /**
   * Helper method for: 1) Registers a Listener that checks for "ColumnarCollectTail" in the final
   * plan. 2) Calls df.tail(tailCount) to physically trigger CollectTailExec. 3) Asserts the
   * returned rows match expectedRows.
   */
  private def verifyTailExec(df: DataFrame, expectedRows: Seq[Row], tailCount: Int): Unit = {

    val latch = new CountDownLatch(1)

    @volatile var listenerException: Option[Throwable] = None

    class TailExecListener extends QueryExecutionListener {

      override def onSuccess(funcName: String, qe: QueryExecution, durationNs: Long): Unit = {
        try {
          val latestPlan = qe.executedPlan.toString()
          if (!latestPlan.contains("ColumnarCollectTail")) {
            throw new Exception("ColumnarCollectTail not found in: " + latestPlan)
          }
        } catch {
          case ex: Throwable =>
            listenerException = Some(ex)
        } finally {
          latch.countDown()
        }
      }

      override def onFailure(funcName: String, qe: QueryExecution, error: Exception): Unit = {
        listenerException = Some(error)
        latch.countDown()
      }
    }

    val tailExecListener = new TailExecListener()
    spark.listenerManager.register(tailExecListener)

    val tailArray = df.tail(tailCount)
    latch.await(10, TimeUnit.SECONDS)
    listenerException.foreach(throw _)

    assert(
      tailArray.sameElements(expectedRows),
      s"""
         |Tail output [${tailArray.mkString(", ")}]
         |did not match expected [${expectedRows.mkString(", ")}].
         """.stripMargin
    )

    spark.listenerManager.unregister(tailExecListener)
  }

  test("ColumnarCollectTailExec - verify CollectTailExec in physical plan") {
    val df = spark.range(0, 10000, 1).toDF("id").orderBy("id")
    val expected = Seq(Row(9996L), Row(9997L), Row(9998L), Row(9999L))
    verifyTailExec(df, expected, tailCount = 4)
  }

  test("ColumnarCollectTailExec - basic tail test") {
    val df = spark.range(0, 10000, 1).toDF("id").orderBy("id")
    val expected = (3000L to 9999L).map(Row(_))
    verifyTailExec(df, expected, tailCount = 7000)
  }

  test("ColumnarCollectTailExec - with filter") {
    val df = spark.range(0, 10000, 1).toDF("id").filter("id % 2 == 0").orderBy("id")
    val expected = (9990L to 9998L by 2).map(Row(_)).takeRight(5)
    verifyTailExec(df, expected, tailCount = 5)
  }

  test("ColumnarCollectTailExec - range with repartition") {
    val df = spark.range(0, 10000, 1).toDF("id").repartition(3).orderBy("id")
    val expected = (9997L to 9999L).map(Row(_))
    verifyTailExec(df, expected, tailCount = 3)
  }

  test("ColumnarCollectTailExec - with distinct values") {
    val df = spark.range(0, 10000, 1).toDF("id").distinct().orderBy("id")
    val expected = (9995L to 9999L).map(Row(_))
    verifyTailExec(df, expected, tailCount = 5)
  }

  test("ColumnarCollectTailExec - chained tail") {
    val df = spark.range(0, 10000, 1).toDF("id").orderBy("id")
    val expected = (9992L to 9999L).map(Row(_))
    verifyTailExec(df, expected, tailCount = 8)
  }

  test("ColumnarCollectTailExec - tail after union") {
    val df1 = spark.range(0, 5000).toDF("id")
    val df2 = spark.range(5000, 10000).toDF("id")
    val unionDf = df1.union(df2).orderBy("id")
    val expected = (9997L to 9999L).map(Row(_))
    verifyTailExec(unionDf, expected, tailCount = 3)
  }

  test("ColumnarCollectTailExec - tail spans across two columnar batches") {
    val df = spark.range(0, 4101).toDF("id").orderBy("id")
    val expected = (4095L to 4100L).map(Row(_))
    verifyTailExec(df, expected, tailCount = 6)
  }

}
