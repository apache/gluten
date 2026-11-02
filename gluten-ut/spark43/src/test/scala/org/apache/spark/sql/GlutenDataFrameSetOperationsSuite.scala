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
package org.apache.spark.sql

import org.apache.gluten.execution.ColumnarUnionExec

import org.apache.spark.SparkConf
import org.apache.spark.sql.catalyst.plans.physical.UnknownPartitioning
import org.apache.spark.sql.execution.ColumnarShuffleExchangeExec
import org.apache.spark.sql.execution.exchange.ReusedExchangeExec
import org.apache.spark.sql.internal.SQLConf

class GlutenDataFrameSetOperationsSuite
  extends DataFrameSetOperationsSuite
  with GlutenSQLTestsTrait {

  override def sparkConf: SparkConf =
    super.sparkConf
      .set("spark.gluten.sql.columnar.backend.ch.enable.coalesce.project.union", "false")
      .set("spark.gluten.sql.columnar.backend.ch.enable.coalesce.aggregation.union", "false")
      .set(SQLConf.SHUFFLE_PARTITIONS.key, "5")

  import testImplicits._

  testGluten("SPARK-52921: union partitioning - reused shuffle") {
    withSQLConf(SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "false") {
      val df1 = Seq((1, 2, 4), (1, 3, 5), (2, 2, 3), (2, 4, 5)).toDF("a", "b", "c")
      val df2 = Seq((1, 2, 4), (1, 3, 5), (2, 2, 3), (2, 4, 5)).toDF("a", "b", "c")

      val union = df1.repartition($"a").union(df2.repartition($"a"))
      val unionExec = union.queryExecution.executedPlan.collect { case u: ColumnarUnionExec => u }
      assert(unionExec.size == 1)

      val shuffle = df1.repartition($"a").queryExecution.executedPlan.collect {
        case s: ColumnarShuffleExchangeExec => s
      }
      assert(shuffle.size == 1)

      val reuseShuffle = union.queryExecution.executedPlan.collect {
        case r: ReusedExchangeExec => r
      }
      assert(reuseShuffle.size == 1)

      val childPartitioning = shuffle.head.outputPartitioning
      val partitioning = unionExec.head.outputPartitioning
      assert(partitioning == childPartitioning)
    }
  }

  testGluten("SPARK-52921: union partitioning - semantic equality") {
    val df1 = Seq((1, 2, 4), (1, 3, 5), (2, 2, 3), (2, 4, 5)).toDF("a", "b", "c")
    val df2 = Seq((4, 1, 5), (2, 4, 6), (1, 4, 2), (3, 5, 1)).toDF("d", "e", "f")

    val correctResult = withSQLConf(SQLConf.UNION_OUTPUT_PARTITIONING.key -> "false") {
      df1.repartition($"a").union(df2.repartition($"d")).collect()
    }

    Seq(true, false).foreach {
      enabled =>
        withSQLConf(
          SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "false",
          SQLConf.UNION_OUTPUT_PARTITIONING.key -> enabled.toString) {

          val union = df1.repartition($"a").union(df2.repartition($"d"))
          val unionExec = union.queryExecution.executedPlan.collect {
            case u: ColumnarUnionExec => u
          }
          assert(unionExec.size == 1)

          val shuffle = df1.repartition($"a").queryExecution.executedPlan.collect {
            case s: ColumnarShuffleExchangeExec => s
          }
          assert(shuffle.size == 1)

          val childPartitioning = shuffle.head.outputPartitioning
          val partitioning = unionExec.head.outputPartitioning
          if (enabled) {
            assert(partitioning == childPartitioning)
          }

          checkAnswer(union, correctResult)

          // Avoid unnecessary shuffle if union output partitioning is enabled
          val shuffledUnion = union.repartition($"a")
          val shuffleNumBefore = union.queryExecution.executedPlan.collect {
            case s: ColumnarShuffleExchangeExec => s
          }
          val shuffleNumAfter = shuffledUnion.queryExecution.executedPlan.collect {
            case s: ColumnarShuffleExchangeExec => s
          }

          if (enabled) {
            assert(shuffleNumBefore.size == shuffleNumAfter.size)
          } else {
            assert(shuffleNumBefore.size + 1 == shuffleNumAfter.size)
          }
          checkAnswer(union, shuffledUnion)
        }
    }
  }

  testGluten("SPARK-52921: union partitioning - range partitioning") {
    val df1 = Seq((1, 2, 4), (1, 3, 5), (2, 2, 3), (2, 4, 5)).toDF("a", "b", "c")
    val df2 = Seq((4, 1, 5), (2, 4, 6), (1, 4, 2), (3, 5, 1)).toDF("d", "e", "f")

    val correctResult = withSQLConf(SQLConf.UNION_OUTPUT_PARTITIONING.key -> "false") {
      df1.repartitionByRange($"a").union(df2.repartitionByRange($"d")).collect()
    }

    Seq(true, false).foreach {
      enabled =>
        withSQLConf(
          SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "false",
          SQLConf.UNION_OUTPUT_PARTITIONING.key -> enabled.toString) {

          val union = df1.repartitionByRange($"a").union(df2.repartitionByRange($"d"))
          val unionExec = union.queryExecution.executedPlan.collect {
            case u: ColumnarUnionExec => u
          }
          assert(unionExec.size == 1)

          // For range partitioning, even children have the same partitioning,
          // the union output partitioning is still UnknownPartitioning.
          val partitioning = unionExec.head.outputPartitioning
          assert(partitioning.isInstanceOf[UnknownPartitioning])

          checkAnswer(union, correctResult)
        }
    }
  }
}
