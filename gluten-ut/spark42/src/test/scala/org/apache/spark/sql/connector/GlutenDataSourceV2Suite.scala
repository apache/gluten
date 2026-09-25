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
package org.apache.spark.sql.connector

import org.apache.gluten.execution.SortExecTransformer

import org.apache.spark.sql.{GlutenSQLTestsBaseTrait, Row}
import org.apache.spark.sql.execution.ColumnarShuffleExchangeExec
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.functions.row_number
import org.apache.spark.sql.internal.SQLConf

import test.org.apache.spark.sql.connector.{JavaOrderAndPartitionAwareDataSource, JavaPartitionAwareDataSource}

class GlutenDataSourceV2Suite extends DataSourceV2Suite with GlutenSQLTestsBaseTrait {
  import testImplicits._

  testGluten("partitioning reporting") {
    import org.apache.spark.sql.functions.{count, sum}
    withSQLConf(SQLConf.V2_BUCKETING_ENABLED.key -> "true") {
      Seq(classOf[PartitionAwareDataSource], classOf[JavaPartitionAwareDataSource]).foreach {
        cls =>
          withClue(cls.getName) {
            val df = spark.read.format(cls.getName).load()
            checkAnswer(df, Seq(Row(1, 4), Row(1, 4), Row(3, 6), Row(2, 6), Row(4, 2), Row(4, 2)))

            val groupByColA = df.groupBy(col("i")).agg(sum(col("j")))
            checkAnswer(groupByColA, Seq(Row(1, 8), Row(2, 6), Row(3, 6), Row(4, 4)))
            assert(collectFirst(groupByColA.queryExecution.executedPlan) {
              case e: ColumnarShuffleExchangeExec => e
            }.isEmpty)

            val groupByColAB = df.groupBy(col("i"), col("j")).agg(count("*"))
            checkAnswer(groupByColAB, Seq(Row(1, 4, 2), Row(2, 6, 1), Row(3, 6, 1), Row(4, 2, 2)))
            assert(collectFirst(groupByColAB.queryExecution.executedPlan) {
              case e: ColumnarShuffleExchangeExec => e
            }.isEmpty)

            val groupByColB = df.groupBy(col("j")).agg(sum(col("i")))
            checkAnswer(groupByColB, Seq(Row(2, 8), Row(4, 2), Row(6, 5)))
            assert(collectFirst(groupByColB.queryExecution.executedPlan) {
              case e: ColumnarShuffleExchangeExec => e
            }.isDefined)

            val groupByAPlusB = df.groupBy(col("i") + col("j")).agg(count("*"))
            checkAnswer(groupByAPlusB, Seq(Row(5, 2), Row(6, 2), Row(8, 1), Row(9, 1)))
            assert(collectFirst(groupByAPlusB.queryExecution.executedPlan) {
              case e: ColumnarShuffleExchangeExec => e
            }.isDefined)
          }
      }
    }
  }

  testGluten("ordering and partitioning reporting") {
    withSQLConf(SQLConf.V2_BUCKETING_ENABLED.key -> "true") {
      Seq(
        classOf[OrderAndPartitionAwareDataSource],
        classOf[JavaOrderAndPartitionAwareDataSource]
      ).foreach {
        cls =>
          withClue(cls.getName) {
            // we test report ordering (together with report partitioning) with these
            // transformations:
            // - groupBy("i").flatMapGroups:
            //   hash-partitions by "i" and sorts each partition by "i"
            //   requires partitioning and sort by "i"
            // - aggregation function over window partitioned by "i" and ordered by "j":
            //   hash-partitions by "i" and sorts each partition by "j"
            //   requires partitioning by "i" and sort by "i" and "j"
            Seq(
              // with no partitioning and no order, we expect shuffling AND sorting
              (None, None, (true, true), (true, true)),
              // partitioned by i and no order, we expect NO shuffling BUT sorting
              (Some("i"), None, (false, true), (false, true)),
              // partitioned by i and in-partition sorted by i,
              // we expect NO shuffling AND sorting for groupBy but sorting for window function
              (Some("i"), Some("i"), (false, false), (false, true)),
              // partitioned by i and in-partition sorted by j, we expect NO shuffling BUT sorting
              (Some("i"), Some("j"), (false, true), (false, true)),
              // partitioned by i and in-partition sorted by i,j, we expect NO shuffling NOR sorting
              (Some("i"), Some("i,j"), (false, false), (false, false)),
              // partitioned by j and in-partition sorted by i, we expect shuffling AND sorting
              (Some("j"), Some("i"), (true, true), (true, true)),
              // partitioned by j and in-partition sorted by i,j, we expect shuffling and sorting
              (Some("j"), Some("i,j"), (true, true), (true, true))
            ).foreach {
              testParams =>
                val (partitionKeys, orderKeys, groupByExpects, windowFuncExpects) = testParams

                withClue(f"${partitionKeys.orNull} ${orderKeys.orNull}") {
                  val df = spark.read
                    .option("partitionKeys", partitionKeys.orNull)
                    .option("orderKeys", orderKeys.orNull)
                    .format(cls.getName)
                    .load()
                  checkAnswer(
                    df,
                    Seq(Row(1, 4), Row(1, 5), Row(3, 5), Row(2, 6), Row(4, 1), Row(4, 2)))

                  // groupBy(i).flatMapGroups
                  {
                    val groupBy = df
                      .groupBy($"i")
                      .as[Int, (Int, Int)]
                      .flatMapGroups {
                        (i: Int, it: Iterator[(Int, Int)]) => Iterator.single((i, it.length))
                      }
                    checkAnswer(
                      groupBy.toDF(),
                      Seq(Row(1, 2), Row(2, 1), Row(3, 1), Row(4, 2))
                    )

                    val (shuffleExpected, sortExpected) = groupByExpects
                    assert(collectFirst(groupBy.queryExecution.executedPlan) {
                      case e: ColumnarShuffleExchangeExec => e
                    }.isDefined === shuffleExpected)
                    assert(collectFirst(groupBy.queryExecution.executedPlan) {
                      case e: SortExecTransformer => e
                    }.isDefined === sortExpected)
                  }

                  // aggregation function over window partitioned by i and ordered by j
                  {
                    val windowPartByColIOrderByColJ = df.withColumn(
                      "no",
                      row_number().over(Window.partitionBy(col("i")).orderBy(col("j"))))
                    checkAnswer(
                      windowPartByColIOrderByColJ,
                      Seq(
                        Row(1, 4, 1),
                        Row(1, 5, 2),
                        Row(2, 6, 1),
                        Row(3, 5, 1),
                        Row(4, 1, 1),
                        Row(4, 2, 2)
                      ))

                    val (shuffleExpected, sortExpected) = windowFuncExpects
                    assert(collectFirst(windowPartByColIOrderByColJ.queryExecution.executedPlan) {
                      case e: ColumnarShuffleExchangeExec => e
                    }.isDefined === shuffleExpected)
                    assert(collectFirst(windowPartByColIOrderByColJ.queryExecution.executedPlan) {
                      case e: SortExecTransformer => e
                    }.isDefined === sortExpected)
                  }
                }
            }
          }
      }
    }
  }
}
