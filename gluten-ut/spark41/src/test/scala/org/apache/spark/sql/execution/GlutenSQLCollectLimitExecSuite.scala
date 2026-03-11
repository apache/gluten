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
package org.apache.spark.sql.execution

import org.apache.gluten.execution.ColumnarCollectLimitBaseExec

import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, GlutenSQLTestsTrait, Row}

class GlutenSQLCollectLimitExecSuite extends GlutenSQLTestsTrait {

  override def sparkConf: SparkConf = {
    super.sparkConf
      .set("spark.shuffle.manager", "org.apache.spark.shuffle.sort.ColumnarShuffleManager")
  }

  private def assertGlutenOperatorMatch[T: reflect.ClassTag](
      df: DataFrame,
      checkMatch: Boolean): Unit = {
    val executedPlan = getExecutedPlan(df)

    val operatorFound = executedPlan.exists {
      plan =>
        try {
          implicitly[reflect.ClassTag[T]].runtimeClass.isInstance(plan)
        } catch {
          case _: Throwable => false
        }
    }

    val assertionCondition = operatorFound == checkMatch
    val assertionMessage =
      if (checkMatch) {
        s"Operator ${implicitly[reflect.ClassTag[T]].runtimeClass.getSimpleName} not found " +
          s"in executed plan:\n $executedPlan"
      } else {
        s"Operator ${implicitly[reflect.ClassTag[T]].runtimeClass.getSimpleName} was found " +
          s"in executed plan:\n $executedPlan"
      }

    assert(assertionCondition, assertionMessage)
  }

  test("ColumnarCollectLimitExec - basic limit test") {
    val df = spark.range(0, 1000, 1).toDF("id").limit(5)
    val expectedData = Seq(Row(0L), Row(1L), Row(2L), Row(3L), Row(4L))

    checkAnswer(df, expectedData)

    assertGlutenOperatorMatch[ColumnarCollectLimitBaseExec](df, checkMatch = true)
  }

  test("ColumnarCollectLimitExec - with filter") {
    val df = spark
      .range(0, 20, 1)
      .toDF("id")
      .filter("id % 2 == 0")
      .limit(5)
    val expectedData = Seq(Row(0L), Row(2L), Row(4L), Row(6L), Row(8L))

    checkAnswer(df, expectedData)

    assertGlutenOperatorMatch[ColumnarCollectLimitBaseExec](df, checkMatch = true)
  }

  test("ColumnarCollectLimitExec - range with repartition") {

    val df = spark
      .range(0, 10, 1)
      .toDF("id")
      .repartition(3)
      .orderBy("id")
      .limit(3)
    val expectedData = Seq(Row(0L), Row(1L), Row(2L))

    checkAnswer(df, expectedData)
  }

  ignore("ColumnarCollectLimitExec - with distinct values") {
    val df = spark
      .range(0, 10, 1)
      .toDF("id")
      .select("id")
      .distinct()
      .limit(5)
    val expectedData = Seq(Row(0L), Row(1L), Row(2L), Row(3L), Row(4L))

    checkAnswer(df, expectedData)

    assertGlutenOperatorMatch[ColumnarCollectLimitBaseExec](df, checkMatch = true)
  }

  test("ColumnarCollectLimitExec - chained limit") {
    val df = spark
      .range(0, 10, 1)
      .toDF("id")
      .limit(8)
      .limit(3)
    val expectedData = Seq(Row(0L), Row(1L), Row(2L))

    checkAnswer(df, expectedData)

    assertGlutenOperatorMatch[ColumnarCollectLimitBaseExec](df, checkMatch = true)
  }

  test("ColumnarCollectLimitExec - limit after union") {
    val df1 = spark.range(0, 5).toDF("id")
    val df2 = spark.range(5, 10).toDF("id")
    val unionDf = df1.union(df2).limit(3)

    val expectedData = Seq(Row(0L), Row(1L), Row(2L))

    checkAnswer(unionDf, expectedData)

    assertGlutenOperatorMatch[ColumnarCollectLimitBaseExec](unionDf, checkMatch = true)
  }

  test("ColumnarCollectLimitExec - offset test") {
    val df1 = spark.range(0, 10, 1).toDF("id").limit(5).offset(2)
    val expectedData1 = Seq(Row(2L), Row(3L), Row(4L))

    checkAnswer(df1, expectedData1)
    assertGlutenOperatorMatch[ColumnarCollectLimitBaseExec](df1, checkMatch = true)

    val df2 = spark.range(0, 20, 1).toDF("id").limit(12).offset(5)
    val expectedData2 = Seq(Row(5L), Row(6L), Row(7L), Row(8L), Row(9L), Row(10L), Row(11L))

    checkAnswer(df2, expectedData2)
    assertGlutenOperatorMatch[ColumnarCollectLimitBaseExec](df2, checkMatch = true)

    val df3 = spark.range(0, 30, 1).toDF("id").limit(10).offset(3)
    val expectedData3 = Seq(Row(3L), Row(4L), Row(5L), Row(6L), Row(7L), Row(8L), Row(9L))

    checkAnswer(df3, expectedData3)
    assertGlutenOperatorMatch[ColumnarCollectLimitBaseExec](df3, checkMatch = true)

    val df4 = spark.range(0, 15, 1).toDF("id").limit(8).offset(4)
    val expectedData4 = Seq(Row(4L), Row(5L), Row(6L), Row(7L))

    checkAnswer(df4, expectedData4)
    assertGlutenOperatorMatch[ColumnarCollectLimitBaseExec](df4, checkMatch = true)

    val df5 = spark.range(0, 50, 1).toDF("id").limit(20).offset(10)
    val expectedData5 = Seq(
      Row(10L),
      Row(11L),
      Row(12L),
      Row(13L),
      Row(14L),
      Row(15L),
      Row(16L),
      Row(17L),
      Row(18L),
      Row(19L))

    checkAnswer(df5, expectedData5)
    assertGlutenOperatorMatch[ColumnarCollectLimitBaseExec](df5, checkMatch = true)
  }

  test("ColumnarCollectLimitExec - pure offset test") {
    val df1 = spark.range(0, 20, 1).toDF("id").offset(5)
    val expectedData1 = Seq(
      Row(5L),
      Row(6L),
      Row(7L),
      Row(8L),
      Row(9L),
      Row(10L),
      Row(11L),
      Row(12L),
      Row(13L),
      Row(14L),
      Row(15L),
      Row(16L),
      Row(17L),
      Row(18L),
      Row(19L))

    checkAnswer(df1, expectedData1)
    assertGlutenOperatorMatch[ColumnarCollectLimitBaseExec](df1, checkMatch = true)

    val df2 = spark.range(0, 50, 1).toDF("id").offset(10)
    val expectedData2 = (10L to 49L).map(Row(_))

    checkAnswer(df2, expectedData2)
    assertGlutenOperatorMatch[ColumnarCollectLimitBaseExec](df2, checkMatch = true)

    val df3 = spark.range(0, 100, 2).toDF("id").offset(15)
    val expectedData3 = (30L to 98L by 2).map(Row(_))

    checkAnswer(df3, expectedData3)
    assertGlutenOperatorMatch[ColumnarCollectLimitBaseExec](df3, checkMatch = true)

    val df4 = spark.range(0, 30, 1).toDF("id").offset(20)
    val expectedData4 = (20L to 29L).map(Row(_))

    checkAnswer(df4, expectedData4)
    assertGlutenOperatorMatch[ColumnarCollectLimitBaseExec](df4, checkMatch = true)

    val df5 = spark.range(0, 200, 5).toDF("id").offset(10)
    val expectedData5 = (50L to 195L by 5).map(Row(_))

    checkAnswer(df5, expectedData5)
    assertGlutenOperatorMatch[ColumnarCollectLimitBaseExec](df5, checkMatch = true)

    val df6 = spark.range(0, 5, 1).toDF("id").limit(10)
    val expectedData6 = (0L to 4L).map(Row(_))

    checkAnswer(df6, expectedData6)
  }

  test("ColumnarCollectLimitExec - offset with filter") {
    val df = spark.range(0, 10, 1).toDF("id").filter("id % 2 == 0").limit(5).offset(2)
    val expectedData = Seq(Row(4L), Row(6L), Row(8L))

    checkAnswer(df, expectedData)
    assertGlutenOperatorMatch[ColumnarCollectLimitBaseExec](df, checkMatch = true)
  }

  test("ColumnarCollectLimitExec - offset after union") {
    val df1 = spark.range(0, 5).toDF("id")
    val df2 = spark.range(5, 10).toDF("id")
    val unionDf = df1.union(df2).limit(6).offset(3)

    val expectedData = Seq(Row(3L), Row(4L), Row(5L))
    checkAnswer(unionDf, expectedData)
    assertGlutenOperatorMatch[ColumnarCollectLimitBaseExec](unionDf, checkMatch = true)
  }

  test("ColumnarCollectLimitExec - single partition with limit, offset, and limit + offset") {

    val singlePartitionDF = spark.range(0, 10, 1).toDF("id").coalesce(1)

    val limitDF = singlePartitionDF.limit(5)
    val expectedLimitData = Seq(Row(0L), Row(1L), Row(2L), Row(3L), Row(4L))
    checkAnswer(limitDF, expectedLimitData)
    assertGlutenOperatorMatch[ColumnarCollectLimitBaseExec](limitDF, checkMatch = true)

    val offsetDF = singlePartitionDF.offset(3)
    val expectedOffsetData = Seq(Row(3L), Row(4L), Row(5L), Row(6L), Row(7L), Row(8L), Row(9L))
    checkAnswer(offsetDF, expectedOffsetData)
    assertGlutenOperatorMatch[ColumnarCollectLimitBaseExec](offsetDF, checkMatch = true)

    val limitOffsetDF = singlePartitionDF.limit(5).offset(2)
    val expectedLimitOffsetData = Seq(Row(2L), Row(3L), Row(4L))
    checkAnswer(limitOffsetDF, expectedLimitOffsetData)
    assertGlutenOperatorMatch[ColumnarCollectLimitBaseExec](limitOffsetDF, checkMatch = true)
  }

}
