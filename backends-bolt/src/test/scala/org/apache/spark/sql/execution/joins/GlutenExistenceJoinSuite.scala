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
package org.apache.spark.sql.execution.joins

import org.apache.gluten.execution.{BoltBroadcastNestedLoopJoinExecTransformer, BoltWholeStageTransformerSuite}

import org.apache.spark.sql.{Column, DataFrame, Row}
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.plans.ExistenceJoin
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.classic.ClassicConversions._
import org.apache.spark.sql.classic.ClassicDataset
import org.apache.spark.sql.test.SQLTestUtils
import org.apache.spark.sql.types._

class GlutenExistenceJoinSuite extends BoltWholeStageTransformerSuite with SQLTestUtils {

  // TODO: remove this if we can suppress unused import error.
  locally {
    new ColumnConstructorExt(Column)
  }

  override protected val resourcePath: String = "N/A"
  override protected val fileFormat: String = "N/A"

  test("existence join with broadcast nested loop join") {

    spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "-1")
    spark.conf.set("spark.sql.join.preferSortMergeJoin", "false")

    val left: DataFrame = spark.createDataFrame(
      sparkContext.parallelize(
        Seq(
          Row(1, "a"),
          Row(2, "b"),
          Row(3, "c")
        )),
      new StructType().add("id", IntegerType).add("val", StringType)
    )

    val right: DataFrame = spark.createDataFrame(
      sparkContext.parallelize(
        Seq(
          Row(1, "x"),
          Row(3, "y")
        )),
      new StructType().add("id", IntegerType).add("val", StringType)
    )

    val leftPlan = left.logicalPlan
    val rightPlan = right.logicalPlan

    val existsAttr = AttributeReference("exists", BooleanType, nullable = false)()

    val joinCondition: Expression = LessThan(leftPlan.output.head, rightPlan.output.head)

    val existenceJoin = Join(
      left = leftPlan,
      right = rightPlan,
      joinType = ExistenceJoin(existsAttr),
      condition = Some(joinCondition),
      hint = JoinHint.NONE
    )

    val project = Project(
      projectList = leftPlan.output :+ existsAttr,
      child = existenceJoin
    )

    val df = ClassicDataset.ofRows(spark, project)

    assert(existenceJoin.joinType == ExistenceJoin(existsAttr))
    assert(existenceJoin.condition.contains(joinCondition))
    val expected = Seq(
      Row(1, "a", true),
      Row(2, "b", true),
      Row(3, "c", false)
    )
    assert(df.collect() === expected)
    val count = collect(df.queryExecution.executedPlan) {
      case _: BoltBroadcastNestedLoopJoinExecTransformer => true
    }.size

    assert(count == 1, s"Expected 1 BoltBroadcastNestedLoopJoinExecTransformer, but found $count")
  }
}
