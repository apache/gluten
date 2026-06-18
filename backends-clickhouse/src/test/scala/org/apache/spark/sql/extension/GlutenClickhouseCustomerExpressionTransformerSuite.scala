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
package org.apache.spark.sql.extension

import org.apache.gluten.backendsapi.clickhouse.CHConfig
import org.apache.gluten.execution.{GlutenClickHouseWholeStageTransformerSuite, ProjectExecTransformer}
import org.apache.gluten.expression.ExpressionConverter

import org.apache.spark.SparkConf
import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.FunctionIdentifier
import org.apache.spark.sql.catalyst.analysis.FunctionRegistryBase
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.util.{IntervalUtils, TypeUtils}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.CalendarInterval

case class CustomAdd(
    left: Expression,
    right: Expression,
    override val failOnError: Boolean = SQLConf.get.ansiEnabled)
  extends BinaryArithmetic
  with CustomAdd.Compatibility {

  def this(left: Expression, right: Expression) = this(left, right, SQLConf.get.ansiEnabled)

  override def inputType: AbstractDataType = TypeCollection.NumericAndInterval

  override def symbol: String = "+"

  override def decimalMethod: String = "$plus"

  override def calendarIntervalMethod: String = if (failOnError) "addExact" else "add"

  private lazy val numeric = TypeUtils.getNumeric(dataType, failOnError)

  override protected def nullSafeEval(input1: Any, input2: Any): Any = dataType match {
    case CalendarIntervalType if failOnError =>
      IntervalUtils.addExact(
        input1.asInstanceOf[CalendarInterval],
        input2.asInstanceOf[CalendarInterval])
    case CalendarIntervalType =>
      IntervalUtils.add(
        input1.asInstanceOf[CalendarInterval],
        input2.asInstanceOf[CalendarInterval])
    case _: DayTimeIntervalType =>
      Math.addExact(input1.asInstanceOf[Long], input2.asInstanceOf[Long])
    case _: YearMonthIntervalType =>
      Math.addExact(input1.asInstanceOf[Int], input2.asInstanceOf[Int])
    case _ => numeric.plus(input1, input2)
  }

  override def exactMathMethod: Option[String] = Some("addExact")

  override protected def withNewChildrenInternal(
      newLeft: Expression,
      newRight: Expression
  ): CustomAdd = copy(left = newLeft, right = newRight)

  override protected val evalMode: EvalMode.Value = EvalMode.LEGACY
}

object CustomAdd {
  trait Compatibility {
    protected val evalMode: EvalMode.Value
  }
}

class GlutenClickhouseCustomerExpressionTransformerSuite
  extends GlutenClickHouseWholeStageTransformerSuite {

  override def sparkConf: SparkConf = {
    super.sparkConf
      .set("spark.sql.adaptive.enabled", "false")
      .set(
        CHConfig.EXTENDED_EXPRESSION_TRAN_CONF.key,
        "org.apache.spark.sql.extension.CustomerExpressionTransformer")
  }

  override def beforeAll(): Unit = {
    super.beforeAll()
    val (expressionInfo, builder) =
      FunctionRegistryBase.build[CustomAdd]("custom_add", None)
    spark.sessionState.functionRegistry.registerFunction(
      FunctionIdentifier.apply("custom_add"),
      expressionInfo,
      builder
    )
  }

  test("test custom expression transformer") {
    spark
      .createDataFrame(Seq((1, 1.1), (2, 2.2)))
      .createOrReplaceTempView("custom_table")

    val df = spark.sql(s"""
                          |select custom_add(_1, 100), custom_add(_2, 200) from custom_table
                          |""".stripMargin)
    checkAnswer(df, Seq(Row(101, 201.1), Row(102, 202.2)))

    val projectTransformers = df.queryExecution.executedPlan.collect {
      case p: ProjectExecTransformer => p
    }
    assert(!projectTransformers.isEmpty, s"query plan: ${df.queryExecution.executedPlan}")

    val projectExecTransformer = projectTransformers(0)
    val childOut = projectExecTransformer.child.output
    val exprTransformers =
      ExpressionConverter
        .replaceWithExpressionTransformer(
          projectExecTransformer.projectList(0).asInstanceOf[Alias].child,
          attributeSeq = childOut)
    assert(exprTransformers.isInstanceOf[CustomAddExpressionTransformer])
  }
}
