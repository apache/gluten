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
package org.apache.gluten.extension.columnar.validator

import org.apache.spark.SparkFunSuite
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.util.GenericArrayData
import org.apache.spark.sql.execution.{FilterExec, LeafExecNode, ProjectExec, SparkPlan}
import org.apache.spark.sql.types._

class GlutenTimestampNtzValidatorSuite extends SparkFunSuite {
  import GlutenTimestampNtzValidatorSuite.InputPlan

  private val id = AttributeReference("id", IntegerType, nullable = false)()
  private val input = InputPlan(Seq(id))

  private def validator(supportsNtz: Boolean, enableValidation: Boolean): Validator = {
    new Validators.FallbackByTimestampNTZ(
      Some(new GlutenTimestampNtzValidatorSuite.TestConfig(enableValidation)),
      supportsNtz)
  }

  test("timestamp literals inside projections and filters respect backend support") {
    for {
      dataType <- Seq(TimestampNTZType, TimestampType)
      supportsNtz <- Seq(false, true)
      enableValidation <- Seq(false, true)
    } {
      val timestamps = Literal(
        new GenericArrayData(Array(1704067200000000L, 1704070800000000L)),
        ArrayType(dataType, containsNull = false))
      val hour = Hour(
        ElementAt(timestamps, Add(id, Literal(1)), failOnError = false),
        Some("America/Los_Angeles"))
      val plans = Seq[SparkPlan](
        ProjectExec(Seq(Alias(hour, "hour")()), input),
        FilterExec(EqualTo(hour, Literal(0)), input))
      plans.foreach {
        plan =>
          withClue(s"$dataType, supportsNtz=$supportsNtz, validation=$enableValidation, $plan") {
            assert(plan.output.forall(_.dataType == IntegerType))
            assert(plan.children.forall(_.output.forall(_.dataType == IntegerType)))
            val expected = if (dataType == TimestampNTZType && !supportsNtz) {
              Validator.Failed(s"${plan.nodeName} has TimestampNTZType in expressions")
            } else {
              Validator.Passed
            }
            assert(validator(supportsNtz, enableValidation).validate(plan) == expected)
          }
      }
    }
  }

  test("unsupported backends reject NTZ nested in literal types") {
    val structType = StructType(Seq(StructField("timestamps", ArrayType(TimestampNTZType))))
    val dataTypes = Seq(
      TimestampNTZType,
      ArrayType(TimestampNTZType),
      structType,
      MapType(TimestampNTZType, StringType),
      MapType(StringType, ArrayType(structType)))
    for {
      dataType <- dataTypes
      literal <- Seq(Literal.default(dataType), Literal.create(null, dataType))
      enableValidation <- Seq(false, true)
    } {
      val plan = ProjectExec(Seq(Alias(IsNull(literal), "missing")()), input)
      withClue(s"$literal, validation=$enableValidation") {
        assert(plan.output.map(_.dataType) == Seq(BooleanType))
        assert(
          validator(supportsNtz = false, enableValidation = enableValidation).validate(plan) ==
            Validator.Failed("Project has TimestampNTZType in expressions"))
      }
    }
  }

  test("existing NTZ input schema validation is unchanged") {
    val timestamp = AttributeReference("ts", TimestampNTZType)()
    val plan = ProjectExec(
      Seq(Alias(Hour(timestamp, Some("UTC")), "hour")()),
      InputPlan(Seq(timestamp)))
    for {
      supportsNtz <- Seq(false, true)
      enableValidation <- Seq(false, true)
    } {
      val expected = if (supportsNtz && !enableValidation) {
        Validator.Passed
      } else {
        Validator.Failed("Project has TimestampNTZType in input/output schema")
      }
      assert(validator(supportsNtz, enableValidation).validate(plan) == expected)
    }
  }
}

object GlutenTimestampNtzValidatorSuite {
  private case class InputPlan(override val output: Seq[Attribute]) extends LeafExecNode {
    override protected def doExecute(): RDD[InternalRow] =
      throw new UnsupportedOperationException("This plan is only used for schema validation.")
  }

  class TestConfig(val enableTimestampNtzValidation: Boolean)
}
