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
import org.apache.spark.sql.catalyst.analysis.ResolvedPartitionSpec
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.aggregate.{CollectList, Count, First, Last, Max, Min, Sum}
import org.apache.spark.sql.catalyst.plans.physical.{HashPartitioning, RangePartitioning, SinglePartition}
import org.apache.spark.sql.catalyst.util.GenericArrayData
import org.apache.spark.sql.connector.catalog.SupportsPartitionManagement
import org.apache.spark.sql.execution.{FilterExec, LeafExecNode, ProjectExec, SortExec, SparkPlan}
import org.apache.spark.sql.execution.datasources.v2.DropPartitionExec
import org.apache.spark.sql.execution.exchange.ShuffleExchangeExec
import org.apache.spark.sql.execution.window.WindowExec
import org.apache.spark.sql.types._

import org.mockito.Mockito.mock

class GlutenTimestampNtzValidatorSuite extends SparkFunSuite {
  import GlutenTimestampNtzValidatorSuite.InputPlan

  private val id = AttributeReference("id", IntegerType, nullable = false)()
  private val input = InputPlan(Seq(id))

  private def validator(supportsNtz: Boolean, enableValidation: Boolean): Validator = {
    new Validators.FallbackByTimestampNTZ(
      Some(new GlutenTimestampNtzValidatorSuite.TestConfig(enableValidation)),
      supportsNtz)
  }

  private def windowPlan(spec: WindowSpecDefinition): WindowExec = {
    val sum = Alias(WindowExpression(Sum(id).toAggregateExpression(), spec), "sum")()
    WindowExec(Seq(sum), spec.partitionSpec, spec.orderSpec, input)
  }

  private val ntz = AttributeReference("ntz", TimestampNTZType)()
  private val tz = AttributeReference("tz", TimestampType)()
  private val ntzInput = InputPlan(Seq(id, ntz))
  // A non-Attribute key that's genuinely TimestampNTZType.
  private val computedNtz = Cast(tz, TimestampNTZType)

  private def ntzWindowPlan(
      windowFunctions: Seq[Expression],
      partitionSpec: Seq[Expression] = Seq(id),
      orderSpec: Seq[SortOrder] = Seq(SortOrder(id, Ascending))): WindowExec = {
    val spec = WindowSpecDefinition(
      partitionSpec,
      orderSpec,
      SpecifiedWindowFrame(RowFrame, UnboundedPreceding, CurrentRow))
    val aliases = windowFunctions.zipWithIndex.map {
      case (fn, i) => Alias(WindowExpression(fn, spec), s"w$i")()
    }
    WindowExec(aliases, spec.partitionSpec, spec.orderSpec, ntzInput)
  }

  /** Passed only if supported, NTZ is backed, and validation isn't forced; else Failed. */
  private def checkNtzWhitelist(
      plan: SparkPlan,
      expectSupported: Boolean,
      failureMessage: String,
      clue: String): Unit = {
    for {
      supportsNtz <- Seq(false, true)
      enableValidation <- Seq(false, true)
    } {
      withClue(s"$clue, supportsNtz=$supportsNtz, validation=$enableValidation") {
        val expected = if (expectSupported && supportsNtz && !enableValidation) {
          Validator.Passed
        } else {
          Validator.Failed(failureMessage)
        }
        assert(validator(supportsNtz, enableValidation).validate(plan) == expected)
      }
    }
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

  test("window metadata without a data type does not fail NTZ validation") {
    for {
      frameType <- Seq(RowFrame, RangeFrame)
      supportsNtz <- Seq(false, true)
      enableValidation <- Seq(false, true)
    } {
      val spec = WindowSpecDefinition(
        Seq(id),
        Seq(SortOrder(id, Ascending)),
        SpecifiedWindowFrame(frameType, UnboundedPreceding, CurrentRow))
      withClue(s"$frameType, supportsNtz=$supportsNtz, validation=$enableValidation") {
        assert(validator(
          supportsNtz,
          enableValidation).validate(windowPlan(spec)) == Validator.Passed)
      }
    }
  }

  test("NTZ expressions in window specifications and frame bounds still trigger fallback") {
    for {
      dataType <- Seq(TimestampNTZType, TimestampType)
      supportsNtz <- Seq(false, true)
      enableValidation <- Seq(false, true)
    } {
      val hour = Hour(Literal(1704070800000000L, dataType), Some("UTC"))
      val spec = WindowSpecDefinition(
        Seq(id),
        Seq(SortOrder(id, Ascending)),
        SpecifiedWindowFrame(RowFrame, UnboundedPreceding, CurrentRow))
      val specs = Seq(
        spec.copy(partitionSpec = Seq(hour)),
        spec.copy(orderSpec = Seq(SortOrder(hour, Ascending))),
        spec.copy(frameSpecification = SpecifiedWindowFrame(RowFrame, CurrentRow, hour))
      )
      specs.foreach {
        windowSpec =>
          val plan = windowPlan(windowSpec)
          withClue(
            s"$dataType, supportsNtz=$supportsNtz, validation=$enableValidation, $windowSpec") {
            assert(plan.output.map(_.dataType) == Seq(IntegerType, LongType))
            val expected = if (dataType == TimestampNTZType && !supportsNtz) {
              Validator.Failed("Window has TimestampNTZType in expressions")
            } else {
              Validator.Passed
            }
            assert(validator(supportsNtz, enableValidation).validate(plan) == expected)
          }
      }
    }
  }

  test("partition command metadata without a data type does not fail NTZ validation") {
    val plan = DropPartitionExec(
      mock(classOf[SupportsPartitionManagement]),
      Seq(ResolvedPartitionSpec(Seq("id"), InternalRow(1))),
      ignoreIfNotExists = false,
      purge = false,
      refreshCache = () => ()
    )
    for {
      supportsNtz <- Seq(false, true)
      enableValidation <- Seq(false, true)
    } {
      withClue(s"supportsNtz=$supportsNtz, validation=$enableValidation") {
        assert(plan.output.isEmpty)
        assert(validator(supportsNtz, enableValidation).validate(plan) == Validator.Passed)
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

  test("supported NTZ window functions pass, unsupported ones fail") {
    val supported = Seq(
      "Lag" -> new Lag(ntz),
      "Lead" -> new Lead(ntz),
      "NthValue" -> new NthValue(ntz, Literal(1)),
      "First" -> new First(ntz).toAggregateExpression(),
      "Last" -> new Last(ntz).toAggregateExpression(),
      "Count" -> Count(Seq(ntz)).toAggregateExpression(),
      "Min" -> Min(ntz).toAggregateExpression(),
      "Max" -> Max(ntz).toAggregateExpression()
    )
    val unsupported = Seq(
      "Sum" -> Sum(ntz).toAggregateExpression(),
      "CollectList" -> CollectList(ntz).toAggregateExpression())
    (supported.map { case (n, f) => (n, f, true) } ++
      unsupported.map { case (n, f) => (n, f, false) }).foreach {
      case (name, windowFunction, expectSupported) =>
        checkNtzWhitelist(
          ntzWindowPlan(Seq(windowFunction)),
          expectSupported,
          "Window has TimestampNTZType in input/output schema",
          name)
    }
    // Rank's children mirror the order key, so give it a real NTZ orderSpec.
    checkNtzWhitelist(
      ntzWindowPlan(Seq(Rank(Seq(ntz))), orderSpec = Seq(SortOrder(ntz, Ascending))),
      expectSupported = true,
      "Window has TimestampNTZType in input/output schema",
      "Rank"
    )
  }

  test("a mix of supported and unsupported window expressions fails as a whole") {
    // Sum isn't whitelisted, so the node fails despite Lag being supported.
    val plan = ntzWindowPlan(Seq(new Lag(ntz), Sum(ntz).toAggregateExpression()))
    checkNtzWhitelist(
      plan,
      expectSupported = false,
      "Window has TimestampNTZType in input/output schema",
      "Lag + Sum")
  }

  test("bare NTZ partition/order keys pass, computed NTZ keys fail") {
    assert(computedNtz.dataType == TimestampNTZType)
    val cases = Seq(
      "bare partition key" ->
        (ntzWindowPlan(Seq(new Lag(ntz)), partitionSpec = Seq(ntz)), true),
      "computed partition key" ->
        (ntzWindowPlan(Seq(new Lag(ntz)), partitionSpec = Seq(computedNtz)), false),
      "bare order key" ->
        (ntzWindowPlan(Seq(new Lag(ntz)), orderSpec = Seq(SortOrder(ntz, Ascending))), true),
      "computed order key" -> (
        ntzWindowPlan(Seq(new Lag(ntz)), orderSpec = Seq(SortOrder(computedNtz, Ascending))),
        false)
    )
    cases.foreach {
      case (description, (plan, expectSupported)) =>
        checkNtzWhitelist(
          plan,
          expectSupported,
          "Window has TimestampNTZType in input/output schema",
          description)
    }
  }

  test("SortExec with bare and computed NTZ sort keys") {
    assert(computedNtz.dataType == TimestampNTZType)
    val cases = Seq(
      "bare sort key" ->
        (SortExec(Seq(SortOrder(ntz, Ascending)), global = true, ntzInput), true),
      "computed sort key" ->
        (SortExec(Seq(SortOrder(computedNtz, Ascending)), global = true, ntzInput), false)
    )
    cases.foreach {
      case (description, (plan, expectSupported)) =>
        checkNtzWhitelist(
          plan,
          expectSupported,
          "Sort has TimestampNTZType in input/output schema",
          description)
    }
  }

  test("ShuffleExchangeExec with hash and range partitioning over NTZ keys") {
    assert(computedNtz.dataType == TimestampNTZType)
    val cases = Seq(
      "bare hash key" ->
        (ShuffleExchangeExec(HashPartitioning(Seq(ntz), 4), ntzInput), true),
      "computed hash key" ->
        (ShuffleExchangeExec(HashPartitioning(Seq(computedNtz), 4), ntzInput), false),
      "bare range key" -> (
        ShuffleExchangeExec(
          RangePartitioning(Seq(SortOrder(ntz, Ascending)), 4),
          ntzInput),
        true),
      "computed range key" -> (
        ShuffleExchangeExec(
          RangePartitioning(Seq(SortOrder(computedNtz, Ascending)), 4),
          ntzInput),
        false),
      // Non-hash/range partitioning has no keys to check, so it defaults to supported.
      "single partition" -> (ShuffleExchangeExec(SinglePartition, ntzInput), true)
    )
    cases.foreach {
      case (description, (plan, expectSupported)) =>
        checkNtzWhitelist(
          plan,
          expectSupported,
          "Exchange has TimestampNTZType in input/output schema",
          description)
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
