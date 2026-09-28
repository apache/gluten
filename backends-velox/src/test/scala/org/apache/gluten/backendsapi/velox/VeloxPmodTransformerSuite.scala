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
package org.apache.gluten.backendsapi.velox

import org.apache.gluten.config.GlutenConfig
import org.apache.gluten.expression.{ConverterUtils, LiteralTransformer}
import org.apache.gluten.substrait.`type`.TypeNode
import org.apache.gluten.substrait.SubstraitContext
import org.apache.gluten.substrait.expression.ExpressionNode

import org.apache.spark.sql.catalyst.expressions.{AttributeReference, BoundReference, Coalesce, Expression, Literal, Pmod}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types._

import io.substrait.proto.Expression.ScalarFunction
import org.scalatest.funsuite.AnyFunSuite

class VeloxPmodTransformerSuite extends AnyFunSuite {
  private val primitiveTypes =
    Seq(ByteType, ShortType, IntegerType, LongType, FloatType, DoubleType)

  private def inMode[T](ansi: Boolean)(body: => T): T = {
    val conf = new SQLConf
    conf.setConfString(SQLConf.ANSI_ENABLED.key, ansi.toString)
    SQLConf.withExistingConf(conf)(body)
  }

  private def pmod(left: Expression, right: Expression, ansi: Boolean): Pmod =
    inMode(ansi)(new Pmod(left, right))

  private class CapabilityValidator(available: Boolean) extends VeloxValidatorApi {
    var calls: Seq[(String, ScalarFunction)] = Seq.empty
    override def doNativeValidateExpression(
        context: SubstraitContext,
        expression: ExpressionNode,
        inputType: TypeNode): Boolean = {
      val scalar = expression.toProtobuf.getScalarFunction
      val entries = context.registeredFunction.entrySet().iterator()
      var name = ""
      while (entries.hasNext) {
        val entry = entries.next()
        if (entry.getValue == scalar.getFunctionReference) name = entry.getKey
      }
      calls :+= name -> scalar
      available
    }
  }

  test(
    "pmod serializes captured mode and original nullability with the complete native signature") {
    val api = new VeloxSparkPlanExecApi
    for (
      dataType <- primitiveTypes; ansi <- Seq(false, true);
      leftNullable <- Seq(false, true); rightNullable <- Seq(false, true)
    ) {
      val expression = pmod(
        BoundReference(0, dataType, leftNullable),
        BoundReference(1, dataType, rightNullable),
        ansi)
      inMode(!ansi) {
        val value = LiteralTransformer(Literal.default(dataType))
        val transformer = api.genPmodTransformer("pmod", value, value, expression)
        val context = new SubstraitContext
        val scalar = transformer.doTransform(context).toProtobuf.getScalarFunction
        assert(context.registeredFunction.containsKey(ConverterUtils.makeFuncName(
          "pmod_with_mode",
          Seq(dataType, dataType, BooleanType, BooleanType))))
        assert(scalar.getArgumentsCount == 4)
        assert(scalar.getArguments(2).getValue.getLiteral.getBoolean == ansi)
        assert(scalar.getArguments(3).getValue.getLiteral.getBoolean ==
          (!leftNullable && !rightNullable))
        assert(scalar.getOutputType ==
          ConverterUtils.getTypeNode(expression.dataType, expression.nullable).toProtobuf)
      }
    }
  }

  test("pmod capability is mandatory even when general native validation is disabled") {
    val conf = new SQLConf
    conf.setConfString(GlutenConfig.NATIVE_VALIDATION_ENABLED.key, "false")
    SQLConf.withExistingConf(conf) {
      for (dataType <- primitiveTypes; ansi <- Seq(false, true); available <- Seq(false, true)) {
        val expression = pmod(
          BoundReference(0, dataType, nullable = true),
          BoundReference(1, dataType, nullable = true),
          ansi)
        val validator = new CapabilityValidator(available)
        assert(validator.doExprValidate("pmod", expression) == available)
        assert(validator.calls.size == 1)
        val (name, scalar) = validator.calls.head
        assert(name == ConverterUtils.makeFuncName(
          "pmod_with_mode",
          Seq(dataType, dataType, BooleanType, BooleanType)))
        assert(scalar.getArgumentsCount == 4)
        assert(scalar.getArguments(2).getValue.getLiteral.getBoolean == ansi)
      }
    }
  }

  test("decimal pmod is rejected without probing primitive native capability") {
    val validator = new CapabilityValidator(available = true)
    for (dataType <- Seq(DecimalType(10, 2), DecimalType(38, 18)); ansi <- Seq(false, true)) {
      assert(!validator.doExprValidate(
        "pmod",
        pmod(
          BoundReference(0, dataType, nullable = true),
          BoundReference(1, dataType, nullable = true),
          ansi)))
    }
    assert(validator.calls.isEmpty)
  }

  test("ambiguous ANSI composed-error ordering retains Spark execution") {
    val computed = Coalesce(Seq(BoundReference(0, IntegerType, nullable = true), Literal(1)))
    assert(!computed.nullable)
    for (ansi <- Seq(false, true); rightNullable <- Seq(false, true)) {
      val validator = new CapabilityValidator(available = true)
      val expression = pmod(
        computed,
        BoundReference(1, IntegerType, rightNullable),
        ansi)
      val supported = !ansi || rightNullable
      assert(validator.doExprValidate("pmod", expression) == supported)
      assert(validator.calls.nonEmpty == supported)
    }
  }

  test("nonnullable literal and attribute-like operands retain native capability") {
    for (
      left <- Seq(
        Literal(10),
        BoundReference(0, IntegerType, nullable = false),
        AttributeReference("value", IntegerType, nullable = false)())
    ) {
      val validator = new CapabilityValidator(available = true)
      assert(validator.doExprValidate("pmod", pmod(left, Literal(0), ansi = true)))
      assert(validator.calls.size == 1)
    }
  }

  test("capability validation failures explicitly retain Spark execution") {
    val validator = new VeloxValidatorApi {
      override def doNativeValidateExpression(
          context: SubstraitContext,
          expression: ExpressionNode,
          inputType: TypeNode): Boolean =
        throw new IllegalStateException("captured PMOD capability is unavailable")
    }
    assert(!validator.doExprValidate(
      "pmod",
      pmod(
        BoundReference(0, IntegerType, nullable = true),
        Literal(1),
        ansi = true)))
  }
}
