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

import org.apache.spark.sql.catalyst.expressions.{BoundReference, Literal, Round}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types._

import io.substrait.proto.Expression.ScalarFunction
import org.scalatest.funsuite.AnyFunSuite

class VeloxRoundTransformerSuite extends AnyFunSuite {
  private class CapabilityValidator(available: Boolean) extends VeloxValidatorApi {
    var calls: Seq[(String, ScalarFunction)] = Seq.empty
    override def doNativeValidateExpression(
        context: SubstraitContext,
        expression: ExpressionNode,
        inputType: TypeNode): Boolean = {
      val scalar = expression.toProtobuf.getScalarFunction
      val names = context.registeredFunction.entrySet().iterator()
      var name = ""
      while (names.hasNext) {
        val entry = names.next()
        if (entry.getValue == scalar.getFunctionReference) {
          name = entry.getKey
        }
      }
      calls :+= name -> scalar
      available
    }
  }

  test("round captures integral ANSI in a capability-specific native call") {
    val api = new VeloxSparkPlanExecApi
    Seq(ByteType, ShortType, IntegerType, LongType).foreach {
      dataType =>
        Seq(false, true).foreach {
          ansi =>
            val value = Literal.default(dataType)
            val scale = Literal(-1)
            val expression = new Round(value, scale, ansi)
            val transformer = api.genRoundTransformer(
              "round",
              Seq(LiteralTransformer(value), LiteralTransformer(scale)),
              expression)
            assert(transformer.substraitExprName == "spark_round")
            val context = new SubstraitContext
            val scalar = transformer.doTransform(context).toProtobuf.getScalarFunction
            val signature = ConverterUtils.makeFuncName(
              "spark_round",
              Seq(dataType, IntegerType, BooleanType))
            assert(context.registeredFunction.containsKey(signature))
            assert(scalar.getArgumentsCount == 3)
            assert(scalar.getArguments(2).getValue.getLiteral.getBoolean == ansi)
        }
    }
  }

  test("round capability checks cannot be disabled by general native validation") {
    val conf = new SQLConf
    conf.setConfString(GlutenConfig.NATIVE_VALIDATION_ENABLED.key, "false")
    SQLConf.withExistingConf(conf) {
      Seq(false, true).foreach {
        available =>
          val validator = new CapabilityValidator(available)
          val expression = new Round(BoundReference(0, LongType, nullable = true), Literal(-1))
          assert(validator.doExprValidate("round", expression) == available)
          assert(validator.calls.size == 1)
          assert(validator.calls.head._1 == ConverterUtils.makeFuncName(
            "spark_round",
            Seq(LongType, IntegerType, BooleanType)))
          assert(validator.calls.head._2.getArgumentsCount == 3)
      }
    }
  }

  test("decimal and floating round probe the exact specialized names and output types") {
    Seq(FloatType, DoubleType, DecimalType(38, 38)).foreach {
      dataType =>
        val validator = new CapabilityValidator(available = true)
        val expression = new Round(BoundReference(0, dataType, nullable = true), Literal(0))
        assert(validator.doExprValidate("round", expression))
        assert(validator.calls.size == 1)
        val (name, function) = validator.calls.head
        val functionName = if (dataType.isInstanceOf[DecimalType]) {
          "decimal_spark_round"
        } else {
          "spark_round"
        }
        assert(name == ConverterUtils.makeFuncName(functionName, Seq(dataType, IntegerType)))
        assert(function.getArgumentsCount == 2)
        if (dataType.isInstanceOf[DecimalType]) {
          assert(function.getOutputType.getDecimal.getPrecision == 1)
          assert(function.getOutputType.getDecimal.getScale == 0)
        }
    }
  }

  test("round rejects unqualified scales before requesting native capability") {
    val validator = new CapabilityValidator(available = true)
    val value = BoundReference(0, IntegerType, nullable = true)
    Seq(Int.MinValue, -401, 401, Int.MaxValue).foreach {
      scale => assert(!validator.doExprValidate("round", new Round(value, Literal(scale))))
    }
    assert(!validator.doExprValidate(
      "round",
      new Round(value, BoundReference(1, IntegerType, nullable = false))))
    assert(validator.calls.isEmpty)
  }

  test("round capability exceptions explicitly retain Spark fallback") {
    val validator = new VeloxValidatorApi {
      override def doNativeValidateExpression(
          context: SubstraitContext,
          expression: ExpressionNode,
          inputType: TypeNode): Boolean = {
        throw new IllegalStateException("capability unavailable")
      }
    }
    val expression = new Round(BoundReference(0, IntegerType, nullable = true), Literal(0))
    assert(!validator.doExprValidate("round", expression))
  }
}
