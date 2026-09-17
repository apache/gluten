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

import org.apache.gluten.expression.LiteralTransformer
import org.apache.gluten.substrait.SubstraitContext

import org.apache.spark.sql.catalyst.expressions.{BRound, Literal}
import org.apache.spark.sql.types.Decimal

import org.scalatest.funsuite.AnyFunSuite

class VeloxBRoundTransformerSuite extends AnyFunSuite {
  private val api = new VeloxSparkPlanExecApi

  test("integral bround carries the expression's ANSI mode in a native literal") {
    Seq(Literal(127.toByte), Literal(32767.toShort), Literal(Int.MaxValue), Literal(Long.MaxValue))
      .foreach {
        value =>
          Seq(false, true).foreach {
            ansi =>
              val scale = Literal(-1)
              val original = new BRound(value, scale, ansi)
              val transformed = api.genBRoundTransformer(
                "bround",
                Seq(LiteralTransformer(value), LiteralTransformer(scale)),
                original)
              val function =
                transformed.doTransform(new SubstraitContext).toProtobuf.getScalarFunction
              assert(function.getArgumentsCount == 3)
              val mode = function.getArguments(2).getValue.getLiteral
              assert(mode.hasBoolean)
              assert(mode.getBoolean == ansi)
          }
      }
  }

  test("floating and decimal bround retain their two-argument signatures") {
    Seq(Literal(2.5f), Literal(2.5d), Literal(Decimal("2.5"))).foreach {
      value =>
        val scale = Literal(1)
        val original = new BRound(value, scale)
        val transformed = api.genBRoundTransformer(
          "bround",
          Seq(LiteralTransformer(value), LiteralTransformer(scale)),
          original)
        val function = transformed.doTransform(new SubstraitContext).toProtobuf.getScalarFunction
        assert(function.getArgumentsCount == 2)
    }
  }
}
