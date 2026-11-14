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

import org.apache.spark.sql.catalyst.expressions.{Add, BoundReference, Cast, Literal, Round}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types._

import org.scalatest.funsuite.AnyFunSuite

class VeloxIntegralRoundSuite extends AnyFunSuite {
  test("integral ROUND eligibility defers to native validation") {
    val validator = new VeloxValidatorApi
    for (dataType <- Seq(ByteType, ShortType, IntegerType, LongType)) {
      val value = BoundReference(0, dataType, nullable = true)
      Seq(Int.MinValue, -401, -1, 0, 1, 400, 401, Int.MaxValue).foreach {
        scale => assert(validator.doExprValidate("round", Round(value, Literal(scale))))
      }
      assert(validator.doExprValidate(
        "round",
        Round(value, Literal.create(null, IntegerType))))
      assert(validator.doExprValidate(
        "round",
        Round(value, BoundReference(1, IntegerType, nullable = false))))
      Seq(
        Add(Literal(0), Literal(0)),
        Add(Literal(400), Literal(1)),
        Cast(Literal.create(null, NullType), IntegerType)).foreach {
        scale =>
          assert(scale.foldable)
          assert(scale.dataType == IntegerType)
          assert(validator.doExprValidate("round", Round(value, scale)))
      }
    }

    val conf = new SQLConf
    conf.setConfString(SQLConf.ANSI_ENABLED.key, "true")
    SQLConf.withExistingConf(conf) {
      val value = BoundReference(0, LongType, nullable = true)
      val scale = Cast(Literal("invalid-scale"), IntegerType)
      intercept[Exception](scale.eval())
      assert(validator.doExprValidate("round", Round(value, scale)))
    }
  }
}
