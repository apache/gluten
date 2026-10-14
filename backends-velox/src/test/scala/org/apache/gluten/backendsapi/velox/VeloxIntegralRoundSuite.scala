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

import org.apache.spark.sql.catalyst.expressions.{BoundReference, Literal, Round}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types._

import org.scalatest.funsuite.AnyFunSuite

class VeloxIntegralRoundSuite extends AnyFunSuite {
  test("integral ROUND rejects unsafe scales even when general native validation is disabled") {
    val conf = new SQLConf
    conf.setConfString(GlutenConfig.NATIVE_VALIDATION_ENABLED.key, "false")
    conf.setConfString(SQLConf.ANSI_ENABLED.key, "false")
    SQLConf.withExistingConf(conf) {
      val validator = new VeloxValidatorApi
      for (dataType <- Seq(ByteType, ShortType, IntegerType, LongType)) {
        val value = BoundReference(0, dataType, nullable = true)
        Seq(0, 1, 400).foreach {
          scale => assert(validator.doExprValidate("round", Round(value, Literal(scale))))
        }
        Seq(Int.MinValue, -401, -400, -1, 401, Int.MaxValue).foreach {
          scale => assert(!validator.doExprValidate("round", Round(value, Literal(scale))))
        }
        assert(validator.doExprValidate(
          "round",
          Round(value, Literal.create(null, IntegerType))))
        assert(!validator.doExprValidate(
          "round",
          Round(value, BoundReference(1, IntegerType, nullable = false))))
      }
    }
  }

  test("negative integral ROUND preserves Spark's captured overflow mode") {
    val conf = new SQLConf
    SQLConf.withExistingConf(conf) {
      val validator = new VeloxValidatorApi
      val value = BoundReference(0, ByteType, nullable = true)
      conf.setConfString(SQLConf.ANSI_ENABLED.key, "false")
      val legacy = Round(value, Literal(-1))
      assert(!validator.doExprValidate("round", legacy))
      conf.setConfString(SQLConf.ANSI_ENABLED.key, "true")
      assert(!validator.doExprValidate("round", legacy))
      val current = Round(value, Literal(-1))
      assert(!validator.doExprValidate("round", current))
    }
  }
}
