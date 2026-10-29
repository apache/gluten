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
package org.apache.gluten.expression

import org.apache.gluten.backendsapi.bolt.BoltSparkPlanExecApi
import org.apache.gluten.config.GlutenConfig
import org.apache.gluten.exception.GlutenNotSupportException

import org.apache.spark.sql.catalyst.expressions.{Base64, Literal, RuntimeReplaceable}
import org.apache.spark.sql.catalyst.expressions.objects.StaticInvoke
import org.apache.spark.sql.internal.SQLConf

import org.scalatest.funsuite.AnyFunSuite

class BoltBase64ConfigSuite extends AnyFunSuite {
  private val key = "spark.sql.chunkBase64String.enabled"
  private val input = Array.fill[Byte](58)(97.toByte)
  private def registered(conf: SQLConf): Boolean = conf.getAllDefinedConfs.exists(_._1 == key)
  private def nativeChunks(conf: SQLConf): Boolean = {
    val serialized = GlutenConfig.getNativeSessionConf("bolt", conf.getAllConfs)
    serialized.getOrElse(key, "true").toBoolean
  }
  private def evaluate(expr: Base64): Any = expr match {
    case r: RuntimeReplaceable => r.replacement.eval()
    case _ => expr.eval()
  }
  private def chunks(value: Any): Boolean = value.toString.contains("\r\n")
  private def transform(expr: StaticInvoke): Unit = {
    val child = LiteralTransformer(Literal(input))
    new BoltSparkPlanExecApi().genBase64StaticInvokeTransformer("base64", child, expr)
  }
  test("Base64 native configuration preserves actual Spark default") {
    val conf = new SQLConf
    SQLConf.withExistingConf(conf) {
      val expr = Base64(Literal(input))
      assert(nativeChunks(conf) == chunks(evaluate(expr)))
      if (registered(conf)) {
        assert(conf.getConfString(key).toBoolean == chunks(evaluate(expr)))
        transform(expr.asInstanceOf[RuntimeReplaceable].replacement.asInstanceOf[StaticInvoke])
      }
    }
  }
  for (enabled <- Seq(true, false)) {
    test(s"Base64 native configuration preserves explicit $enabled using original Spark behavior") {
      val conf = new SQLConf
      conf.setConfString(key, enabled.toString)
      SQLConf.withExistingConf(conf) {
        val expr = Base64(Literal(input))
        assert(nativeChunks(conf) == chunks(evaluate(expr)))
        if (registered(conf)) {
          assert(chunks(evaluate(expr)) == enabled)
          transform(expr.asInstanceOf[RuntimeReplaceable].replacement.asInstanceOf[StaticInvoke])
        } else {
          assert(chunks(evaluate(expr)))
          assert(!expr.isInstanceOf[RuntimeReplaceable])
        }
      }
    }
  }
  test("Base64 preserves expression bound flag after session configuration changes") {
    val conf = new SQLConf
    SQLConf.withExistingConf(conf) {
      if (registered(conf)) {
        for (captured <- Seq(false, true)) {
          conf.setConfString(key, captured.toString)
          val expr = Base64(Literal(input))
          conf.setConfString(key, (!captured).toString)
          assert(chunks(evaluate(expr)) == captured)
          intercept[GlutenNotSupportException] {
            transform(expr.asInstanceOf[RuntimeReplaceable].replacement.asInstanceOf[StaticInvoke])
          }
        }
      } else {
        assert(chunks(evaluate(Base64(Literal(input)))))
      }
    }
  }
}
