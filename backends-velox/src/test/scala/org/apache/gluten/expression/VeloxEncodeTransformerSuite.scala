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

import org.apache.gluten.backendsapi.velox.VeloxSparkPlanExecApi
import org.apache.gluten.config.GlutenConfig
import org.apache.gluten.exception.GlutenNotSupportException
import org.apache.gluten.substrait.SubstraitContext

import org.apache.spark.sql.catalyst.expressions.{BoundReference, Encode, Expression, Literal}
import org.apache.spark.sql.catalyst.expressions.objects.StaticInvoke
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types._

import org.scalatest.funsuite.AnyFunSuite

import scala.collection.JavaConverters._

class VeloxEncodeTransformerSuite extends AnyFunSuite {
  private val input = BoundReference(0, StringType, nullable = true)

  private def withReplacement(f: SQLConf => Unit): Unit = {
    val conf = new SQLConf
    conf.setConfString(GlutenConfig.SPARK_LEGACY_CODING_ERROR_ACTION, "true")
    SQLConf.withExistingConf(conf) {
      f(conf)
    }
  }

  private def encode(
      charset: Expression,
      legacyCharsets: Boolean = false,
      legacyErrorAction: Boolean = true): StaticInvoke =
    StaticInvoke(
      classOf[Encode],
      BinaryType,
      "encode",
      Seq(input, charset, Literal(legacyCharsets), Literal(legacyErrorAction)))

  private def transform(expr: Expression): ExpressionTransformer =
    new VeloxSparkPlanExecApi().genEncodeTransformer(
      ExpressionNames.ENCODE,
      Seq(LiteralTransformer(Literal("input")), LiteralTransformer(Literal("unused"))),
      expr)

  test("encode canonicalizes aliases and lowers policy arguments out of the native signature") {
    withReplacement {
      _ =>
        for (
          (charset, canonical) <- Seq(
            "uTf-8" -> "UTF-8",
            "UTF8" -> "UTF-8",
            "latin1" -> "ISO-8859-1",
            "UTF-16" -> "UTF-16",
            "UTF-32" -> "UTF-32")
        ) {
          val transformer = transform(encode(Literal(charset), legacyCharsets = true))
          assert(transformer.children(1).original.eval().toString == canonical)
          val context = new SubstraitContext
          val node = transformer.doTransform(context).toProtobuf.getScalarFunction
          assert(node.getArgumentsCount == 2)
          assert(
            context.registeredFunction.keySet().asScala.toSeq ==
              Seq(ConverterUtils.makeFuncName("encode", Seq(StringType, StringType))))
        }
    }
  }

  test("encode rejects dynamic, null, oversized, invalid and disallowed charset names") {
    withReplacement {
      _ =>
        val charsets = Seq(
          BoundReference(1, StringType, nullable = true),
          Literal.create(null, StringType),
          Literal("UTF8"),
          Literal("windows-1252"),
          Literal("UTF-8 "),
          Literal(" UTF-8"),
          Literal("\u0131SO-8859-1"),
          Literal("U\u017f-ASCII"),
          Literal("UTF-8\u0000"),
          Literal("x" * 65)
        )
        charsets.foreach {
          charset =>
            intercept[GlutenNotSupportException] {
              transform(encode(charset))
            }
        }
        for (charset <- Seq("not-a-charset", "UTF-8 ", "x" * 65)) {
          intercept[GlutenNotSupportException] {
            transform(encode(Literal(charset), legacyCharsets = true))
          }
        }
    }
  }

  test("encode rejects strict and stale expression-bound coding-error policies") {
    withReplacement {
      conf =>
        intercept[GlutenNotSupportException] {
          transform(encode(Literal("UTF-8"), legacyErrorAction = false))
        }
        val captured = encode(Literal("US-ASCII"))
        conf.setConfString(GlutenConfig.SPARK_LEGACY_CODING_ERROR_ACTION, "false")
        intercept[GlutenNotSupportException] {
          transform(captured)
        }
    }
  }

  test("encode disable switch and argument validation cannot be bypassed by direct expressions") {
    withReplacement {
      conf =>
        assert(transform(Encode(input, Literal("UTF-8"))).children.size == 2)
        conf.setConfString(GlutenConfig.NATIVE_ENCODE_ENABLED.key, "false")
        for (expr <- Seq(encode(Literal("UTF-8")), Encode(input, Literal("UTF-8")))) {
          intercept[GlutenNotSupportException] {
            transform(expr)
          }
        }
        conf.setConfString(GlutenConfig.NATIVE_ENCODE_ENABLED.key, "true")
        for (
          arguments <- Seq(
            Seq(input, Literal("UTF-8")),
            Seq(input, Literal("UTF-8"), Literal(false), input))
        ) {
          intercept[GlutenNotSupportException] {
            transform(StaticInvoke(classOf[Encode], BinaryType, "encode", arguments))
          }
        }
    }
  }
}
