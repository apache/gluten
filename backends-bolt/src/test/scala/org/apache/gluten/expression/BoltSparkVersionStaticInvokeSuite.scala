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

import org.apache.gluten.backendsapi.{BackendsApiManager, SparkPlanExecApi}
import org.apache.gluten.exception.GlutenNotSupportException
import org.apache.gluten.substrait.SubstraitContext

import org.apache.spark.{SPARK_REVISION, SPARK_VERSION_SHORT}
import org.apache.spark.sql.catalyst.expressions.{Expression, Literal, SparkVersion}
import org.apache.spark.sql.catalyst.expressions.objects.StaticInvoke
import org.apache.spark.sql.types.{DataType, IntegerType, StringType}
import org.apache.spark.util.SparkReflectionUtil

import org.scalatest.funsuite.AnyFunSuite

import java.lang.reflect.InvocationTargetException

class BoltSparkVersionStaticInvokeSuite extends AnyFunSuite {
  private val implClass =
    SparkReflectionUtil.classForName(
      "org.apache.spark.sql.catalyst.expressions.ExpressionImplUtils")
  private def invoke(
      clazz: Class[_] = implClass,
      name: String = "getSparkVersion",
      arguments: Seq[Expression] = Nil,
      resultType: DataType = StringType): StaticInvoke = {
    StaticInvoke(clazz, resultType, name, arguments, returnNullable = false)
  }
  private def transform(expr: Expression): ExpressionTransformer =
    ExpressionConverter.replaceWithExpressionTransformer(expr, Nil)
  private def value(expr: Expression): String =
    transform(expr).doTransform(new SubstraitContext).toProtobuf.getLiteral.getString
  private def unsupported(expr: StaticInvoke): Unit = {
    val error = intercept[GlutenNotSupportException](transform(expr))
    assert(error.getMessage ==
      s"Not supported to transform StaticInvoke with object: ${expr.objectName}, " +
      s"function: ${expr.functionName}")
  }

  test("Spark version StaticInvoke reuses the literal only when the method exists") {
    val expr = invoke()
    if (implClass.getMethods.exists(_.getName == "getSparkVersion")) {
      val protobuf = transform(expr).doTransform(new SubstraitContext).toProtobuf
      assert(protobuf.hasLiteral)
      assert(protobuf.getLiteral.getString == s"$SPARK_VERSION_SHORT $SPARK_REVISION")
      assert(value(expr) == value(SparkVersion()))
    } else {
      unsupported(expr)
    }
  }

  test("legacy Spark version literal remains unchanged") {
    assert(value(SparkVersion()) == s"$SPARK_VERSION_SHORT $SPARK_REVISION")
  }

  test("unrelated StaticInvoke signatures retain the original fallback exception") {
    unsupported(invoke(clazz = classOf[String]))
    unsupported(invoke(name = "getOtherVersion"))
    unsupported(invoke(arguments = Seq(Literal("unexpected"))))
    unsupported(invoke(resultType = IntegerType))
  }

  test("an unmodified backend API default retains the original StaticInvoke fallback") {
    val originalApi = BackendsApiManager.getSparkPlanExecApiInstance
    val defaultHook = () => {
      val method = classOf[SparkPlanExecApi].getDeclaredMethod(
        "genSparkVersionStaticInvokeTransformer$",
        classOf[SparkPlanExecApi])
      method.invoke(null, originalApi).asInstanceOf[Option[ExpressionTransformer]]
    }
    assert(defaultHook().isEmpty)
    val dispatcher = ExpressionConverter.getClass.getDeclaredMethods.find {
      method => method.getName == "replaceStaticInvokeWithExpressionTransformer"
    }.get
    dispatcher.setAccessible(true)
    val expr = invoke()
    val args = Array[AnyRef](expr, Nil, Map.empty[Class[_], String], defaultHook)
    val error = intercept[InvocationTargetException] {
      dispatcher.invoke(ExpressionConverter, args: _*)
    }.getCause
    assert(error.isInstanceOf[GlutenNotSupportException])
    assert(error.getMessage ==
      s"Not supported to transform StaticInvoke with object: ${expr.objectName}, " +
      s"function: ${expr.functionName}")
  }
}
