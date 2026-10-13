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

import org.apache.gluten.config.VeloxConfig
import org.apache.gluten.exception.GlutenNotSupportException

import org.apache.spark.sql.catalyst.expressions.Literal
import org.apache.spark.sql.expression.UDFResolver

import org.scalatest.BeforeAndAfterEach
import org.scalatest.funsuite.AnyFunSuite

class UDFResolverSuite extends AnyFunSuite with BeforeAndAfterEach {

  // These are JVM-global and populated once per JVM, so they are restored rather than cleared.
  // All four, not just UDFNames: registering a function by name writes a Registry* set as well,
  // and a name left in one of those stays visible to getUdfExpression / getUdafExpression after
  // the test that registered it, which would send a later test into native resolution with no
  // library loaded.
  private val nameSets = Seq(
    UDFResolver.UDFNames,
    UDFResolver.UDAFNames,
    UDFResolver.RegistryUDFNames,
    UDFResolver.RegistryUDAFNames)

  private var savedNames: Seq[Set[String]] = Seq.empty

  override protected def beforeEach(): Unit = {
    savedNames = nameSets.map(_.toSet)
    nameSets.foreach(_.clear())
  }

  override protected def afterEach(): Unit = {
    nameSets.zip(savedNames).foreach {
      case (names, saved) =>
        names.clear()
        names ++= saved
    }
  }

  private def describedNames(): Seq[String] =
    UDFResolver.getFunctionDescriptions.map(_._1.funcName)

  test("registration by name is off unless it is turned on") {
    assert(VeloxConfig.NATIVE_UDF_BYPASS_REGISTRATION.defaultValue.contains(false))
  }

  test("a name with no dot is described") {
    UDFResolver.UDFNames += "myudf_increment"
    assert(describedNames() == Seq("myudf_increment"))
  }

  test("a dotted name is skipped, it is a hive udf class name") {
    UDFResolver.UDFNames += "org.apache.spark.sql.hive.execution.UDFStringString"
    assert(describedNames().isEmpty)
  }

  test("a name colliding with a spark built-in is skipped") {
    UDFResolver.UDFNames += "abs"
    assert(describedNames().isEmpty)
  }

  test("a name colliding with a spark built-in does not skip the others") {
    UDFResolver.UDFNames ++= Seq("abs", "myudf_increment", "upper")
    assert(describedNames() == Seq("myudf_increment"))
  }

  test("names differing only in case are skipped as a group") {
    UDFResolver.UDFNames ++= Seq("Foo", "foo", "myudf_increment")
    assert(describedNames() == Seq("myudf_increment"))
  }

  test("names are described in sorted order") {
    UDFResolver.UDFNames ++= Seq("b_udf", "a_udf")
    assert(describedNames() == Seq("a_udf", "b_udf"))
  }

  // Looking up an unregistered name used to fail inside UDFMap.getOrElse. A function declared
  // by name alone has no UDFMap entry, so the lookup no longer throws there and the miss has to
  // be caught after binding instead.
  test("an unregistered udf is not supported") {
    intercept[GlutenNotSupportException] {
      UDFResolver.getUdfExpression("not_registered", "not_registered")(Seq(Literal(1)))
    }
  }

  test("an unregistered udaf is not supported") {
    intercept[GlutenNotSupportException] {
      UDFResolver.getUdafExpression("not_registered")(Seq(Literal(1)))
    }
  }

  // A function declared by name alone is offloaded through the same UDFNames / UDAFNames gates
  // as one with a stated signature; only where its types come from differs.
  test("a udf declared by name alone is registered") {
    UDFResolver.registerRegistryUDF("myudf_map_cardinality")
    assert(UDFResolver.UDFNames.contains("myudf_map_cardinality"))
    assert(describedNames() == Seq("myudf_map_cardinality"))
  }

  test("a udaf declared by name alone is registered") {
    UDFResolver.registerRegistryUDAF("myudaf_arbitrary")
    assert(UDFResolver.UDAFNames.contains("myudaf_arbitrary"))
  }
}
