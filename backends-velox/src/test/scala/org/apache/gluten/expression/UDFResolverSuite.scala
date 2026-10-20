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

import org.apache.spark.sql.expression.UDFResolver

import org.scalatest.BeforeAndAfterEach
import org.scalatest.funsuite.AnyFunSuite

class UDFResolverSuite extends AnyFunSuite with BeforeAndAfterEach {

  // UDFNames is JVM-global and populated once per JVM, so it is restored rather than cleared.
  private var savedNames: Set[String] = Set.empty

  override protected def beforeEach(): Unit = {
    savedNames = UDFResolver.UDFNames.toSet
    UDFResolver.UDFNames.clear()
  }

  override protected def afterEach(): Unit = {
    UDFResolver.UDFNames.clear()
    UDFResolver.UDFNames ++= savedNames
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
}
