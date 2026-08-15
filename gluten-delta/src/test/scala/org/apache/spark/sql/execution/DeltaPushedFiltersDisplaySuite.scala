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
package org.apache.spark.sql.execution

import org.apache.spark.sql.catalyst.expressions.{AttributeReference, EqualTo, Expression, IsNotNull, Literal}
import org.apache.spark.sql.execution.datasources.DataSourceStrategy
import org.apache.spark.sql.types.StringType

import org.scalatest.funsuite.AnyFunSuite

class DeltaPushedFiltersDisplaySuite extends AnyFunSuite {

  private def translateFilter(expression: Expression, supportNested: Boolean): String =
    DataSourceStrategy.translateFilter(expression, supportNested).get.toString

  test("CDF display translation disables identifier quoting and preserves literal backticks") {
    val specialName = AttributeReference("id with space", StringType)()
    assert(
      translateFilter(IsNotNull(specialName), supportNested = true) ===
        "IsNotNull(`id with space`)")
    assert(
      translateFilter(IsNotNull(specialName), supportNested = false) ===
        "IsNotNull(id with space)")

    val ordinaryName = AttributeReference("name", StringType)()
    assert(
      translateFilter(
        EqualTo(ordinaryName, Literal("`literal`")),
        supportNested = false) ===
        "EqualTo(name,`literal`)")
  }
}
