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
package org.apache.gluten.substrait.rel

import org.apache.gluten.substrait.SubstraitContext

import io.substrait.proto.Rel
import org.scalatest.funsuite.AnyFunSuite

class OneRowRelNodeSuite extends AnyFunSuite {

  test("builds exactly one empty row with an empty schema") {
    val context = new SubstraitContext
    val rel = RelBuilder.makeOneRowReadRel(context, 0L)
    val read = rel.toProtobuf.getRead

    assert(read.hasBaseSchema)
    assert(read.hasVirtualTable)
    assert(read.getCommon.hasDirect)
    assert(read.getBaseSchema.getNamesCount == 0)
    assert(read.getBaseSchema.getStruct.getTypesCount == 0)
    assert(read.getVirtualTable.getExpressionsCount == 1)
    assert(read.getVirtualTable.getExpressions(0).getFieldsCount == 0)
    assert(rel.childNodes().isEmpty)
    assert(context.registeredRelMap.size() == 1)
    assert(context.registeredRelMap.get(0L).size() == 1)

    val roundTripped = Rel.parseFrom(rel.toProtobuf.toByteArray)
    assert(roundTripped == rel.toProtobuf)
  }

  test("serialization is repeatable and does not register extra relations") {
    val context = new SubstraitContext
    val operatorId = context.nextOperatorId("one-row")
    val rel = RelBuilder.makeOneRowReadRel(context, operatorId)
    val first = rel.toProtobuf

    assert(rel.toProtobuf == first)
    assert(context.registeredRelMap.size() == 1)
    assert(context.registeredRelMap.get(operatorId).size() == 1)
  }

  test("separate one-row sources register with their own operators") {
    val context = new SubstraitContext
    val firstId = context.nextOperatorId("first")
    val secondId = context.nextOperatorId("second")
    val first = RelBuilder.makeOneRowReadRel(context, firstId)
    val second = RelBuilder.makeOneRowReadRel(context, secondId)

    assert(firstId != secondId)
    assert(first.toProtobuf == second.toProtobuf)
    assert(context.registeredRelMap.size() == 2)
    assert(context.registeredRelMap.get(firstId).size() == 1)
    assert(context.registeredRelMap.get(secondId).size() == 1)
  }
}
