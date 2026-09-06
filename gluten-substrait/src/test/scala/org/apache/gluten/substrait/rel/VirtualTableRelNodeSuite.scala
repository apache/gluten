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

import org.apache.gluten.substrait.`type`.I32TypeNode
import org.apache.gluten.substrait.SubstraitContext

import io.substrait.proto.{Expression, Rel}
import org.scalatest.funsuite.AnyFunSuite

import java.util.{Arrays, Collections}

import scala.collection.JavaConverters._
import scala.reflect.ClassTag

class VirtualTableRelNodeSuite extends AnyFunSuite {
  private def assertRejectedWithoutRegistration[T <: Throwable: ClassTag](
      message: String)(build: SubstraitContext => Unit): Unit = {
    val context = new SubstraitContext
    val error = intercept[T](build(context))
    assert(error.getMessage.contains(message))
    assert(context.registeredRelMap.isEmpty)
  }

  test("builds one empty row with an empty schema") {
    val context = new SubstraitContext
    val rel = RelBuilder.makeVirtualTableReadRel(
      Collections.emptyList(),
      Collections.emptyList(),
      Collections.singletonList(Collections.emptyList()),
      context,
      0L)
    val read = rel.toProtobuf.getRead

    assert(read.getBaseSchema.getNamesCount == 0)
    assert(read.getBaseSchema.getStruct.getTypesCount == 0)
    assert(read.getVirtualTable.getExpressionsCount == 1)
    assert(read.getVirtualTable.getExpressions(0).getFieldsCount == 0)
    assert(context.registeredRelMap.size() == 1)
    assert(context.registeredRelMap.get(0L).size() == 1)

    val roundTripped = Rel.parseFrom(rel.toProtobuf.toByteArray)
    assert(roundTripped == rel.toProtobuf)
  }

  test("rejects an empty row set without registering the relation") {
    val context = new SubstraitContext

    val error = intercept[IllegalArgumentException] {
      RelBuilder.makeVirtualTableReadRel(
        Collections.emptyList(),
        Collections.emptyList(),
        Collections.emptyList(),
        context,
        0L)
    }
    assert(error.getMessage.contains("at least one row"))
    assert(context.registeredRelMap.isEmpty)
  }

  test("rejects rows whose width differs from the schema and wraps valid literals") {
    val literal = Expression.Literal.newBuilder().setI32(1).build()

    val error = intercept[IllegalArgumentException] {
      RelBuilder.makeVirtualTableReadRel(
        Collections.singletonList(new I32TypeNode(false)),
        Collections.singletonList("c0"),
        Collections.singletonList(Collections.emptyList()),
        new SubstraitContext,
        0L
      )
    }
    assert(error.getMessage.contains("0 fields"))

    val valid = RelBuilder.makeVirtualTableReadRel(
      Collections.singletonList(new I32TypeNode(false)),
      Collections.singletonList("c0"),
      Collections.singletonList(Collections.singletonList(literal)),
      new SubstraitContext,
      0L
    )
    val field = valid.toProtobuf.getRead.getVirtualTable.getExpressions(0).getFields(0)
    assert(field.hasLiteral)
    assert(field.getLiteral == literal)
  }

  test("rejects malformed schema and null inputs without registering the relation") {
    assertRejectedWithoutRegistration[IllegalArgumentException]("1 types but 0 names") {
      context =>
        RelBuilder.makeVirtualTableReadRel(
          Collections.singletonList(new I32TypeNode(false)),
          Collections.emptyList(),
          Collections.singletonList(Collections.emptyList()),
          context,
          0L)
    }

    val nullCases = Seq[(String, SubstraitContext => Unit)](
      "types" -> {
        context =>
          RelBuilder.makeVirtualTableReadRel(
            null,
            Collections.emptyList(),
            Collections.singletonList(Collections.emptyList()),
            context,
            0L)
      },
      "names" -> {
        context =>
          RelBuilder.makeVirtualTableReadRel(
            Collections.emptyList(),
            null,
            Collections.singletonList(Collections.emptyList()),
            context,
            0L)
      },
      "rows" -> {
        context =>
          RelBuilder.makeVirtualTableReadRel(
            Collections.emptyList(),
            Collections.emptyList(),
            null,
            context,
            0L)
      },
      "row" -> {
        context =>
          RelBuilder.makeVirtualTableReadRel(
            Collections.emptyList(),
            Collections.emptyList(),
            Collections.singletonList(null),
            context,
            0L)
      },
      "literal" -> {
        context =>
          RelBuilder.makeVirtualTableReadRel(
            Collections.singletonList(new I32TypeNode(false)),
            Collections.singletonList("c0"),
            Collections.singletonList(Collections.singletonList(null)),
            context,
            0L
          )
      }
    )

    nullCases.foreach {
      case (message, build) =>
        assertRejectedWithoutRegistration[NullPointerException](message)(build)
    }
  }

  test("batches non-empty rows and preserves empty-schema row cardinality") {
    def i32(value: Int): Expression.Literal =
      Expression.Literal.newBuilder().setI32(value).build()

    val context = new SubstraitContext
    val rel = RelBuilder.makeVirtualTableReadRel(
      Arrays.asList(new I32TypeNode(false), new I32TypeNode(false)),
      Arrays.asList("c0", "c1"),
      Arrays.asList(
        Arrays.asList(i32(1), i32(10)),
        Arrays.asList(i32(2), i32(20)),
        Arrays.asList(i32(3), i32(30))),
      context,
      0L
    )
    val virtualTable = rel.toProtobuf.getRead.getVirtualTable

    assert(virtualTable.getExpressionsCount == 1)
    assert(virtualTable.getExpressions(0).getFieldsCount == 6)
    assert(
      virtualTable.getExpressions(0).getFieldsList.asScala
        .map(_.getLiteral.getI32) == Seq(1, 2, 3, 10, 20, 30))
    assert(context.registeredRelMap.get(0L).size() == 1)

    val emptyRow = Collections.emptyList[Expression.Literal]()
    val emptySchemaRel = RelBuilder.makeVirtualTableReadRel(
      Collections.emptyList(),
      Collections.emptyList(),
      Arrays.asList(emptyRow, emptyRow),
      new SubstraitContext,
      0L)
    assert(emptySchemaRel.toProtobuf.getRead.getVirtualTable.getExpressionsCount == 2)
    assert(
      emptySchemaRel.toProtobuf.getRead.getVirtualTable.getExpressionsList.asScala
        .forall(_.getFieldsCount == 0))
  }
}
