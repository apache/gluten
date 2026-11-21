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

import com.google.protobuf.Descriptors.Descriptor
import io.substrait.proto.{AggregateFunction, DdlRel, Rel}
import org.scalatest.funsuite.AnyFunSuite

import scala.collection.JavaConverters._

/**
 * Pins the wire tags of the vendored `Rel.rel_type` oneof. Producer and consumer share one schema,
 * so a renumber round-trips cleanly through the generated classes and cannot be caught by
 * exercising them; these assert on the descriptors instead.
 *
 * Gluten's own relations live in the 1000 graft range so that relations added by upstream cannot
 * collide with them.
 */
class RelProtoSuite extends AnyFunSuite {

  private def assertFieldNumbers(descriptor: Descriptor, expected: (String, Int)*): Unit =
    expected.foreach {
      case (name, number) =>
        val field = descriptor.findFieldByName(name)
        assert(field != null, s"${descriptor.getName} has no field named $name")
        assert(field.getNumber === number, s"${descriptor.getName} field $name changed its number")
    }

  private val standardRelations = Seq(
    "read" -> 1,
    "filter" -> 2,
    "fetch" -> 3,
    "aggregate" -> 4,
    "sort" -> 5,
    "join" -> 6,
    "project" -> 7,
    "set" -> 8,
    "extension_single" -> 9,
    "extension_multi" -> 10,
    "extension_leaf" -> 11,
    "cross" -> 12,
    "hash_join" -> 13,
    "merge_join" -> 14,
    "exchange" -> 15,
    "expand" -> 16,
    "window" -> 17,
    "nested_loop_join" -> 18,
    "write" -> 19,
    "ddl" -> 20,
    "reference" -> 21,
    "update" -> 22,
    "top_n" -> 23,
    "lateral_join" -> 24
  )

  private val glutenRelations = Seq("generate" -> 1000, "windowGroupLimit" -> 1001)

  test("the standard relations carry their expected tags") {
    assertFieldNumbers(Rel.getDescriptor, standardRelations: _*)
  }

  test("Gluten-local relations live in the 1000 graft range") {
    assertFieldNumbers(Rel.getDescriptor, glutenRelations: _*)
  }

  test("rel_type holds exactly the standard and Gluten-local relations") {
    val rel = Rel.getDescriptor
    val oneof = rel.getOneofs.asScala.find(_.getName == "rel_type")
    assert(oneof.isDefined, "Rel has no rel_type oneof")
    val actual = oneof.get.getFields.asScala.map(f => f.getName -> f.getNumber).toSet
    assert(actual === (standardRelations ++ glutenRelations).toSet)
  }

  test("ReferenceRel is a top-level message") {
    val reference = Rel.getDescriptor.findFieldByName("reference")
    assert(reference.getMessageType.getFullName === "substrait.ReferenceRel")
    assert(
      AggregateFunction.getDescriptor.findNestedTypeByName("ReferenceRel") === null,
      "ReferenceRel must not be nested inside AggregateFunction")
  }

  test("DdlRel carries common and advanced_extension") {
    assertFieldNumbers(DdlRel.getDescriptor, "common" -> 8, "advanced_extension" -> 9)
  }
}
