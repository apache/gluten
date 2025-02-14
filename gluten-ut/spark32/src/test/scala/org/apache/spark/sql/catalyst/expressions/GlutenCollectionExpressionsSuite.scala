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
package org.apache.spark.sql.catalyst.expressions

import org.apache.spark.sql.GlutenTestsTrait
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.analysis.TypeCheckResult
import org.apache.spark.sql.types._

import scala.util.Random

class GlutenCollectionExpressionsSuite extends CollectionExpressionsSuite with GlutenTestsTrait {
  testGluten("Shuffle") {
    // Primitive-type elements
    val ai0 = Literal.create(Seq(1, 2, 3, 4, 5), ArrayType(IntegerType, containsNull = false))
    val ai1 = Literal.create(Seq(1, 2, 3), ArrayType(IntegerType, containsNull = false))
    val ai2 = Literal.create(Seq(null, 1, null, 3), ArrayType(IntegerType, containsNull = true))
    val ai3 = Literal.create(Seq(2, null, 4, null), ArrayType(IntegerType, containsNull = true))
    val ai4 = Literal.create(Seq(null, null, null), ArrayType(IntegerType, containsNull = true))
    val ai5 = Literal.create(Seq(1), ArrayType(IntegerType, containsNull = false))
    val ai6 = Literal.create(Seq.empty, ArrayType(IntegerType, containsNull = false))
    val ai7 = Literal.create(null, ArrayType(IntegerType, containsNull = true))

    checkEvaluation(Shuffle(ai0, Some(0)), Array(2, 1, 5, 4, 3))
    checkEvaluation(Shuffle(ai1, Some(0)), Array(2, 1, 3))
    checkEvaluation(Shuffle(ai2, Some(0)), Array(1, null, null, 3))
    checkEvaluation(Shuffle(ai3, Some(0)), Array(null, 2, 4, null))
    checkEvaluation(Shuffle(ai4, Some(0)), Array(null, null, null))
    checkEvaluation(Shuffle(ai5, Some(0)), Array(1))
    checkEvaluation(Shuffle(ai6, Some(0)), Array.empty)
    checkEvaluation(Shuffle(ai7, Some(0)), null)

    // Non-primitive-type elements
    val as0 = Literal.create(Seq("a", "b", "c", "d"), ArrayType(StringType, containsNull = false))
    val as1 = Literal.create(Seq("a", "b", "c"), ArrayType(StringType, containsNull = false))
    val as2 = Literal.create(Seq(null, "a", null, "c"), ArrayType(StringType, containsNull = true))
    val as3 = Literal.create(Seq("b", null, "d", null), ArrayType(StringType, containsNull = true))
    val as4 = Literal.create(Seq(null, null, null), ArrayType(StringType, containsNull = true))
    val as5 = Literal.create(Seq("a"), ArrayType(StringType, containsNull = false))
    val as6 = Literal.create(Seq.empty, ArrayType(StringType, containsNull = false))
    val as7 = Literal.create(null, ArrayType(StringType, containsNull = true))
    val aa =
      Literal.create(Seq(Seq("a", "b"), Seq("c", "d"), Seq("e")), ArrayType(ArrayType(StringType)))

    checkEvaluation(Shuffle(as0, Some(0)), Array("b", "a", "c", "d"))
    checkEvaluation(Shuffle(as1, Some(0)), Array("b", "a", "c"))
    checkEvaluation(Shuffle(as2, Some(0)), Array("a", null, null, "c"))
    checkEvaluation(Shuffle(as3, Some(0)), Array(null, "b", "d", null))
    checkEvaluation(Shuffle(as4, Some(0)), Array(null, null, null))
    checkEvaluation(Shuffle(as5, Some(0)), Array("a"))
    checkEvaluation(Shuffle(as6, Some(0)), Array.empty)
    checkEvaluation(Shuffle(as7, Some(0)), null)
    checkEvaluation(Shuffle(aa, Some(0)), Array(Array("c", "d"), Array("a", "b"), Array("e")))

    val r = new Random(1234)
    val seed1 = Some(r.nextLong())
    assert(
      evaluateWithoutCodegen(Shuffle(ai0, seed1)) ===
        evaluateWithoutCodegen(Shuffle(ai0, seed1)))
    assert(
      evaluateWithMutableProjection(Shuffle(ai0, seed1)) ===
        evaluateWithMutableProjection(Shuffle(ai0, seed1)))
    assert(
      evaluateWithUnsafeProjection(Shuffle(ai0, seed1)) ===
        evaluateWithUnsafeProjection(Shuffle(ai0, seed1)))

    val seed2 = Some(r.nextLong())
    assert(
      evaluateWithoutCodegen(Shuffle(ai0, seed1)) !==
        evaluateWithoutCodegen(Shuffle(ai0, seed2)))
    assert(
      evaluateWithMutableProjection(Shuffle(ai0, seed1)) !==
        evaluateWithMutableProjection(Shuffle(ai0, seed2)))
    assert(
      evaluateWithUnsafeProjection(Shuffle(ai0, seed1)) !==
        evaluateWithUnsafeProjection(Shuffle(ai0, seed2)))
  }

  testGluten("MapFromEntries") {
    def arrayType(keyType: DataType, valueType: DataType): DataType = {
      ArrayType(StructType(Seq(StructField("a", keyType), StructField("b", valueType))), true)
    }

    def row(values: Any*): InternalRow = create_row(values: _*)

    // Primitive-type keys and values
    val aiType = arrayType(IntegerType, IntegerType)
    val ai0 = Literal.create(Seq(row(1, 10), row(2, 20), row(3, 20)), aiType)
    val ai1 = Literal.create(Seq(row(1, null), row(2, 20), row(3, null)), aiType)
    val ai2 = Literal.create(Seq.empty, aiType)
    val ai3 = Literal.create(null, aiType)
    // Ignore duplicated key as 'last_win' not supported by Velox for now
    // val ai4 = Literal.create(Seq(row(1, 10), row(1, 20)), aiType)
    // The map key is null
    val ai5 = Literal.create(Seq(row(1, 10), row(null, 20)), aiType)
    val ai6 = Literal.create(Seq(null, row(2, 20), null), aiType)

    checkEvaluation(MapFromEntries(ai0), create_map(1 -> 10, 2 -> 20, 3 -> 20))
    checkEvaluation(MapFromEntries(ai1), create_map(1 -> null, 2 -> 20, 3 -> null))
    checkEvaluation(MapFromEntries(ai2), Map.empty)
    checkEvaluation(MapFromEntries(ai3), null)

    // Map key can't be null
    checkExceptionInExpression[RuntimeException](MapFromEntries(ai5), "Cannot use null as map key")
    checkEvaluation(MapFromEntries(ai6), null)

    // Non-primitive-type keys and values
    val asType = arrayType(StringType, StringType)
    val as0 = Literal.create(Seq(row("a", "aa"), row("b", "bb"), row("c", "bb")), asType)
    val as1 = Literal.create(Seq(row("a", null), row("b", "bb"), row("c", null)), asType)
    val as2 = Literal.create(Seq.empty, asType)
    val as3 = Literal.create(null, asType)
    val as5 = Literal.create(Seq(row("a", "aa"), row(null, "bb")), asType)
    val as6 = Literal.create(Seq(null, row("b", "bb"), null), asType)

    checkEvaluation(MapFromEntries(as0), create_map("a" -> "aa", "b" -> "bb", "c" -> "bb"))
    checkEvaluation(MapFromEntries(as1), create_map("a" -> null, "b" -> "bb", "c" -> null))
    checkEvaluation(MapFromEntries(as2), Map.empty)
    checkEvaluation(MapFromEntries(as3), null)

    // Map key can't be null
    checkExceptionInExpression[RuntimeException](MapFromEntries(as5), "Cannot use null as map key")
    checkEvaluation(MapFromEntries(as6), null)

    // map key can't be map
    val structOfMap = row(create_map(1 -> 1), 1)
    val map = MapFromEntries(
      Literal.create(
        Seq(structOfMap),
        arrayType(keyType = MapType(IntegerType, IntegerType), valueType = IntegerType)))
    map.checkInputDataTypes() match {
      case TypeCheckResult.TypeCheckSuccess => fail("should not allow map as map key")
      case TypeCheckResult.TypeCheckFailure(msg) =>
        assert(msg.contains("The key of map cannot be/contain map"))
    }
  }
}
