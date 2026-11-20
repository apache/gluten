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
package org.apache.gluten.expressions

import org.apache.gluten.expression.{ExpressionConverter, ExpressionTransformer}
import org.apache.gluten.substrait.SubstraitContext

import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.catalyst.expressions.{And, ArrayTransform, AttributeReference, Expression, GetArrayItem, GetArrayStructFields, GetMapValue, GetStructField, IsNotNull, LambdaFunction, Literal, NamedLambdaVariable}
import org.apache.spark.sql.types.{ArrayType, DataType, IntegerType, MapType, StringType, StructField, StructType}

class GlutenExpressionConverterSchemaPruningSuite extends SparkFunSuite {
  private val originalSchema = StructType(
    Seq(StructField("unused", IntegerType), StructField("value", IntegerType)))

  private def nestedType(container: String, schema: StructType): DataType = container match {
    case "struct" => schema
    case "map" => MapType(StringType, schema)
    case _ => ArrayType(schema)
  }

  private def extractField(container: String, attribute: Expression): Expression = {
    val schema = attribute.dataType match {
      case struct: StructType => struct
      case MapType(_, struct: StructType, _) => struct
      case ArrayType(struct: StructType, _) => struct
    }
    val ordinal = schema.fieldIndex("value")
    container match {
      case "struct" => GetStructField(attribute, ordinal, Some("value"))
      case "map" =>
        GetStructField(GetMapValue(attribute, Literal("key")), ordinal, Some("value"))
      case "array" =>
        GetStructField(
          GetArrayItem(attribute, Literal(0), failOnError = false),
          ordinal,
          Some("value"))
      case "array_fields" =>
        GetArrayStructFields(
          attribute,
          schema("value"),
          ordinal,
          schema.length,
          containsNull = true)
    }
  }

  private def structOrdinals(transformer: ExpressionTransformer): Seq[Int] = {
    val ordinal = transformer.original match {
      case field: GetStructField =>
        Seq(field.ordinal)
      case fields: GetArrayStructFields =>
        Seq(fields.ordinal)
      case _ => Seq.empty
    }
    ordinal ++ transformer.children.flatMap(structOrdinals)
  }

  for (container <- Seq("struct", "map", "array", "array_fields")) {
    test(s"$container extraction preserves lambda variable binding") {
      val variable =
        NamedLambdaVariable("value", nestedType(container, originalSchema), nullable = true)
      val input = AttributeReference("nested", ArrayType(variable.dataType))()
      val field = extractField(container, variable)
      assert(field.references.exists(_.exprId == variable.exprId))
      val expression = ArrayTransform(input, LambdaFunction(field, Seq(variable)))
      val transformer = ExpressionConverter.replaceWithExpressionTransformer(expression, Seq(input))
      assert(structOrdinals(transformer) == Seq(1))
      transformer.doTransform(new SubstraitContext).toProtobuf
    }

    for (pruning <- Seq(false, true)) {
      test(s"rebind $container extraction: nested pruning=$pruning") {
        val original = AttributeReference("nested", nestedType(container, originalSchema))()
        val schema = if (pruning) StructType(Seq(originalSchema("value"))) else originalSchema
        val input = original.withDataType(nestedType(container, schema))
        val predicate = IsNotNull(extractField(container, original))
        val rewritten = IsNotNull(extractField(container, input))
        val expectedOrdinal = if (pruning) 0 else 1
        val transformers = ExpressionConverter.replaceWithExpressionTransformer(
          Seq(predicate),
          Seq(input))
        assert(transformers.flatMap(structOrdinals) == Seq(expectedOrdinal))
        if (container == "array_fields") {
          val fields = transformers.head.children.head.original.asInstanceOf[GetArrayStructFields]
          assert(fields.numFields == schema.length)
        }

        // Both versions share an exprId. Check mixed conditions in both orders.
        for (condition <- Seq(And(predicate, rewritten), And(rewritten, predicate))) {
          val transformer =
            ExpressionConverter.replaceWithExpressionTransformer(condition, Seq(input))
          assert(structOrdinals(transformer) == Seq(expectedOrdinal, expectedOrdinal))
          transformer.doTransform(new SubstraitContext).toProtobuf
        }
      }
    }

    for (pruning <- Seq(false, true)) {
      test(
        s"ambiguous $container field falls back: pruning=$pruning") {
        val duplicates = StructType(
          Seq(
            StructField("value", IntegerType),
            StructField("value", IntegerType),
            StructField("unused", IntegerType)))
        val schema = if (pruning) {
          StructType(duplicates.fields.take(2))
        } else {
          StructType(duplicates.fields.map(_.copy(nullable = false)))
        }
        val original = AttributeReference("nested", nestedType(container, duplicates))()
        val expression = extractField(container, original)
        val input = Seq(original.withDataType(nestedType(container, schema)))
        val failure = intercept[UnsupportedOperationException] {
          ExpressionConverter.replaceWithExpressionTransformer(expression, input)
        }
        assert(failure.getMessage.contains("ambiguous field value"))
        assert(!ExpressionConverter.canReplaceWithExpressionTransformer(expression, input))
      }
    }

    test(s"missing $container field triggers unsupported-expression fallback") {
      val original = AttributeReference("nested", nestedType(container, originalSchema))()
      val expression = extractField(container, original)
      val input = Seq(original.withDataType(
        nestedType(container, StructType(Seq(originalSchema("unused"))))))
      val failure = intercept[UnsupportedOperationException] {
        ExpressionConverter.replaceWithExpressionTransformer(expression, input)
      }
      assert(failure.getCause != null)
      intercept[UnsupportedOperationException] {
        ExpressionConverter.replaceWithExpressionTransformer(Seq(expression), input)
      }
      assert(!ExpressionConverter.canReplaceWithExpressionTransformer(expression, input))
    }
  }

  test("expression binding preserves duplicate field ordinals when the layout is unchanged") {
    val schema =
      StructType(Seq(StructField("duplicate", IntegerType), StructField("duplicate", IntegerType)))
    val attribute = AttributeReference("nested", schema)()
    for (ordinal <- Seq(0, 1)) {
      val expression = GetStructField(attribute, ordinal, Some("duplicate"))
      val transformer = ExpressionConverter.replaceWithExpressionTransformer(
        expression,
        Seq(attribute.withNullability(false)))
      assert(structOrdinals(transformer) == Seq(ordinal))
    }
  }

  test("expression binding selects same-named input attributes by exprId") {
    val schema =
      StructType(Seq(StructField("unused", IntegerType), StructField("value", IntegerType)))
    val left = AttributeReference("nested", schema)()
    val right = AttributeReference("nested", schema)()
    val input = Seq(left.withDataType(StructType(Seq(schema("value")))), right)
    val expressions = Seq(left, right).map(GetStructField(_, 1, Some("value")))
    val transformers = ExpressionConverter.replaceWithExpressionTransformer(expressions, input)
    assert(transformers.map(structOrdinals) == Seq(Seq(0), Seq(1)))
  }

  test("expression binding reports a missing input attribute as unsupported") {
    val schema = StructType(Seq(StructField("value", IntegerType)))
    val attribute = AttributeReference("nested", schema)()
    val expression = GetStructField(attribute, 0, Some("value"))
    val failure = intercept[UnsupportedOperationException] {
      ExpressionConverter.replaceWithExpressionTransformer(expression, Seq.empty)
    }
    assert(failure.getMessage.contains("no input attribute"))
    assert(failure.getMessage.contains("in []"))
    assert(!ExpressionConverter.canReplaceWithExpressionTransformer(expression, Seq.empty))
  }

  test("converter rejects null input for attribute references") {
    val schema = StructType(Seq(StructField("value", IntegerType)))
    val expression = GetStructField(AttributeReference("nested", schema)(), 0, Some("value"))
    intercept[UnsupportedOperationException] {
      ExpressionConverter.replaceWithExpressionTransformer(expression, null)
    }
    intercept[UnsupportedOperationException] {
      ExpressionConverter.replaceWithExpressionTransformer(Seq(expression), null)
    }
    // Literal translation does not require input attributes.
    ExpressionConverter.replaceWithExpressionTransformer(Literal(1), null)
    ExpressionConverter.replaceWithExpressionTransformer(Seq(Literal(1)), null)
  }
}
