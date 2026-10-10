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

import org.apache.gluten.execution.{ProjectExecTransformer, VeloxWholeStageTransformerSuite}

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.catalyst.expressions.{AttributeReference, Expression}
import org.apache.spark.sql.catalyst.expressions.GetStructField
import org.apache.spark.sql.execution.ProjectExec
import org.apache.spark.sql.types.{IntegerType, LongType, StringType, StructField, StructType}

class StructFieldBindSuite extends VeloxWholeStageTransformerSuite {
  override protected val resourcePath: String = ""
  override protected val fileFormat: String = "parquet"

  private def bind(expr: Expression, input: Seq[AttributeReference]) =
    ExpressionConverter
      .replaceWithExpressionTransformer(expr, input)
      .asInstanceOf[VeloxGetStructFieldTransformer]

  private def readsStructField(expressions: Seq[Expression]): Boolean =
    expressions.exists(_.find(_.isInstanceOf[GetStructField]).isDefined)

  // The struct fields must be read by a native projection, or the result comparison would not
  // exercise the binding.
  private def assertStructReadOffloaded(df: DataFrame): Unit = {
    val plan = df.queryExecution.executedPlan
    assert(
      collect(plan) { case p: ProjectExecTransformer if readsStructField(p.projectList) => p }
        .nonEmpty,
      plan)
  }

  test("struct field binding resolves the root attribute by exprId, not by name") {
    // Two same-named struct attributes with different layouts, as after a join of two tables
    // that both have a struct column `s`.
    val target =
      AttributeReference(
        "s",
        StructType(Seq(StructField("id", IntegerType), StructField("v", StringType))))()
    val decoy =
      AttributeReference(
        "s",
        StructType(Seq(StructField("v", IntegerType), StructField("z", StringType))))()

    val transformer = bind(GetStructField(target, ordinal = 1, Some("v")), Seq(target, decoy))

    assert(transformer.ordinal == 1)
  }

  test("struct field binding keeps the ordinal of a duplicate field name") {
    val s = AttributeReference(
      "s",
      StructType(Seq(StructField("a", IntegerType), StructField("a", LongType))))()

    assert(bind(GetStructField(s, ordinal = 0), Seq(s)).ordinal == 0)
    assert(bind(GetStructField(s, ordinal = 1), Seq(s)).ordinal == 1)
  }

  test("struct field binding looks fields up by name in a pruned input struct") {
    // The input carries the same attribute with a pruned struct type, so the expression's
    // ordinals do not apply and each level is found by name: s.b.c is field 0 of field 1.
    val inner = StructType(Seq(StructField("a", IntegerType), StructField("c", StringType)))
    val s = AttributeReference(
      "s",
      StructType(Seq(StructField("id", IntegerType), StructField("b", inner))))()
    val pruned = AttributeReference(
      "s",
      StructType(
        Seq(
          StructField("id", IntegerType),
          StructField("b", StructType(Seq(StructField("c", StringType)))))))(exprId = s.exprId)
    val level1 = GetStructField(s, ordinal = 1, Some("b"))
    val level2 = GetStructField(level1, ordinal = 1, Some("c"))

    val transformer = bind(level2, Seq(pruned))

    assert(transformer.ordinal == 0)
    assert(transformer.child.asInstanceOf[VeloxGetStructFieldTransformer].ordinal == 1)
  }

  test("struct field binding falls back on an ambiguous name in a pruned input struct") {
    val s = AttributeReference(
      "s",
      StructType(
        Seq(
          StructField("a", IntegerType),
          StructField("a", LongType),
          StructField("z", StringType))))()
    val pruned = AttributeReference(
      "s",
      StructType(Seq(StructField("a", IntegerType), StructField("a", LongType))))(
      exprId = s.exprId)

    val e = intercept[UnsupportedOperationException] {
      bind(GetStructField(s, ordinal = 1), Seq(pruned))
    }
    assert(e.getMessage.contains("ambiguous field a"), e.getMessage)
  }

  test("same-named struct columns with different layouts in a join") {
    // Both join sides carry a struct column `s`, with the fields in a different order. The
    // query reads every field, so nested column pruning keeps the whole structs above the join
    // and the projection there sees both `s` attributes.
    withTempPath {
      dir =>
        val left = new java.io.File(dir, "left").getCanonicalPath
        val right = new java.io.File(dir, "right").getCanonicalPath
        spark
          .sql("select id, named_struct('x', id, 'y', id * 100) as s from range(5)")
          .write
          .parquet(left)
        spark
          .sql("select id, named_struct('y', id * 100, 'x', id) as s from range(5)")
          .write
          .parquet(right)
        withTempView("l", "r") {
          spark.read.parquet(left).createOrReplaceTempView("l")
          spark.read.parquet(right).createOrReplaceTempView("r")
          runQueryAndCompare("select l.s.x, l.s.y, r.s.x, r.s.y from l join r on l.id = r.id") {
            assertStructReadOffloaded
          }
        }
    }
  }

  test("struct fields with duplicate names fall back when the input struct type differs") {
    // The empty branch makes the first field of the union's struct nullable. Once the empty branch
    // is removed, the projection reads the other branch's struct under the same exprId but with a
    // different type, so fields are looked up by name, and two fields named a must fall back.
    runQueryAndCompare(
      "select s.* from (" +
        "select /*+ REPARTITION(2) */ named_struct('a', id, 'a', id * 10) as s from range(5) " +
        "union all " +
        "select named_struct('a', cast(null as bigint), 'a', id) as s from range(5) where false)",
      noFallBack = false
    ) {
      df =>
        val plan = df.queryExecution.executedPlan
        assert(
          collect(plan) { case p: ProjectExec if readsStructField(p.projectList) => p }.nonEmpty,
          plan)
    }
  }

  test("struct fields with duplicate names read through the generate rewrite") {
    // The rewrite reads the inline output through a struct whose fields are named after the
    // aliases, so both columns read a field named x and only the ordinal tells them apart.
    runQueryAndCompare(
      "select v.* from (select id, array(named_struct('p', id, 'q', id * 10)) as arr " +
        "from range(5)) lateral view inline(arr) v as x, x")(assertStructReadOffloaded)
  }
}
