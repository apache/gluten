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
package org.apache.gluten.extension

import org.apache.gluten.config.BoltConfig
import org.apache.gluten.execution.{BoltWholeStageTransformerSuite, FilterExecTransformer, GenerateExecTransformer, ProjectExecTransformer}

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.execution.LeafExecNode
import org.apache.spark.sql.types.{ArrayType, IntegerType, LongType, MetadataBuilder}

class BoltGeneratorProjectRuleSuite extends BoltWholeStageTransformerSuite {
  override protected val resourcePath: String = "/tpch-data-parquet"
  override protected val fileFormat: String = "parquet"

  private case class InputPlan(attrs: Seq[Attribute]) extends LeafExecNode {
    override def output: Seq[Attribute] = attrs
    override protected def doExecute(): RDD[InternalRow] =
      throw new UnsupportedOperationException("Plan validation only")
  }

  private def generator(): GenerateExecTransformer = {
    val input = AttributeReference("input", IntegerType, nullable = false)()
    val array = AttributeReference("array", ArrayType(IntegerType, containsNull = false))()
    val generated = AttributeReference("generated", IntegerType, nullable = false)()
    GenerateExecTransformer(
      Explode(array),
      Seq(input),
      false,
      Seq(generated),
      InputPlan(Seq(input, array)))
  }

  private def aliases(gen: GenerateExecTransformer): Seq[NamedExpression] = {
    val metadata = new MetadataBuilder().putString("source", "original").build()
    Seq(
      Alias(gen.requiredChildOutput.head, "renamed_input")(
        qualifier = Seq("renamed"),
        explicitMetadata = Some(metadata)),
      Alias(gen.generatorOutput.head, "renamed_generated")(
        qualifier = Seq("renamed"),
        explicitMetadata = Some(metadata))
    )
  }

  private def enabled(f: => Unit): Unit =
    withSQLConf(BoltConfig.ENABLE_COLUMNAR_PROJECT_REMOVE.key -> "true")(f)

  test("Remove generator project preserves aliases and input bindings") {
    enabled {
      val gen = generator()
      val project = ProjectExecTransformer(aliases(gen), gen)
      val result = RemoveProjectExecBeforeGeneratorRule(project)
      assert(result.isInstanceOf[GenerateExecTransformer])
      assert(result.output == project.output)
      assert(result.output.map(_.qualifier) == project.output.map(_.qualifier))
      assert(result.output.map(_.metadata) == project.output.map(_.metadata))
      val rewritten = result.asInstanceOf[GenerateExecTransformer]
      assert(rewritten.requiredChildOutput.forall(
        a => rewritten.child.output.exists(_.exprId == a.exprId)))
      assert(rewritten.doValidate().ok())
    }
  }

  test("Remove generator project preserves filter bindings to renamed generated attributes") {
    enabled {
      val gen = generator()
      val filter = FilterExecTransformer(EqualTo(gen.generatorOutput.head, Literal(1)), gen)
      val project = ProjectExecTransformer(aliases(gen), filter)
      val result = RemoveProjectExecBeforeGeneratorRule(project)
      assert(result.isInstanceOf[FilterExecTransformer])
      assert(result.output == project.output)
      val rewritten = result.asInstanceOf[FilterExecTransformer]
      assert(rewritten.condition.references.subsetOf(rewritten.child.outputSet))
      assert(rewritten.doValidate().ok())
    }
  }

  test("Duplicate generator project inputs retain the original plan") {
    enabled {
      val gen = generator()
      val project = ProjectExecTransformer(
        Seq(
          Alias(gen.requiredChildOutput.head, "first")(),
          Alias(gen.requiredChildOutput.head, "second")()),
        gen)
      assert(RemoveProjectExecBeforeGeneratorRule(project) eq project)
    }
  }

  test("Reordered generator project output retains the original plan") {
    enabled {
      val gen = generator()
      val project = ProjectExecTransformer(aliases(gen).reverse, gen)
      assert(RemoveProjectExecBeforeGeneratorRule(project) eq project)
    }
  }

  test("Computed generator project output retains the original plan") {
    enabled {
      val gen = generator()
      val project = ProjectExecTransformer(
        Seq(Alias(Add(gen.requiredChildOutput.head, Literal(1)), "computed")(), aliases(gen).last),
        gen)
      assert(RemoveProjectExecBeforeGeneratorRule(project) eq project)
    }
  }

  test("Disabled generator project removal retains the original plan") {
    withSQLConf(BoltConfig.ENABLE_COLUMNAR_PROJECT_REMOVE.key -> "false") {
      val gen = generator()
      val project = ProjectExecTransformer(aliases(gen), gen)
      assert(RemoveProjectExecBeforeGeneratorRule(project) eq project)
    }
  }

  test("Pushed generator projects preserve shared generator input bindings") {
    withSQLConf(BoltConfig.ENABLE_COLUMNAR_PROJECT_PUSHDOWN.key -> "true") {
      val gen = generator()
      val array = gen.generator.children.head
      val project = ProjectExecTransformer(
        Seq(Alias(Size(array), "size")(), gen.generatorOutput.head),
        gen)
      val result = PushdownProjectExecBeforeGeneratorRule(spark)(project)
      assert(result ne project)
      val rewritten = result.collect { case g: GenerateExecTransformer => g }.head
      assert(rewritten.generator.references.subsetOf(rewritten.child.outputSet))
      assert(rewritten.requiredChildOutput.forall(
        a => rewritten.child.output.exists(_.exprId == a.exprId)))
      assert(result.output == project.output)
    }
  }

  test("Pushed generator projects preserve filter input bindings") {
    withSQLConf(BoltConfig.ENABLE_COLUMNAR_PROJECT_PUSHDOWN.key -> "true") {
      val gen = generator()
      val input = gen.requiredChildOutput.head
      val filter = FilterExecTransformer(GreaterThan(input, Literal(0)), gen)
      val project = ProjectExecTransformer(
        Seq(Alias(Cast(input, LongType), "next")(), gen.generatorOutput.head),
        filter)
      val result = PushdownProjectExecBeforeGeneratorRule(spark)(project)
      assert(result ne project)
      val rewritten = result.collect { case f: FilterExecTransformer => f }.head
      assert(rewritten.condition.references.subsetOf(rewritten.child.outputSet))
      assert(result.output == project.output)
    }
  }

  test("Generator project pushdown evaluates nondeterministic expressions per generated row") {
    val session = spark
    import session.implicits._
    withTempView("generator_pushdown_input") {
      Seq(Seq(1, 2, 3)).toDF("a").createOrReplaceTempView("generator_pushdown_input")
      for (outer <- Seq("", "OUTER "); computed <- Seq("", "size(a) AS n, ")) {
        val query = "SELECT rand(7) AS r, " + computed + "x FROM generator_pushdown_input " +
          "LATERAL VIEW " + outer + "explode(a) t AS x"
        def rows(enabled: Boolean): Seq[Row] = {
          var result = Seq.empty[Row]
          withSQLConf(BoltConfig.ENABLE_COLUMNAR_PROJECT_PUSHDOWN.key -> enabled.toString) {
            result = spark.sql(query).collect().toSeq
          }
          result
        }
        val control = rows(false)
        assert(control.map(_.getDouble(0)).distinct.size == 3)
        assert(rows(true) == control)
      }
    }
  }

  test("Generator project pushdown does not evaluate errors for discarded generated rows") {
    val session = spark
    import session.implicits._
    withTempView("generator_pushdown_input") {
      Seq((Seq(1, 2, 3), "bad"))
        .toDF("a", "s")
        .createOrReplaceTempView("generator_pushdown_input")
      val query = "SELECT raise_error(s) AS value, x FROM generator_pushdown_input " +
        "LATERAL VIEW explode(a) t AS x WHERE x < 0"
      for (enabled <- Seq(false, true)) {
        withSQLConf(BoltConfig.ENABLE_COLUMNAR_PROJECT_PUSHDOWN.key -> enabled.toString) {
          assert(spark.sql(query).collect().isEmpty)
        }
      }
    }
  }

  test("Potentially throwing generator projections retain their original position") {
    withSQLConf(BoltConfig.ENABLE_COLUMNAR_PROJECT_PUSHDOWN.key -> "true") {
      val gen = generator()
      val input = gen.requiredChildOutput.head
      val projects = Seq(
        ProjectExecTransformer(
          Seq(Alias(Add(input, Literal(1)), "next")(), gen.generatorOutput.head),
          gen),
        ProjectExecTransformer(
          Seq(Alias(Add(input, Literal(1)), "next")(), gen.generatorOutput.head),
          FilterExecTransformer(GreaterThan(input, Literal(0)), gen))
      )
      projects.foreach(p => assert(PushdownProjectExecBeforeGeneratorRule(spark)(p) eq p))
    }
  }
}
