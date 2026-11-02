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

import org.apache.gluten.execution.SortExecTransformer

import org.apache.spark.sql.{catalyst, GlutenQueryTestUtil, GlutenSQLTestsBaseTrait, Row}
import org.apache.spark.sql.catalyst.analysis.{Resolver, UnresolvedAttribute}
import org.apache.spark.sql.catalyst.expressions.{Length, SortOrder}
import org.apache.spark.sql.catalyst.plans.QueryPlan
import org.apache.spark.sql.execution.adaptive.AdaptiveSparkPlanHelper
import org.apache.spark.sql.functions.length

class GlutenSortSuite extends SortSuite with GlutenSQLTestsBaseTrait with AdaptiveSparkPlanHelper {
  import testImplicits._

  protected val resolver: Resolver = conf.resolver

  protected def attr(name: String): UnresolvedAttribute = {
    UnresolvedAttribute(name)
  }

  protected def resolveAttrs[T <: QueryPlan[T]](
      expr: catalyst.expressions.Expression,
      plan: QueryPlan[T]): catalyst.expressions.Expression = {

    expr.transform {
      case UnresolvedAttribute(Seq(attrName)) =>
        plan.output.find(attr => resolver(attr.name, attrName)).get
      case UnresolvedAttribute(nameParts) =>
        val attrName = nameParts.mkString(".")
        fail(s"cannot resolve a nested attr: $attrName")
    }
  }

  testGluten("post-project outputOrdering check") {
    val input = Seq(
      ("Hello", 4, 2.0),
      ("Hello Bob", 10, 1.0),
      ("Hello Bob", 1, 3.0)
    )

    val df = input.toDF("a", "b", "c").orderBy(length($"a").desc, $"b".desc)
    GlutenQueryTestUtil.checkAnswer(
      df,
      Seq(
        Row("Hello Bob", 10, 1.0),
        Row("Hello Bob", 1, 3.0),
        Row("Hello", 4, 2.0)
      )
    )

    val ordering = Seq(
      catalyst.expressions.SortOrder(
        Length(attr("a")),
        catalyst.expressions.Descending,
        catalyst.expressions.NullsLast,
        Seq.empty
      ),
      catalyst.expressions.SortOrder(
        attr("b"),
        catalyst.expressions.Descending,
        catalyst.expressions.NullsLast,
        Seq.empty
      )
    )

    assert(
      getExecutedPlan(df).exists {
        case _: SortExecTransformer => true
        case _ => false
      }
    )
    val plan = stripAQEPlan(df.queryExecution.executedPlan)
    val actualOrdering = plan.outputOrdering
    val expectedOrdering = ordering.map(resolveAttrs(_, plan).asInstanceOf[SortOrder])
    assert(actualOrdering.length == expectedOrdering.length)
    actualOrdering.zip(expectedOrdering).foreach {
      case (actual, expected) =>
        assert(actual.semanticEquals(expected), "ordering must match")
    }
  }
}
