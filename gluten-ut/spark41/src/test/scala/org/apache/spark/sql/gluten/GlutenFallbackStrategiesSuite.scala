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
package org.apache.spark.sql.gluten

import org.apache.gluten.backendsapi.BackendsApiManager
import org.apache.gluten.config.GlutenConfig
import org.apache.gluten.execution.{BasicScanExecTransformer, GlutenPlan}
import org.apache.gluten.extension.GlutenSessionExtensions
import org.apache.gluten.extension.caller.CallerInfo
import org.apache.gluten.extension.columnar.{FallbackTags, RemoveFallbackTagRule}
import org.apache.gluten.extension.columnar.ColumnarRuleApplier.ColumnarRuleCall
import org.apache.gluten.extension.columnar.MiscColumnarRules.RemoveTopmostColumnarToRow
import org.apache.gluten.extension.columnar.heuristic.{ExpandFallbackPolicy, HeuristicApplier}
import org.apache.gluten.extension.columnar.transition.{Convention, InsertBackendTransitions}

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{GlutenSQLTestsTrait, SparkSession}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution._

class GlutenFallbackStrategiesSuite extends GlutenSQLTestsTrait {
  import GlutenFallbackStrategiesSuite._

  testGluten("Fall back the whole query if one unsupported") {
    withSQLConf((GlutenConfig.COLUMNAR_QUERY_FALLBACK_THRESHOLD.key, "1")) {
      val originalPlan = UnaryOp2(UnaryOp1(UnaryOp2(UnaryOp1(LeafOp()))))
      val rule = newRuleApplier(
        spark,
        List(
          _ =>
            _ => {
              UnaryOp2(UnaryOp1Transformer(UnaryOp2(UnaryOp1Transformer(LeafOp()))))
            },
          c => InsertBackendTransitions(c.outputsColumnar)))
      val outputPlan = rule.apply(originalPlan, false)
      // Expect to fall back the entire plan.
      assert(outputPlan == originalPlan)
    }
  }

  testGluten("Fall back the whole plan if meeting the configured threshold") {
    withSQLConf((GlutenConfig.COLUMNAR_WHOLESTAGE_FALLBACK_THRESHOLD.key, "1")) {
      CallerInfo.withLocalValue(isAqe = true, isCache = false) {
        val originalPlan = UnaryOp2(UnaryOp1(UnaryOp2(UnaryOp1(LeafOp()))))
        val rule = newRuleApplier(
          spark,
          List(
            _ =>
              _ => {
                UnaryOp2(UnaryOp1Transformer(UnaryOp2(UnaryOp1Transformer(LeafOp()))))
              },
            c => InsertBackendTransitions(c.outputsColumnar)))
        val outputPlan = rule.apply(originalPlan, false)
        // Expect to fall back the entire plan.
        assert(outputPlan == originalPlan)
      }
    }
  }

  testGluten("Don't fall back the whole plan if NOT meeting the configured threshold") {
    withSQLConf((GlutenConfig.COLUMNAR_WHOLESTAGE_FALLBACK_THRESHOLD.key, "4")) {
      CallerInfo.withLocalValue(isAqe = true, isCache = false) {
        val originalPlan = UnaryOp2(UnaryOp1(UnaryOp2(UnaryOp1(LeafOp()))))
        val rule = newRuleApplier(
          spark,
          List(
            _ =>
              _ => {
                UnaryOp2(UnaryOp1Transformer(UnaryOp2(UnaryOp1Transformer(LeafOp()))))
              },
            c => InsertBackendTransitions(c.outputsColumnar)))
        val outputPlan = rule.apply(originalPlan, false)
        // Expect to get the plan with columnar rule applied.
        assert(outputPlan != originalPlan)
      }
    }
  }

  testGluten(
    "Fall back the whole plan if meeting the configured threshold (leaf node is" +
      " transformable)") {
    withSQLConf((GlutenConfig.COLUMNAR_WHOLESTAGE_FALLBACK_THRESHOLD.key, "2")) {
      CallerInfo.withLocalValue(isAqe = true, isCache = false) {
        val originalPlan = UnaryOp2(UnaryOp1(UnaryOp2(UnaryOp1(LeafOp()))))
        val rule = newRuleApplier(
          spark,
          List(
            _ =>
              _ => {
                UnaryOp2(UnaryOp1Transformer(UnaryOp2(UnaryOp1Transformer(LeafOpTransformer()))))
              },
            c => InsertBackendTransitions(c.outputsColumnar)))
        val outputPlan = rule.apply(originalPlan, false)
        // Expect to fall back the entire plan.
        assert(outputPlan == originalPlan)
      }
    }
  }

  testGluten(
    "Don't Fall back the whole plan if NOT meeting the configured threshold (" +
      "leaf node is transformable)") {
    withSQLConf((GlutenConfig.COLUMNAR_WHOLESTAGE_FALLBACK_THRESHOLD.key, "3")) {
      CallerInfo.withLocalValue(isAqe = true, isCache = false) {
        val originalPlan = UnaryOp2(UnaryOp1(UnaryOp2(UnaryOp1(LeafOp()))))
        val rule = newRuleApplier(
          spark,
          List(
            _ =>
              _ => {
                UnaryOp2(UnaryOp1Transformer(UnaryOp2(UnaryOp1Transformer(LeafOpTransformer()))))
              },
            c => InsertBackendTransitions(c.outputsColumnar)))
        val outputPlan = rule.apply(originalPlan, false)
        // Expect to get the plan with columnar rule applied.
        assert(outputPlan != originalPlan)
      }
    }
  }

  testGluten("Tag not transformable more than once") {
    val originalPlan = UnaryOp1(LeafOp(supportsColumnar = true))
    FallbackTags.add(originalPlan, "fake reason")
    val rule = FallbackEmptySchemaRelation()
    val newPlan = rule.apply(originalPlan)
    val reason = FallbackTags.get(newPlan).reason()
    assert(
      reason.contains("fake reason") &&
        reason.contains("at least one of its children has empty output"))
  }

  testGluten("test enabling/disabling Gluten at thread level") {
    spark.sql("create table fallback_by_thread_config (a int) using parquet")
    spark.sql("insert overwrite fallback_by_thread_config select id as a from range(3)")
    val sql =
      """
        |select *
        |from fallback_by_thread_config as t0
        |""".stripMargin

    val noFallbackPlan = spark.sql(sql).queryExecution.executedPlan
    val noFallbackScanExec = noFallbackPlan.collect { case _: BasicScanExecTransformer => true }
    assert(noFallbackScanExec.size == 1)

    val thread = new Thread(
      () => {
        spark.sparkContext
          .setLocalProperty(GlutenSessionExtensions.GLUTEN_ENABLE_FOR_THREAD_KEY, "false")
        val fallbackPlan = spark.sql(sql).queryExecution.executedPlan
        val fallbackScanExec = fallbackPlan.collect {
          case e: FileSourceScanExec if !e.isInstanceOf[BasicScanExecTransformer] => true
        }
        assert(fallbackScanExec.size == 1)

        spark.sparkContext
          .setLocalProperty(GlutenSessionExtensions.GLUTEN_ENABLE_FOR_THREAD_KEY, null)
        val noFallbackPlan = spark.sql(sql).queryExecution.executedPlan
        val noFallbackScanExec = noFallbackPlan.collect { case _: BasicScanExecTransformer => true }
        assert(noFallbackScanExec.size == 1)
      })
    thread.start()
    thread.join(10000)
  }
}

private object GlutenFallbackStrategiesSuite {
  def newRuleApplier(
      spark: SparkSession,
      transformBuilders: Seq[ColumnarRuleCall => Rule[SparkPlan]]): HeuristicApplier = {
    new HeuristicApplier(
      spark,
      transformBuilders,
      List(c => p => ExpandFallbackPolicy(c.caller.isAqe(), p)),
      List(
        c => RemoveTopmostColumnarToRow(c.session, c.caller.isAqe()),
        _ => ColumnarCollapseTransformStages(GlutenConfig.get)
      ),
      List(_ => RemoveFallbackTagRule()),
      Nil
    )
  }

  // TODO: Generalize the code among shim versions.
  case class FallbackEmptySchemaRelation() extends Rule[SparkPlan] {
    override def apply(plan: SparkPlan): SparkPlan = plan.transformDown {
      case p =>
        if (p.children.exists(_.output.isEmpty)) {
          // Some backends are not eligible to offload plan with zero-column input.
          // If any child have empty output, mark the plan and that child as UNSUPPORTED.
          FallbackTags.add(p, "at least one of its children has empty output")
          p.children.foreach {
            child =>
              if (child.output.isEmpty) {
                FallbackTags.add(child, "at least one of its children has empty output")
              }
          }
        }
        p
    }
  }

  case class LeafOp(override val supportsColumnar: Boolean = false) extends LeafExecNode {
    override protected def doExecute(): RDD[InternalRow] = throw new UnsupportedOperationException()
    override def output: Seq[Attribute] = Seq.empty
  }

  case class UnaryOp1(child: SparkPlan, override val supportsColumnar: Boolean = false)
    extends UnaryExecNode {
    override protected def doExecute(): RDD[InternalRow] = throw new UnsupportedOperationException()
    override def output: Seq[Attribute] = child.output
    override protected def withNewChildInternal(newChild: SparkPlan): UnaryOp1 =
      copy(child = newChild)
  }

  case class UnaryOp2(child: SparkPlan, override val supportsColumnar: Boolean = false)
    extends UnaryExecNode {
    override protected def doExecute(): RDD[InternalRow] = throw new UnsupportedOperationException()
    override def output: Seq[Attribute] = child.output
    override protected def withNewChildInternal(newChild: SparkPlan): UnaryOp2 =
      copy(child = newChild)
  }

// For replacing LeafOp.
  case class LeafOpTransformer() extends LeafExecNode with GlutenPlan {
    override def batchType(): Convention.BatchType = BackendsApiManager.getSettings.primaryBatchType
    override def rowType0(): Convention.RowType = Convention.RowType.None
    override protected def doExecute(): RDD[InternalRow] = throw new UnsupportedOperationException()
    override def output: Seq[Attribute] = Seq.empty
  }

// For replacing UnaryOp1.
  case class UnaryOp1Transformer(override val child: SparkPlan)
    extends UnaryExecNode
    with GlutenPlan {
    override def batchType(): Convention.BatchType = BackendsApiManager.getSettings.primaryBatchType
    override def rowType0(): Convention.RowType = Convention.RowType.None
    override protected def doExecute(): RDD[InternalRow] = throw new UnsupportedOperationException()
    override def output: Seq[Attribute] = child.output
    override protected def withNewChildInternal(newChild: SparkPlan): UnaryOp1Transformer =
      copy(child = newChild)
  }
}
