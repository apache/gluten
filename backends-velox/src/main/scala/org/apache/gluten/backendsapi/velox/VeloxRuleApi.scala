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
package org.apache.gluten.backendsapi.velox

import org.apache.gluten.backendsapi.{BackendsApiManager, RuleApi}
import org.apache.gluten.config.GlutenConfig
import org.apache.gluten.extension._
import org.apache.gluten.extension.columnar._
import org.apache.gluten.extension.columnar.MiscColumnarRules.{PreventBatchTypeMismatchInTableCache, RemoveGlutenTableCacheColumnarToRow, RemoveTopmostColumnarToRow, RewriteSubqueryBroadcast}
import org.apache.gluten.extension.columnar.V2WritePostRule
import org.apache.gluten.extension.columnar.heuristic.{ExpandFallbackPolicy, HeuristicTransform}
import org.apache.gluten.extension.columnar.offload.{OffloadExchange, OffloadJoin, OffloadOthers}
import org.apache.gluten.extension.columnar.rewrite._
import org.apache.gluten.extension.columnar.transition.{InsertTransitions, RemoveTransitions}
import org.apache.gluten.extension.columnar.validator.{Validator, Validators}
import org.apache.gluten.extension.injector.{Injector, SparkInjector}
import org.apache.gluten.extension.injector.GlutenInjector.LegacyInjector
import org.apache.gluten.sql.shims.SparkShimLoader

import org.apache.spark.sql.execution._
import org.apache.spark.sql.execution.datasources.noop.GlutenNoopWriterRule

class VeloxRuleApi extends RuleApi {
  import VeloxRuleApi._

  override def injectRules(injector: Injector): Unit = {
    injectSpark(injector.spark)
    injectLegacy(injector.gluten.legacy)
  }
}

object VeloxRuleApi {

  /**
   * Registers Spark rules or extensions, except for Gluten's columnar rules that are supposed to be
   * injected through [[injectLegacy]].
   */
  private def injectSpark(injector: SparkInjector): Unit = {
    // Inject the regular Spark rules directly.
    injector.injectOptimizerRule(CollectRewriteRule.apply)
    injector.injectOptimizerRule(HLLRewriteRule.apply)
    injector.injectOptimizerRule(CollapseGetJsonObjectExpressionRule.apply)
    injector.injectOptimizerRule(RewriteCastFromArray.apply)
    injector.injectOptimizerRule(RewriteUnboundedWindow.apply)

    if (!BackendsApiManager.getSettings.enableJoinKeysRewrite()) {
      injector.injectPlannerStrategy(_ => org.apache.gluten.extension.GlutenJoinKeysCapture())
    }

    if (BackendsApiManager.getSettings.supportAppendDataExec()) {
      injector.injectPlannerStrategy(SparkShimLoader.getSparkShims.getRewriteCreateTableAsSelect(_))
    }
  }

  /**
   * Registers Gluten's columnar rules. These rules will be executed by default in Gluten for
   * columnar query planning.
   */
  private def injectLegacy(injector: LegacyInjector): Unit = {
    // Legacy: Pre-transform rules.
    injector.injectPreTransform(_ => RemoveTransitions)
    injector.injectPreTransform(_ => PushDownInputFileExpression.PreOffload)
    injector.injectPreTransform(c => FallbackOnANSIMode.apply(c.session))
    injector.injectPreTransform(c => FallbackMultiCodegens.apply(c.session))
    injector.injectPreTransform(c => MergeTwoPhasesHashBaseAggregate(c.session))
    injector.injectPreTransform(_ => RewriteSubqueryBroadcast())
    injector.injectPreTransform(
      c =>
        BloomFilterMightContainJointRewriteRule.apply(
          c.session,
          c.caller.isBloomFilterStatFunction()))
    injector.injectPreTransform(_ => EliminateRedundantGetTimestamp)

    // Legacy: The legacy transform rule.
    val offloads = Seq(OffloadOthers(), OffloadExchange(), OffloadJoin()).map(_.toStrcitRule())
    val validatorBuilder: GlutenConfig => Validator = conf =>
      Validators.newValidator(conf, offloads)
    val rewrites =
      Seq(
        RewriteIn,
        RewriteMultiChildrenCount,
        RewriteJoin,
        PullOutPreProject,
        PullOutPostProject,
        ProjectColumnPruning)
    injector.injectTransform(
      c =>
        HeuristicTransform.WithRewrites(
          validatorBuilder(new GlutenConfig(c.sqlConf)),
          rewrites,
          offloads))

    // Legacy: Post-transform rules.
    injector.injectPostTransform(_ => AppendBatchResizeForShuffleInputAndOutput())
    injector.injectPostTransform(_ => GpuBufferBatchResizeForShuffleInputOutput())
    injector.injectPostTransform(_ => UnionTransformerRule())
    injector.injectPostTransform(_ => PartialFallbackRules())
    injector.injectPostTransform(_ => RemoveNativeWriteFilesSortAndProject())
    injector.injectPostTransform(_ => PushDownFilterToScan)
    injector.injectPostTransform(_ => PushDownInputFileExpression.PostOffload)
    injector.injectPostTransform(_ => EnsureLocalSortRequirements)
    injector.injectPostTransform(_ => EliminateLocalSort)
    injector.injectPostTransform(_ => CollapseProjectExecTransformer)
    injector.injectPostTransform(c => FlushableHashAggregateRule.apply(c.session))
    injector.injectPostTransform(_ => CollectLimitTransformerRule())
    injector.injectPostTransform(_ => CollectTailTransformerRule())
    injector.injectPostTransform(_ => V2WritePostRule())
    injector.injectPostTransform(c => InsertTransitions.create(c.outputsColumnar, VeloxBatchType))

    // Gluten columnar: Fallback policies.
    injector.injectFallbackPolicy(c => p => ExpandFallbackPolicy(c.caller.isAqe(), p))

    // Gluten columnar: Post rules.
    injector.injectPost(c => RemoveTopmostColumnarToRow(c.session, c.caller.isAqe()))
    SparkShimLoader.getSparkShims
      .getExtendedColumnarPostRules()
      .foreach(each => injector.injectPost(c => each(c.session)))
    injector.injectPost(c => ColumnarCollapseTransformStages(new GlutenConfig(c.sqlConf)))
    injector.injectPost(_ => GenerateTransformStageId())
    injector.injectPost(c => CudfNodeValidationRule(new GlutenConfig(c.sqlConf)))

    injector.injectPost(c => GlutenNoopWriterRule(c.session))

    // Gluten columnar: Final rules.
    injector.injectFinal(c => RemoveGlutenTableCacheColumnarToRow(c.session))
    injector.injectFinal(
      c => PreventBatchTypeMismatchInTableCache(c.caller.isCache(), Set(VeloxBatchType)))
    injector.injectFinal(
      c => GlutenAutoAdjustStageResourceProfile(new GlutenConfig(c.sqlConf), c.session))
    injector.injectFinal(c => GlutenFallbackReporter(new GlutenConfig(c.sqlConf), c.session))
    injector.injectFinal(_ => RemoveFallbackTagRule())
  }
}
