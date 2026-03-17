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
package org.apache.gluten.backendsapi.clickhouse

import org.apache.gluten.backendsapi.RuleApi
import org.apache.gluten.config.GlutenConfig
import org.apache.gluten.extension._
import org.apache.gluten.extension.columnar._
import org.apache.gluten.extension.columnar.MiscColumnarRules.{RemoveGlutenTableCacheColumnarToRow, RemoveTopmostColumnarToRow, RewriteSubqueryBroadcast}
import org.apache.gluten.extension.columnar.heuristic.{ExpandFallbackPolicy, HeuristicTransform}
import org.apache.gluten.extension.columnar.offload.{OffloadExchange, OffloadJoin, OffloadOthers}
import org.apache.gluten.extension.columnar.rewrite._
import org.apache.gluten.extension.columnar.transition.{InsertTransitions, RemoveTransitions}
import org.apache.gluten.extension.columnar.validator.{Validator, Validators}
import org.apache.gluten.extension.injector.{Injector, SparkInjector}
import org.apache.gluten.extension.injector.GlutenInjector.LegacyInjector
import org.apache.gluten.parser.{GlutenCacheFilesSqlParser, GlutenClickhouseSqlParser}
import org.apache.gluten.sql.shims.SparkShimLoader

import org.apache.spark.sql.catalyst.{CHAggregateFunctionRewriteRule, EqualToRewrite}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.delta.DeltaLogFileIndex
import org.apache.spark.sql.delta.rules.CHOptimizeMetadataOnlyDeltaQuery
import org.apache.spark.sql.execution._
import org.apache.spark.sql.execution.adaptive.AdaptiveSparkPlanHelper
import org.apache.spark.sql.execution.datasources.noop.GlutenNoopWriterRule
import org.apache.spark.sql.execution.datasources.v2.V2CommandExec
import org.apache.spark.util.SparkPlanRules

class CHRuleApi extends RuleApi {
  import CHRuleApi._
  override def injectRules(injector: Injector): Unit = {
    injectSpark(injector.spark)
    injectLegacy(injector.gluten.legacy)
  }
}

object CHRuleApi {

  /**
   * Registers Spark rules or extensions, except for Gluten's columnar rules that are supposed to be
   * injected through [[injectLegacy]].
   */
  private def injectSpark(injector: SparkInjector): Unit = {
    // Inject the regular Spark rules directly.
    injector.injectQueryStagePrepRule(FallbackBroadcastHashJoinPrepQueryStage.apply)
    injector.injectQueryStagePrepRule(spark => CHAQEPropagateEmptyRelation(spark))
    injector.injectParser(
      (spark, parserInterface) => new GlutenCacheFilesSqlParser(spark, parserInterface))
    injector.injectParser(
      (spark, parserInterface) => new GlutenClickhouseSqlParser(spark, parserInterface))
    injector.injectResolutionRule(spark => new JoinAggregateToAggregateUnion(spark))
    // CoalesceAggregationUnion and CoalesceProjectionUnion should follows
    // JoinAggregateToAggregateUnion
    injector.injectResolutionRule(spark => CoalesceAggregationUnion(spark))
    injector.injectResolutionRule(spark => CoalesceProjectionUnion(spark))
    injector.injectResolutionRule(spark => RewriteToDateExpresstionRule(spark))
    injector.injectResolutionRule(spark => RewriteDateTimestampComparisonRule(spark))
    injector.injectResolutionRule(spark => CollapseGetJsonObjectExpressionRule(spark))
    injector.injectResolutionRule(spark => RepalceFromJsonWithGetJsonObject(spark))
    injector.injectOptimizerRule(spark => CommonSubexpressionEliminateRule(spark))
    injector.injectOptimizerRule(spark => AggregateIfToFilterRule(spark))
    injector.injectOptimizerRule(spark => SimplifySumRule(spark))
    injector.injectOptimizerRule(spark => ExtendedColumnPruning(spark))
    injector.injectOptimizerRule(spark => CHAggregateFunctionRewriteRule(spark))
    injector.injectOptimizerRule(_ => CountDistinctWithoutExpand)
    injector.injectOptimizerRule(_ => EqualToRewrite)
    injector.injectPreCBORule(spark => new CHOptimizeMetadataOnlyDeltaQuery(spark))
  }

  /**
   * Registers Gluten's columnar rules. These rules will be executed by default in Gluten for
   * columnar query planning.
   */
  private def injectLegacy(injector: LegacyInjector): Unit = {
    // Legacy: Pre-transform rules.
    injector.injectPreTransform(_ => RemoveTransitions)
    injector.injectPreTransform(_ => PushDownInputFileExpression.PreOffload)
    injector.injectPreTransform(c => FallbackOnANSIMode(c.session))
    injector.injectPreTransform(c => FallbackMultiCodegens(c.session))
    injector.injectPreTransform(_ => RewriteSubqueryBroadcast())
    injector.injectPreTransform(c => FallbackBroadcastHashJoin(c.session))
    injector.injectPreTransform(c => MergeTwoPhasesHashBaseAggregate(c.session))
    injector.injectPreTransform(_ => WriteFilesWithBucketValue)

    // Legacy: The legacy transform rule.
    val offloads = Seq(OffloadOthers(), OffloadExchange(), OffloadJoin())
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
        intercept(
          HeuristicTransform
            .WithRewrites(validatorBuilder(new GlutenConfig(c.sqlConf)), rewrites, offloads)))

    // Legacy: Post-transform rules.
    injector.injectPostTransform(_ => PruneNestedColumnsInHiveTableScan)
    injector.injectPostTransform(_ => PushDownFilterToScan)
    injector.injectPostTransform(_ => PushDownInputFileExpression.PostOffload)
    injector.injectPostTransform(_ => EnsureLocalSortRequirements)
    injector.injectPostTransform(_ => EliminateLocalSort)
    injector.injectPostTransform(_ => CollapseProjectExecTransformer)
    injector.injectPostTransform(c => RewriteSortMergeJoinToHashJoinRule(c.session))
    injector.injectPostTransform(c => PushdownAggregatePreProjectionAheadExpand(c.session))
    injector.injectPostTransform(c => LazyAggregateExpandRule(c.session))
    injector.injectPostTransform(c => ConverRowNumbertWindowToAggregateRule(c.session))
    injector.injectPostTransform(
      c =>
        intercept(
          SparkPlanRules.extendedColumnarRule(
            new GlutenConfig(c.sqlConf).extendedColumnarTransformRules)(c.session)))
    injector.injectPostTransform(_ => CollectLimitTransformerRule())
    injector.injectPostTransform(_ => CollectTailTransformerRule())
    injector.injectPostTransform(c => InsertTransitions.create(c.outputsColumnar, CHBatchType))
    injector.injectPostTransform(c => RemoveDuplicatedColumns(c.session))
    injector.injectPostTransform(c => AddPreProjectionForHashJoin(c.session))
    injector.injectPostTransform(c => ReplaceSubStringComparison(c.session))
    injector.injectPostTransform(c => EliminateDeduplicateAggregateWithAnyJoin(c.session))
    injector.injectPostTransform(c => FlattenNestedExpressions(c.session))

    // Gluten columnar: Fallback policies.
    injector.injectFallbackPolicy(c => p => ExpandFallbackPolicy(c.caller.isAqe(), p))

    // Gluten columnar: Post rules.
    injector.injectPost(c => RemoveTopmostColumnarToRow(c.session, c.caller.isAqe()))
    injector.injectPost(c => CHRemoveTopmostColumnarToRow(c.session, c.caller.isAqe()))
    SparkShimLoader.getSparkShims
      .getExtendedColumnarPostRules()
      .foreach(each => injector.injectPost(c => intercept(each(c.session))))
    injector.injectPost(c => ColumnarCollapseTransformStages(new GlutenConfig(c.sqlConf)))
    injector.injectPost(
      c =>
        intercept(
          SparkPlanRules.extendedColumnarRule(
            new GlutenConfig(c.sqlConf).extendedColumnarPostRules)(c.session)))
    injector.injectPost(c => GlutenNoopWriterRule.apply(c.session))

    // Gluten columnar: Final rules.
    injector.injectFinal(c => RemoveGlutenTableCacheColumnarToRow(c.session))
    injector.injectFinal(c => GlutenFallbackReporter(new GlutenConfig(c.sqlConf), c.session))
    injector.injectFinal(_ => RemoveFallbackTagRule())
  }

  /**
   * Since https://github.com/apache/gluten/pull/883.
   *
   * TODO: Remove this since tricky to maintain.
   */
  private class CHSparkRuleInterceptor(delegate: Rule[SparkPlan])
    extends Rule[SparkPlan]
    with AdaptiveSparkPlanHelper {
    override val ruleName: String = delegate.ruleName

    override def apply(plan: SparkPlan): SparkPlan = {
      if (skipOn(plan)) {
        return plan
      }
      delegate(plan)
    }

    private def skipOn(plan: SparkPlan): Boolean = {
      // TODO: Currently there are some fallback issues on CH backend when SparkPlan is
      // TODO: SerializeFromObjectExec, ObjectHashAggregateExec and V2CommandExec.
      // For example:
      //   val tookTimeArr = Array(12, 23, 56, 100, 500, 20)
      //   import spark.implicits._
      //   val df = spark.sparkContext.parallelize(tookTimeArr.toSeq, 1).toDF("time")
      //   df.summary().show(100, false)

      def includedDeltaOperator(scanExec: FileSourceScanExec): Boolean = {
        scanExec.relation.location.isInstanceOf[DeltaLogFileIndex]
      }

      val includedUnsupportedPlans = collect(plan) {
        // case s: SerializeFromObjectExec => true
        // case d: DeserializeToObjectExec => true
        // case o: ObjectHashAggregateExec => true
//        case rddScanExec: RDDScanExec if rddScanExec.no
        //        deName.contains("Delta Table State") => true
        case f: FileSourceScanExec if includedDeltaOperator(f) => true
        case v2CommandExec: V2CommandExec => true
        case commandResultExec: CommandResultExec => true
      }

      includedUnsupportedPlans.contains(true)
    }
  }

  private def intercept(delegate: Rule[SparkPlan]): Rule[SparkPlan] = {
    new CHSparkRuleInterceptor(delegate)
  }
}
