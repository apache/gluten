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
package org.apache.gluten.extension.columnar.validator

import org.apache.gluten.backendsapi.{BackendsApiManager, BackendSettingsApi}
import org.apache.gluten.config.GlutenConfig
import org.apache.gluten.execution._
import org.apache.gluten.expression.ExpressionUtils
import org.apache.gluten.extension.columnar.FallbackTags
import org.apache.gluten.extension.columnar.offload.OffloadSingleNode
import org.apache.gluten.sql.shims.SparkShimLoader

import org.apache.spark.internal.Logging
import org.apache.spark.sql.execution._
import org.apache.spark.sql.execution.aggregate.{HashAggregateExec, ObjectHashAggregateExec, SortAggregateExec}
import org.apache.spark.sql.execution.datasources.WriteFilesExec
import org.apache.spark.sql.execution.datasources.v2.{AppendDataExec, BatchScanExec, OverwriteByExpressionExec, OverwritePartitionsDynamicExec, ReplaceDataExec, WriteToDataSourceV2Exec}
import org.apache.spark.sql.execution.exchange.{BroadcastExchangeExec, ShuffleExchangeExec}
import org.apache.spark.sql.execution.joins._
import org.apache.spark.sql.execution.window.WindowExec
import org.apache.spark.sql.hive.HiveTableScanExecTransformer
import org.apache.spark.sql.types.{ArrayType, DataType, MapType, StructType}

object Validators {
  implicit class ValidatorBuilderImplicits(builder: Validator.Builder) {
    private val conf = GlutenConfig.get
    private val settings = BackendsApiManager.getSettings

    /** Fails validation if a plan node was already tagged with TRANSFORM_UNSUPPORTED. */
    def fallbackByHint(): Validator.Builder = {
      builder.add(FallbackByHint)
    }

    /**
     * Fails validation if a plan node includes an expression that is considered too complex to
     * executed by native library. By default, we use a threshold option in config to make the
     * decision.
     */
    def fallbackComplexExpressions(): Validator.Builder = {
      builder.add(new FallbackComplexExpressions(conf.fallbackExpressionsThreshold))
    }

    /** Fails validation on non-scan plan nodes if Gluten is running as scan-only mode. */
    def fallbackIfScanOnly(): Validator.Builder = {
      builder.add(new FallbackIfScanOnly(conf.enableScanOnly))
    }

    /**
     * Fails validation if native-execution of a plan node is not supported by current backend
     * implementation by checking the active BackendSettings.
     */
    def fallbackByBackendSettings(): Validator.Builder = {
      builder.add(new FallbackByBackendSettings(settings))
    }

    /**
     * Fails validation if native-execution of a plan node is disabled by Gluten/Spark
     * configuration.
     */
    def fallbackByUserOptions(): Validator.Builder = {
      builder.add(new FallbackByUserOptions(conf))
    }

    def fallbackByTestInjects(): Validator.Builder = {
      builder.add(new FallbackByTestInjects())
    }

    /** Fails validation if a plan node's input or output schema contains TimestampNTZType. */
    def fallbackByTimestampNTZ(): Validator.Builder = {
      builder.add(new FallbackByTimestampNTZ())
    }

    /**
     * Fails validation on non-scan plan nodes if Gluten is running as scan-only mode. Also, passes
     * validation on filter for the exception that filter + scan is detected. Because filters can be
     * pushed into scan then the filter conditions will be processed only in scan.
     */
    def fallbackIfScanOnlyWithFilterPushed(scanOnly: Boolean): Validator.Builder = {
      builder.add(new FallbackIfScanOnlyWithFilterPushed(scanOnly))
    }

    /**
     * Attempts to offload the input query plan node and check native validation result. Fails when
     * native validation failed.
     */
    def fallbackByNativeValidation(rules: Seq[OffloadSingleNode]): Validator.Builder = {
      builder.add(new FallbackByNativeValidation(rules))
    }
  }

  private object FallbackByHint extends Validator {
    override def validate(plan: SparkPlan): Validator.OutCome = {
      if (FallbackTags.nonEmpty(plan)) {
        val tag = FallbackTags.get(plan)
        return fail(tag.reason())
      }
      pass()
    }
  }

  private class FallbackComplexExpressions(threshold: Int) extends Validator {
    override def validate(plan: SparkPlan): Validator.OutCome = {
      if (ExpressionUtils.hasComplexExpressions(plan, threshold)) {
        return fail(
          s"Disabled because at least one present expression exceeded depth threshold: " +
            s"${plan.nodeName}")
      }
      pass()
    }
  }

  private class FallbackIfScanOnly(scanOnly: Boolean) extends Validator {
    override def validate(plan: SparkPlan): Validator.OutCome = plan match {
      case _: BatchScanExec => pass()
      case _: FileSourceScanExec => pass()
      case p if HiveTableScanExecTransformer.isHiveTableScan(p) => pass()
      case p if scanOnly => fail(p)
      case _ => pass()
    }
  }

  private class FallbackByBackendSettings(settings: BackendSettingsApi) extends Validator {
    override def validate(plan: SparkPlan): Validator.OutCome = plan match {
      case p: ShuffleExchangeExec if !settings.supportColumnarShuffleExec() => fail(p)
      case p: SortMergeJoinExec if !settings.supportSortMergeJoinExec() => fail(p)
      case p: WriteFilesExec if !settings.enableNativeWriteFiles() =>
        fail(p)
      case p: CartesianProductExec if !settings.supportCartesianProductExec() => fail(p)
      case p: TakeOrderedAndProjectExec if !settings.supportColumnarShuffleExec() => fail(p)
      case p: AppendDataExec if !settings.supportAppendDataExec() => fail(p)
      case p: ReplaceDataExec if !settings.supportReplaceDataExec() => fail(p)
      case p: OverwriteByExpressionExec if !settings.supportOverwriteByExpression() => fail(p)
      case p: OverwritePartitionsDynamicExec if !settings.supportOverwritePartitionsDynamic() =>
        fail(p)
      case p: WriteToDataSourceV2Exec if !settings.supportWriteToDataSourceV2() => fail(p)
      case _ => pass()
    }
  }

  private class FallbackByUserOptions(glutenConf: GlutenConfig) extends Validator {
    override def validate(plan: SparkPlan): Validator.OutCome = plan match {
      case p: SortExec if !glutenConf.enableColumnarSort => fail(p)
      case p: WindowExec if !glutenConf.enableColumnarWindow => fail(p)
      case p: SortMergeJoinExec if !glutenConf.enableColumnarSortMergeJoin => fail(p)
      case p: BatchScanExec if !glutenConf.enableColumnarBatchScan => fail(p)
      case p: FileSourceScanExec if !glutenConf.enableColumnarFileScan => fail(p)
      case p: ProjectExec if !glutenConf.enableColumnarProject => fail(p)
      case p: FilterExec if !glutenConf.enableColumnarFilter => fail(p)
      case p: UnionExec if !glutenConf.enableColumnarUnion => fail(p)
      case p: ExpandExec if !glutenConf.enableColumnarExpand => fail(p)
      case p: SortAggregateExec if !glutenConf.forceToUseHashAgg => fail(p)
      case p: ShuffledHashJoinExec if !glutenConf.enableColumnarShuffledHashJoin => fail(p)
      case p: ShuffleExchangeExec if !glutenConf.enableColumnarShuffle => fail(p)
      case p: BroadcastExchangeExec if !glutenConf.enableColumnarBroadcastExchange => fail(p)
      case p: AppendDataExec if !glutenConf.enableAppendData => fail(p)
      case p: ReplaceDataExec if !glutenConf.enableReplaceData => fail(p)
      case p: OverwriteByExpressionExec if !glutenConf.enableOverwriteByExpression => fail(p)
      case p: OverwritePartitionsDynamicExec if !glutenConf.enableOverwritePartitionsDynamic =>
        fail(p)
      case p: WriteToDataSourceV2Exec if !glutenConf.enableColumnarWriteToDataSourceV2 => fail(p)
      case p @ (_: LocalLimitExec | _: GlobalLimitExec) if !glutenConf.enableColumnarLimit =>
        fail(p)
      case p: GenerateExec if !glutenConf.enableColumnarGenerate => fail(p)
      case p: CoalesceExec if !glutenConf.enableColumnarCoalesce => fail(p)
      case p: CartesianProductExec if !glutenConf.cartesianProductTransformerEnabled => fail(p)
      case p: TakeOrderedAndProjectExec
          if !(glutenConf.enableTakeOrderedAndProject && glutenConf.enableColumnarSort &&
            glutenConf.enableColumnarShuffle && glutenConf.enableColumnarProject) =>
        fail(p)
      case p: BroadcastHashJoinExec if !glutenConf.enableColumnarBroadcastJoin =>
        fail(p)
      case p: BroadcastNestedLoopJoinExec
          if !(glutenConf.enableColumnarBroadcastJoin &&
            glutenConf.broadcastNestedLoopJoinTransformerTransformerEnabled) =>
        fail(p)
      case p @ (_: HashAggregateExec | _: SortAggregateExec | _: ObjectHashAggregateExec)
          if !glutenConf.enableColumnarHashAgg =>
        fail(p)
      case p
          if SparkShimLoader.getSparkShims.isWindowGroupLimitExec(
            plan) && !glutenConf.enableColumnarWindowGroupLimit =>
        fail(p)
      case p
          if HiveTableScanExecTransformer.isHiveTableScan(
            p) && !glutenConf.enableColumnarHiveTableScan =>
        fail(p)
      case p: SampleExec
          if !(glutenConf.enableColumnarSample && BackendsApiManager.getSettings
            .supportSampleExec()) =>
        fail(p)
      case p: RangeExec if !glutenConf.enableColumnarRange => fail(p)
      case p: CollectLimitExec if !glutenConf.enableColumnarCollectLimit => fail(p)
      case p: CollectTailExec if !glutenConf.enableColumnarCollectTail => fail(p)
      case _ => pass()
    }
  }

  private class FallbackByTestInjects() extends Validator {
    override def validate(plan: SparkPlan): Validator.OutCome = {
      if (FallbackInjects.shouldFallback(plan)) {
        return fail(plan)
      }
      pass()
    }
  }

  private class FallbackByTimestampNTZ() extends Validator {
    override def validate(plan: SparkPlan): Validator.OutCome = {
      def containsNTZ(dataType: DataType): Boolean = dataType match {
        case dt if dt.catalogString == "timestamp_ntz" => true
        case st: StructType => st.exists(f => containsNTZ(f.dataType))
        case at: ArrayType => containsNTZ(at.elementType)
        case mt: MapType => containsNTZ(mt.keyType) || containsNTZ(mt.valueType)
        case _ => false
      }
      val hasNTZ = plan.output.exists(a => containsNTZ(a.dataType)) ||
        plan.children.exists(_.output.exists(a => containsNTZ(a.dataType)))
      if (hasNTZ) {
        fail(s"${plan.nodeName} has TimestampNTZType in input/output schema")
      } else {
        pass()
      }
    }
  }

  private class FallbackIfScanOnlyWithFilterPushed(scanOnly: Boolean) extends Validator {
    override def validate(plan: SparkPlan): Validator.OutCome = {
      if (!scanOnly) {
        return pass()
      }
      // Scan-only mode
      plan match {
        case _: BatchScanExec => pass()
        case _: FileSourceScanExec => pass()
        case p if HiveTableScanExecTransformer.isHiveTableScan(p) => pass()
        case filter: FilterExec =>
          val childIsScan = filter.child.isInstanceOf[FileSourceScanExec] ||
            filter.child.isInstanceOf[BatchScanExec] || filter.child
              .isInstanceOf[BasicScanExecTransformer]
          if (childIsScan) {
            pass()
          } else {
            fail(filter)
          }
        case other => fail(other)
      }
    }
  }

  private class FallbackByNativeValidation(offloadRules: Seq[OffloadSingleNode])
    extends Validator
    with Logging {
    override def validate(plan: SparkPlan): Validator.OutCome = {
      val offloadedNode = offloadAttempt.apply(plan)
      val out = offloadedNode match {
        case v: ValidatablePlan =>
          v.doValidate().toValidatorOutcome()
        case other =>
          // Currently we assume a plan to be offload-able by default.
          pass()
      }
      out
    }

    private val offloadAttempt: SparkPlan => SparkPlan = {
      node =>
        offloadRules.foldLeft(node) {
          case (node, rule) =>
            rule.offload(node)
        }
    }
  }

  /**
   * A standard validator for legacy planner that does native validation.
   *
   * The native validation is ordered in the latest validator, namely the one created by
   * #fallbackByNativeValidation. The validator accepts offload rules for doing offload attempts,
   * then call native validation code on the offloaded plan.
   *
   * Once the native validation fails, the validator then gives negative outcome.
   */
  def newValidator(conf: GlutenConfig, offloads: Seq[OffloadSingleNode]): Validator = {
    val nativeValidator = Validator.builder().fallbackByNativeValidation(offloads).build()
    newValidator(conf).andThen(nativeValidator)
  }

  /**
   * A validator that doesn't involve native validation.
   *
   * This is typically RAS planner that does native validation inline without relying on tags. Thus,
   * validator `#fallbackByNativeValidation` is not required. See
   * [[org.apache.gluten.extension.columnar.enumerated.RasOffload]].
   *
   * This could also be used in legacy planner for doing trivial offload without the help of rewrite
   * rules.
   */
  def newValidator(conf: GlutenConfig): Validator = {
    Validator
      .builder()
      .fallbackByHint()
      .fallbackIfScanOnlyWithFilterPushed(conf.enableScanOnly)
      .fallbackComplexExpressions()
      .fallbackByBackendSettings()
      .fallbackByUserOptions()
      .fallbackByTimestampNTZ()
      .fallbackByTestInjects()
      .build()
  }
}
