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
package org.apache.gluten.backendsapi.bolt

import org.apache.gluten.backendsapi.SparkPlanExecApi
import org.apache.gluten.config.{BoltConfig, GlutenConfig, HashShuffleWriterType, ReservedKeys, RssSortShuffleWriterType, ShuffleWriterType, SortShuffleWriterType}
import org.apache.gluten.exception.{GlutenExceptionUtil, GlutenNotSupportException}
import org.apache.gluten.execution._
import org.apache.gluten.expression._
import org.apache.gluten.expression.aggregate.{BoltBloomFilterAggregate, BoltCollectList, BoltCollectSet, HLLAdapter}
import org.apache.gluten.extension.columnar.FallbackTags
import org.apache.gluten.shuffle.NeedCustomColumnarBatchSerializer
import org.apache.gluten.sql.shims.SparkShimLoader
import org.apache.gluten.substrait.SubstraitContext
import org.apache.gluten.substrait.expression.{ExpressionBuilder, ExpressionNode, WindowFunctionNode}
import org.apache.gluten.vectorized.{ColumnarBatchSerializer, ColumnarBatchSerializeResult}

import org.apache.spark.{ShuffleDependency, SparkEnv, SparkException}
import org.apache.spark.api.python.{ColumnarArrowEvalPythonExec, PullOutArrowEvalPythonPreProjectHelper}
import org.apache.spark.memory.SparkMemoryUtil
import org.apache.spark.rdd.RDD
import org.apache.spark.serializer.Serializer
import org.apache.spark.shuffle.{GenShuffleReaderParameters, GenShuffleWriterParameters, GlutenShuffleReaderWrapper, GlutenShuffleWriterWrapper}
import org.apache.spark.shuffle.utils.ShuffleUtil
import org.apache.spark.sql.catalyst.catalog.BucketSpec
import org.apache.spark.sql.catalyst.catalog.CatalogTypes.TablePartitionSpec
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.aggregate.{AggregateExpression, CollectList, CollectSet}
import org.apache.spark.sql.catalyst.expressions.objects.StaticInvoke
import org.apache.spark.sql.catalyst.optimizer.BuildSide
import org.apache.spark.sql.catalyst.plans.JoinType
import org.apache.spark.sql.catalyst.plans.physical._
import org.apache.spark.sql.execution._
import org.apache.spark.sql.execution.aggregate.BaseAggregateExec
import org.apache.spark.sql.execution.datasources.FileFormat
import org.apache.spark.sql.execution.exchange.ShuffleExchangeExec
import org.apache.spark.sql.execution.joins.{BuildSideRelation, HashedRelationBroadcastMode}
import org.apache.spark.sql.execution.metric.SQLMetric
import org.apache.spark.sql.execution.python.ArrowEvalPythonExec
import org.apache.spark.sql.execution.unsafe.UnsafeColumnarBuildSideRelation
import org.apache.spark.sql.execution.utils.ExecUtil
import org.apache.spark.sql.expression.{UDFExpression, UDFResolver, UserDefinedAggregateFunction}
import org.apache.spark.sql.hive.BoltHiveUDFTransformer
import org.apache.spark.sql.hive.HiveUDAFInspector
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types._
import org.apache.spark.sql.vectorized.ColumnarBatch
import org.apache.spark.task.TaskResources
import org.apache.spark.unsafe.Platform

import org.apache.commons.lang3.ClassUtils

import javax.ws.rs.core.UriBuilder

import java.util.{ArrayList => JArrayList, List => JList, Locale}

import scala.collection.JavaConverters._

class BoltSparkPlanExecApi extends SparkPlanExecApi {

  /** Transform GetArrayItem to Substrait. */
  override def genGetArrayItemTransformer(
      substraitExprName: String,
      left: ExpressionTransformer,
      right: ExpressionTransformer,
      original: Expression): ExpressionTransformer = {
    GenericExpressionTransformer(substraitExprName, Seq(left, right), original)
  }

  /** Transform NaNvl to Substrait. */
  override def genNaNvlTransformer(
      substraitExprName: String,
      left: ExpressionTransformer,
      right: ExpressionTransformer,
      original: NaNvl): ExpressionTransformer = {
    val condExpr = IsNaN(original.left)
    val condFuncName = ExpressionMappings.expressionsMap(classOf[IsNaN])
    val newExpr = If(condExpr, original.right, original.left)
    IfTransformer(
      substraitExprName,
      GenericExpressionTransformer(condFuncName, Seq(left), condExpr),
      right,
      left,
      newExpr)
  }

  override def genAtLeastNNonNullsTransformer(
      substraitExprName: String,
      children: Seq[ExpressionTransformer],
      original: AtLeastNNonNulls): ExpressionTransformer = {
    GenericExpressionTransformer(
      substraitExprName,
      Seq(LiteralTransformer(Literal(original.n))) ++ children,
      original)
  }

  /** Transform Uuid to Substrait. */
  override def genUuidTransformer(
      substraitExprName: String,
      original: Uuid): ExpressionTransformer = {
    GenericExpressionTransformer(
      substraitExprName,
      Seq(LiteralTransformer(original.randomSeed.get)),
      original)
  }

  override def genTryArithmeticTransformer(
      substraitExprName: String,
      left: ExpressionTransformer,
      right: ExpressionTransformer,
      original: TryEval,
      checkArithmeticExprName: String): ExpressionTransformer = {
    original.child.dataType match {
      case LongType | IntegerType | ShortType | ByteType =>
      case _ => throw new GlutenNotSupportException(s"$substraitExprName is not supported")
    }
    // Offload to bolt for only IntegralTypes.
    GenericExpressionTransformer(
      substraitExprName,
      Seq(GenericExpressionTransformer(checkArithmeticExprName, Seq(left, right), original)),
      original)
  }

  /**
   * Map arithmetic expr to different functions: substraitExprName or try(checkArithmeticExprName)
   * based on EvalMode.
   */
  override def genArithmeticTransformer(
      substraitExprName: String,
      left: ExpressionTransformer,
      right: ExpressionTransformer,
      original: Expression,
      checkArithmeticExprName: String): ExpressionTransformer = {
    if (SparkShimLoader.getSparkShims.withTryEvalMode(original)) {
      original.dataType match {
        case LongType | IntegerType | ShortType | ByteType =>
        case _ =>
          throw new GlutenNotSupportException(s"$substraitExprName with try mode is not supported")
      }
      // Offload to bolt for only IntegralTypes.
      GenericExpressionTransformer(
        ExpressionMappings.expressionsMap(classOf[TryEval]),
        Seq(GenericExpressionTransformer(checkArithmeticExprName, Seq(left, right), original)),
        original)
    } else if (SparkShimLoader.getSparkShims.withAnsiEvalMode(original)) {
      GenericExpressionTransformer(checkArithmeticExprName, Seq(left, right), original)
    } else {
      GenericExpressionTransformer(substraitExprName, Seq(left, right), original)
    }
  }

  override def getDecimalArithmeticExprName(
      exprName: String,
      allowPrecisionLoss: Boolean): String =
    if (!allowPrecisionLoss) { exprName + "_deny_precision_loss" }
    else { exprName }

  /** Transform map_entries to Substrait. */
  override def genMapEntriesTransformer(
      substraitExprName: String,
      child: ExpressionTransformer,
      expr: Expression): ExpressionTransformer = {
    GenericExpressionTransformer(substraitExprName, Seq(child), expr)
  }

  /** Transform map_from_entries to Substrait. */
  override def genMapFromEntriesTransformer(
      substraitExprName: String,
      child: ExpressionTransformer,
      expr: Expression): ExpressionTransformer = {
    GenericExpressionTransformer(substraitExprName, Seq(child), expr)
  }

  /** Transform sequence to Substrait. */
  override def genSequenceTransformer(
      substraitExprName: String,
      children: Seq[ExpressionTransformer],
      expr: Sequence): ExpressionTransformer = {
    if (expr.children.exists(_.dataType.isInstanceOf[TimestampType])) {
      throw new GlutenNotSupportException(
        "sequence with timestamp input is not supported in bolt backend")
    }
    GenericExpressionTransformer(substraitExprName, children, expr)
  }

  /** Transform array filter to Substrait. */
  override def genArrayFilterTransformer(
      substraitExprName: String,
      argument: ExpressionTransformer,
      function: ExpressionTransformer,
      expr: ArrayFilter): ExpressionTransformer = {
    expr.function match {
      // Transformer for array_compact.
      case LambdaFunction(_: IsNotNull, _, _) =>
        GenericExpressionTransformer(ExpressionNames.ARRAY_COMPACT, Seq(argument), expr)
      case LambdaFunction(_, arguments, _) if arguments.size == 2 =>
        throw new GlutenNotSupportException(
          "filter on array with lambda using index argument is not supported yet")
      case _ => GenericExpressionTransformer(substraitExprName, Seq(argument, function), expr)
    }
  }

  /** Transform array forall to Substrait. */
  override def genArrayForAllTransformer(
      substraitExprName: String,
      argument: ExpressionTransformer,
      function: ExpressionTransformer,
      expr: ArrayForAll): ExpressionTransformer = {
    expr.function match {
      case LambdaFunction(_, arguments, _) if arguments.size == 2 =>
        throw new GlutenNotSupportException(
          "forall on array with lambda using index argument is not supported yet")
      case _ => GenericExpressionTransformer(substraitExprName, Seq(argument, function), expr)
    }
  }

  override def genArraySortTransformer(
      substraitExprName: String,
      argument: ExpressionTransformer,
      function: ExpressionTransformer,
      expr: ArraySort): ExpressionTransformer = {
    GenericExpressionTransformer(substraitExprName, Seq(argument, function), expr)
  }

  /** Transform array exists to Substrait */
  override def genArrayExistsTransformer(
      substraitExprName: String,
      argument: ExpressionTransformer,
      function: ExpressionTransformer,
      expr: ArrayExists): ExpressionTransformer = {
    expr.function match {
      case LambdaFunction(_, arguments, _) if arguments.size == 2 =>
        throw new GlutenNotSupportException(
          "exists on array with lambda using index argument is not supported yet")
      case _ => GenericExpressionTransformer(substraitExprName, Seq(argument, function), expr)
    }
  }

  /** Transform array transform to Substrait. */
  override def genArrayTransformTransformer(
      substraitExprName: String,
      argument: ExpressionTransformer,
      function: ExpressionTransformer,
      expr: ArrayTransform): ExpressionTransformer = {
    expr.function match {
      case LambdaFunction(_, arguments, _) if arguments.size == 2 =>
        throw new GlutenNotSupportException(
          "transform on array with lambda using index argument is not supported yet")
      case _ => GenericExpressionTransformer(substraitExprName, Seq(argument, function), expr)
    }
  }

  /** Transform posexplode to Substrait. */
  override def genPosExplodeTransformer(
      substraitExprName: String,
      child: ExpressionTransformer,
      original: PosExplode,
      attrSeq: Seq[Attribute]): ExpressionTransformer = {
    GenericExpressionTransformer(substraitExprName, Seq(child), attrSeq.head)
  }

  /** Transform inline to Substrait. */
  override def genInlineTransformer(
      substraitExprName: String,
      child: ExpressionTransformer,
      expr: Expression): ExpressionTransformer = {
    GenericExpressionTransformer(substraitExprName, Seq(child), expr)
  }

  override def genLikeTransformer(
      substraitExprName: String,
      left: ExpressionTransformer,
      right: ExpressionTransformer,
      original: Like): ExpressionTransformer = {
    GenericExpressionTransformer(
      substraitExprName,
      Seq(left, right, LiteralTransformer(original.escapeChar)),
      original)
  }

  /** Transform make_timestamp to Substrait. */
  override def genMakeTimestampTransformer(
      substraitExprName: String,
      children: Seq[ExpressionTransformer],
      expr: Expression): ExpressionTransformer = {
    GenericExpressionTransformer(substraitExprName, children, expr)
  }

  override def genDateDiffTransformer(
      substraitExprName: String,
      endDate: ExpressionTransformer,
      startDate: ExpressionTransformer,
      original: DateDiff): ExpressionTransformer = {
    GenericExpressionTransformer(substraitExprName, Seq(endDate, startDate), original)
  }

  override def genPreciseTimestampConversionTransformer(
      substraitExprName: String,
      children: Seq[ExpressionTransformer],
      expr: PreciseTimestampConversion): ExpressionTransformer = {
    // Expression used internally to convert the TimestampType to Long and back without losing
    // precision, i.e. in microseconds.
    val (newSubstraitName, newExpr) = expr match {
      case _ @PreciseTimestampConversion(_, TimestampType, LongType) =>
        (ExpressionMappings.expressionsMap(classOf[UnixMicros]), UnixMicros(expr.child))
      case _ @PreciseTimestampConversion(_, LongType, TimestampType) =>
        (
          ExpressionMappings.expressionsMap(classOf[MicrosToTimestamp]),
          MicrosToTimestamp(expr.child))
      case _ =>
        // TimestampNTZType is not supported here.
        throw new GlutenNotSupportException("PreciseTimestampConversion is not supported")
    }
    GenericExpressionTransformer(newSubstraitName, children, newExpr)
  }

  override def genArrayInsertTransformer(
      substraitExprName: String,
      children: Seq[ExpressionTransformer],
      original: Expression): ExpressionTransformer = {
    children match {
      case Seq(left, posExpr, right, _) if posExpr.original == Literal(1) =>
        // Transformer for array_prepend.
        GenericExpressionTransformer(ExpressionNames.ARRAY_PREPEND, Seq(left, right), original)
      case _ =>
        GenericExpressionTransformer(substraitExprName, children, original)
    }
  }

  /**
   * Generate FilterExecTransformer.
   *
   * @param condition
   *   : the filter condition
   * @param child
   *   : the child of FilterExec
   * @return
   *   the transformer of FilterExec
   */
  override def genFilterExecTransformer(
      condition: Expression,
      child: SparkPlan): FilterExecTransformerBase = {
    FilterExecTransformer(condition, child)
  }

  /** Generate HashAggregateExecTransformer. */
  override def genHashAggregateExecTransformer(
      requiredChildDistributionExpressions: Option[Seq[Expression]],
      groupingExpressions: Seq[NamedExpression],
      aggregateExpressions: Seq[AggregateExpression],
      aggregateAttributes: Seq[Attribute],
      initialInputBufferOffset: Int,
      resultExpressions: Seq[NamedExpression],
      child: SparkPlan): HashAggregateExecBaseTransformer =
    RegularHashAggregateExecTransformer(
      requiredChildDistributionExpressions,
      groupingExpressions,
      aggregateExpressions,
      aggregateAttributes,
      initialInputBufferOffset,
      resultExpressions,
      child)

  /**
   * Bolt does not need the sort-based aggregate semantic, so a SortAggregateExec is offloaded as a
   * regular hash aggregate transformer.
   */
  override def offloadSortAggregate(plan: BaseAggregateExec): HashAggregateExecBaseTransformer =
    HashAggregateExecBaseTransformer.from(plan)

  /** Generate HashAggregateExecPullOutHelper */
  override def genHashAggregateExecPullOutHelper(
      aggregateExpressions: Seq[AggregateExpression],
      aggregateAttributes: Seq[Attribute]): HashAggregateExecPullOutBaseHelper =
    HashAggregateExecPullOutHelper(aggregateExpressions, aggregateAttributes)

  override def genColumnarShuffleExchange(shuffle: ShuffleExchangeExec): SparkPlan = {
    def allowHashOnMap[T](f: => T): T = {
      val originalAllowHash = SQLConf.get.getConf(SQLConf.LEGACY_ALLOW_HASH_ON_MAPTYPE)
      try {
        SQLConf.get.setConf(SQLConf.LEGACY_ALLOW_HASH_ON_MAPTYPE, true)
        f
      } finally {
        SQLConf.get.setConf(SQLConf.LEGACY_ALLOW_HASH_ON_MAPTYPE, originalAllowHash)
      }
    }

    val child = shuffle.child

    val newShuffle = shuffle.outputPartitioning match {
      case HashPartitioning(exprs, _) =>
        val hashExpr = new Murmur3Hash(exprs)
        val projectList = Seq(Alias(hashExpr, "hash_partition_key")()) ++ child.output
        val projectTransformer = ProjectExecTransformer(projectList, child)
        val validationResult = projectTransformer.doValidate()
        if (validationResult.ok()) {
          ColumnarShuffleExchangeExec(
            shuffle,
            projectTransformer,
            projectTransformer.output.drop(1))
        } else {
          FallbackTags.add(shuffle, validationResult)
          shuffle.withNewChildren(child :: Nil)
        }
      case RoundRobinPartitioning(num) if SQLConf.get.sortBeforeRepartition && num > 1 =>
        // scalastyle:off line.size.limit
        // Temporarily allow hash on map if it's disabled, otherwise HashExpression will fail to get
        // resolved if its child contains map type.
        // See https://github.com/apache/spark/blob/609bd4839e5d504917de74ed1cb9c23645fba51f/sql/catalyst/src/main/scala/org/apache/spark/sql/catalyst/expressions/hash.scala#L279-L283
        // scalastyle:on line.size.limit
        allowHashOnMap {
          // Bolt hash expression does not support null type and we also do not need to sort
          // null type since the value always be null.
          val columnsForHash = child.output.filterNot(_.dataType == NullType)
          if (columnsForHash.isEmpty) {
            ColumnarShuffleExchangeExec(shuffle, child, child.output)
          } else {
            val hashExpr = new Murmur3Hash(columnsForHash)
            val projectList = Seq(Alias(hashExpr, "hash_partition_key")()) ++ child.output
            val projectTransformer = ProjectExecTransformer(projectList, child)
            val projectBeforeSortValidationResult = projectTransformer.doValidate()
            // Make sure we support offload hash expression
            val projectBeforeSort = if (projectBeforeSortValidationResult.ok()) {
              projectTransformer
            } else {
              val project = ProjectExec(projectList, child)
              FallbackTags.add(project, projectBeforeSortValidationResult)
              project
            }
            val sortOrder = SortOrder(projectBeforeSort.output.head, Ascending)
            val sortByHashCode =
              SortExecTransformer(Seq(sortOrder), global = false, projectBeforeSort)
            val dropSortColumnTransformer =
              ProjectExecTransformer(projectList.drop(1), sortByHashCode)
            val validationResult = dropSortColumnTransformer.doValidate()
            if (validationResult.ok()) {
              ColumnarShuffleExchangeExec(
                shuffle,
                dropSortColumnTransformer,
                dropSortColumnTransformer.output)
            } else {
              FallbackTags.add(shuffle, validationResult)
              shuffle.withNewChildren(child :: Nil)
            }
          }
        }
      case _ =>
        ColumnarShuffleExchangeExec(shuffle, child, null)
    }
    newShuffle
  }

  /** Generate ShuffledHashJoinExecTransformer. */
  override def genShuffledHashJoinExecTransformer(
      leftKeys: Seq[Expression],
      rightKeys: Seq[Expression],
      joinType: JoinType,
      buildSide: BuildSide,
      condition: Option[Expression],
      left: SparkPlan,
      right: SparkPlan,
      isSkewJoin: Boolean): ShuffledHashJoinExecTransformerBase =
    ShuffledHashJoinExecTransformer(
      leftKeys,
      rightKeys,
      joinType,
      buildSide,
      condition,
      left,
      right,
      isSkewJoin)

  /** Generate BroadcastHashJoinExecTransformer. */
  override def genBroadcastHashJoinExecTransformer(
      leftKeys: Seq[Expression],
      rightKeys: Seq[Expression],
      joinType: JoinType,
      buildSide: BuildSide,
      condition: Option[Expression],
      left: SparkPlan,
      right: SparkPlan,
      isNullAwareAntiJoin: Boolean = false): BroadcastHashJoinExecTransformerBase =
    BroadcastHashJoinExecTransformer(
      leftKeys,
      rightKeys,
      joinType,
      buildSide,
      condition,
      left,
      right,
      isNullAwareAntiJoin)

  override def genSampleExecTransformer(
      lowerBound: Double,
      upperBound: Double,
      withReplacement: Boolean,
      seed: Long,
      child: SparkPlan): SampleExecTransformer = {
    SampleExecTransformer(lowerBound, upperBound, withReplacement, seed, child)
  }

  override def genSortMergeJoinExecTransformer(
      leftKeys: Seq[Expression],
      rightKeys: Seq[Expression],
      joinType: JoinType,
      condition: Option[Expression],
      left: SparkPlan,
      right: SparkPlan,
      isSkewJoin: Boolean = false,
      projectList: Seq[NamedExpression] = null): SortMergeJoinExecTransformerBase = {
    SortMergeJoinExecTransformer(
      leftKeys,
      rightKeys,
      joinType,
      condition,
      left,
      right,
      isSkewJoin,
      projectList)
  }

  override def genCartesianProductExecTransformer(
      left: SparkPlan,
      right: SparkPlan,
      condition: Option[Expression]): CartesianProductExecTransformer = {
    CartesianProductExecTransformer(
      ColumnarCartesianProductBridge(left),
      ColumnarCartesianProductBridge(right),
      condition)
  }

  override def genBroadcastNestedLoopJoinExecTransformer(
      left: SparkPlan,
      right: SparkPlan,
      buildSide: BuildSide,
      joinType: JoinType,
      condition: Option[Expression]): BroadcastNestedLoopJoinExecTransformer =
    BoltBroadcastNestedLoopJoinExecTransformer(left, right, buildSide, joinType, condition)

  override def genHashExpressionTransformer(
      substraitExprName: String,
      exprs: Seq[ExpressionTransformer],
      original: HashExpression[_]): ExpressionTransformer = {
    BoltHashExpressionTransformer(substraitExprName, exprs, original)
  }

  /**
   * Generate ShuffleDependency for ColumnarShuffleExchangeExec.
   *
   * @return
   */
  // scalastyle:off argcount
  override def genShuffleDependency(
      rdd: RDD[ColumnarBatch],
      childOutputAttributes: Seq[Attribute],
      projectOutputAttributes: Seq[Attribute],
      newPartitioning: Partitioning,
      serializer: Serializer,
      writeMetrics: Map[String, SQLMetric],
      metrics: Map[String, SQLMetric],
      shuffleWriterType: ShuffleWriterType)
      : ShuffleDependency[Int, ColumnarBatch, ColumnarBatch] = {
    // scalastyle:on argcount
    ExecUtil.genShuffleDependency(
      rdd,
      childOutputAttributes,
      newPartitioning,
      serializer,
      writeMetrics,
      metrics,
      shuffleWriterType)
  }
  // scalastyle:on argcount

  /** Determine whether to use sort-based shuffle based on shuffle partitioning and output. */
  override def getShuffleWriterType(
      partitioning: Partitioning,
      output: Seq[Attribute]): ShuffleWriterType = {
    val conf = GlutenConfig.get
    // todo: remove isUseCelebornShuffleManager here
    if (conf.isUseCelebornShuffleManager) {
      if (conf.celebornShuffleWriterType == ReservedKeys.GLUTEN_SORT_SHUFFLE_WRITER) {
        if (conf.useCelebornRssSort) {
          RssSortShuffleWriterType
        } else if (partitioning != SinglePartition) {
          SortShuffleWriterType
        } else {
          // If not using rss sort, we still use hash shuffle writer for single partitioning.
          HashShuffleWriterType
        }
      } else {
        HashShuffleWriterType
      }
    } else {
      if (
        partitioning != SinglePartition &&
        (partitioning.numPartitions >= GlutenConfig.get.columnarShuffleSortPartitionsThreshold ||
          output.size >= GlutenConfig.get.columnarShuffleSortColumnsThreshold)
      ) {
        SortShuffleWriterType
      } else {
        HashShuffleWriterType
      }
    }
  }

  /**
   * Generate ColumnarShuffleWriter for ColumnarShuffleManager.
   *
   * @return
   */
  override def genColumnarShuffleWriter[K, V](
      parameters: GenShuffleWriterParameters[K, V]): GlutenShuffleWriterWrapper[K, V] = {
    ShuffleUtil.genColumnarShuffleWriter(parameters)
  }

  override def genColumnarShuffleReader[K, C](
      parameters: GenShuffleReaderParameters[K, C]): GlutenShuffleReaderWrapper[K, C] = {
    ShuffleUtil.genColumnarShuffleReader(parameters)
  }

  override def createColumnarWriteFilesExec(
      child: WriteFilesExecTransformer,
      noop: SparkPlan,
      fileFormat: FileFormat,
      partitionColumns: Seq[Attribute],
      bucketSpec: Option[BucketSpec],
      options: Map[String, String],
      staticPartitions: TablePartitionSpec): ColumnarWriteFilesExec = {
    BoltColumnarWriteFilesExec(
      child,
      noop,
      child,
      fileFormat,
      partitionColumns,
      bucketSpec,
      options,
      staticPartitions)
  }

  override def createColumnarArrowEvalPythonExec(
      udfs: Seq[PythonUDF],
      resultAttrs: Seq[Attribute],
      child: SparkPlan,
      evalType: Int): SparkPlan = {
    ColumnarArrowEvalPythonExec(udfs, resultAttrs, child, evalType)
  }

  /**
   * Generate ColumnarBatchSerializer for ColumnarShuffleExchangeExec.
   *
   * @return
   */
  override def createColumnarBatchSerializer(
      schema: StructType,
      metrics: Map[String, SQLMetric],
      shuffleWriterType: ShuffleWriterType): Serializer = {
    val numOutputRows = metrics("numOutputRows")
    val deserializeTime = metrics("deserializeTime")
    val readBatchNumRows = metrics("avgReadBatchNumRows")
    val decompressTime = metrics("decompressTime")
    val totalReadTime = metrics("totalReadTime")
    SparkEnv.get.shuffleManager match {
      case serializer: NeedCustomColumnarBatchSerializer =>
        val className = serializer.columnarBatchSerializerClass()
        val clazz = ClassUtils.getClass(className)
        val constructor =
          clazz.getConstructor(
            classOf[StructType],
            classOf[SQLMetric],
            classOf[SQLMetric],
            classOf[SQLMetric],
            classOf[SQLMetric])
        constructor
          .newInstance(
            schema,
            readBatchNumRows,
            numOutputRows,
            deserializeTime,
            decompressTime,
            totalReadTime)
          .asInstanceOf[Serializer]
      case _ =>
        new ColumnarBatchSerializer(
          schema,
          readBatchNumRows,
          numOutputRows,
          deserializeTime,
          decompressTime,
          totalReadTime)
    }
  }

  /** Create broadcast relation for BroadcastExchangeExec */
  override def createBroadcastRelation(
      mode: BroadcastMode,
      child: SparkPlan,
      numOutputRows: SQLMetric,
      dataSize: SQLMetric,
      buildThreads: SQLMetric): BuildSideRelation = {
    val useOffheapBroadcastBuildRelation =
      BoltConfig.get.enableBroadcastBuildRelationInOffheap
    val serialized: Array[ColumnarBatchSerializeResult] = child
      .executeColumnar()
      .mapPartitions(itr => Iterator(BroadcastUtils.serializeStream(itr)))
      .filter(_.numRows() != 0)
      .collect
    val rawSize = serialized.map(_.sizeInBytes()).sum
    if (rawSize >= GlutenConfig.get.maxBroadcastTableSize) {
      throw new SparkException(
        "Cannot broadcast the table that is larger than " +
          s"${SparkMemoryUtil.bytesToString(GlutenConfig.get.maxBroadcastTableSize)}: " +
          s"${SparkMemoryUtil.bytesToString(rawSize)}")
    }
    numOutputRows += serialized.map(_.numRows()).sum
    dataSize += rawSize
    if (useOffheapBroadcastBuildRelation) {
      TaskResources.runUnsafe {
        val offHeapSerialized = serialized.flatMap(
          _.offHeapData().asScala.map {
            unsafe =>
              val bytes = new Array[Byte](Math.toIntExact(unsafe.size()))
              Platform.copyMemory(
                null,
                unsafe.address(),
                bytes,
                Platform.BYTE_ARRAY_OFFSET,
                unsafe.size())
              bytes
          })
        UnsafeColumnarBuildSideRelation(child.output, offHeapSerialized, mode)
      }
    } else {
      ColumnarBuildSideRelation(
        child.output,
        serialized.flatMap(_.onHeapData().asScala).toArray,
        mode)
    }
  }

  override def doCanonicalizeForBroadcastMode(mode: BroadcastMode): BroadcastMode = {
    mode match {
      case hash: HashedRelationBroadcastMode =>
        // Node: It's different with vanilla Spark.
        // Vanilla Spark build HashRelation at driver side, so it is build keys sensitive.
        // But we broadcast byte array and build HashRelation at executor side,
        // the build keys are actually meaningless for the broadcast value.
        // This change allows us reuse broadcast exchange for different build keys with same table.
        hash.copy(key = Seq.empty)
      case _ => mode.canonicalized
    }
  }

  /**
   * * Expressions.
   */

  /**
   * Generate Alias transformer.
   *
   * @return
   *   a transformer for alias
   */
  override def genAliasTransformer(
      substraitExprName: String,
      child: ExpressionTransformer,
      original: Expression): ExpressionTransformer =
    BoltAliasTransformer(substraitExprName, child, original)

  /** Generate an expression transformer to transform GetMapValue to Substrait. */
  override def genGetMapValueTransformer(
      substraitExprName: String,
      left: ExpressionTransformer,
      right: ExpressionTransformer,
      original: GetMapValue): ExpressionTransformer = {
    GenericExpressionTransformer(
      ExpressionMappings.expressionsMap(classOf[ElementAt]),
      Seq(left, right),
      original)
  }

  override def genStringToMapTransformer(
      substraitExprName: String,
      children: Seq[ExpressionTransformer],
      expr: Expression): ExpressionTransformer = {
    if (
      SQLConf.get.getConf(SQLConf.MAP_KEY_DEDUP_POLICY).toString
        != SQLConf.MapKeyDedupPolicy.EXCEPTION.toString
    ) {
      GlutenExceptionUtil.throwsNotFullySupported(
        ExpressionNames.STR_TO_MAP,
        StrToMapRestrictions.ONLY_SUPPORT_MAP_KEY_DEDUP_POLICY
      )
    }
    GenericExpressionTransformer(substraitExprName, children, expr)
  }

  /** Generate an expression transformer to transform JsonToStructs to Substrait. */
  override def genFromJsonTransformer(
      substraitExprName: String,
      children: Seq[ExpressionTransformer],
      expr: JsonToStructs): ExpressionTransformer = {
    val enablePartialResults =
      try {
        SQLConf.get.getConfString(s"spark.sql.json.enablePartialResults").toBoolean
      } catch {
        case _: NoSuchElementException =>
          // Before spark 3.4, this config is not defined, and partial result parsing is not
          // supported. Therefore we need to return false.
          false
      }
    if (!enablePartialResults) {
      // Bolt only supports partial results mode. We need to fall back this when
      // 'spark.sql.json.enablePartialResults' is set to false or not defined.
      GlutenExceptionUtil.throwsNotFullySupported(
        ExpressionNames.FROM_JSON,
        FromJsonRestrictions.MUST_ENABLE_PARTIAL_RESULTS
      )
    }
    if (expr.options.nonEmpty) {
      GlutenExceptionUtil.throwsNotFullySupported(
        ExpressionNames.FROM_JSON,
        FromJsonRestrictions.NOT_SUPPORT_WITH_OPTIONS)
    }
    if (SQLConf.get.caseSensitiveAnalysis) {
      GlutenExceptionUtil.throwsNotFullySupported(
        ExpressionNames.FROM_JSON,
        FromJsonRestrictions.NOT_SUPPORT_CASE_SENSITIVE)
    }

    val hasDuplicateKey = expr.schema match {
      case s: StructType =>
        s.names.distinct.size != s.names.size ||
        !s.filter(
          f =>
            !s.names
              .filter(
                n => n != f.name && n.toLowerCase(Locale.ROOT) == f.name.toLowerCase(Locale.ROOT))
              .isEmpty)
          .isEmpty
      case other =>
        false
    }
    if (hasDuplicateKey) {
      GlutenExceptionUtil.throwsNotFullySupported(
        ExpressionNames.FROM_JSON,
        FromJsonRestrictions.NOT_SUPPORT_DUPLICATE_KEYS)
    }
    val hasCorruptRecord = expr.schema match {
      case s: StructType =>
        !s.filter(_.name == SQLConf.get.getConf(SQLConf.COLUMN_NAME_OF_CORRUPT_RECORD)).isEmpty
      case other =>
        false
    }
    if (hasCorruptRecord) {
      GlutenExceptionUtil.throwsNotFullySupported(
        ExpressionNames.FROM_JSON,
        FromJsonRestrictions.NOT_SUPPORT_COLUMN_CORRUPT_RECORD)
    }
    GenericExpressionTransformer(substraitExprName, children, expr)
  }

  /** Generate an expression transformer to transform StructsToJson to Substrait. */
  override def genToJsonTransformer(
      substraitExprName: String,
      child: ExpressionTransformer,
      expr: StructsToJson): ExpressionTransformer = {
    if (!expr.options.isEmpty) {
      GlutenExceptionUtil.throwsNotFullySupported(
        ExpressionNames.TO_JSON,
        ToJsonRestrictions.NOT_SUPPORT_WITH_OPTIONS)
    }
    if (
      !SQLConf.get.caseSensitiveAnalysis &&
      ExpressionUtils.hasUppercaseStructFieldName(child.dataType)
    ) {
      GlutenExceptionUtil.throwsNotFullySupported(
        ExpressionNames.TO_JSON,
        ToJsonRestrictions.NOT_SUPPORT_UPPERCASE_STRUCT)
    }
    ToJsonTransformer(substraitExprName, child, expr)
  }

  override def genUnbase64Transformer(
      substraitExprName: String,
      child: ExpressionTransformer,
      expr: UnBase64): ExpressionTransformer = {
    if (SparkShimLoader.getSparkShims.unBase64FunctionFailsOnError(expr)) {
      GlutenExceptionUtil
        .throwsNotFullySupported(
          ExpressionNames.UNBASE64,
          Unbase64Restrictions.NOT_SUPPORT_FAIL_ON_ERROR
        )
    }
    GenericExpressionTransformer(substraitExprName, child, expr)
  }

  override def genBase64StaticInvokeTransformer(
      substraitExprName: String,
      child: ExpressionTransformer,
      expr: StaticInvoke): ExpressionTransformer = {
    if (!SQLConf.get.getConfString("spark.sql.chunkBase64String.enabled", "true").toBoolean) {
      GlutenExceptionUtil
        .throwsNotFullySupported(
          ExpressionNames.BASE64,
          Base64Restrictions.NOT_SUPPORT_DISABLE_CHUNK_BASE64_STRING)
    }
    GenericExpressionTransformer(
      ExpressionNames.BASE64,
      child,
      expr
    )
  }

  /**
   * Define backend-specific expression converter. Bolt's native MapFromArrays implementation only
   * matches Spark's `LAST_WIN` map-key dedup policy. When `spark.sql.mapKeyDedupPolicy` is set to a
   * non `LAST_WIN` value (e.g. `FIRST_WIN`), fall back to vanilla Spark to keep semantics
   * consistent.
   */
  override def extraExpressionConverter(
      substraitExprName: String,
      expr: Expression,
      attributeSeq: Seq[Attribute]): Option[ExpressionTransformer] = {
    expr match {
      case _: MapFromArrays =>
        val policy = SQLConf.get.getConf(SQLConf.MAP_KEY_DEDUP_POLICY).toString
        if (policy == "FIRST_WIN") {
          GlutenExceptionUtil.throwsNotFullySupported(
            ExpressionNames.MAP_FROM_ARRAYS,
            MapFromArraysRestrictions.NOT_SUPPORT_FIRST_WIN_DEDUP_POLICY)
        }
        None
      case _ => None
    }
  }

  /** Generate an expression transformer to transform NamedStruct to Substrait. */
  override def genNamedStructTransformer(
      substraitExprName: String,
      children: Seq[ExpressionTransformer],
      original: CreateNamedStruct,
      attributeSeq: Seq[Attribute]): ExpressionTransformer = {
    BoltNamedStructTransformer(substraitExprName, original, attributeSeq)
  }

  /** Generate an ExpressionTransformer to transform GetStructFiled expression. */
  override def genGetStructFieldTransformer(
      substraitExprName: String,
      childTransformer: ExpressionTransformer,
      ordinal: Int,
      original: GetStructField): ExpressionTransformer = {
    BoltGetStructFieldTransformer(substraitExprName, childTransformer, ordinal, original)
  }

  /**
   * To align with spark in casting string type input to other types, add trim node for trimming
   * space or whitespace. See spark's Cast.scala.
   */
  override def genCastWithNewChild(c: Cast): Cast = {
    // scalastyle:off nonascii
    // Common whitespace to be trimmed, including: ' ', '\n', '\r', '\f', etc.
    val trimWhitespaceStr = " \t\n\u000B\u000C\u000D\u001C\u001D\u001E\u001F"
    // Space separator.
    val trimSpaceSepStr = "\u1680\u2008\u2009\u200A\u205F\u3000" +
      ('\u2000' to '\u2006').toList.mkString
    // Line separator.
    val trimLineSepStr = "\u2028"
    // Paragraph separator.
    val trimParaSepStr = "\u2029"
    // Needs to be trimmed for casting to float/double/decimal
    val trimSpaceStr = ('\u0000' to '\u0020').toList.mkString
    // ISOControl characters, refer java.lang.Character.isISOControl(int)
    val isoControlStr = (('\u0000' to '\u001F') ++ ('\u007F' to '\u009F')).toList.mkString
    // scalastyle:on nonascii
    if (BoltConfig.get.castFromVarcharAddTrimNode && c.child.dataType == StringType) {
      val trimStr = c.dataType match {
        case BinaryType | _: ArrayType | _: MapType | _: StructType | _: UserDefinedType[_] =>
          None
        case FloatType | DoubleType | _: DecimalType =>
          Some(trimSpaceStr)
        case _ =>
          Some(
            (trimWhitespaceStr + trimSpaceSepStr + trimLineSepStr
              + trimParaSepStr + isoControlStr).toSet.mkString
          )
      }
      trimStr
        .map {
          trim =>
            c.withNewChildren(Seq(StringTrim(c.child, Some(Literal(trim))))).asInstanceOf[Cast]
        }
        .getOrElse(c)
    } else {
      c
    }
  }

  /** Define backend specfic expression mappings. */
  override def extraExpressionMappings: Seq[Sig] = {
    Seq(
      Sig[HLLAdapter](ExpressionNames.APPROX_DISTINCT),
      Sig[UDFExpression](ExpressionNames.UDF_PLACEHOLDER),
      Sig[UserDefinedAggregateFunction](ExpressionNames.UDAF_PLACEHOLDER),
      Sig[NaNvl](ExpressionNames.NANVL),
      Sig[BoltCollectList](ExpressionNames.COLLECT_LIST),
      Sig[CollectList](ExpressionNames.COLLECT_LIST),
      Sig[BoltCollectSet](ExpressionNames.COLLECT_SET),
      Sig[CollectSet](ExpressionNames.COLLECT_SET),
      Sig[BoltBloomFilterMightContain](ExpressionNames.MIGHT_CONTAIN),
      Sig[BoltBloomFilterAggregate](ExpressionNames.BLOOM_FILTER_AGG),
      Sig[MapFilter](ExpressionNames.MAP_FILTER),
      // For test purpose.
      Sig[BoltDummyExpression](BoltDummyExpression.BOLT_DUMMY_EXPRESSION)
    )
  }

  override def rewriteSpillPath(path: String): String = {
    val fs = BoltConfig.get.boltSpillFileSystem
    fs match {
      case "local" =>
        path
      case "heap-over-local" =>
        val rewritten = UriBuilder
          .fromPath(path)
          .scheme("jol")
          .toString
        rewritten
      case other =>
        throw new IllegalStateException(s"Unsupported fs: $other")
    }
  }

  override def validatePaimonScanCapabilities(
      hasPrimaryKeys: Boolean,
      allSplitsRawConvertible: Boolean,
      deletionVectorsEnabled: Boolean,
      changelogProducer: String,
      mergeEngine: String): ValidationResult = {
    if (deletionVectorsEnabled) {
      return ValidationResult.failed(
        "[Paimon Fallback]: The scan with deletion vector is not supported")
    }

    if (hasPrimaryKeys && allSplitsRawConvertible) {
      return ValidationResult.succeeded
    }

    if (changelogProducer.equalsIgnoreCase("LOOKUP")) {
      return ValidationResult.failed(
        "[Paimon Fallback] LOOKUP ChangelogProducer is not supported")
    }

    if (!hasPrimaryKeys) {
      return ValidationResult.succeeded
    }

    val morEnabled =
      SQLConf.get.getConfString("spark.gluten.paimon.native.mor.source.enabled", "false").toBoolean
    if (!morEnabled) {
      return ValidationResult.failed("[Paimon Fallback]: The paimon mor source is not enabled.")
    }

    val aggregateEngineEnabled = SQLConf.get
      .getConfString("spark.gluten.paimon.native.mor.aggregate.engine.enabled", "false")
      .toBoolean
    val partialUpdateEngineEnabled = SQLConf.get
      .getConfString("spark.gluten.paimon.native.mor.partial.update.engine.enabled", "false")
      .toBoolean

    val supported =
      mergeEngine.equalsIgnoreCase("DEDUPLICATE") ||
        (mergeEngine.equalsIgnoreCase("AGGREGATE") && aggregateEngineEnabled) ||
        (mergeEngine.equalsIgnoreCase("PARTIAL_UPDATE") && partialUpdateEngineEnabled)

    if (supported) {
      ValidationResult.succeeded
    } else {
      ValidationResult.failed(
        s"""
           |[Paimon Fallback] the configured Paimon merge engine $mergeEngine
           | is either not supported or does not have its corresponding configuration
           | flag enabled.""".stripMargin)
    }
  }

  override def rewritePaimonPushdownFilters(
      filters: Seq[Expression],
      primaryKeys: Set[String],
      metadataColumns: Set[String]): Seq[Expression] = {
    val isMetadataColumn: Attribute => Boolean = attr => metadataColumns.contains(attr.name)
    val touchesMetadata: Expression => Boolean =
      expr => expr.references.exists(attr => metadataColumns.contains(attr.name))

    def isAllowedExpr(expr: Expression): Boolean = expr match {
      case EqualTo(attr: Attribute, _: Literal) =>
        primaryKeys.contains(attr.name) && !isMetadataColumn(attr)
      case EqualTo(_: Literal, attr: Attribute) =>
        primaryKeys.contains(attr.name) && !isMetadataColumn(attr)
      case GreaterThan(attr: Attribute, _: Literal) =>
        primaryKeys.contains(attr.name) && !isMetadataColumn(attr)
      case GreaterThan(_: Literal, attr: Attribute) =>
        primaryKeys.contains(attr.name) && !isMetadataColumn(attr)
      case LessThan(attr: Attribute, _: Literal) =>
        primaryKeys.contains(attr.name) && !isMetadataColumn(attr)
      case LessThan(_: Literal, attr: Attribute) =>
        primaryKeys.contains(attr.name) && !isMetadataColumn(attr)
      case Like(attr: Attribute, _: Literal, _) =>
        primaryKeys.contains(attr.name) && !isMetadataColumn(attr)
      case Like(_: Literal, attr: Attribute, _) =>
        primaryKeys.contains(attr.name) && !isMetadataColumn(attr)
      case And(left, right) => isAllowedExpr(left) && isAllowedExpr(right)
      case Or(left, right) => isAllowedExpr(left) && isAllowedExpr(right)
      case Not(child) => isAllowedExpr(child)
      case Alias(child, _) => isAllowedExpr(child)
      case _ => false
    }

    val noMetadataFilters = filters.filterNot(touchesMetadata)
    if (primaryKeys.nonEmpty) {
      noMetadataFilters.filter(isAllowedExpr)
    } else {
      noMetadataFilters
    }
  }

  override def genGenerateTransformer(
      generator: Generator,
      requiredChildOutput: Seq[Attribute],
      outer: Boolean,
      generatorOutput: Seq[Attribute],
      child: SparkPlan): GenerateExecTransformerBase = {
    GenerateExecTransformer(generator, requiredChildOutput, outer, generatorOutput, child)
  }

  override def genPreProjectForGenerate(generate: GenerateExec): SparkPlan = {
    PullOutGenerateProjectHelper.pullOutPreProject(generate)
  }

  override def genPostProjectForGenerate(generate: GenerateExec): SparkPlan = {
    generate.generator match {
      case _: JsonTuple =>
        PullOutGenerateProjectHelper.pullOutPostProject(generate)
      case _ =>
        generate
    }
  }

  override def genPreProjectForArrowEvalPythonExec(
      arrowEvalPythonExec: ArrowEvalPythonExec): SparkPlan = {
    PullOutArrowEvalPythonPreProjectHelper.pullOutPreProject(arrowEvalPythonExec)
  }

  override def maybeCollapseTakeOrderedAndProject(plan: SparkPlan): SparkPlan = {
    // This to-top-n optimization assumes exchange operators were already placed in input plan.
    plan.transformUp {
      case p @ LimitExecTransformer(SortExecTransformer(sortOrder, _, child, _), 0, count) =>
        val global = child.outputPartitioning.satisfies(AllTuples)
        val topN = TopNTransformer(count, sortOrder, global, child)
        if (topN.doValidate().ok()) {
          topN
        } else {
          p
        }
      case other => other
    }
  }

  override def genHiveUDFTransformer(
      expr: Expression,
      attributeSeq: Seq[Attribute]): ExpressionTransformer = {
    BoltHiveUDFTransformer.replaceWithExpressionTransformer(expr, attributeSeq)
  }

  override def genColumnarCollectLimitExec(
      limit: Int,
      child: SparkPlan,
      offset: Int): ColumnarCollectLimitBaseExec =
    ColumnarCollectLimitExec(limit, child, offset)

  override def genColumnarRangeExec(rangeExec: RangeExec): ColumnarRangeBaseExec =
    ColumnarRangeExec(rangeExec.range)

  override def genColumnarTailExec(limit: Int, child: SparkPlan): ColumnarCollectTailBaseExec =
    ColumnarCollectTailExec(limit, child)

  override def genColumnarToCarrierRow(plan: SparkPlan): SparkPlan = {
    BoltColumnarToCarrierRowExec.enforce(plan)
  }

  override def genTimestampAddTransformer(
      substraitExprName: String,
      left: ExpressionTransformer,
      right: ExpressionTransformer,
      original: Expression): ExpressionTransformer = {
    // Since spark 3.3.0
    val extract =
      SparkShimLoader.getSparkShims.extractExpressionTimestampAddUnit(original)
    if (extract.isEmpty) {
      throw new UnsupportedOperationException(s"Not support expression TimestampAdd.")
    }
    TimestampAddTransformer(substraitExprName, extract.get.head, left, right, original)
  }

  override def genTimestampDiffTransformer(
      substraitExprName: String,
      left: ExpressionTransformer,
      right: ExpressionTransformer,
      original: Expression): ExpressionTransformer = {
    // Since spark 3.3.0
    val extract =
      SparkShimLoader.getSparkShims.extractExpressionTimestampDiffUnit(original)
    if (extract.isEmpty) {
      throw new UnsupportedOperationException(s"Not support expression TimestampDiff.")
    }
    TimestampDiffTransformer(substraitExprName, extract.get, left, right, original)
  }

  override def genToUnixTimestampTransformer(
      substraitExprName: String,
      timeExp: ExpressionTransformer,
      format: ExpressionTransformer,
      original: Expression): ExpressionTransformer = {
    ToUnixTimestampTransformer(substraitExprName, timeExp, format, original)
  }

  override def genMonthsBetweenTransformer(
      substraitExprName: String,
      date1: ExpressionTransformer,
      date2: ExpressionTransformer,
      roundOff: ExpressionTransformer,
      original: MonthsBetween): ExpressionTransformer = {
    MonthsBetweenTransformer(substraitExprName, date1, date2, roundOff, original)
  }

  override def genWindowFunctionsNode(
      windowExpression: Seq[NamedExpression],
      windowExpressionNodes: JList[WindowFunctionNode],
      originalInputAttributes: Seq[Attribute],
      context: SubstraitContext): Unit = {
    windowExpression.foreach {
      windowExpr =>
        val aliasExpr = windowExpr.asInstanceOf[Alias]
        val columnName = s"${aliasExpr.name}_${aliasExpr.exprId.id}"
        val wExpression = aliasExpr.child.asInstanceOf[WindowExpression]
        wExpression.windowFunction match {
          case wf @ (RowNumber() | Rank(_) | DenseRank(_) | CumeDist() | PercentRank(_)) =>
            val aggWindowFunc = wf.asInstanceOf[AggregateWindowFunction]
            val frame = aggWindowFunc.frame.asInstanceOf[SpecifiedWindowFrame]
            val windowFunctionNode = ExpressionBuilder.makeWindowFunction(
              WindowFunctionsBuilder.create(context, aggWindowFunc).toInt,
              new JArrayList[ExpressionNode](),
              columnName,
              ConverterUtils.getTypeNode(aggWindowFunc.dataType, aggWindowFunc.nullable),
              frame.upper,
              frame.lower,
              frame.frameType.sql,
              originalInputAttributes.asJava
            )
            windowExpressionNodes.add(windowFunctionNode)
          case aggExpression: AggregateExpression =>
            val frame = wExpression.windowSpec.frameSpecification.asInstanceOf[SpecifiedWindowFrame]
            val originalAggFunc = aggExpression.aggregateFunction
            val aggregateFunc =
              try {
                AggregateFunctionsBuilder.getSubstraitFunctionName(originalAggFunc)
                originalAggFunc
              } catch {
                case e: GlutenNotSupportException =>
                  HiveUDAFInspector.getUDAFClassName(originalAggFunc) match {
                    case Some(udafClass) if UDFResolver.UDAFNames.contains(udafClass) =>
                      UDFResolver.getUdafExpression(udafClass)(originalAggFunc.children)
                    case _ => throw e
                  }
              }

            val childrenNodeList = aggregateFunc.children
              .map(
                ExpressionConverter
                  .replaceWithExpressionTransformer(_, originalInputAttributes)
                  .doTransform(context))
              .asJava

            val functionId = BoltAggregateFunctionsBuilder
              .create(context, aggregateFunc, aggExpression.mode)
              .toInt
            val windowFunctionNode = ExpressionBuilder.makeWindowFunction(
              functionId,
              childrenNodeList,
              columnName,
              ConverterUtils.getTypeNode(aggExpression.dataType, aggExpression.nullable),
              frame.upper,
              frame.lower,
              frame.frameType.sql,
              originalInputAttributes.asJava
            )
            windowExpressionNodes.add(windowFunctionNode)
          case wf @ (_: Lead | _: Lag) =>
            val offsetWf = wf.asInstanceOf[FrameLessOffsetWindowFunction]
            val frame = offsetWf.frame.asInstanceOf[SpecifiedWindowFrame]
            val childrenNodeList = new JArrayList[ExpressionNode]()
            childrenNodeList.add(
              ExpressionConverter
                .replaceWithExpressionTransformer(
                  offsetWf.input,
                  attributeSeq = originalInputAttributes)
                .doTransform(context))
            val offset = offsetWf.offset.eval(EmptyRow).asInstanceOf[Int]
            val offsetNode = ExpressionBuilder.makeLiteral(Math.abs(offset.toLong), LongType, false)
            childrenNodeList.add(offsetNode)
            if (offsetWf.default.dataType != NullType) {
              childrenNodeList.add(
                ExpressionConverter
                  .replaceWithExpressionTransformer(
                    offsetWf.default,
                    attributeSeq = originalInputAttributes)
                  .doTransform(context))
            }
            val windowFunctionNode = ExpressionBuilder.makeWindowFunction(
              WindowFunctionsBuilder.create(context, offsetWf).toInt,
              childrenNodeList,
              columnName,
              ConverterUtils.getTypeNode(offsetWf.dataType, offsetWf.nullable),
              frame.upper,
              frame.lower,
              frame.frameType.sql,
              offsetWf.ignoreNulls,
              originalInputAttributes.asJava
            )
            windowExpressionNodes.add(windowFunctionNode)
          case wf @ NthValue(input, offset: Literal, ignoreNulls: Boolean) =>
            val frame = wExpression.windowSpec.frameSpecification.asInstanceOf[SpecifiedWindowFrame]
            val childrenNodeList = new JArrayList[ExpressionNode]()
            childrenNodeList.add(
              ExpressionConverter
                .replaceWithExpressionTransformer(input, attributeSeq = originalInputAttributes)
                .doTransform(context))
            childrenNodeList.add(LiteralTransformer(offset).doTransform(context))
            val windowFunctionNode = ExpressionBuilder.makeWindowFunction(
              WindowFunctionsBuilder.create(context, wf).toInt,
              childrenNodeList,
              columnName,
              ConverterUtils.getTypeNode(wf.dataType, wf.nullable),
              frame.upper,
              frame.lower,
              frame.frameType.sql,
              ignoreNulls,
              originalInputAttributes.asJava
            )
            windowExpressionNodes.add(windowFunctionNode)
          case wf @ NTile(buckets: Expression) =>
            val frame = wExpression.windowSpec.frameSpecification.asInstanceOf[SpecifiedWindowFrame]
            val childrenNodeList = new JArrayList[ExpressionNode]()
            val literal = buckets.asInstanceOf[Literal]
            childrenNodeList.add(LiteralTransformer(literal).doTransform(context))
            val windowFunctionNode = ExpressionBuilder.makeWindowFunction(
              WindowFunctionsBuilder.create(context, wf).toInt,
              childrenNodeList,
              columnName,
              ConverterUtils.getTypeNode(wf.dataType, wf.nullable),
              frame.upper,
              frame.lower,
              frame.frameType.sql,
              originalInputAttributes.asJava
            )
            windowExpressionNodes.add(windowFunctionNode)
          case _ =>
            throw new GlutenNotSupportException(
              "unsupported window function type: " +
                wExpression.windowFunction)
        }
    }
  }
}
