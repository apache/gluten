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

import org.apache.gluten.backendsapi.SparkPlanExecApi
import org.apache.gluten.config.{GlutenConfig, HashShuffleWriterType, ReservedKeys, RssSortShuffleWriterType, ShuffleWriterType, SortShuffleWriterType, VeloxConfig}
import org.apache.gluten.exception.{GlutenExceptionUtil, GlutenNotSupportException}
import org.apache.gluten.execution._
import org.apache.gluten.expression._
import org.apache.gluten.expression.aggregate.{HLLAdapter, VeloxBloomFilterAggregate, VeloxCollectList, VeloxCollectSet}
import org.apache.gluten.extension.JoinKeysTag
import org.apache.gluten.extension.columnar.FallbackTags
import org.apache.gluten.shuffle.NeedCustomColumnarBatchSerializer
import org.apache.gluten.sql.shims.SparkShimLoader
import org.apache.gluten.vectorized.{ColumnarBatchSerializer, ColumnarBatchSerializeResult}

import org.apache.spark.{ShuffleDependency, SparkEnv, SparkException}
import org.apache.spark.api.python.{ColumnarArrowEvalPythonExec, PullOutArrowEvalPythonPreProjectHelper}
import org.apache.spark.internal.Logging
import org.apache.spark.memory.SparkMemoryUtil
import org.apache.spark.rdd.RDD
import org.apache.spark.serializer.Serializer
import org.apache.spark.shuffle.{GenShuffleReaderParameters, GenShuffleWriterParameters, GlutenShuffleReaderWrapper, GlutenShuffleWriterWrapper}
import org.apache.spark.shuffle.utils.ShuffleUtil
import org.apache.spark.sql.catalyst.catalog.BucketSpec
import org.apache.spark.sql.catalyst.catalog.CatalogTypes.TablePartitionSpec
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.aggregate.{AggregateExpression, CollectList, CollectSet}
import org.apache.spark.sql.catalyst.expressions.objects.{AssertNotNull, StaticInvoke}
import org.apache.spark.sql.catalyst.optimizer.BuildSide
import org.apache.spark.sql.catalyst.plans.JoinType
import org.apache.spark.sql.catalyst.plans.physical._
import org.apache.spark.sql.execution._
import org.apache.spark.sql.execution.adaptive.AQEShuffleReadExec
import org.apache.spark.sql.execution.datasources.FileFormat
import org.apache.spark.sql.execution.exchange.ShuffleExchangeExec
import org.apache.spark.sql.execution.joins.{BuildSideRelation, HashedRelationBroadcastMode, SparkHashJoinUtils}
import org.apache.spark.sql.execution.metric.SQLMetric
import org.apache.spark.sql.execution.python.ArrowEvalPythonExec
import org.apache.spark.sql.execution.unsafe.UnsafeColumnarBuildSideRelation
import org.apache.spark.sql.execution.utils.ExecUtil
import org.apache.spark.sql.expression.{UDFExpression, UserDefinedAggregateFunction}
import org.apache.spark.sql.hive.VeloxHiveUDFTransformer
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types._
import org.apache.spark.sql.vectorized.ColumnarBatch
import org.apache.spark.task.TaskResources

import org.apache.commons.lang3.ClassUtils

import javax.ws.rs.core.UriBuilder

import java.util.Locale

import scala.collection.JavaConverters._
import scala.collection.mutable.ArrayBuffer

class VeloxSparkPlanExecApi extends SparkPlanExecApi with Logging {

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
    // Offload to velox for only IntegralTypes.
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
      // Offload to velox for only IntegralTypes.
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

  override def getDecimalArithmeticExprName(exprName: String): String = if (
    !SQLConf.get.decimalOperationsAllowPrecisionLoss
  ) { exprName + "_deny_precision_loss" }
  else { exprName }

  /** Transform map_entries to Substrait. */
  override def genMapEntriesTransformer(
      substraitExprName: String,
      child: ExpressionTransformer,
      expr: Expression): ExpressionTransformer = {
    GenericExpressionTransformer(substraitExprName, Seq(child), expr)
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
        val hashExpr = if (exprs.isEmpty) {
          // In Spark, a hash expression with empty input is not resolvable and an
          // `WRONG_NUM_ARGS.WITHOUT_SUGGESTION` error will be reported when validating the project
          // transformer. So we directly return the seed here, which is the intended hashed value
          // for empty input given Spark's murmur3 hash logic.
          Literal(new Murmur3Hash(Nil).seed, IntegerType)
        } else {
          new Murmur3Hash(exprs)
        }
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
          // Velox hash expression does not support null type and we also do not need to sort
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
    VeloxBroadcastNestedLoopJoinExecTransformer(left, right, buildSide, joinType, condition)

  override def genHashExpressionTransformer(
      substraitExprName: String,
      exprs: Seq[ExpressionTransformer],
      original: HashExpression[_]): ExpressionTransformer = {
    VeloxHashExpressionTransformer(substraitExprName, exprs, original)
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
    VeloxColumnarWriteFilesExec(
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
    SparkEnv.get.shuffleManager match {
      case serializer: NeedCustomColumnarBatchSerializer =>
        val className = serializer.columnarBatchSerializerClass()
        val clazz = ClassUtils.getClass(className)
        val constructor =
          clazz.getConstructor(
            classOf[StructType],
            classOf[SQLMetric],
            classOf[SQLMetric],
            classOf[ShuffleWriterType])
        constructor
          .newInstance(schema, readBatchNumRows, numOutputRows, shuffleWriterType)
          .asInstanceOf[Serializer]
      case _ =>
        new ColumnarBatchSerializer(
          schema,
          readBatchNumRows,
          numOutputRows,
          deserializeTime,
          decompressTime,
          shuffleWriterType)
    }
  }

  /** Create broadcast relation for BroadcastExchangeExec */
  override def createBroadcastRelation(
      mode: BroadcastMode,
      child: SparkPlan,
      numOutputRows: SQLMetric,
      dataSize: SQLMetric): BuildSideRelation = {

    val buildKeys = mode match {
      case mode1: HashedRelationBroadcastMode =>
        mode1.key
      case _ =>
        // IdentityBroadcastMode
        Seq.empty
    }
    var offload = true
    val (newChild, newOutput, newBuildKeys) =
      if (VeloxConfig.get.enableBroadcastBuildOncePerExecutor) {

        // Try to lookup from TreeNodeTag using child's logical plan
        // Need to recursively find logicalLink in case of AQE or other wrappers
        @scala.annotation.tailrec
        def findLogicalLink(
            plan: SparkPlan): Option[org.apache.spark.sql.catalyst.plans.logical.LogicalPlan] = {
          plan.logicalLink match {
            case some @ Some(_) => some
            case None =>
              plan.children match {
                case Seq(child) => findLogicalLink(child)
                case _ => None
              }
          }
        }

        val newBuildKeys = findLogicalLink(child)
          .flatMap(_.getTagValue(JoinKeysTag.ORIGINAL_JOIN_KEYS))
          .getOrElse {
            if (SparkHashJoinUtils.canRewriteAsLongType(buildKeys) && buildKeys.nonEmpty) {
              SparkHashJoinUtils.getOriginalKeysFromPacked(buildKeys.head)
            } else {
              buildKeys
            }
          }

        val noNeedPreOp = newBuildKeys.forall {
          case _: AttributeReference | _: BoundReference => true
          case _ => false
        }

        if (noNeedPreOp) {
          (child, child.output, Seq.empty[Expression])
        } else {
          // pre projection in case of expression join keys
          val appendedProjections = new ArrayBuffer[NamedExpression]()
          val preProjectionBuildKeys = newBuildKeys.zipWithIndex.map {
            case (e, idx) =>
              e match {
                case b: BoundReference => child.output(b.ordinal)
                case a: AttributeReference => a
                case o: Expression =>
                  val newExpr = Alias(o, "col_" + idx)()
                  appendedProjections += newExpr
                  newExpr
              }
          }

          def wrapChild(child: SparkPlan): SparkPlan = {
            val childWithAdapter =
              ColumnarCollapseTransformStages.wrapInputIteratorTransformer(child)
            val projectExecTransformer =
              ProjectExecTransformer(child.output ++ appendedProjections, childWithAdapter)
            val validationResult = projectExecTransformer.doValidate()
            if (validationResult.ok()) {
              WholeStageTransformer(
                ProjectExecTransformer(child.output ++ appendedProjections, childWithAdapter))(
                ColumnarCollapseTransformStages
                  .getTransformStageCounter(childWithAdapter)
                  .incrementAndGet()
              )
            } else {
              offload = false
              child
            }
          }

          val newChild = child match {
            case wt: WholeStageTransformer =>
              val projectTransformer =
                ProjectExecTransformer(child.output ++ appendedProjections, wt.child)
              if (projectTransformer.doValidate().ok()) {
                wt.withNewChildren(
                  Seq(ProjectExecTransformer(child.output ++ appendedProjections, wt.child)))

              } else {
                offload = false
                child
              }
            case w: WholeStageCodegenExec =>
              w.withNewChildren(Seq(ProjectExec(child.output ++ appendedProjections, w.child)))
            case r: AQEShuffleReadExec if r.supportsColumnar =>
              // when aqe is open
              // TODO: remove this after pushdowning preprojection
              wrapChild(r)
            case r2c: RowToVeloxColumnarExec =>
              wrapChild(r2c)
            case union: ColumnarUnionExec =>
              wrapChild(union)
            case ordered: TakeOrderedAndProjectExecTransformer =>
              wrapChild(ordered)
            case a2v: ArrowColumnarToVeloxColumnarExec =>
              wrapChild(a2v)
            case other =>
              offload = false
              logWarning(
                "Not supported operator " + other.nodeName +
                  " for BroadcastRelation and fallback to shuffle hash join")
              child
          }

          if (offload) {
            (
              newChild,
              (child.output ++ appendedProjections).map(_.toAttribute),
              preProjectionBuildKeys)
          } else {
            (child, child.output, Seq.empty[Expression])
          }
        }
      } else {
        offload = false
        (child, child.output, buildKeys)
      }

    val useOffheapBroadcastBuildRelation =
      VeloxConfig.get.enableBroadcastBuildRelationInOffheap

    val serialized: Seq[ColumnarBatchSerializeResult] = newChild
      .executeColumnar()
      .mapPartitions(itr => Iterator(BroadcastUtils.serializeStream(itr)))
      .filter(_.numRows != 0)
      .collect
    val rawSize = serialized.map(_.sizeInBytes()).sum
    if (rawSize >= GlutenConfig.get.maxBroadcastTableSize) {
      throw new SparkException(
        "Cannot broadcast the table that is larger than " +
          s"${SparkMemoryUtil.bytesToString(GlutenConfig.get.maxBroadcastTableSize)}: " +
          s"${SparkMemoryUtil.bytesToString(rawSize)}")
    }
    numOutputRows += serialized.map(_.numRows).sum
    dataSize += rawSize

    if (useOffheapBroadcastBuildRelation) {
      TaskResources.runUnsafe {
        UnsafeColumnarBuildSideRelation(
          newOutput,
          serialized.flatMap(_.offHeapData().asScala),
          mode,
          newBuildKeys,
          offload)
      }
    } else {
      ColumnarBuildSideRelation(
        newOutput,
        serialized.flatMap(_.onHeapData().asScala).toArray,
        mode,
        newBuildKeys,
        offload)
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
    VeloxAliasTransformer(substraitExprName, child, original)

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
      SQLConf.get.getConf(SQLConf.MAP_KEY_DEDUP_POLICY)
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
      // Velox only supports partial results mode. We need to fall back this when
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

  /** Generate an expression transformer to transform NamedStruct to Substrait. */
  override def genNamedStructTransformer(
      substraitExprName: String,
      children: Seq[ExpressionTransformer],
      original: CreateNamedStruct,
      attributeSeq: Seq[Attribute]): ExpressionTransformer = {
    VeloxNamedStructTransformer(substraitExprName, original, attributeSeq)
  }

  /** Generate an ExpressionTransformer to transform GetStructFiled expression. */
  override def genGetStructFieldTransformer(
      substraitExprName: String,
      childTransformer: ExpressionTransformer,
      ordinal: Int,
      original: GetStructField): ExpressionTransformer = {
    VeloxGetStructFieldTransformer(substraitExprName, childTransformer, ordinal, original)
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
    if (VeloxConfig.get.castFromVarcharAddTrimNode && c.child.dataType == StringType) {
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
      Sig[VeloxCollectList](ExpressionNames.COLLECT_LIST),
      Sig[CollectList](ExpressionNames.COLLECT_LIST),
      Sig[VeloxCollectSet](ExpressionNames.COLLECT_SET),
      Sig[CollectSet](ExpressionNames.COLLECT_SET),
      Sig[VeloxBloomFilterMightContain](ExpressionNames.MIGHT_CONTAIN),
      Sig[VeloxBloomFilterAggregate](ExpressionNames.BLOOM_FILTER_AGG),
      Sig[MapFilter](ExpressionNames.MAP_FILTER),
      Sig[AssertNotNull](ExpressionNames.ASSERT_NOT_NULL),
      // For test purpose.
      Sig[VeloxDummyExpression](VeloxDummyExpression.VELOX_DUMMY_EXPRESSION)
    )
  }

  override def rewriteSpillPath(path: String): String = {
    val fs = VeloxConfig.get.veloxSpillFileSystem
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
    PullOutGenerateProjectHelper.pullOutPostProject(generate)
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
    VeloxHiveUDFTransformer.replaceWithExpressionTransformer(expr, attributeSeq)
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
    VeloxColumnarToCarrierRowExec.enforce(plan)
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

  override def getErrorMessage(raiseError: RaiseError): Expression = {
    SparkShimLoader.getSparkShims.getErrorMessage(raiseError) match {
      case Some(msg) => msg
      case None =>
        GlutenExceptionUtil.throwsNotFullySupported(
          ExpressionNames.RAISE_ERROR,
          RaiseErrorRestrictions.ONLY_SUPPORT_ERROR_MESSAGE)
    }
  }
}
