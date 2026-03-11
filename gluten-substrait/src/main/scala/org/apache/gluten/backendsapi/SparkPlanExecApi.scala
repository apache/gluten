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
package org.apache.gluten.backendsapi

import org.apache.gluten.config.{HashShuffleWriterType, ShuffleWriterType}
import org.apache.gluten.exception.GlutenNotSupportException
import org.apache.gluten.execution._
import org.apache.gluten.expression._
import org.apache.gluten.sql.shims.SparkShimLoader
import org.apache.gluten.substrait.SubstraitContext
import org.apache.gluten.substrait.expression.{ExpressionBuilder, ExpressionNode, WindowFunctionNode}

import org.apache.spark.ShuffleDependency
import org.apache.spark.rdd.RDD
import org.apache.spark.serializer.Serializer
import org.apache.spark.shuffle.{GenShuffleReaderParameters, GenShuffleWriterParameters, GlutenShuffleReaderWrapper, GlutenShuffleWriterWrapper}
import org.apache.spark.sql.catalyst.catalog.BucketSpec
import org.apache.spark.sql.catalyst.catalog.CatalogTypes.TablePartitionSpec
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.aggregate.AggregateExpression
import org.apache.spark.sql.catalyst.expressions.objects.StaticInvoke
import org.apache.spark.sql.catalyst.optimizer.BuildSide
import org.apache.spark.sql.catalyst.plans.JoinType
import org.apache.spark.sql.catalyst.plans.physical.{BroadcastMode, Partitioning}
import org.apache.spark.sql.execution._
import org.apache.spark.sql.execution.datasources.FileFormat
import org.apache.spark.sql.execution.exchange.ShuffleExchangeExec
import org.apache.spark.sql.execution.joins.BuildSideRelation
import org.apache.spark.sql.execution.metric.SQLMetric
import org.apache.spark.sql.execution.python.ArrowEvalPythonExec
import org.apache.spark.sql.execution.window._
import org.apache.spark.sql.hive.HiveUDFTransformer
import org.apache.spark.sql.types.{DecimalType, LongType, NullType, StructType}
import org.apache.spark.sql.vectorized.ColumnarBatch

import java.io.{ObjectInputStream, ObjectOutputStream}
import java.util.{ArrayList => JArrayList, List => JList}

import scala.collection.JavaConverters._

trait SparkPlanExecApi {

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
  def genFilterExecTransformer(condition: Expression, child: SparkPlan): FilterExecTransformerBase

  def genProjectExecTransformer(
      projectList: Seq[NamedExpression],
      child: SparkPlan): ProjectExecTransformer =
    ProjectExecTransformer.createUnsafe(projectList, child)

  /** Generate HashAggregateExecTransformer. */
  def genHashAggregateExecTransformer(
      requiredChildDistributionExpressions: Option[Seq[Expression]],
      groupingExpressions: Seq[NamedExpression],
      aggregateExpressions: Seq[AggregateExpression],
      aggregateAttributes: Seq[Attribute],
      initialInputBufferOffset: Int,
      resultExpressions: Seq[NamedExpression],
      child: SparkPlan): HashAggregateExecBaseTransformer

  /** Generate HashAggregateExecPullOutHelper */
  def genHashAggregateExecPullOutHelper(
      aggregateExpressions: Seq[AggregateExpression],
      aggregateAttributes: Seq[Attribute]): HashAggregateExecPullOutBaseHelper

  def genColumnarShuffleExchange(shuffle: ShuffleExchangeExec): SparkPlan

  /** Generate ShuffledHashJoinExecTransformer. */
  def genShuffledHashJoinExecTransformer(
      leftKeys: Seq[Expression],
      rightKeys: Seq[Expression],
      joinType: JoinType,
      buildSide: BuildSide,
      condition: Option[Expression],
      left: SparkPlan,
      right: SparkPlan,
      isSkewJoin: Boolean): ShuffledHashJoinExecTransformerBase

  /** Generate BroadcastHashJoinExecTransformer. */
  def genBroadcastHashJoinExecTransformer(
      leftKeys: Seq[Expression],
      rightKeys: Seq[Expression],
      joinType: JoinType,
      buildSide: BuildSide,
      condition: Option[Expression],
      left: SparkPlan,
      right: SparkPlan,
      isNullAwareAntiJoin: Boolean = false): BroadcastHashJoinExecTransformerBase

  def genSampleExecTransformer(
      lowerBound: Double,
      upperBound: Double,
      withReplacement: Boolean,
      seed: Long,
      child: SparkPlan): SampleExecTransformer

  /** Generate ShuffledHashJoinExecTransformer. */
  def genSortMergeJoinExecTransformer(
      leftKeys: Seq[Expression],
      rightKeys: Seq[Expression],
      joinType: JoinType,
      condition: Option[Expression],
      left: SparkPlan,
      right: SparkPlan,
      isSkewJoin: Boolean = false,
      projectList: Seq[NamedExpression] = null): SortMergeJoinExecTransformerBase

  /** Generate CartesianProductExecTransformer. */
  def genCartesianProductExecTransformer(
      left: SparkPlan,
      right: SparkPlan,
      condition: Option[Expression]): CartesianProductExecTransformer

  def genBroadcastNestedLoopJoinExecTransformer(
      left: SparkPlan,
      right: SparkPlan,
      buildSide: BuildSide,
      joinType: JoinType,
      condition: Option[Expression]): BroadcastNestedLoopJoinExecTransformer

  def genAliasTransformer(
      substraitExprName: String,
      child: ExpressionTransformer,
      original: Expression): ExpressionTransformer =
    AliasTransformer(substraitExprName, child, original)

  /** Generate an expression transformer to transform GetMapValue to Substrait. */
  def genGetMapValueTransformer(
      substraitExprName: String,
      left: ExpressionTransformer,
      right: ExpressionTransformer,
      original: GetMapValue): ExpressionTransformer

  def genStringToMapTransformer(
      substraitExprName: String,
      children: Seq[ExpressionTransformer],
      expr: Expression): ExpressionTransformer = {
    GenericExpressionTransformer(substraitExprName, children, expr)
  }

  def genFromJsonTransformer(
      substraitExprName: String,
      children: Seq[ExpressionTransformer],
      expr: JsonToStructs): ExpressionTransformer = {
    GenericExpressionTransformer(substraitExprName, children, expr)
  }

  def genToJsonTransformer(
      substraitExprName: String,
      child: ExpressionTransformer,
      expr: StructsToJson): ExpressionTransformer = {
    GenericExpressionTransformer(substraitExprName, child, expr)
  }

  def genUnbase64Transformer(
      substraitExprName: String,
      child: ExpressionTransformer,
      expr: UnBase64): ExpressionTransformer = {
    GenericExpressionTransformer(substraitExprName, child, expr)
  }

  def genBase64StaticInvokeTransformer(
      substraitExprName: String,
      child: ExpressionTransformer,
      expr: StaticInvoke): ExpressionTransformer = {
    GenericExpressionTransformer(substraitExprName, child, expr)
  }

  /** Transform GetArrayItem to Substrait. */
  def genGetArrayItemTransformer(
      substraitExprName: String,
      left: ExpressionTransformer,
      right: ExpressionTransformer,
      original: Expression): ExpressionTransformer

  /** Transform NaNvl to Substrait. */
  def genNaNvlTransformer(
      substraitExprName: String,
      left: ExpressionTransformer,
      right: ExpressionTransformer,
      original: NaNvl): ExpressionTransformer = {
    GenericExpressionTransformer(substraitExprName, Seq(left, right), original)
  }

  def genAtLeastNNonNullsTransformer(
      substraitExprName: String,
      children: Seq[ExpressionTransformer],
      original: AtLeastNNonNulls): ExpressionTransformer = {
    throw new GlutenNotSupportException("AtLeastNNonNulls is not supported")
  }

  def genUuidTransformer(substraitExprName: String, original: Uuid): ExpressionTransformer = {
    GenericExpressionTransformer(substraitExprName, Seq(), original)
  }

  def genTryArithmeticTransformer(
      substraitExprName: String,
      left: ExpressionTransformer,
      right: ExpressionTransformer,
      original: TryEval,
      checkArithmeticExprName: String): ExpressionTransformer = {
    throw new GlutenNotSupportException(s"$checkArithmeticExprName is not supported")
  }

  def genTryEvalTransformer(
      substraitExprName: String,
      child: ExpressionTransformer,
      original: TryEval): ExpressionTransformer = {
    throw new GlutenNotSupportException(s"try_eval(${original.child.prettyName}) is not supported")
  }

  def genArithmeticTransformer(
      substraitExprName: String,
      left: ExpressionTransformer,
      right: ExpressionTransformer,
      original: Expression,
      checkArithmeticExprName: String): ExpressionTransformer = {
    GenericExpressionTransformer(substraitExprName, Seq(left, right), original)
  }

  def getDecimalArithmeticExprName(exprName: String): String = exprName

  /** Transform map_entries to Substrait. */
  def genMapEntriesTransformer(
      substraitExprName: String,
      child: ExpressionTransformer,
      expr: Expression): ExpressionTransformer = {
    throw new GlutenNotSupportException("map_entries is not supported")
  }

  /** Transform array filter to Substrait. */
  def genArrayFilterTransformer(
      substraitExprName: String,
      argument: ExpressionTransformer,
      function: ExpressionTransformer,
      expr: ArrayFilter): ExpressionTransformer = {
    throw new GlutenNotSupportException("filter(on array) is not supported")
  }

  /** Transform array forall to Substrait. */
  def genArrayForAllTransformer(
      substraitExprName: String,
      argument: ExpressionTransformer,
      function: ExpressionTransformer,
      expr: ArrayForAll): ExpressionTransformer = {
    throw new GlutenNotSupportException("all_match is not supported")
  }

  /** Transform array array_sort to Substrait. */
  def genArraySortTransformer(
      substraitExprName: String,
      argument: ExpressionTransformer,
      function: ExpressionTransformer,
      expr: ArraySort): ExpressionTransformer = {
    throw new GlutenNotSupportException("array_sort(on array) is not supported")
  }

  /** Transform array exists to Substrait */
  def genArrayExistsTransformer(
      substraitExprName: String,
      argument: ExpressionTransformer,
      function: ExpressionTransformer,
      expr: ArrayExists): ExpressionTransformer = {
    throw new GlutenNotSupportException("any_match is not supported")
  }

  /** Transform array transform to Substrait. */
  def genArrayTransformTransformer(
      substraitExprName: String,
      argument: ExpressionTransformer,
      function: ExpressionTransformer,
      expr: ArrayTransform): ExpressionTransformer = {
    throw new GlutenNotSupportException("transform(on array) is not supported")
  }

  /** Transform inline to Substrait. */
  def genInlineTransformer(
      substraitExprName: String,
      child: ExpressionTransformer,
      expr: Expression): ExpressionTransformer = {
    throw new GlutenNotSupportException("inline is not supported")
  }

  /** Transform posexplode to Substrait. */
  def genPosExplodeTransformer(
      substraitExprName: String,
      child: ExpressionTransformer,
      original: PosExplode,
      attributeSeq: Seq[Attribute]): ExpressionTransformer

  /** Transform make_timestamp to Substrait. */
  def genMakeTimestampTransformer(
      substraitExprName: String,
      children: Seq[ExpressionTransformer],
      expr: Expression): ExpressionTransformer = {
    throw new GlutenNotSupportException("make_timestamp is not supported")
  }

  def genRegexpReplaceTransformer(
      substraitExprName: String,
      children: Seq[ExpressionTransformer],
      expr: RegExpReplace): ExpressionTransformer = {
    GenericExpressionTransformer(substraitExprName, children, expr)
  }

  def genPreciseTimestampConversionTransformer(
      substraitExprName: String,
      children: Seq[ExpressionTransformer],
      expr: PreciseTimestampConversion): ExpressionTransformer = {
    throw new GlutenNotSupportException("PreciseTimestampConversion is not supported")
  }

  def genArrayInsertTransformer(
      substraitExprName: String,
      children: Seq[ExpressionTransformer],
      expr: Expression): ExpressionTransformer = {
    throw new GlutenNotSupportException("ArrayInsert is not supported")
  }

  // For date_add(cast('2001-01-01' as Date), interval 1 day), backends may handle it in different
  // ways
  def genDateAddTransformer(
      attributeSeq: Seq[Attribute],
      substraitExprName: String,
      children: Seq[Expression],
      expr: Expression): ExpressionTransformer = {
    val childrenTransformers =
      children.map(ExpressionConverter.replaceWithExpressionTransformer(_, attributeSeq))
    GenericExpressionTransformer(substraitExprName, childrenTransformers, expr)
  }

  /**
   * Generate ShuffleDependency for ColumnarShuffleExchangeExec.
   *
   * childOutputAttributes may be different from outputAttributes, for example, the
   * childOutputAttributes include additional shuffle key columns
   *
   * @return
   */
  // scalastyle:off argcount
  def genShuffleDependency(
      rdd: RDD[ColumnarBatch],
      childOutputAttributes: Seq[Attribute],
      outputAttributes: Seq[Attribute],
      newPartitioning: Partitioning,
      serializer: Serializer,
      writeMetrics: Map[String, SQLMetric],
      metrics: Map[String, SQLMetric],
      shuffleWriterType: ShuffleWriterType): ShuffleDependency[Int, ColumnarBatch, ColumnarBatch]

  /** Determine whether to use sort-based shuffle based on shuffle partitioning and output. */
  def getShuffleWriterType(
      partitioning: Partitioning,
      output: Seq[Attribute]): ShuffleWriterType = {
    HashShuffleWriterType
  }

  /**
   * Generate ColumnarShuffleWriter for ColumnarShuffleManager.
   *
   * @return
   */
  def genColumnarShuffleWriter[K, V](
      parameters: GenShuffleWriterParameters[K, V]): GlutenShuffleWriterWrapper[K, V]

  def genColumnarShuffleReader[K, C](
      parameters: GenShuffleReaderParameters[K, C]): GlutenShuffleReaderWrapper[K, C]

  /**
   * Generate ColumnarBatchSerializer for ColumnarShuffleExchangeExec.
   *
   * @return
   */
  def createColumnarBatchSerializer(
      schema: StructType,
      metrics: Map[String, SQLMetric],
      shuffleWriterType: ShuffleWriterType): Serializer

  /** Create broadcast relation for BroadcastExchangeExec */
  def createBroadcastRelation(
      mode: BroadcastMode,
      child: SparkPlan,
      numOutputRows: SQLMetric,
      dataSize: SQLMetric): BuildSideRelation

  def doCanonicalizeForBroadcastMode(mode: BroadcastMode): BroadcastMode = {
    mode.canonicalized
  }

  /** Create ColumnarWriteFilesExec */
  def createColumnarWriteFilesExec(
      child: WriteFilesExecTransformer,
      noop: SparkPlan,
      fileFormat: FileFormat,
      partitionColumns: Seq[Attribute],
      bucketSpec: Option[BucketSpec],
      options: Map[String, String],
      staticPartitions: TablePartitionSpec): ColumnarWriteFilesExec

  /** Create ColumnarArrowEvalPythonExec, for velox backend */
  def createColumnarArrowEvalPythonExec(
      udfs: Seq[PythonUDF],
      resultAttrs: Seq[Attribute],
      child: SparkPlan,
      evalType: Int): SparkPlan

  def genGetStructFieldTransformer(
      substraitExprName: String,
      childTransformer: ExpressionTransformer,
      ordinal: Int,
      original: GetStructField): ExpressionTransformer = {
    GetStructFieldTransformer(substraitExprName, childTransformer, original)
  }

  def genNamedStructTransformer(
      substraitExprName: String,
      children: Seq[ExpressionTransformer],
      original: CreateNamedStruct,
      attributeSeq: Seq[Attribute]): ExpressionTransformer = {
    GenericExpressionTransformer(substraitExprName, children, original)
  }

  def genStringTranslateTransformer(
      substraitExprName: String,
      srcExpr: ExpressionTransformer,
      matchingExpr: ExpressionTransformer,
      replaceExpr: ExpressionTransformer,
      original: StringTranslate): ExpressionTransformer = {
    GenericExpressionTransformer(
      substraitExprName,
      Seq(srcExpr, matchingExpr, replaceExpr),
      original)
  }

  def genLikeTransformer(
      substraitExprName: String,
      left: ExpressionTransformer,
      right: ExpressionTransformer,
      original: Like): ExpressionTransformer

  /**
   * Generate an ExpressionTransformer to transform TruncTimestamp expression.
   * TruncTimestampTransformer is the default implementation.
   */
  def genTruncTimestampTransformer(
      substraitExprName: String,
      format: ExpressionTransformer,
      timestamp: ExpressionTransformer,
      timeZoneId: Option[String] = None,
      original: TruncTimestamp): ExpressionTransformer = {
    TruncTimestampTransformer(substraitExprName, format, timestamp, original)
  }

  def genToUnixTimestampTransformer(
      substraitExprName: String,
      timeExp: ExpressionTransformer,
      format: ExpressionTransformer,
      original: Expression): ExpressionTransformer

  def genDateDiffTransformer(
      substraitExprName: String,
      endDate: ExpressionTransformer,
      startDate: ExpressionTransformer,
      original: DateDiff): ExpressionTransformer

  def genCastWithNewChild(c: Cast): Cast = c

  def genHashExpressionTransformer(
      substraitExprName: String,
      exprs: Seq[ExpressionTransformer],
      original: HashExpression[_]): ExpressionTransformer = {
    GenericExpressionTransformer(substraitExprName, exprs, original)
  }

  /** Define backend-specific expression mappings. */
  def extraExpressionMappings: Seq[Sig] = Seq.empty

  /** Define backend-specific expression converter. */
  def extraExpressionConverter(
      substraitExprName: String,
      expr: Expression,
      attributeSeq: Seq[Attribute]): Option[ExpressionTransformer] =
    None

  /**
   * Define whether the join operator is fallback because of the join operator is not supported by
   * backend
   */
  def joinFallback(
      JoinType: JoinType,
      leftOutputSet: AttributeSet,
      right: AttributeSet,
      condition: Option[Expression]): Boolean = false

  /** default function to generate window function node */
  def genWindowFunctionsNode(
      windowExpression: Seq[NamedExpression],
      windowExpressionNodes: JList[WindowFunctionNode],
      originalInputAttributes: Seq[Attribute],
      context: SubstraitContext): Unit = {
    windowExpression.map {
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
            val aggregateFunc = aggExpression.aggregateFunction
            val substraitAggFuncName = ExpressionMappings.expressionsMap.get(aggregateFunc.getClass)
            if (substraitAggFuncName.isEmpty) {
              throw new GlutenNotSupportException(s"Not currently supported: $aggregateFunc.")
            }

            val childrenNodeList = aggregateFunc.children
              .map(
                ExpressionConverter
                  .replaceWithExpressionTransformer(_, originalInputAttributes)
                  .doTransform(context))
              .asJava

            val windowFunctionNode = ExpressionBuilder.makeWindowFunction(
              AggregateFunctionsBuilder.create(context, aggExpression.aggregateFunction).toInt,
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
            // Spark only accepts foldable offset. Converts it to LongType literal.
            val offset = offsetWf.offset.eval(EmptyRow).asInstanceOf[Int]
            // Velox only allows negative offset. WindowFunctionsBuilder#create converts
            // lag/lead with negative offset to the function with positive offset. So just
            // makes offsetNode store positive value.
            val offsetNode = ExpressionBuilder.makeLiteral(Math.abs(offset.toLong), LongType, false)
            childrenNodeList.add(offsetNode)
            // NullType means Null is the default value. Don't pass it to native.
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

  def rewriteSpillPath(path: String): String = path

  def supportPushDownFilterToScan(sparkExecNode: LeafExecNode): Boolean = true

  /** Return whether the filter is supported in scan. */
  def isSupportedScanFilter(filter: Expression, sparkExecNode: LeafExecNode): Boolean = {
    ExpressionConverter.canReplaceWithExpressionTransformer(
      ExpressionConverter.replaceAttributeReference(filter),
      sparkExecNode.output) &&
    (!filter.references.exists(
      attr => BackendsApiManager.getSparkPlanExecApiInstance.isRowIndexMetadataColumn(attr.name)))
  }

  def genGenerateTransformer(
      generator: Generator,
      requiredChildOutput: Seq[Attribute],
      outer: Boolean,
      generatorOutput: Seq[Attribute],
      child: SparkPlan
  ): GenerateExecTransformerBase

  def genPreProjectForGenerate(generate: GenerateExec): SparkPlan

  def genPostProjectForGenerate(generate: GenerateExec): SparkPlan

  def genPreProjectForArrowEvalPythonExec(arrowEvalPythonExec: ArrowEvalPythonExec): SparkPlan =
    arrowEvalPythonExec

  def maybeCollapseTakeOrderedAndProject(plan: SparkPlan): SparkPlan = plan

  def genDecimalRoundExpressionOutput(decimalType: DecimalType, toScale: Int): DecimalType = {
    val p = decimalType.precision
    val s = decimalType.scale
    // After rounding we may need one more digit in the integral part,
    // e.g. `ceil(9.9, 0)` -> `10`, `ceil(99, -1)` -> `100`.
    val integralLeastNumDigits = p - s + 1
    if (toScale < 0) {
      // negative scale means we need to adjust `-scale` number of digits before the decimal
      // point, which means we need at lease `-scale + 1` digits (after rounding).
      val newPrecision = math.max(integralLeastNumDigits, -toScale + 1)
      // We have to accept the risk of overflow as we can't exceed the max precision.
      DecimalType(math.min(newPrecision, DecimalType.MAX_PRECISION), 0)
    } else {
      val newScale = math.min(s, toScale)
      // We have to accept the risk of overflow as we can't exceed the max precision.
      DecimalType(math.min(integralLeastNumDigits + newScale, 38), newScale)
    }
  }

  def genWindowGroupLimitTransformer(
      partitionSpec: Seq[Expression],
      orderSpec: Seq[SortOrder],
      rankLikeFunction: Expression,
      limit: Int,
      mode: GlutenWindowGroupLimitMode,
      child: SparkPlan): SparkPlan =
    WindowGroupLimitExecTransformer(partitionSpec, orderSpec, rankLikeFunction, limit, mode, child)

  def genHiveUDFTransformer(
      expr: Expression,
      attributeSeq: Seq[Attribute]): ExpressionTransformer = {
    HiveUDFTransformer.replaceWithExpressionTransformer(expr, attributeSeq)
  }

  def genStringSplitTransformer(
      substraitExprName: String,
      srcExpr: ExpressionTransformer,
      regexExpr: ExpressionTransformer,
      limitExpr: ExpressionTransformer,
      original: StringSplit): ExpressionTransformer =
    GenericExpressionTransformer(substraitExprName, Seq(srcExpr, regexExpr, limitExpr), original)

  def genColumnarCollectLimitExec(
      limit: Int,
      plan: SparkPlan,
      offset: Int): ColumnarCollectLimitBaseExec

  def genColumnarRangeExec(rangeExec: RangeExec): ColumnarRangeBaseExec

  def genColumnarTailExec(limit: Int, plan: SparkPlan): ColumnarCollectTailBaseExec

  def genColumnarToCarrierRow(plan: SparkPlan): SparkPlan

  def expressionFlattenSupported(expr: Expression): Boolean = false

  def genFlattenedExpressionTransformer(
      substraitName: String,
      children: Seq[ExpressionTransformer],
      expr: Expression): ExpressionTransformer =
    GenericExpressionTransformer(substraitName, children, expr)

  def isSupportRDDScanExec(plan: RDDScanExec): Boolean = false

  def getRDDScanTransform(plan: RDDScanExec): RDDScanTransformer =
    throw new GlutenNotSupportException("RDDScanExec is not supported")

  def copyColumnarBatch(batch: ColumnarBatch): ColumnarBatch =
    throw new GlutenNotSupportException("Copying ColumnarBatch is not supported")

  def serializeColumnarBatch(output: ObjectOutputStream, batch: ColumnarBatch): Unit =
    throw new GlutenNotSupportException("Serialize ColumnarBatch is not supported")

  def deserializeColumnarBatch(input: ObjectInputStream): ColumnarBatch =
    throw new GlutenNotSupportException("Deserialize ColumnarBatch is not supported")

  def genTimestampAddTransformer(
      substraitExprName: String,
      left: ExpressionTransformer,
      right: ExpressionTransformer,
      original: Expression): ExpressionTransformer

  def genTimestampDiffTransformer(
      substraitExprName: String,
      left: ExpressionTransformer,
      right: ExpressionTransformer,
      original: Expression): ExpressionTransformer = {
    throw new GlutenNotSupportException("timestampdiff is not supported")
  }

  def genMonthsBetweenTransformer(
      substraitExprName: String,
      date1: ExpressionTransformer,
      date2: ExpressionTransformer,
      roundOff: ExpressionTransformer,
      original: MonthsBetween): ExpressionTransformer

  def isRowIndexMetadataColumn(columnName: String): Boolean = {
    SparkShimLoader.getSparkShims.isRowIndexMetadataColumn(columnName)
  }

  def getErrorMessage(raiseError: RaiseError): Expression = {
    throw new GlutenNotSupportException(s"${ExpressionNames.RAISE_ERROR} is not supported")
  }
}
