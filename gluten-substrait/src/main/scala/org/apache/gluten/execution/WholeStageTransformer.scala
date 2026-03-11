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
package org.apache.gluten.execution

import org.apache.gluten.backendsapi.BackendsApiManager
import org.apache.gluten.config.GlutenConfig
import org.apache.gluten.expression._
import org.apache.gluten.extension.columnar.transition.Convention
import org.apache.gluten.metrics.{GlutenTimeMetric, MetricsUpdater}
import org.apache.gluten.substrait.`type`.{TypeBuilder, TypeNode}
import org.apache.gluten.substrait.SubstraitContext
import org.apache.gluten.substrait.plan.{PlanBuilder, PlanNode}
import org.apache.gluten.substrait.rel.{LocalFilesNode, RelNode, SplitInfo}
import org.apache.gluten.substrait.rel.LocalFilesNode.ReadFileFormat
import org.apache.gluten.utils.SubstraitPlanPrinterUtil

import org.apache.spark._
import org.apache.spark.rdd.RDD
import org.apache.spark.softaffinity.SoftAffinity
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Attribute, SortOrder}
import org.apache.spark.sql.catalyst.plans.physical.Partitioning
import org.apache.spark.sql.catalyst.trees.TreeNodeTag
import org.apache.spark.sql.execution._
import org.apache.spark.sql.execution.metric.SQLMetric
import org.apache.spark.sql.utils.SparkInputMetricsUtil.InputMetricsWrapper
import org.apache.spark.sql.vectorized.ColumnarBatch

import com.google.common.collect.Lists
import org.apache.hadoop.fs.viewfs.ViewFileSystemUtils

import scala.collection.JavaConverters._
import scala.collection.mutable

case class TransformContext(outputAttributes: Seq[Attribute], root: RelNode)

case class WholeStageTransformContext(
    root: PlanNode,
    substraitContext: SubstraitContext = null,
    enableCudf: Boolean = false)

/** Base interface for a query plan that can be interpreted to Substrait representation. */
trait TransformSupport extends ValidatablePlan {

  override def batchType(): Convention.BatchType = {
    BackendsApiManager.getSettings.primaryBatchType
  }

  override def rowType0(): Convention.RowType = {
    Convention.RowType.None
  }

  final override def doExecute(): RDD[InternalRow] = {
    throw new UnsupportedOperationException(
      s"${this.getClass.getSimpleName} doesn't support doExecute")
  }

  def isCudf: Boolean = getTagValue[Boolean](CudfTag.CudfTag).getOrElse(false)

  // Use super.nodeName will cause exception scala 213 Super calls can only target methods
  // for FileSourceScan.
  override def nodeName: String =
    if (isCudf) {
      "Cudf" + getClass.getSimpleName.replaceAll("Exec$", "")
    } else getClass.getSimpleName

  /**
   * Returns all the RDDs of ColumnarBatch which generates the input rows.
   *
   * @note
   *   Right now we support up to two RDDs
   */
  def columnarInputRDDs: Seq[RDD[ColumnarBatch]]

  // Since https://github.com/apache/incubator-gluten/pull/2185.
  protected def doNativeValidation(context: SubstraitContext, node: RelNode): ValidationResult = {
    if (node != null && enableNativeValidation) {
      val planNode = PlanBuilder.makePlan(context, Lists.newArrayList(node))
      BackendsApiManager.getValidatorApiInstance
        .doNativeValidateWithFailureReason(planNode)
    } else {
      ValidationResult.succeeded
    }
  }

  final def transform(context: SubstraitContext): TransformContext = {
    if (isCanonicalizedPlan) {
      throw new IllegalStateException(
        "A canonicalized plan is not supposed to be executed transform.")
    }
    if (TransformerState.underValidationState) {
      doTransform(context)
    } else {
      // Materialize subquery first before going to do transform.
      executeQuery {
        doTransform(context)
      }
    }
  }

  protected def doTransform(context: SubstraitContext): TransformContext = {
    throw new UnsupportedOperationException(
      s"This operator doesn't support doTransform with SubstraitContext.")
  }

  def metricsUpdater(): MetricsUpdater

  protected def getColumnarInputRDDs(plan: SparkPlan): Seq[RDD[ColumnarBatch]] = {
    plan match {
      case c: TransformSupport =>
        c.columnarInputRDDs
      case _ =>
        Seq(plan.executeColumnar())
    }
  }

  // When true, it will not generate relNode, nor will it generate native metrics.
  def isNoop: Boolean = false
}

trait LeafTransformSupport extends TransformSupport with LeafExecNode {
  final override def columnarInputRDDs: Seq[RDD[ColumnarBatch]] = Seq.empty

  /** Returns the split infos that will be processed by the underlying native engine. */
  def getSplitInfos: Seq[SplitInfo]

  /** Returns the partitions generated by this data source scan. */
  def getPartitions: Seq[Partition]

  /** Returns the partitions generated by this data source scan and tied with ReadFileFormat. */
  def getPartitionWithReadFileFormats: Seq[(Partition, ReadFileFormat)]

  /** Returns distinct partition's ReadFileFormats. */
  def getDistinctPartitionReadFileFormats: Set[ReadFileFormat] = Set.empty
}

trait UnaryTransformSupport extends TransformSupport with UnaryExecNode {
  final override def columnarInputRDDs: Seq[RDD[ColumnarBatch]] = {
    getColumnarInputRDDs(child)
  }
}

case class WholeStageTransformer(child: SparkPlan, materializeInput: Boolean = false)(
    var transformStageId: Int
) extends WholeStageTransformerGenerateTreeStringShim
  with UnaryTransformSupport {

  def stageId: Int = transformStageId

  def wholeStageTransformerContextDefined: Boolean = wholeStageTransformerContext.isDefined

  // For WholeStageCodegen-like operator, only pipeline time will be handled in graph plotting.
  // See SparkPlanGraph.scala:205 for reference.
  // Note: "metrics" is made transient to avoid sending driver-side metrics to tasks.
  @transient override lazy val metrics: Map[String, SQLMetric] =
    BackendsApiManager.getMetricsApiInstance.genWholeStageTransformerMetrics(sparkContext)

  @transient
  private var wholeStageTransformerContext: Option[WholeStageTransformContext] = None

  private var outputSchemaForPlan: Option[TypeNode] = None

  private def inferSchemaFromAttributes(attrs: Seq[Attribute]): TypeNode = {
    val outputTypeNodeList = new java.util.ArrayList[TypeNode]()
    for (attr <- attrs) {
      outputTypeNodeList.add(ConverterUtils.getTypeNode(attr.dataType, attr.nullable))
    }

    TypeBuilder.makeStruct(false, outputTypeNodeList)
  }

  def setOutputSchemaForPlan(expectOutput: Seq[Attribute]): Unit = {
    if (outputSchemaForPlan.isDefined) {
      return
    }

    // Fixes issue-1874
    outputSchemaForPlan = Some(inferSchemaFromAttributes(expectOutput))
  }

  def substraitPlan: PlanNode = {
    if (wholeStageTransformerContext.isDefined) {
      wholeStageTransformerContext.get.root
    } else {
      generateWholeStageTransformContext().root
    }
  }

  def substraitPlanJson: String = {
    SubstraitPlanPrinterUtil.substraitPlanToJson(substraitPlan.toProtobuf)
  }

  def nativePlanString(details: Boolean = true): String = {
    BackendsApiManager.getTransformerApiInstance.getNativePlanString(
      substraitPlan.toProtobuf.toByteArray,
      details)
  }

  override def output: Seq[Attribute] = child.output

  override def outputPartitioning: Partitioning = child.outputPartitioning

  override def outputOrdering: Seq[SortOrder] = child.outputOrdering

  override def otherCopyArgs: Seq[AnyRef] = Seq(transformStageId.asInstanceOf[Integer])

  // It's misleading with "Codegen" used. But we have to keep "WholeStageCodegen" prefixed to
  // make whole stage transformer clearly plotted in UI, like spark's whole stage codegen.
  // See buildSparkPlanGraphNode in SparkPlanGraph.scala of Spark.
  override def nodeName: String = s"WholeStageCodegenTransformer ($transformStageId)"

  override def verboseStringWithOperatorId(): String = {
    val nativePlan = if (glutenConf.getConf(GlutenConfig.INJECT_NATIVE_PLAN_STRING_TO_EXPLAIN)) {
      s"Native Plan:\n${nativePlanString()}"
    } else {
      ""
    }
    super.verboseStringWithOperatorId() ++ nativePlan
  }

  private def generateWholeStageTransformContext(): WholeStageTransformContext = {
    val substraitContext = new SubstraitContext
    val childCtx = child
      .asInstanceOf[TransformSupport]
      .transform(substraitContext)
    if (childCtx == null) {
      throw new IllegalStateException(s"WholeStageTransformer can't do Transform on $child")
    }

    val outNames = childCtx.outputAttributes.map(ConverterUtils.genColumnNameWithExprId).asJava

    val planNode = if (BackendsApiManager.getSettings.needOutputSchemaForPlan()) {
      val outputSchema =
        outputSchemaForPlan.getOrElse(inferSchemaFromAttributes(childCtx.outputAttributes))

      PlanBuilder.makePlan(
        substraitContext,
        Lists.newArrayList(childCtx.root),
        outNames,
        outputSchema,
        null)
    } else {
      PlanBuilder.makePlan(substraitContext, Lists.newArrayList(childCtx.root), outNames)
    }

    WholeStageTransformContext(planNode, substraitContext, isCudf)
  }

  def doWholeStageTransform(): WholeStageTransformContext = {
    val context = generateWholeStageTransformContext()
    if (glutenConf.getConf(GlutenConfig.CACHE_WHOLE_STAGE_TRANSFORMER_CONTEXT)) {
      wholeStageTransformerContext = Some(context)
    }
    context
  }

  /** Find all [[LeafTransformSupport]] in one WholeStageTransformer */
  private def findAllLeafTransformers(): Seq[LeafTransformSupport] = {

    def collectLeafTransformers(plan: SparkPlan): Seq[LeafTransformSupport] = plan match {
      case transformer: LeafTransformSupport =>
        Seq(transformer)
      case shj: HashJoinLikeExecTransformer =>
        collectLeafTransformers(shj.streamedPlan) ++ collectLeafTransformers(shj.buildPlan)
      case t: TransformSupport =>
        t.children.flatMap(collectLeafTransformers)
      case _ =>
        Seq.empty
    }

    collectLeafTransformers(child)
  }

  /**
   * If containing leaf exec transformer this "whole stage" generates a RDD which itself takes care
   * of [[LeafTransformSupport]] there won't be any other RDD for leaf operator. As a result,
   * genFirstStageIterator rather than genFinalStageIterator will be invoked
   */
  private def generateWholeStageRDD(
      leafTransformers: Seq[LeafTransformSupport],
      wsCtx: WholeStageTransformContext,
      inputRDDs: ColumnarInputRDDsWrapper,
      pipelineTime: SQLMetric): RDD[ColumnarBatch] = {

    // If these are two leaf transformers, they must have same partitions,
    // otherwise, exchange will be inserted. We should combine the two leaf
    // transformers' partitions with same index, and set them together in
    // the substraitContext.
    // We use transpose to do that, You can refer to
    // the diagram below.
    // leaf1  p11 p12 p13 p14 ... p1n
    // leaf2  p21 p22 p23 p24 ... p2n
    // transpose =>
    // leaf1 | leaf2
    //  p11  |  p21    => substraitContext.setSplitInfo([p11, p21])
    //  p12  |  p22    => substraitContext.setSplitInfo([p12, p22])
    //  p13  |  p23    ...
    //  p14  |  p24
    //      ...
    //  p1n  |  p2n    => substraitContext.setSplitInfo([p1n, p2n])
    // The data in partition may be empty, for example,
    // if these are two batch scan transformer with keyGroupPartitioning,
    // they have same partitionValues,
    // but some partitions maybe empty for hose partition values that are not present.
    val allSplitInfos = leafTransformers.map(_.getSplitInfos).transpose

    if (GlutenConfig.get.enableHdfsViewfs) {
      val viewfsToHdfsCache: mutable.Map[String, String] = mutable.Map.empty
      allSplitInfos.foreach {
        splitInfos =>
          splitInfos.foreach {
            case splitInfo: LocalFilesNode =>
              val newPaths = ViewFileSystemUtils.convertViewfsToHdfs(
                splitInfo.getPaths.asScala.toSeq,
                viewfsToHdfsCache,
                sparkContext.hadoopConfiguration)
              splitInfo.setPaths(newPaths.asJava)
          }
      }
    }

    val inputPartitions =
      BackendsApiManager.getIteratorApiInstance.genPartitions(
        wsCtx,
        allSplitInfos,
        leafTransformers)

    val rdd = new GlutenWholeStageColumnarRDD(
      sparkContext,
      inputPartitions,
      inputRDDs,
      pipelineTime,
      leafInputMetricsUpdater(),
      BackendsApiManager.getMetricsApiInstance.metricsUpdatingFunction(
        child,
        wsCtx.substraitContext.registeredRelMap,
        wsCtx.substraitContext.registeredJoinParams,
        wsCtx.substraitContext.registeredAggregationParams
      ),
      wsCtx.enableCudf
    )

    val allInputPartitions = leafTransformers.map(_.getPartitions)
    SoftAffinity.updateFilePartitionLocations(allInputPartitions, rdd.id)

    leafTransformers.foreach {
      case batchScan: BatchScanExecTransformerBase =>
        batchScan.doPostDriverMetrics()
      case _ =>
    }

    rdd
  }

  override def doExecuteColumnar(): RDD[ColumnarBatch] = {
    assert(child.isInstanceOf[TransformSupport])
    val pipelineTime: SQLMetric = longMetric("pipelineTime")
    // We should do transform first to make sure all subqueries are materialized
    val wsCtx = GlutenTimeMetric.withMillisTime {
      doWholeStageTransform()
    }(
      t =>
        logOnLevel(
          GlutenConfig.get.substraitPlanLogLevel,
          s"$nodeName generating the substrait plan took: $t ms."))
    val inputRDDs = new ColumnarInputRDDsWrapper(columnarInputRDDs)

    val leafTransformers = findAllLeafTransformers()
    if (leafTransformers.nonEmpty) {
      generateWholeStageRDD(leafTransformers, wsCtx, inputRDDs, pipelineTime)
    } else {

      /**
       * the whole stage contains NO [[LeafTransformSupport]]. This is the default case for:
       *   - SCAN of clickhouse backend. See
       *     BackendsApiManager.getSettings.excludeScanExecFromCollapsedStage.
       *   - Test case where query plan is constructed from simple DataFrames, e.g.
       *     GlutenDataFrameAggregateSuite.
       *
       * In these cases, separate RDDs take care of SCAN. As a result, genFinalStageIterator rather
       * than genFirstStageIterator will be invoked.
       */
      new WholeStageZippedPartitionsRDD(
        sparkContext,
        inputRDDs,
        sparkContext.getConf,
        wsCtx,
        pipelineTime,
        BackendsApiManager.getMetricsApiInstance.metricsUpdatingFunction(
          child,
          wsCtx.substraitContext.registeredRelMap,
          wsCtx.substraitContext.registeredJoinParams,
          wsCtx.substraitContext.registeredAggregationParams
        ),
        materializeInput
      )
    }
  }

  override def metricsUpdater(): MetricsUpdater = {
    child match {
      case transformer: TransformSupport => transformer.metricsUpdater()
      case _ => MetricsUpdater.None
    }
  }

  private def leafInputMetricsUpdater(): InputMetricsWrapper => Unit = {
    val leaves = child.collect {
      case plan: TransformSupport if plan.children.forall(!_.isInstanceOf[TransformSupport]) =>
        plan
    }
    val leafMetricsUpdater = leaves.map(_.metricsUpdater())

    (inputMetrics: InputMetricsWrapper) => {
      leafMetricsUpdater.foreach(_.updateInputMetrics(inputMetrics))
    }
  }

  override protected def withNewChildInternal(newChild: SparkPlan): WholeStageTransformer =
    copy(child = newChild, materializeInput = materializeInput)(transformStageId)
}

/**
 * This `columnarInputRDDs` would contain [[BroadcastBuildSideRDD]], but the dependency and
 * partition of [[BroadcastBuildSideRDD]] is meaningless. [[BroadcastBuildSideRDD]] should only be
 * used to hold the broadcast value and generate iterator for join.
 */
class ColumnarInputRDDsWrapper(columnarInputRDDs: Seq[RDD[ColumnarBatch]]) extends Serializable {
  def getDependencies: Seq[Dependency[ColumnarBatch]] = {
    assert(
      columnarInputRDDs
        .filterNot(_.isInstanceOf[BroadcastBuildSideRDD])
        .map(_.partitions.length)
        .toSet
        .size <= 1)

    columnarInputRDDs.flatMap {
      case _: BroadcastBuildSideRDD => Nil
      case rdd => new OneToOneDependency[ColumnarBatch](rdd) :: Nil
    }
  }

  def getPartitions(index: Int): Seq[Partition] = {
    columnarInputRDDs.filterNot(_.isInstanceOf[BroadcastBuildSideRDD]).map(_.partitions(index))
  }

  def getPartitionLength: Int = {
    assert(columnarInputRDDs.nonEmpty)
    val nonBroadcastRDD = columnarInputRDDs.find(!_.isInstanceOf[BroadcastBuildSideRDD])
    assert(nonBroadcastRDD.isDefined)
    nonBroadcastRDD.get.partitions.length
  }

  def getIterators(
      inputColumnarRDDPartitions: Seq[Partition],
      context: TaskContext): Seq[Iterator[ColumnarBatch]] = {
    var index = 0
    columnarInputRDDs.flatMap {
      case broadcast: BroadcastBuildSideRDD =>
        broadcast.genBroadcastBuildSideIterator() :: Nil
      case cartesian: CartesianColumnarBatchRDD =>
        val partition =
          inputColumnarRDDPartitions(index).asInstanceOf[CartesianColumnarBatchRDDPartition]
        index += 1
        cartesian.getIterators(partition, context)
      case rdd =>
        val it = rdd.iterator(inputColumnarRDDPartitions(index), context)
        index += 1
        it :: Nil
    }
  }
}

object CudfTag {
  val CudfTag = TreeNodeTag[Boolean]("org.apache.gluten.CudfTag")
}
